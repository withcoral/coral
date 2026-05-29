# Roadie (Community)

**Version:** 0.1.0
**Backend:** HTTP (Roadie Backstage Catalog API)
**Tables:** 1
**Base URL:** `https://api.roadie.so/api/catalog`

Query Software Catalog entities including components, APIs, systems, users, and groups from Roadie using Coral SQL.

This integration provides read-only access to Roadie's Backstage Catalog API for catalog auditing, ownership analysis, service inventory reporting, and platform visibility workflows.

Coral does not support catalog mutations, entity registration, ownership updates, or deletion operations.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export ROADIE_API_TOKEN=your_api_token_here
coral source add --file sources/community/roadie/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Roadie Catalog API access requires a valid bearer token. Roadie supports **two token types**, and both work with this source. They use bearer authentication and grant the same Catalog API access — choose based on whether the token should be tied to your account or to an automated system.

| Input | Description |
| --- | --- |
| `ROADIE_API_TOKEN` | A Roadie User Token or Service Token authorized to read the Catalog API |

### User Token

A user token is tied to your personal Roadie account. It is the right choice for local development, personal scripts, and IDE/MCP use.

1. Make sure the **`Roadie API Key Access`** policy is assigned to your user. Without it you cannot generate a user token — ask a Roadie administrator if you don't have it.
2. Go to **Administration → Account** and open the **Roadie API Access** section.
3. Add a token description and click **Generate Token**.
4. Copy the token immediately and store it securely. Roadie does not display it again.

### Service Token

A service token is **not** tied to an individual user. Prefer it for CI/CD pipelines, shared integrations, and team tooling that shouldn't depend on a single person's account.

1. Go to **Administration → Service Tokens**.
2. Click **Create Service Token**.
3. Add a description and click **Generate**.
4. Copy the token immediately and store it securely. Roadie does not display it again.

Returned entities are restricted by the permissions associated with the supplied token. Entities not visible to the token cannot be queried through Coral.

Official docs:

- [Roadie API Authorization — User Tokens & Service Tokens](https://roadie.io/docs/api/authorization/)
- [Backstage Software Catalog API — `GET /entities/by-query`](https://backstage.io/docs/features/software-catalog/software-catalog-api/#get-entitiesby-query)
- [Roadie Catalog API](https://roadie.io/docs/api/catalog/)

## Tables

| Table | Description | Optional pushdown filters |
| --- | --- | --- |
| `roadie.entities` | Software catalog entities (Components, APIs, Systems, Users, Groups, etc.) | `name`, `kind`, `type`, `lifecycle`, `owner`, `namespace`, `catalog_filter` |

### `roadie.entities`

Software catalog entities managed within Roadie.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Name of the catalog entity |
| `kind` | Utf8 | Entity kind (Component, API, System, Group, User, etc.) |
| `type` | Utf8 | Entity subtype (service, website, library, etc.) |
| `lifecycle` | Utf8 | Lifecycle classification (production, experimental, deprecated, etc.) |
| `owner` | Utf8 | Team or group responsible for the entity |
| `description` | Utf8 | Entity description |
| `namespace` | Utf8 | Namespace associated with the entity |

#### Pushdown filters

Use these SQL filters to narrow results on the Roadie API instead of scanning the full catalog locally:

| SQL filter | Roadie `filter` mapping |
| --- | --- |
| `name` | `metadata.name=<value>` |
| `kind` | `kind=<value>` |
| `type` | `spec.type=<value>` |
| `lifecycle` | `spec.lifecycle=<value>` |
| `owner` | `spec.owner=<value>` |
| `namespace` | `metadata.namespace=<value>` |
| `catalog_filter` | Raw Backstage filter string passed through as-is |

Coral combines common multi-filter queries into one `filter` parameter. For example, `WHERE kind = 'Component' AND lifecycle = 'production'` is sent as `filter=kind=Component,spec.lifecycle=production`.

Exact entity lookups push down fully. For example:

```sql
SELECT name, kind, owner
FROM roadie.entities
WHERE kind = 'Component'
  AND namespace = 'default'
  AND name = 'payment-service';
```

is sent as `filter=kind=Component,metadata.namespace=default,metadata.name=payment-service`, so Roadie returns just the matching entity instead of a broad scan filtered locally.

For advanced Backstage filter syntax, use `catalog_filter` directly:

```sql
SELECT name, kind, type
FROM roadie.entities
WHERE catalog_filter = 'kind=component,spec.type=service'
LIMIT 25;
```

The table paginates with Roadie `cursor` and `limit`. Coral applies `fetch_limit_default: 500` so broad inventory queries stay bounded unless SQL sets an explicit `LIMIT`.

## Example queries

### Look up a single entity by name

```sql
SELECT
  name,
  kind,
  owner,
  lifecycle
FROM roadie.entities
WHERE kind = 'Component'
  AND name = 'payment-service';
```

### Catalog breakdown by kind

```sql
SELECT
  kind,
  COUNT(*) AS entity_count
FROM (
  SELECT kind FROM roadie.entities LIMIT 10000
)
GROUP BY kind
ORDER BY entity_count DESC;
```

This counts entities within an explicitly bounded scan of up to 10,000 rows. The bound is deliberate: without a table-scan `LIMIT`, Coral applies the manifest `fetch_limit_default: 500`, so an unbounded `COUNT(*)` over a catalog with more than 500 returned entities would total only the first fetched slice and understate the real counts. Raise the inner `LIMIT` for very large catalogs, or narrow with a pushdown filter such as `WHERE kind = 'Component'` when you only need one entity class.

### Production components

```sql
SELECT
  name,
  owner,
  lifecycle
FROM roadie.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
ORDER BY owner
LIMIT 25;
```

### APIs and their owners

```sql
SELECT
  name,
  owner
FROM roadie.entities
WHERE kind = 'API'
ORDER BY name
LIMIT 25;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/roadie/manifest.yaml
Coral manifest schema validation: passed for sources/community/roadie/manifest.yaml
make lint-sources: passed
Live API tests: passed with a Roadie API token
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/roadie/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export ROADIE_API_TOKEN=your_api_token_here
coral source add --file sources/community/roadie/manifest.yaml
coral source test roadie
```

Validate table access with representative SQL:

```bash
coral sql "SELECT name, kind FROM roadie.entities LIMIT 5"
coral sql "SELECT name, owner, lifecycle FROM roadie.entities WHERE kind = 'Component' AND lifecycle = 'production' LIMIT 5"
coral sql "SELECT name, kind, owner FROM roadie.entities WHERE kind = 'Component' AND namespace = 'default' AND name = 'payment-service'"
coral sql "SELECT name, owner FROM roadie.entities WHERE kind = 'API' ORDER BY name LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'roadie'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'roadie' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ roadie connected successfully

roadie (1 table)
└─ entities

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT name, kind FROM roadie.entities LIMIT 1
  1 row
```

Representative query:

```sql
SELECT name, kind, type, lifecycle, owner
FROM roadie.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
LIMIT 3;
```

Example output:

```text
name              | kind      | type    | lifecycle  | owner
payment-service   | Component | service | production | group:default/platform
user-profile-api  | Component | service | production | group:default/identity
docs-site         | Component | website | production | group:default/developer-experience
```

## Limitations

- Read-only access only.
- Catalog entity creation, modification, and deletion are not supported.
- Query results are limited by Roadie API permissions associated with the provided token.
- Large catalogs are fetched incrementally using Roadie's cursor-based pagination (`?cursor=<cursor>`).
- Returned data reflects the current visibility scope of the authenticated account.
- Multi-filter pushdown covers the declared single-filter and common multi-filter combinations (including the `kind` + `namespace` + `name` exact-entity lookup). Other multi-filter combinations may apply only the most specific matching pushdown route and evaluate remaining predicates locally.
