# Backstage (Community)

**Version:** 0.1.0
**Backend:** HTTP (Backstage Software Catalog REST API)
**Tables:** 1
**Base URL:** `{{input.BACKSTAGE_URL}}/api/catalog`

Query software catalog entities, ownership relationships, and service metadata from your internal developer portal using Coral SQL.

This integration provides read-only access to Backstage's Software Catalog API for service registry auditing, letting engineers and managers run relational analysis over software architecture, ownership gaps, and lifecycle distribution.

Coral exposes read-only `GET` tables. Modifying catalog structures, registering locations, and triggering ingestion mutations are out of scope.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export BACKSTAGE_URL=http://localhost:7007
export BACKSTAGE_TOKEN=your-backstage-token-here
coral source add --file sources/community/backstage/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Backstage Catalog API access requires a valid bearer token. Coral sends the token as `Authorization: Bearer <token>`.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `BACKSTAGE_URL` | variable | yes | Backstage base URL without trailing slash and without `/api/catalog`, for example `http://localhost:7007` |
| `BACKSTAGE_TOKEN` | secret | yes | Backstage Auth Service bearer token or identity token authorized to read the Catalog API |

A token can be either a static Backstage service-to-service token or a user identity token issued by the Auth Service. Prefer a service token for CI/CD pipelines and shared tooling that shouldn't depend on a single person's session, and an identity token for local development and personal use.

Returned entities are restricted by the permissions associated with the supplied token. Entities not visible to the token cannot be queried through Coral.

Official docs:

- [Backstage Software Catalog API — `GET /entities/by-query`](https://backstage.io/docs/features/software-catalog/software-catalog-api/#get-entitiesby-query)
- [Backstage Service-to-Service Auth](https://backstage.io/docs/auth/service-to-service-auth/)

## Tables

| Table | API Endpoint | Optional pushdown filters | Pagination |
| --- | --- | --- | --- |
| `backstage.entities` | `GET /entities/by-query` | `catalog_filter` | Cursor (`limit` / `pageInfo.nextCursor`) |

The source uses Backstage's recommended paginated query endpoint, `GET /entities/by-query`. Coral reads rows from `items` and follows `pageInfo.nextCursor` automatically, so large catalogs are fetched page by page rather than in one unbounded request.

### `backstage.entities`

Entities tracked inside the Backstage software catalog.

| Column | Type | Description |
| --- | --- | --- |
| `entity_ref` | Utf8 | Durable entity reference `kind:namespace/name` — prefer this for joins/external refs |
| `catalog_filter` | Utf8 | Backstage filter-string pushdown (virtual) |
| `uid` | Utf8 | Backstage-assigned UID (output-only, not stable for external references) |
| `name` | Utf8 | Technical name of the entity |
| `namespace` | Utf8 | Namespace grouping the entity (defaults to `default`) |
| `kind` | Utf8 | High-level category (`Component`, `API`, `User`, ...) |
| `type` | Utf8 | Sub-categorization within the kind |
| `lifecycle` | Utf8 | Operational maturity phase |
| `owner` | Utf8 | Raw declared owner from `spec.owner` (pre-processing) |
| `owned_by` | Utf8 | Processed ownership from the `relations` graph (`ownedBy` target refs), joined with `, ` |
| `relation_types` | Utf8 | Relation types present on the entity (e.g., `ownedBy`, `dependsOn`), joined with `, ` |
| `relation_targets` | Utf8 | All related entity refs from the `relations` graph, joined with `, ` |
| `title` | Utf8 | Display title |
| `description` | Utf8 | Entity description |

#### Entity references

Backstage documents `metadata.uid` as output-only and not stable for external references. The `entity_ref` column provides the durable `kind:namespace/name` identifier built from the entity's immutable fields; use it for joins and audit workflows. Note that Backstage's internal relation target refs are lowercased, so compare on a normalized form if you join `entity_ref` against `relation_targets`.

#### Ownership and relations

`spec.owner` is the raw declared owner. After Backstage processes the catalog, ownership is represented in the entity's `relations` graph as `ownedBy` edges. The `owned_by` column surfaces those processed owner refs, and `relation_types` / `relation_targets` expose the broader relationship graph (e.g., `dependsOn`, `partOf`).

#### Pushdown filtering

`catalog_filter` is passed straight to Backstage's `filter` query parameter for server-side filtering:

```sql
SELECT name, kind, type
FROM backstage.entities
WHERE catalog_filter = 'kind=component,spec.type=service'
LIMIT 25;
```

Predicates on other columns are applied locally by Coral after the page is fetched.

## Example queries

### Find production components and their processed owners

```sql
SELECT
  name,
  lifecycle,
  owned_by
FROM backstage.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
ORDER BY name ASC;
```

### Catalog breakdown by kind and type

```sql
SELECT
  kind,
  type,
  COUNT(*) AS entity_count
FROM backstage.entities
GROUP BY kind, type
ORDER BY entity_count DESC;
```

### Components owned by a specific group (server-side filter)

```sql
SELECT
  entity_ref,
  name,
  owned_by
FROM backstage.entities
WHERE catalog_filter = 'kind=component,relations.ownedBy=group:default/platform'
LIMIT 50;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/backstage/manifest.yaml
Coral manifest schema validation: passed for sources/community/backstage/manifest.yaml
make lint-sources: passed
Live API tests: passed with a Backstage token
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/backstage/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export BACKSTAGE_URL=http://localhost:7007
export BACKSTAGE_TOKEN=your-backstage-token-here
coral source add --file sources/community/backstage/manifest.yaml
coral source test backstage
```

Validate table access with representative SQL:

```bash
coral sql "SELECT entity_ref, kind FROM backstage.entities LIMIT 5"
coral sql "SELECT name, lifecycle, owned_by FROM backstage.entities WHERE kind = 'Component' AND lifecycle = 'production' LIMIT 5"
coral sql "SELECT name, kind, type FROM backstage.entities WHERE catalog_filter = 'kind=component,spec.type=service' LIMIT 5"
coral sql "SELECT entity_ref, name, owned_by FROM backstage.entities WHERE catalog_filter = 'kind=component,relations.ownedBy=group:default/platform' LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'backstage'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'backstage' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ backstage connected successfully

backstage (1 table)
└─ entities

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT name, kind FROM backstage.entities LIMIT 1
  1 row
```

Representative query:

```sql
SELECT entity_ref, kind, type, lifecycle, owned_by
FROM backstage.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
LIMIT 3;
```

Example output:

```text
entity_ref                           | kind      | type    | lifecycle  | owned_by
component:default/payment-service     | Component | service | production | group:default/platform
component:default/user-profile-api    | Component | service | production | group:default/identity
component:default/docs-site           | Component | website | production | group:default/developer-experience
```

## Limitations

- Read-only retrieval scope; catalog mutations and location registration are unsupported.
- `catalog_filter` is pushed to Backstage's `filter` parameter; other predicates are applied locally after each page is fetched.
- Relation columns (`owned_by`, `relation_types`, `relation_targets`) are flattened/joined representations of the `relations` graph, not a normalized relations table.
- Query results are limited by the permissions of the supplied token.
- `metadata.uid` is output-only and not stable for external references; prefer `entity_ref` for joins and audit workflows.
