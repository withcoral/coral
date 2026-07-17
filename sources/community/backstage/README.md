# Backstage (Community)

**Version:** 0.1.0
**Backend:** HTTP (Backstage Software Catalog REST API)
**Tables:** 1
**Base URL:** `{{input.BACKSTAGE_URL}}/api/catalog`

Query software catalog entities, ownership relationships, and service metadata from your internal developer portal through Coral SQL. Read-only access for service registry auditing, ownership analysis, and lifecycle reporting.

Coral exposes read-only `GET` tables. Catalog mutations, location registration, and ingestion triggers are out of scope.

## Install

```bash
export BACKSTAGE_URL=http://localhost:7007
export BACKSTAGE_TOKEN=your-backstage-token-here
coral source add --file sources/community/backstage/manifest.yaml
```

## Authentication

Coral sends the token as `Authorization: Bearer <token>`.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `BACKSTAGE_URL` | variable | yes | Backstage base URL, no trailing slash and without `/api/catalog` (e.g. `http://localhost:7007`) |
| `BACKSTAGE_TOKEN` | secret | yes | Backstage Auth Service bearer token or identity token with Catalog API read access |

Either a service-to-service token or a user identity token works. Prefer a service token for CI/CD and shared tooling; an identity token for local development.

Returned entities are restricted by the permissions of the supplied token.

Docs: [Software Catalog API](https://backstage.io/docs/features/software-catalog/software-catalog-api/) · [get-entities-by-query](https://backstage.io/docs/features/software-catalog/api/get-entities-by-query/) · [Service-to-service auth](https://backstage.io/docs/auth/service-to-service-auth/)

## Tables

| Table | Endpoint | Filters | Pagination |
| --- | --- | --- | --- |
| `backstage.entities` | `GET /entities/by-query` | optional: `catalog_filter` | Cursor (`limit` / `pageInfo.nextCursor`) |

This uses Backstage's recommended paginated query endpoint. Rows are read from `items` and Coral follows `pageInfo.nextCursor` automatically, so large catalogs are fetched page by page rather than in one unbounded request. A `fields` selector is sent so only the modeled fields are returned.

### `backstage.entities`

| Column | Type | Description |
| --- | --- | --- |
| `entity_ref` | Utf8 | Entity reference `kind:namespace/name`, raw-cased from the API (e.g. `Component:default/payment-service`) — prefer over `uid` for joins |
| `catalog_filter` | Utf8 | Backstage filter-string pushdown (virtual) |
| `uid` | Utf8 | Backstage-assigned UID (output-only, not stable for external references) |
| `name` | Utf8 | Technical name of the entity |
| `namespace` | Utf8 | Namespace grouping the entity; `default` when the entity omits `metadata.namespace` |
| `kind` | Utf8 | High-level category (`Component`, `API`, `User`, ...) |
| `type` | Utf8 | Sub-categorization within the kind |
| `lifecycle` | Utf8 | Operational maturity phase (e.g. `production`) |
| `owner` | Utf8 | Raw declared owner from `spec.owner`, before processing |
| `owned_by` | Utf8 | Processed ownership: `ownedBy` target refs from the `relations` graph, joined with `, ` |
| `relation_types` | Utf8 | Relation types on the entity (e.g. `ownedBy`, `dependsOn`), joined with `, ` |
| `relation_targets` | Utf8 | All related entity refs from the `relations` graph, joined with `, ` |
| `title` | Utf8 | Display title |
| `description` | Utf8 | Entity description |

#### Entity references

Backstage documents `metadata.uid` as output-only and not stable for external references, so `entity_ref` gives a `kind:namespace/name` identifier built from the entity's kind, namespace, and name. Use it over `uid` for joins and audit workflows. Missing namespaces are filled with `default`, matching the `namespace` column.

**Casing caveat.** `entity_ref` emits the kind exactly as the by-query API returns it — `Component:default/payment-service`, not `component:default/payment-service`. Backstage's own reference docs say externally passed refs should be complete and lowercased, and relation `targetRef` values are lowercase. This source does not normalize, so joins against `relation_targets` must be case-insensitive:

```sql
SELECT a.name, b.name AS related
FROM backstage.entities a
JOIN backstage.entities b
  ON LOWER(a.entity_ref) = LOWER(b.relation_targets);
```

Lowercase `entity_ref` yourself before passing refs to other Backstage APIs.

#### Ownership and relations

`spec.owner` is the raw declared owner. After Backstage processes the catalog, ownership lives in the `relations` graph as `ownedBy` edges — `owned_by` surfaces those resolved refs, which can differ from the raw `spec.owner`. `relation_types` and `relation_targets` expose the wider graph (e.g. `dependsOn`, `partOf`).

#### Pushdown filtering

`catalog_filter` is passed straight to Backstage's `filter` query parameter for server-side filtering:

```sql
SELECT name, kind, type
FROM backstage.entities
WHERE catalog_filter = 'kind=component,spec.type=service'
LIMIT 25;
```

Predicates on other columns are applied locally after each page is fetched, so prefer `catalog_filter` on large catalogs.

## Example queries

Production components and their processed owners:

```sql
SELECT name, lifecycle, owned_by
FROM backstage.entities
WHERE catalog_filter = 'kind=component,spec.lifecycle=production'
ORDER BY name ASC;
```

Catalog breakdown by kind and type:

```sql
SELECT kind, type, COUNT(*) AS entity_count
FROM backstage.entities
GROUP BY kind, type
ORDER BY entity_count DESC;
```

Components owned by a specific group (server-side filter):

```sql
SELECT entity_ref, name, owned_by
FROM backstage.entities
WHERE catalog_filter = 'kind=component,relations.ownedBy=group:default/platform'
LIMIT 50;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/backstage/manifest.yaml
coral source test backstage
```

Live output:

```text
✓ backstage connected successfully

backstage (1 table)
└─ entities

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT name, kind FROM backstage.entities LIMIT 1
  1 row
```

## Limitations

- Read-only; catalog mutations and location registration are unsupported.
- `catalog_filter` is the only pushdown; other predicates are applied locally after each page is fetched.
- Relation columns (`owned_by`, `relation_types`, `relation_targets`) are flattened joined strings, not a normalized relations table.
- Results are limited by the permissions of the supplied token.
- `metadata.uid` is output-only and not stable for external references; prefer `entity_ref`.
