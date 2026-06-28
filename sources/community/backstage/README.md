# Backstage (Community)

**Version:** 0.1.0
**Backend:** HTTP (Backstage Software Catalog REST API)
**Tables:** 1
**Base URL:** `{{input.BACKSTAGE_URL}}/api/catalog`

Query software catalog entities, ownership relationships, and service metadata from your internal developer portal using Coral SQL.

This integration acts as a service registry auditor, letting engineers and managers run relational analysis over software architecture, ownership gaps, and lifecycle distribution.

Coral exposes read-only `GET` tables. Modifying catalog structures, registering locations, or triggering ingestion mutations are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/backstage/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `BACKSTAGE_URL` | variable | yes | Backstage base URL without trailing slash and without `/api/catalog`, for example `http://localhost:7007` |
| `BACKSTAGE_TOKEN` | secret | yes | Backstage Auth Service bearer token or identity token |

Coral sends the token as `Authorization: Bearer <token>`.

---

# Tables Overview

| Table | API Endpoint | Filters | Pagination |
|---|---|---|---|
| `entities` | `GET /entities/by-query` | optional pushdown: `catalog_filter` | Cursor (`limit` / `pageInfo.nextCursor`) |

The source uses Backstage's recommended paginated query endpoint, `GET /entities/by-query`. Coral reads rows from `items` and follows `pageInfo.nextCursor` automatically, so large catalogs are fetched page by page rather than in one unbounded request.

---

# Table Reference

## backstage.entities

Entities tracked inside the Backstage software catalog.

| Column | Type | Description |
|---|---|---|
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

### Entity references

Backstage documents `metadata.uid` as output-only and not stable for external references. The `entity_ref` column provides the durable `kind:namespace/name` identifier built from the entity's immutable fields; use it for joins and audit workflows. Note that Backstage's internal relation target refs are lowercased, so compare on a normalized form if you join `entity_ref` against `relation_targets`.

### Ownership and relations

`spec.owner` is the raw declared owner. After Backstage processes the catalog, ownership is represented in the entity's `relations` graph as `ownedBy` edges. The `owned_by` column surfaces those processed owner refs, and `relation_types` / `relation_targets` expose the broader relationship graph (e.g., `dependsOn`, `partOf`).

### Pushdown filtering

`catalog_filter` is passed straight to Backstage's `filter` query parameter for server-side filtering:

```sql
SELECT name, kind, type
FROM backstage.entities
WHERE catalog_filter = 'kind=component,spec.type=service'
LIMIT 25;
```

Predicates on other columns are applied locally by Coral after the page is fetched.

---

# Example Queries

## Find Production Components and Their Processed Owners

```sql
SELECT name, lifecycle, owned_by
FROM backstage.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
ORDER BY name ASC;
```

## Catalog Breakdown by Kind and Type

```sql
SELECT kind, type, COUNT(*) AS entity_count
FROM backstage.entities
GROUP BY kind, type
ORDER BY entity_count DESC;
```

## Components Owned by a Specific Group (server-side filter)

```sql
SELECT entity_ref, name, owned_by
FROM backstage.entities
WHERE catalog_filter = 'kind=component,relations.ownedBy=group:default/platform'
LIMIT 50;
```

---

# Validation

Run formatting and schema validation locally before opening a pull request.

## Lint Sources

```bash
make lint-sources
```

## Validate Coral Source Schema

```bash
coral source lint sources/community/backstage/manifest.yaml
```

## Execute Live Connection Test

```bash
export BACKSTAGE_URL=http://localhost:7007
export BACKSTAGE_TOKEN=your-backstage-token-here

coral source add --file sources/community/backstage/manifest.yaml
coral source test backstage
coral sql "SELECT entity_ref, kind, owned_by FROM backstage.entities LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test backstage`
> run against this manifest. Do not ship placeholder output.

```text
$ coral source test backstage

✓ backstage connected successfully

  backstage (1 table)
  └─ entities

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT name, kind FROM backstage.entities LIMIT 1
  1 row
```

---

# Limitations

- Read-only retrieval scope; catalog mutations and location registration are unsupported.
- `catalog_filter` is pushed to Backstage's `filter` parameter; other predicates are applied locally after each page is fetched.
- Relation columns (`owned_by`, `relation_types`, `relation_targets`) are flattened/joined representations of the `relations` graph, not a normalized relations table.
- Visibility depends on the permissions of the supplied token.
