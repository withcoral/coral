# Backstage (Community)

**Version:** 0.1.0
**Backend:** HTTP (Backstage Software Catalog REST API)
**Tables:** 1
**Base URL:** `{{input.BACKSTAGE_URL}}/api/catalog`

Query metadata definitions, dependency ownership structures, and ecosystem blueprints indexed inside your internal developer portal using Coral SQL.

This integration serves as a global service registry auditor, allowing engineers and managers to run relational analysis over software architectures, ownership mapping gaps, or microservice lifecycle distribution metrics.

Coral exposes read-only `GET` tables. Modifying catalog structures, registering new locations, or triggering ingestion mutations are out of scope.

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
| `BACKSTAGE_URL` | variable | yes | Backstage base URL without trailing slash and without `/api/catalog`, for example `http://localhost:7007` or `https://backstage.infra.local` |
| `BACKSTAGE_TOKEN` | secret | yes | Backstage Auth Service Bearer Token or Identity Token |

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `entities` | `GET /entities` | — | None (returns direct flat JSON array with column field pushdown support) |

---

# Table Reference

## backstage.entities

Ecosystem assets tracked inside the Backstage software catalog.

| Column | Type | Description |
|---|---|---|
| `uid` | Utf8 | Globally unique ID assigned by Backstage |
| `name` | Utf8 | Technical name of the entity |
| `namespace` | Utf8 | Isolation namespace grouping the entity (defaults to `default`) |
| `kind` | Utf8 | High-level category of the entity (for example `Component`, `API`, or `User`) |
| `type` | Utf8 | Sub-categorization type defined within the specific entity kind |
| `lifecycle` | Utf8 | Operational maturity phase of the software |
| `owner` | Utf8 | Group or User entity reference declaring responsibility for the object |
| `title` | Utf8 | Display title shown in Backstage interfaces |
| `description` | Utf8 | Description associated with the catalog entity |

---

# Example Queries

## Find Production Components Missing an Owner

```sql
SELECT
  name,
  type,
  lifecycle,
  description
FROM backstage.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
  AND owner IS NULL
ORDER BY name ASC;
```

---

## Catalog Breakdown by Architecture Type

```sql
SELECT
  kind,
  type,
  COUNT(*) AS entity_count
FROM backstage.entities
GROUP BY kind, type
ORDER BY entity_count DESC;
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
```

---

# Representative Live Output

```text
$ coral source test backstage

✓ backstage connected successfully

  backstage (1 table)
  └─ entities

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT name, kind FROM backstage.entities LIMIT 1

+----------------+-----------+
| name           | kind      |
+----------------+-----------+
| catalog-broker | Component |
+----------------+-----------+

1 row
```

---

# Limitations

- Read-only retrieval scope
- Entity registration via locations or model lifecycle mutation paths are unsupported
- Targets raw flat entities payloads directly
- Complex graph resolution fields such as `relations[]` or downstream entity ancestry dependencies are out of scope for the base schema
- Backstage uses a specialized array-based query string syntax for filtering (for example `?filter=kind=Component`)
- This integration evaluates filtering engine-side using standard Coral SQL processing
- To reduce network overhead across large enterprise catalogs, the driver supports field selection optimization using Backstage `?fields=` query projections where supported
