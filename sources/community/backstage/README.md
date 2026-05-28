# Backstage (Community)

**Version:** 0.1.0
**Backend:** HTTP (Backstage Software Catalog REST API)
**Tables:** 1
**Base URL:** `{{input.BACKSTAGE_URL}}/api/catalog`

Query software catalog entities, ownership metadata, lifecycle definitions, and architectural inventory information directly through Coral SQL using the Backstage Catalog API.

This integration is intended for internal developer portal auditing workflows, allowing engineering teams to analyze ownership gaps, software lifecycle distribution, service metadata consistency, and ecosystem topology across cataloged platform assets.

Coral exposes read-only `GET` tables. Catalog mutation workflows such as location registration, ingestion triggering, or entity lifecycle modification are out of scope.

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
| `entities` | `GET /entities` | — | None (maps directly from the root JSON array response) |

---

# Table Reference

## backstage.entities

Software ecosystem assets tracked inside the Backstage software catalog.

| Column | Type | Description |
|---|---|---|
| `uid` | Utf8 | Globally unique ID assigned by Backstage |
| `name` | Utf8 | Technical name of the entity |
| `namespace` | Utf8 | Namespace grouping the entity |
| `kind` | Utf8 | High-level category of the entity (for example `Component`, `API`, or `User`) |
| `type` | Utf8 | Sub-category type defined within the entity kind |
| `lifecycle` | Utf8 | Operational lifecycle stage of the software |
| `owner` | Utf8 | Declared owner reference associated with the entity |
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
- Entity registration, ingestion triggering, and lifecycle mutation workflows are unsupported
- Targets the flat catalog entities payload directly
- Deep relationship graph traversal fields such as `relations[]` or entity ancestry resolution are out of scope for the base schema
- Backstage filtering uses a specialized query-string syntax (for example `?filter=kind=Component`)
- This source currently maps the raw entities payload directly and evaluates filtering through standard Coral SQL processing
- Large enterprise catalogs may require careful query optimization because pagination and specialized Backstage filter pushdowns are not currently implemented
