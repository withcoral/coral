# Roadie (Community)

**Version:** 0.1.0
**Backend:** HTTP (Roadie Backstage Catalog API)
**Tables:** 1
**Base URL:** `https://{{input.ROADIE_TENANT}}.roadie.so/api/catalog`

Query software catalog entities including Components, APIs, Systems, Resources, Users, and Groups from Roadie using Coral SQL.

This source provides read-only access to the Roadie Software Catalog. Catalog registration, ownership modifications, lifecycle updates, and entity deletion operations are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/roadie/manifest.yaml
```

## Inputs

| Input            | Kind     | Required | Description                                                                                              |
| ---------------- | -------- | -------- | -------------------------------------------------------------------------------------------------------- |
| ROADIE_TENANT    | variable | yes      | Roadie organization subdomain. If your instance is `https://acme.roadie.so`, the tenant value is `acme`. |
| ROADIE_API_TOKEN | secret   | yes      | API token authorized to access the Roadie Software Catalog API.                                          |

### Authentication Notes

The data returned by this source is limited by the permissions associated with the provided API token.

Entities that are not visible to the token will not be returned by the Roadie Catalog API and therefore cannot be queried through Coral.

---

# Tables Overview

| Table    | Endpoint    | Method | Pagination |
| -------- | ----------- | ------ | ---------- |
| entities | `/entities` | GET    | None       |

---

# Table Reference

## roadie.entities

Software catalog entities stored within Roadie.

| Column      | Type | Description                                                           |
| ----------- | ---- | --------------------------------------------------------------------- |
| name        | Utf8 | Entity metadata name.                                                 |
| kind        | Utf8 | Entity kind such as Component, API, Resource, Group, User, or System. |
| type        | Utf8 | Entity subtype defined in the catalog specification.                  |
| lifecycle   | Utf8 | Lifecycle classification such as production or experimental.          |
| owner       | Utf8 | Owning team, group, or user reference.                                |
| description | Utf8 | Entity description text.                                              |
| namespace   | Utf8 | Catalog namespace associated with the entity.                         |

---

# Example Queries

## Catalog Breakdown by Kind

```sql
SELECT
  kind,
  COUNT(*) AS entity_count
FROM roadie.entities
GROUP BY kind
ORDER BY entity_count DESC;
```

## Production Components

```sql
SELECT
  name,
  owner,
  lifecycle
FROM roadie.entities
WHERE kind = 'Component'
  AND lifecycle = 'production'
ORDER BY owner;
```

## APIs and Their Owners

```sql
SELECT
  name,
  owner
FROM roadie.entities
WHERE kind = 'API'
ORDER BY name;
```

---

# Validation

Validate the source locally before opening a pull request.

## Lint Sources

```bash
make lint-sources
```

## Validate Manifest

```bash
coral source lint sources/community/roadie/manifest.yaml
```

## Test Connectivity

```bash
export ROADIE_TENANT=acme
export ROADIE_API_TOKEN=your_api_token

coral source add --file sources/community/roadie/manifest.yaml
coral source test roadie
```

---

# Limitations

* Read-only source.
* Does not support catalog mutations, registrations, or ownership changes.
* Query results are restricted by Roadie API permissions.
* SQL filtering is performed by Coral after retrieving catalog entities from the Roadie API.
* Large catalogs may increase query execution time because the full entity collection is retrieved before SQL evaluation.
