# OpenMetadata Coral Source

## What This Source Exposes

This source exposes [OpenMetadata](https://open-metadata.org/) catalog and
governance metadata as SQL tables in Coral. It is designed for data operations
visibility: tables, BI dashboards, pipelines, users, teams, owners, tags, and
glossary terms.

It is read-only and does not mutate catalog metadata.

## Authentication

Provide an OpenMetadata JWT or bot token as `OPENMETADATA_JWT_TOKEN`. Use a
token with read access to catalog entities, governance metadata, users, and
teams.

Required inputs:

| Input | Kind | Default | Description |
|---|---|---|---|
| `OPENMETADATA_API_BASE` | variable | `http://localhost:8585/api` | Base URL for the OpenMetadata API, without a trailing slash. |
| `OPENMETADATA_JWT_TOKEN` | secret | - | JWT or bot token with read access to OpenMetadata entities. |

## Setup

```bash
OPENMETADATA_API_BASE=https://openmetadata.example.com/api \
OPENMETADATA_JWT_TOKEN=your_token_here \
coral source add --file sources/community/openmetadata/manifest.yaml
```

Or run interactively:

```bash
coral source add --interactive --file sources/community/openmetadata/manifest.yaml
```

## Tables

| Table | Purpose |
|---|---|
| `openmetadata.tables` | Cataloged table entities with owners, tags, domains, and columns. |
| `openmetadata.dashboards` | BI dashboard entities ingested into OpenMetadata. |
| `openmetadata.pipelines` | Pipeline entities and orchestration metadata. |
| `openmetadata.users` | OpenMetadata users with teams and roles. |
| `openmetadata.teams` | OpenMetadata teams with parent teams and users. |
| `openmetadata.glossary_terms` | Business glossary terms with owners, tags, children, and related terms. |

## Example Queries

Tables without owners:

```sql
SELECT fully_qualified_name, database_schema_name, owners
FROM openmetadata.tables
WHERE owners IS NULL OR owners = ''
ORDER BY fully_qualified_name;
```

Dashboards by owner:

```sql
SELECT owners, COUNT(*) AS dashboard_count
FROM openmetadata.dashboards
GROUP BY owners
ORDER BY dashboard_count DESC;
```

Recently updated data assets:

```sql
SELECT fully_qualified_name, service_name, owners, updated_at
FROM openmetadata.tables
ORDER BY updated_at DESC
LIMIT 25;
```

Glossary terms that have no owner:

```sql
SELECT fully_qualified_name, glossary_name, description
FROM openmetadata.glossary_terms
WHERE owners IS NULL OR owners = '';
```

Users with admin access:

```sql
SELECT name, email, teams, roles
FROM openmetadata.users
WHERE is_admin = true
ORDER BY name;
```

## Limitations

- List endpoints use OpenMetadata cursor pagination through the `paging.after`
  field.
- The source requests commonly useful fields such as `owners`, `tags`,
  `domains`, `columns`, and `tasks`; availability depends on your OpenMetadata
  version and permissions.
- This source does not fetch raw lineage graphs yet. It focuses on inventory,
  ownership, governance, and entity metadata.
- This source does not create, update, or delete catalog entities.

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/openmetadata/manifest.yaml
Coral manifest schema validation: passed for sources/community/openmetadata/manifest.yaml
git diff --check: passed
make lint-sources: passed
Live API tests: not run; require real OpenMetadata credentials
```

Testing note: live API tests require user-provided credentials and are intentionally not part of default CI. The manifest includes `test_queries`, and no credentials or customer data are committed.
