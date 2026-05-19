# DataHub Connector

**Version:** 0.1.0
**Backend:** HTTP (GraphQL)
**Tables:** 6 (`datasets`, `dataflows`, `datajobs`, `tags`, `domains`, `users`)
**Base URL:** `http://localhost:8080` (override with `DATAHUB_GMS_URL`)

Connects to a self-hosted [DataHub](https://datahubproject.io) instance and
exposes the data catalog as queryable SQL tables. Covers dataset inventory,
pipeline lineage, governance tags, business domains, and registered users via
the DataHub GraphQL API.

Works with DataHub OSS (local Docker quickstart) and any self-hosted deployment.

## Authentication

DataHub OSS running locally is unauthenticated by default — set `DATAHUB_TOKEN`
to any non-empty placeholder value and the connector works without a real token.

For authenticated deployments, generate a Personal Access Token in the DataHub
UI under **Settings â†’ Access Tokens** and pass it as `DATAHUB_TOKEN`.

```bash
# Local unauthenticated OSS (Docker quickstart)
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_TOKEN=no-auth \
coral source add --file sources/community/datahub/manifest.yaml

# Authenticated remote deployment
DATAHUB_GMS_URL=https://datahub.example.com \
DATAHUB_TOKEN=your_personal_access_token \
coral source add --file sources/community/datahub/manifest.yaml
```

Or run interactively to be prompted for each value:

```bash
coral source add --file sources/community/datahub/manifest.yaml --interactive
```

| Input | Kind | Default | Description |
|---|---|---|---|
| `DATAHUB_GMS_URL` | variable | `http://localhost:8080` | Base URL of your DataHub GMS instance. Do **not** include `/api/graphql`. |
| `DATAHUB_TOKEN` | secret | â€” | Personal Access Token. Use any placeholder for unauthenticated local instances. |

## Tables

### `datasets`

All datasets registered in the DataHub catalog across all platforms.

```sql
SELECT urn, name, platform, qualified_name, description, owners
FROM datahub.datasets
ORDER BY platform, name
```

| Column | Type | Description |
|---|---|---|
| `urn` | Utf8 | Unique DataHub Resource Name |
| `name` | Utf8 | Dataset name |
| `platform` | Utf8 | Data platform (e.g. `hive`, `s3`, `kafka`, `hdfs`) |
| `qualified_name` | Utf8 | Fully qualified name when available |
| `description` | Utf8 | Dataset description |
| `owners` | Utf8 | Comma-joined owner usernames |

---

### `dataflows`

Orchestration pipelines and DAGs registered in DataHub (e.g. Airflow DAGs).

```sql
SELECT urn, name, platform, description
FROM datahub.dataflows
ORDER BY platform, name
```

| Column | Type | Description |
|---|---|---|
| `urn` | Utf8 | Unique DataFlow URN |
| `name` | Utf8 | Pipeline or DAG name |
| `platform` | Utf8 | Orchestration platform (e.g. `airflow`) |
| `description` | Utf8 | Pipeline description |

---

### `datajobs`

Individual tasks and jobs within pipelines (e.g. Airflow tasks within a DAG).

```sql
SELECT urn, name, description, flow_urn, flow_platform
FROM datahub.datajobs
```

| Column | Type | Description |
|---|---|---|
| `urn` | Utf8 | Unique DataJob URN |
| `name` | Utf8 | Task or job name |
| `description` | Utf8 | Task description |
| `flow_urn` | Utf8 | Parent pipeline URN â€” join to `datahub.dataflows.urn` |
| `flow_platform` | Utf8 | Orchestration platform of the parent pipeline |

---

### `tags`

Governance and classification tags registered in DataHub.

```sql
SELECT urn, name, description
FROM datahub.tags
ORDER BY name
```

| Column | Type | Description |
|---|---|---|
| `urn` | Utf8 | Unique Tag URN |
| `name` | Utf8 | Tag display name |
| `description` | Utf8 | Tag description |

---

### `domains`

Business domains used to group and govern metadata entities.

```sql
SELECT urn, name, description
FROM datahub.domains
ORDER BY name
```

| Column | Type | Description |
|---|---|---|
| `urn` | Utf8 | Unique Domain URN |
| `name` | Utf8 | Domain name (e.g. Marketing, Engineering) |
| `description` | Utf8 | Domain description |

---

### `users`

DataHub users (CorpUsers) registered in the catalog.

```sql
SELECT urn, username, display_name, email
FROM datahub.users
ORDER BY username
```

| Column | Type | Description |
|---|---|---|
| `urn` | Utf8 | Unique CorpUser URN |
| `username` | Utf8 | Login username (often an email address) |
| `display_name` | Utf8 | Human-readable display name |
| `email` | Utf8 | User email address |

---

## Quick start

```bash
# 1. Add the source
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_TOKEN=no-auth \
coral source add --file sources/community/datahub/manifest.yaml

# 2. Restart the server
coral server stop && coral server start

# 3. Explore
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'datahub'"

# All datasets with owners
coral sql "SELECT name, platform, owners, description FROM datahub.datasets ORDER BY platform, name"

# All pipelines and their tasks
coral sql "SELECT f.name AS pipeline, j.name AS task, j.description FROM datahub.datajobs j JOIN datahub.dataflows f ON j.flow_urn = f.urn"

# Governance overview
coral sql "SELECT name, description FROM datahub.domains ORDER BY name"
coral sql "SELECT name, description FROM datahub.tags ORDER BY name"

# Who owns what
coral sql "SELECT d.name AS dataset, d.platform, d.owners FROM datahub.datasets d ORDER BY d.owners, d.platform"
```

## Cascading queries

```text
datahub.domains
  â†’ urn (domain grouping)

datahub.tags
  â†’ urn (applied to datasets and other entities)

datahub.users
  â†’ username (matches owners column in datasets)

datahub.dataflows
  â†’ urn
    â†’ datahub.datajobs WHERE flow_urn = dataflows.urn
      â†’ task-level lineage and descriptions

datahub.datasets
  â†’ urn (cross-reference with lineage, tags, domains)
  â†’ owners â†’ datahub.users.username
```

## Notes

- All tables use the DataHub GraphQL `search` API with `query: "*"` to return
  all registered entities. SQL `WHERE` clauses are evaluated client-side by
  Coral after the full result set is fetched.
- `DATAHUB_TOKEN` is required by the connector even for unauthenticated local
  instances â€” pass any non-empty placeholder (e.g. `no-auth`) and DataHub OSS
  will ignore the `Authorization` header.
- `DATAHUB_GMS_URL` should be the **base URL only** â€” do not append
  `/api/graphql`. The connector appends that path automatically.
- Tested against DataHub OSS v0.14+ (Docker quickstart). The GraphQL `search`
  API shape used here has been stable since v0.9.
