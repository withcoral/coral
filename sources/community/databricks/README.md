# Databricks

Query Databricks jobs, job runs, clusters, SQL warehouses, and Unity Catalog metadata from Databricks REST APIs.

## Setup

### Get a Personal Access Token

1. Open your Databricks workspace.
2. Go to user settings.
3. Create a personal access token.
4. Copy the token when it is shown.

Use a token for a user or service principal with read access to the jobs, compute resources, SQL warehouses, catalogs, schemas, and tables you want to inspect.

### Add the Source

```bash
coral source add databricks
```

When prompted, provide:

- `DATABRICKS_HOST`: Workspace hostname without scheme or trailing slash, such as `adb-1234567890123456.7.azuredatabricks.net`.
- `DATABRICKS_TOKEN`: Databricks personal access token.

## Tables

### `jobs`

Databricks jobs visible to the authenticated user.

Useful for job inventory, ownership review, schedule inspection, and finding `job_id` values for `job_runs`.

Optional filters:

- `name`
- `expand_tasks`

### `job_runs`

Recent Databricks job runs and execution state.

Useful for orchestration monitoring, failed-run analysis, runtime summaries, and retry inspection.

Optional filters:

- `job_id`
- `active_only`
- `completed_only`
- `start_time_from`
- `start_time_to`
- `run_type`
- `expand_tasks`

`start_time_from` and `start_time_to` are Databricks epoch-millisecond filter values.

### `clusters`

Active and configured Databricks compute clusters.

Useful for compute inventory, runtime review, cluster policy inspection, and identifying stale or terminated clusters.

### `sql_warehouses`

Databricks SQL warehouses available in the workspace.

Useful for warehouse inventory, size and scaling review, state checks, Photon/serverless visibility, and ownership summaries.

### `catalogs`

Unity Catalog catalogs visible to the authenticated user.

Useful for governance inventory, owner review, storage-root inspection, and finding `catalog_name` values for `schemas`.

### `schemas`

Unity Catalog schemas in a specific catalog.

Requires:

- `catalog_name`

Useful for catalog-level inventory, ownership review, schema storage inspection, and finding `schema_name` values for `tables`.

### `tables`

Unity Catalog tables and views in a specific schema.

Requires:

- `catalog_name`
- `schema_name`

Useful for table and view inventory, type and format summaries, owner review, and governance metadata analysis.

## Authentication

The source uses Databricks bearer authentication:

```text
Authorization: Bearer <DATABRICKS_TOKEN>
```

## Limits

- This source exposes read-only Databricks REST API endpoints only.
- Jobs, job runs, catalogs, schemas, and tables use token pagination.
- `schemas` and `tables` require filters so large Unity Catalog scans stay explicit.
- Notebook contents, view SQL definitions, write APIs, access tokens, secrets, and credentials are intentionally excluded.
- The source avoids raw cluster Spark configuration and job task payloads because they can contain sensitive workspace-specific values.

## Example Queries

### List jobs by creator

```sql
SELECT creator_user_name, COUNT(*) AS job_count
FROM databricks.jobs
GROUP BY creator_user_name
ORDER BY job_count DESC
```

### Review failed recent job runs

```sql
SELECT run_id, job_id, run_name, life_cycle_state, result_state, start_time, end_time
FROM databricks.job_runs
WHERE completed_only = 'true'
  AND result_state = 'FAILED'
ORDER BY start_time DESC
LIMIT 20
```

### Find running clusters

```sql
SELECT cluster_id, cluster_name, creator_user_name, spark_version, num_workers, state
FROM databricks.clusters
WHERE state = 'RUNNING'
ORDER BY cluster_name
```

### Summarize SQL warehouse states

```sql
SELECT state, warehouse_type, COUNT(*) AS warehouse_count
FROM databricks.sql_warehouses
GROUP BY state, warehouse_type
ORDER BY warehouse_count DESC
```

### List schemas in a catalog

```sql
SELECT catalog_name, name, owner, created_at
FROM databricks.schemas
WHERE catalog_name = 'main'
ORDER BY name
```

### List tables and views in a schema

```sql
SELECT full_name, table_type, data_source_format, owner, updated_at
FROM databricks.tables
WHERE catalog_name = 'main'
  AND schema_name = 'default'
ORDER BY full_name
```
