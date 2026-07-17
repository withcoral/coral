# dbt Cloud community source

The `dbt_cloud` community source exposes read-only dbt Cloud job, run, and
environment data through Coral SQL.

## Setup

Create a service token in dbt Cloud:

- Open dbt Cloud and go to **Account Settings** > **Service Tokens**
- Click **+ New Token** and give it a name
- Add the least-privilege role needed for this source:
  - **Job viewer** (Enterprise plan) — read-only access to job results and runs
  - **Stakeholder / Read-Only** (Enterprise plan) — read-only access to jobs, runs, and environments
  - **Read-Only** service token (Starter plan) — use when Enterprise roles are unavailable
- Copy the generated token

Find your account ID in dbt Cloud under **Account Settings** >
**Account information**.

Find your base URL in dbt Cloud under **Account Settings** >
**Account information** > **Access URL**. Examples:

- US multi-tenant: `https://cloud.getdbt.com`
- US cell-based: `https://<account_prefix>.us1.dbt.com`
- Europe: `https://emea.dbt.com`
- Australia: `https://au.dbt.com`

Then install the source:

```sh
export DBT_CLOUD_ACCOUNT_ID="<account_id>"
export DBT_CLOUD_API_TOKEN="<token>"
export DBT_CLOUD_BASE_URL="https://cloud.getdbt.com"
cargo run -p coral-cli -- source add --file sources/community/dbt_cloud/manifest.yaml
```

## Tables

| Table | Purpose |
| --- | --- |
| `dbt_cloud.jobs` | dbt Cloud jobs with schedules, execution state, and orchestration metadata. |
| `dbt_cloud.runs` | Job runs with execution state, duration, and status metadata. |
| `dbt_cloud.environments` | Environments with deployment metadata and execution configuration. |

All tables are read-only. This source does not create, trigger, or modify
dbt Cloud resources.

## Example queries

List all jobs:

```sql
SELECT id, name, project_id, environment_id, state
FROM dbt_cloud.jobs
ORDER BY name;
```

Most frequently failing runs:

```sql
SELECT job_id, count(*) AS failures
FROM dbt_cloud.runs
WHERE status = 20
GROUP BY job_id
ORDER BY failures DESC;
```

Longest-running runs:

```sql
SELECT id, duration, run_duration, status, started_at
FROM dbt_cloud.runs
ORDER BY started_at DESC
LIMIT 20;
```

Runs for a specific job:

```sql
SELECT id, status, duration, started_at, finished_at
FROM dbt_cloud.runs
WHERE job_id = 123
ORDER BY started_at DESC
LIMIT 50;
```

List all environments with deployment type:

```sql
SELECT id, name, type, deployment_type, dbt_version, project_id
FROM dbt_cloud.environments
ORDER BY name;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/dbt_cloud/manifest.yaml
```

Install and test with real credentials:

```sh
export DBT_CLOUD_ACCOUNT_ID="<account_id>"
export DBT_CLOUD_API_TOKEN="<token>"
export DBT_CLOUD_BASE_URL="https://cloud.getdbt.com"
cargo run -p coral-cli -- source add --file sources/community/dbt_cloud/manifest.yaml
cargo run -p coral-cli -- source test dbt_cloud
```

Inspect the registered source:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'dbt_cloud'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name FROM coral.columns WHERE schema_name = 'dbt_cloud' ORDER BY table_name, ordinal_position"
```

## API reference

- [dbt Cloud API v2 overview](https://docs.getdbt.com/dbt-cloud/api-v2)
- [List Jobs endpoint](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Jobs)
- [List Runs endpoint](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Runs)
- [List Environments endpoint](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Environments)
- [Service tokens](https://docs.getdbt.com/docs/dbt-apis/service-tokens)
- [Enterprise permissions](https://docs.getdbt.com/docs/platform/manage-access/enterprise-permissions)
- [Access URLs by region](https://docs.getdbt.com/docs/platform/about-platform/access-regions-ip-addresses#api-access-urls)

## Notes

- Uses the dbt Cloud Administrative API v2 with `limit`/`offset` pagination.
  Maximum page size is 100 records per request.
- Set `DBT_CLOUD_BASE_URL` to your account access URL from Account Settings.
  See [Access URLs by region](https://docs.getdbt.com/docs/platform/about-platform/access-regions-ip-addresses#api-access-urls).
- Run duration fields (`duration`, `queued_duration`, `run_duration`) are
  returned as strings by the dbt Cloud API (for example, `"00:01:23"`).
- The `environments.type` field is `development` or `deployment`. The
  production/staging classification is in `deployment_type`, not `type`.
- Run status codes: 1 = Queued, 2 = Starting, 3 = Running, 10 = Success,
  20 = Error, 30 = Cancelled.
- Nested fields are preserved as JSON in the `raw` column for each table.
- The `environments` table uses the v2 list endpoint; dbt Cloud v3 introduces
  project-scoped endpoints which may be preferred in future revisions.
- The Discovery API (GraphQL) is not used in this source. Models, tests,
  and sources metadata require the Discovery API and are out of scope for v1.