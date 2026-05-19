# dbt Cloud Source

Query dbt Cloud metadata including models, jobs, runs, environments, and exposures using SQL.

## Authentication

1. Go to [dbt Cloud](https://cloud.getdbt.com) → Account Settings → Service Tokens
2. Create a token with **Metadata Only** permissions
3. Find your Account ID in the URL: `https://cloud.getdbt.com/#/accounts/<account_id>/`

```bash
coral source add --file sources/community/dbt_cloud/manifest.yaml
```

## Example Queries

Most frequently failing jobs:
```sql
SELECT job_id, count(*) AS failures
FROM dbt_cloud.runs
WHERE status = 'error'
GROUP BY job_id
ORDER BY failures DESC
```

Longest-running jobs:
```sql
SELECT id, duration, status
FROM dbt_cloud.runs
ORDER BY duration DESC
LIMIT 20
```

All environments:
```sql
SELECT id, name, type, dbt_version
FROM dbt_cloud.environments
```

## Tables

| Table | Description |
|-------|-------------|
| `dbt_cloud.jobs` | Jobs, schedules, and orchestration metadata |
| `dbt_cloud.environments` | Environments and deployment configuration |
| `dbt_cloud.runs` | Job runs with execution state and duration |