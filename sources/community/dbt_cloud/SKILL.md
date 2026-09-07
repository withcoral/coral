---
name: dbt-cloud
description: Query dbt Cloud jobs, runs, and environments
---

# dbt Cloud Health Skill

Use this skill to query dbt Cloud jobs, runs, and environments through Coral SQL.
The dbt_cloud source exposes three tables: `dbt_cloud.jobs`, `dbt_cloud.runs`, and `dbt_cloud.environments`.

## Source Setup

Before querying, ensure the dbt_cloud source is added:
- `DBT_CLOUD_ACCOUNT_ID`: your account ID
- `DBT_CLOUD_API_TOKEN`: service token with read access
- `DBT_CLOUD_BASE_URL`: e.g. https://<account_prefix>.us1.dbt.com

## Workflow

1. Identify what the user wants: job health, run failures, environment info, or cross-source analysis.
2. Discover tables with `list_catalog` or `search_catalog` if scope is unclear.
3. Always query `dbt_cloud.jobs` first to get job_id values before filtering runs.
4. Use available filters on `dbt_cloud.runs`: job_id, status, project_id, environment_id.
5. Status codes: 1=Queued, 2=Starting, 3=Running, 10=Success, 20=Error, 30=Cancelled.
6. For cross-source joins (e.g. with github), complete each source scan first, then join locally.
7. Keep queries focused: select only needed columns, always add LIMIT unless full output is requested.
8. Summarize findings clearly: job name, status, timing, and recommended next action.

## Query Rules

- Filter runs by status=20 to find errors; status=10 for successes.
- Use `job_id` filter to push filtering to the API — do not scan all runs unnecessarily.
- `duration`, `queued_duration`, `run_duration` are strings (e.g. "00:01:23") — use for display, not arithmetic.
- `raw` column contains the full JSON object — use it only when a specific field is not in named columns.
- Virtual columns (job_id_filter, status_filter, etc.) echo applied filters — useful for verification.
- Secret inputs always return `value = NULL`; use `is_set` to check credentials.

## Common Queries

```sql
-- Recent failed runs
SELECT r.id, j.name as job_name, r.status, r.started_at, r.duration
FROM dbt_cloud.runs r
JOIN dbt_cloud.jobs j ON j.id = r.job_id
WHERE r.status = 20
ORDER BY r.started_at DESC
LIMIT 20;

-- Job health summary
SELECT j.name, j.id, j.state, j.created_at
FROM dbt_cloud.jobs j
ORDER BY j.updated_at DESC
LIMIT 50;

-- Environment overview
SELECT e.name, e.type, e.deployment_type, e.state
FROM dbt_cloud.environments e
ORDER BY e.name;
```

## Boundaries

- Do not scan all runs without a filter — use job_id or status to limit API calls.
- Do not treat empty results as failures — the account may have no runs matching the filter.

## Feedback

If the MCP `feedback` tool is available, file feedback when Coral blocks progress or the dbt Cloud API returns unexpected results.
