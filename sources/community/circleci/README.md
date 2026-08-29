# CircleCI source

**Version:** 0.1.0  
**Backend:** HTTP  
**Tables:** 5  
**Base URL:** `https://circleci.com/api/v2` (override with `CIRCLECI_API_BASE` env var)

This bundled source lets Coral query core CircleCI API v2 data with a personal API token.

The first version is intentionally pipeline-centric and read-only. It focuses
on five useful surfaces:

- the current user
- organizations and teams available to the current user
- recent pipelines for one organization
- workflows inside one pipeline
- jobs inside one workflow

## Authentication

Requires a `CIRCLECI_TOKEN` environment variable. Create a **personal API token** (not a project token) with access to the organizations and projects you want to query at [CircleCI token guide](https://circleci.com/docs/managing-api-tokens/). This source queries user and organization-level endpoints like `/me` and pipeline listings.

```bash
export CIRCLECI_TOKEN="your_token"
```

Override the API base if needed (defaults to `https://circleci.com/api/v2`):

```bash
export CIRCLECI_API_BASE="https://circleci.com/api/v2"
```

## Quick start

```sh
coral source add circleci
coral source test circleci
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'circleci' ORDER BY table_name"
```

If you update the token later, run `coral source add circleci` again so Coral
refreshes the stored credentials.

## Inspect the installed shape

After adding the source, inspect what Coral sees:

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'circleci'
ORDER BY table_name;
```

```sql
SELECT table_name, column_name, data_type, is_nullable
FROM coral.columns
WHERE schema_name = 'circleci'
ORDER BY table_name, ordinal_position;
```

```sql
SELECT key, kind, required, is_set, default_value
FROM coral.inputs
WHERE schema_name = 'circleci'
ORDER BY key;
```

This is useful for confirming required filters and seeing which nested CircleCI
payloads stay as JSON.

## Tables

| Table | Notes |
|---|---|
| `me` | Current user for the configured token |
| `me_collaborations` | Organizations and teams accessible to current user; use for discovering `org_slug` values |
| `pipelines` | Recent pipelines for one organization; requires `org_slug` |
| `pipeline_workflows` | Workflows inside one pipeline; requires `pipeline_id` |
| `workflow_jobs` | Jobs inside one workflow; requires `workflow_id` |

## About `org_slug`

`pipelines` requires an `org_slug` because CircleCI's list-pipelines endpoint is
organization-scoped. Use the `me_collaborations` table to discover available organization slugs:

```sql
SELECT slug, name
FROM circleci.me_collaborations
ORDER BY name;
```

For GitHub OAuth organizations, the slug typically looks like:

- `gh/my-org`

For GitHub App or GitLab-backed projects, CircleCI documents alternate slug
formats using `circleci` plus provider IDs. Use the slug format shown in your
CircleCI organization settings.

This source does **not** expose full project inventory in v1. It is built
around recent pipeline discovery.

## How to query it

Check the current user:

```sql
SELECT id, login, name, avatar_url
FROM circleci.me;
```

Discover available organizations and teams:

```sql
SELECT slug, name
FROM circleci.me_collaborations
ORDER BY name;
```

List recent pipelines for one organization:

```sql
SELECT id, number, state, created_at, project_slug
FROM circleci.pipelines
WHERE org_slug = 'gh/my-org'
ORDER BY created_at DESC
LIMIT 20;
```

Inspect workflows for one pipeline:

```sql
SELECT id, name, status, created_at, stopped_at
FROM circleci.pipeline_workflows
WHERE pipeline_id = 'YOUR_PIPELINE_ID'
ORDER BY created_at DESC
LIMIT 20;
```

Inspect jobs for one workflow:

```sql
SELECT job_number, name, status, type, started_at, stopped_at
FROM circleci.workflow_jobs
WHERE workflow_id = 'YOUR_WORKFLOW_ID'
ORDER BY started_at DESC
LIMIT 20;
```

## Table behavior notes

- `me_collaborations` returns organizations and teams where the current user has access; use this to discover valid `org_slug` values for the pipelines table.
- `pipeline_workflows` is a lookup-style table. It requires one `pipeline_id` and uses `created_at` (workflow creation time).
- `workflow_jobs` is a lookup-style table. It requires one `workflow_id`.
- `pipelines` now exposes `project_slug` as a first-class column for identifying which repo/project a pipeline belongs to.
- `pipelines` also exposes `updated_at` and `trigger_parameters` for more detailed pipeline inspection.
- `trigger`, `vcs`, `errors`, `dependencies`, `requires`, and `raw` remain JSON so the source stays stable across different CircleCI accounts.
- This source intentionally does not expose reruns, cancellations, artifacts, tests, contexts, or insights in v1.
