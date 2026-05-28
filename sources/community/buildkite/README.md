# Buildkite source

Query Buildkite CI/CD data through Coral SQL.

This community source exposes read-only Buildkite REST API data for DevOps,
SRE, platform engineering, and release-readiness workflows. It helps answer:

- Which pipelines have active build pressure?
- Which builds are failing or blocked?
- Which agents are connected and available?
- Which jobs have annotations worth reviewing?
- Which scheduled builds are configured for a pipeline?

## Configuration

Create a Buildkite API access token from:

`Personal Settings -> API Access Tokens`

Recommended read scopes:

- `read_organizations`
- `read_pipelines`
- `read_builds`
- `read_agents`

Set the required inputs:

```bash
export BUILDKITE_API_TOKEN="bkua_..."
export BUILDKITE_ORG="my-great-org"
```

Add and test the source:

```bash
coral source add --file sources/community/buildkite/manifest.yaml
coral source test buildkite
```

## Example Queries

List visible organizations:

```sql
SELECT id, slug, name, web_url
FROM buildkite.organizations
LIMIT 20;
```

Find pipelines with active work:

```sql
SELECT
  slug,
  name,
  repository,
  running_builds_count,
  waiting_jobs_count
FROM buildkite.pipelines
ORDER BY running_builds_count DESC
LIMIT 20;
```

Inspect failed builds:

```sql
SELECT
  pipeline__slug,
  number,
  state,
  branch,
  message,
  web_url,
  finished_at
FROM buildkite.builds
WHERE state = 'failed'
LIMIT 25;
```

Check connected agent capacity:

```sql
SELECT
  id,
  name,
  connection_state,
  hostname,
  version,
  meta_data,
  last_job_finished_at
FROM buildkite.agents
LIMIT 50;
```

Read annotations for a job:

```sql
SELECT context, style, body_html
FROM buildkite.job_annotations
WHERE job_id = 'job-uuid';
```

Review schedules for a pipeline:

```sql
SELECT id, label, branch, cronline, enabled
FROM buildkite.schedules
WHERE pipeline_slug = 'deploy-production';
```

## Tables

- `buildkite.organizations`
- `buildkite.pipelines`
- `buildkite.builds`
- `buildkite.agents`
- `buildkite.job_annotations`
- `buildkite.schedules`

## Notes

- This source only performs read-only `GET` requests.
- Organization-scoped tables use the `BUILDKITE_ORG` input.
- `buildkite.job_annotations` requires a `job_id` filter.
- `buildkite.schedules` requires a `pipeline_slug` filter.
