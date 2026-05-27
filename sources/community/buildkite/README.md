# Buildkite (Community)

**Version:** 0.1.0
**Backend:** HTTP (Buildkite REST API v2)
**Tables:** 5
**Base URL:** `https://api.buildkite.com/v2`

Query organizations, pipelines, builds, and agents from Buildkite via SQL.
Designed for CI/CD analytics: build cycle times, pipeline success rates, and
agent infrastructure monitoring. Pairs naturally with the bundled **GitHub**
and **Bitbucket** sources for cross-source engineering metrics.

## Setup

### 1. Create a Buildkite API access token

1. Go to [https://buildkite.com/user/api-access-tokens](https://buildkite.com/user/api-access-tokens)
2. Click **New API Access Token**
3. Give it a description and select the following scopes:
   - `read_organizations`
   - `read_pipelines`
   - `read_builds`
   - `read_agents`
4. Copy the token.

### 2. Set your token

```sh
export BUILDKITE_TOKEN="<your-api-token>"
```

### 3. Add the source

```sh
cargo run -p coral-cli -- source add --file sources/community/buildkite/manifest.yaml
```

### 4. Verify

```sh
cargo run -p coral-cli -- sql "SELECT slug, name FROM buildkite.organizations LIMIT 5"
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `buildkite.organizations` | Organizations accessible by the token | — |
| `buildkite.pipelines` | Pipelines in an organization | `org_slug` |
| `buildkite.builds` | Builds for a specific pipeline | `org_slug`, `pipeline_slug` |
| `buildkite.org_builds` | Builds across all pipelines in an org | `org_slug` |
| `buildkite.agents` | Agents connected to an organization | `org_slug` |

All tables are read-only. This source does not create, modify, or delete any
Buildkite data.

### `organizations`

Lists all organizations accessible by the API token. Use `slug` as `org_slug`
in all other tables. Start here to discover your organization slug.

### `pipelines`

Lists all pipelines in an organization. Use `slug` as `pipeline_slug` in the
`builds` table. `repository` shows the connected source repository.

### `builds`

Lists builds for a specific pipeline. Requires both `org_slug` and
`pipeline_slug`. Use `state` to filter results locally:

| Value | Meaning |
|---|---|
| `passed` | Build completed successfully |
| `failed` | Build failed |
| `running` | Build is currently running |
| `scheduled` | Build is waiting to run |
| `blocked` | Build is blocked on a manual step |
| `canceled` | Build was canceled |
| `skipped` | Build was skipped |

`pipeline_slug` and `pipeline_name` are sourced from the nested `pipeline`
object in the API response.

### `org_builds`

Lists builds across all pipelines in an organization. Requires only `org_slug`.
Use when you need a cross-pipeline view. For pipeline-specific queries, prefer
`buildkite.builds` which is more targeted and efficient.

### `agents`

Lists agents connected to an organization. Use `connection_state` to monitor
agent availability. `hostname` and `ip_address` identify the machine running
the agent.

## Filters and pagination

All tables use page-based pagination (`page`, `per_page`). The default page
size is 30; the maximum is 100. Always use `LIMIT` when querying organizations
with many pipelines or builds.

## Example queries

List your organizations:

```sql
SELECT slug, name, created_at
FROM buildkite.organizations
LIMIT 10;
```

List pipelines in an organization:

```sql
SELECT slug, name, default_branch, repository, created_at
FROM buildkite.pipelines
WHERE org_slug = 'my-org'
ORDER BY name
LIMIT 20;
```

Recent builds for a pipeline:

```sql
SELECT number, state, branch, commit, creator_name, created_at, finished_at
FROM buildkite.builds
WHERE org_slug = 'my-org'
  AND pipeline_slug = 'my-pipeline'
ORDER BY created_at DESC
LIMIT 20;
```

Failed builds for a pipeline:

```sql
SELECT number, branch, commit, creator_name, created_at, finished_at
FROM buildkite.builds
WHERE org_slug = 'my-org'
  AND pipeline_slug = 'my-pipeline'
  AND state = 'failed'
ORDER BY created_at DESC
LIMIT 20;
```

Builds across all pipelines in an org:

```sql
SELECT number, pipeline_slug, pipeline_name, state, branch, created_at
FROM buildkite.org_builds
WHERE org_slug = 'my-org'
ORDER BY created_at DESC
LIMIT 20;
```

Connected agents:

```sql
SELECT name, connection_state, hostname, version, os
FROM buildkite.agents
WHERE org_slug = 'my-org'
  AND connection_state = 'connected'
ORDER BY name
LIMIT 20;
```

Build cycle times for a pipeline (passed builds only):

```sql
SELECT
  number,
  branch,
  creator_name,
  created_at,
  started_at,
  finished_at
FROM buildkite.builds
WHERE org_slug = 'my-org'
  AND pipeline_slug = 'my-pipeline'
  AND state = 'passed'
ORDER BY finished_at DESC
LIMIT 50;
```

Cross-source: Buildkite builds alongside Bitbucket pull requests:

```sql
SELECT
  b.number      AS build_number,
  b.state       AS build_state,
  b.branch,
  b.created_at  AS build_started,
  pr.title      AS pr_title,
  pr.author_nickname
FROM buildkite.builds b
LEFT JOIN bitbucket.pull_requests pr
  ON b.branch = pr.source_branch
  AND pr.workspace = 'my-workspace'
  AND pr.repo_slug = 'my-repo'
WHERE b.org_slug = 'my-org'
  AND b.pipeline_slug = 'my-pipeline'
ORDER BY b.created_at DESC
LIMIT 20;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/buildkite/manifest.yaml
```

Add the source and validate each table:

```sh
export BUILDKITE_TOKEN="<your-api-token>"
cargo run -p coral-cli -- source add --file sources/community/buildkite/manifest.yaml

# organizations — no required filters
cargo run -p coral-cli -- sql "SELECT slug, name, created_at FROM buildkite.organizations LIMIT 5"

# pipelines — requires org_slug
cargo run -p coral-cli -- sql "SELECT slug, name, default_branch FROM buildkite.pipelines WHERE org_slug = 'your-org' LIMIT 5"

# builds — requires org_slug and pipeline_slug
cargo run -p coral-cli -- sql "SELECT number, state, branch, created_at, finished_at FROM buildkite.builds WHERE org_slug = 'your-org' AND pipeline_slug = 'your-pipeline' LIMIT 5"

# org_builds — requires org_slug only
cargo run -p coral-cli -- sql "SELECT number, pipeline_slug, state, created_at FROM buildkite.org_builds WHERE org_slug = 'your-org' LIMIT 5"

# agents — requires org_slug
cargo run -p coral-cli -- sql "SELECT name, connection_state, hostname, version FROM buildkite.agents WHERE org_slug = 'your-org' LIMIT 5"
```

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'buildkite'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'buildkite' ORDER BY table_name, ordinal_position"
```

## Notes

- **API token scopes:** create the token with `read_organizations`,
  `read_pipelines`, `read_builds`, and `read_agents` scopes. The token
  is sent as `Authorization: Bearer`.
- **`pipeline_slug` and `pipeline_name`** in `builds` and `org_builds` are
  sourced from the nested `pipeline.slug` and `pipeline.name` fields in the
  Buildkite API response, not top-level fields.
- **`creator_name` and `creator_email`** are sourced from the nested
  `creator.name` and `creator.email` fields in the API response.
- **`builds` vs `org_builds`:** use `builds` when you know the pipeline slug
  for targeted queries; use `org_builds` for cross-pipeline views.
- **Page-based pagination:** all tables paginate by `page` and `per_page`.
  Always use `LIMIT` on large organizations.
- **Rate limits:** the Buildkite API enforces rate limits per token. Reduce
  query frequency if you hit limits.

## Out of scope for v1

- Jobs table (requires `org_slug`, `pipeline_slug`, and `build_number`)
- Annotations
- Artifacts
- Teams and team members
- Write operations of any kind
