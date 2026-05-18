# Buildkite Community Source

Query Buildkite organizations, pipelines, builds, and agents through Coral SQL
using the [Buildkite REST API](https://buildkite.com/docs/apis/rest-api).

## Setup

### 1. Create a Buildkite API token

Create a token from your Buildkite user settings under **API Access Tokens**.
Grant read access for the organizations, pipelines, builds, and agents you plan
to inspect.

### 2. Add the source

```bash
export BUILDKITE_API_TOKEN="<your-token>"
coral source add --file sources/community/buildkite/manifest.yaml
```

### 3. Verify

```bash
coral source test buildkite
```

The default test queries read `buildkite.current_user` and a small page from
`buildkite.organizations`, which validates that the token is accepted by the API
and exercises Link-header pagination.

## Tables

### `buildkite.current_user`

Returns the current authenticated Buildkite user.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | User ID |
| `graphql_id` | Utf8 | GraphQL node ID |
| `name` | Utf8 | Display name |
| `email` | Utf8 | Email address |
| `avatar_url` | Utf8 | Avatar URL |
| `created_at` | Timestamp | User creation time |

### `buildkite.organizations`

Lists organizations visible to the token.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Organization ID |
| `graphql_id` | Utf8 | GraphQL node ID |
| `name` | Utf8 | Organization name |
| `slug` | Utf8 | Organization slug |
| `url` | Utf8 | API URL |
| `web_url` | Utf8 | Browser URL |
| `pipelines_url` | Utf8 | Pipelines API URL |
| `agents_url` | Utf8 | Agents API URL |

### `buildkite.pipelines`

Lists pipelines in an organization.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Pipeline ID |
| `graphql_id` | Utf8 | GraphQL node ID |
| `name` | Utf8 | Pipeline name |
| `slug` | Utf8 | Pipeline slug |
| `repository` | Utf8 | Repository URL |
| `branch_configuration` | Utf8 | Branch filter configuration |
| `default_branch` | Utf8 | Default branch |
| `provider` | Json | Source control provider metadata |
| `steps` | Json | Pipeline steps |
| `created_at` | Timestamp | Creation time |
| `archived_at` | Timestamp | Archive time |
| `archived` | Boolean | Whether the pipeline is archived |
| `url` | Utf8 | API URL |
| `web_url` | Utf8 | Browser URL |
| `org_slug` | Utf8 | Organization slug used for the request |

**Required filter:** `org_slug`

### `buildkite.organization_builds`

Lists builds across an organization.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Build ID |
| `graphql_id` | Utf8 | GraphQL node ID |
| `number` | Int64 | Build number |
| `state` | Utf8 | Build state |
| `blocked` | Boolean | Whether the build is blocked |
| `message` | Utf8 | Build message |
| `commit` | Utf8 | Git commit SHA |
| `branch` | Utf8 | Git branch |
| `tag` | Utf8 | Git tag |
| `source` | Utf8 | Build source |
| `creator__id` | Utf8 | Creator user ID |
| `creator__email` | Utf8 | Creator email |
| `pipeline__slug` | Utf8 | Pipeline slug |
| `jobs` | Json | Build jobs |
| `created_at` | Timestamp | Creation time |
| `scheduled_at` | Timestamp | Scheduled time |
| `started_at` | Timestamp | Start time |
| `finished_at` | Timestamp | Finish time |
| `url` | Utf8 | API URL |
| `web_url` | Utf8 | Browser URL |
| `org_slug` | Utf8 | Organization slug used for the request |

**Required filter:** `org_slug`  
**Optional filters:** `state`, `branch`, `commit`, `creator`

### `buildkite.pipeline_builds`

Lists builds for a single pipeline.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Build ID |
| `graphql_id` | Utf8 | GraphQL node ID |
| `number` | Int64 | Build number |
| `state` | Utf8 | Build state |
| `blocked` | Boolean | Whether the build is blocked |
| `message` | Utf8 | Build message |
| `commit` | Utf8 | Git commit SHA |
| `branch` | Utf8 | Git branch |
| `tag` | Utf8 | Git tag |
| `source` | Utf8 | Build source |
| `creator__id` | Utf8 | Creator user ID |
| `creator__email` | Utf8 | Creator email |
| `pipeline__slug` | Utf8 | Pipeline slug |
| `jobs` | Json | Build jobs |
| `created_at` | Timestamp | Creation time |
| `scheduled_at` | Timestamp | Scheduled time |
| `started_at` | Timestamp | Start time |
| `finished_at` | Timestamp | Finish time |
| `url` | Utf8 | API URL |
| `web_url` | Utf8 | Browser URL |
| `org_slug` | Utf8 | Organization slug used for the request |
| `pipeline_slug` | Utf8 | Pipeline slug used for the request |

**Required filters:** `org_slug`, `pipeline_slug`  
**Optional filters:** `state`, `branch`, `commit`, `creator`

### `buildkite.agents`

Lists Buildkite agents connected to an organization.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Agent ID |
| `graphql_id` | Utf8 | GraphQL node ID |
| `name` | Utf8 | Agent name |
| `connection_state` | Utf8 | Current connection state |
| `ip_address` | Utf8 | Agent IP address |
| `hostname` | Utf8 | Agent host name |
| `user_agent` | Utf8 | Agent user-agent string |
| `version` | Utf8 | Agent version |
| `job` | Json | Current job, if any |
| `created_at` | Timestamp | Creation time |
| `org_slug` | Utf8 | Organization slug used for the request |

**Required filter:** `org_slug`

## Example queries

```sql
-- Validate the token and see the authenticated user
SELECT id, name, email
FROM buildkite.current_user;

-- Discover organizations and use `slug` in other tables
SELECT name, slug
FROM buildkite.organizations
ORDER BY name;

-- List pipelines in an organization
SELECT name, slug, repository, default_branch
FROM buildkite.pipelines
WHERE org_slug = 'my-org'
ORDER BY name;

-- Find failed builds across an organization
SELECT pipeline__slug, number, branch, commit, finished_at, web_url
FROM buildkite.organization_builds
WHERE org_slug = 'my-org' AND state = 'failed'
ORDER BY finished_at DESC
LIMIT 20;

-- Check active agents
SELECT name, connection_state, hostname, version
FROM buildkite.agents
WHERE org_slug = 'my-org'
ORDER BY name;
```

## Validation

```bash
export BUILDKITE_API_TOKEN="<your-token>"
coral source lint sources/community/buildkite/manifest.yaml
coral source add --file sources/community/buildkite/manifest.yaml
coral source test buildkite
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'buildkite'"
coral sql "SELECT * FROM coral.columns WHERE schema_name = 'buildkite'"
coral sql "SELECT name, slug FROM buildkite.organizations LIMIT 5"
```

## Limitations

- **Read-only.** This source does not create, rebuild, cancel, unblock, or
  otherwise mutate Buildkite resources.
- **Token visibility.** Returned organizations, pipelines, builds, and agents
  depend on the token's Buildkite permissions.
- **Job details.** Build jobs are exposed as JSON on build rows. A future source
  revision could expose jobs as a separate table.

## Out of scope for v1

- Build artifacts
- Test analytics
- Annotations
- Teams and access control
- Write operations
