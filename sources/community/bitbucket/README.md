# Bitbucket (Community)

**Version:** 0.1.0
**Backend:** HTTP (Bitbucket Cloud REST API v2.0)
**Tables:** 3
**Base URL:** `https://api.bitbucket.org/2.0`

Query repositories, pull requests, and CI/CD pipelines from Bitbucket Cloud
via SQL. Designed for engineering analytics: PR cycle times, deployment
tracking, and compliance reporting. Pairs naturally with the bundled **Jira**
source for full Atlassian-stack coverage.

## Setup

### 1. Create a Bitbucket OAuth consumer

In your Bitbucket workspace, go to
**Settings → OAuth consumers → Add consumer**.

Add the following redirect URI exactly as shown:

```
http://127.0.0.1:53682/oauth/callback
```

Grant the following permissions:

| Permission | Scope |
|---|---|
| Repositories | Read |
| Pull requests | Read |
| Pipelines | Read |

Copy the **Key** (this is your `BITBUCKET_CLIENT_ID`) and the **Secret**
(`BITBUCKET_CLIENT_SECRET`) from the consumer you just created.

> **Note:** the `account` scope is not required. Only repository, pull
> request, and pipeline read access is needed.

### 2. Set your credentials

```sh
export BITBUCKET_CLIENT_ID="<your-consumer-key>"
export BITBUCKET_CLIENT_SECRET="<your-consumer-secret>"
```

### 3. Add the source

```sh
cargo run -p coral-cli -- source add --file sources/community/bitbucket/manifest.yaml
```

Coral will open your browser to the Bitbucket authorization page. After you
approve the requested scopes, Coral stores the access token automatically and
handles token refresh.

> **Auth note:** this source uses `Authorization: Bearer <token>` and supports
> any Bitbucket Bearer token — OAuth access tokens from the flow above, or
> Bitbucket repository access tokens (which also use Bearer auth). Bitbucket
> App passwords are the Basic auth variant and are **not** compatible with
> this source.

### 4. Verify

```sh
cargo run -p coral-cli -- sql "SELECT slug, name, is_private FROM bitbucket.repositories WHERE workspace = 'your-workspace' LIMIT 5"
```

Replace `your-workspace` with your Bitbucket workspace slug.

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `bitbucket.repositories` | Repositories in a workspace | `workspace` | — |
| `bitbucket.pull_requests` | Pull requests in a repository | `workspace`, `repo_slug` | `state` |
| `bitbucket.pipelines` | CI/CD pipeline runs for a repository | `workspace`, `repo_slug` | — |

All tables are read-only. This source does not create, modify, or delete any
Bitbucket data.

### `repositories`

Lists all repositories in a workspace. The `slug` column is the value to
pass as `repo_slug` when querying `pull_requests` or `pipelines`.

### `pull_requests`

Lists pull requests for one repository. The optional `state` filter is pushed
down to the Bitbucket API:

| Value | Meaning |
|---|---|
| `OPEN` | Pull request is open |
| `MERGED` | Pull request was merged |
| `DECLINED` | Pull request was declined |
| `SUPERSEDED` | Pull request was superseded by another |

The `summary` column contains the PR description as plain markup text
(sourced from `summary.raw` in the API response).

### `pipelines`

Lists CI/CD pipeline runs for one repository. `state_name` holds the
top-level run state (for example `COMPLETED` or `IN_PROGRESS`).
`state_result_name` holds the result within a completed run (for example
`SUCCESSFUL` or `FAILED`). Filter by these columns locally after fetching.

## Filters and pagination

All tables use page-based pagination (`page`, `pagelen`). The default page
size is 50; the maximum is 100. Always use `LIMIT` when querying large
workspaces or repositories.

Bitbucket Cloud does not expose a workspace-discovery endpoint without
elevated admin scopes. Pass the workspace slug directly as a required filter,
matching the behaviour of the bundled Jira and Confluence sources.

## Example queries

List repositories in a workspace:

```sql
SELECT slug, name, language, is_private, updated_on
FROM bitbucket.repositories
WHERE workspace = 'my-workspace'
ORDER BY updated_on DESC
LIMIT 20;
```

Open pull requests for a repository (state pushed down to the API):

```sql
SELECT id, title, author_nickname, source_branch, destination_branch, created_on
FROM bitbucket.pull_requests
WHERE workspace = 'my-workspace'
  AND repo_slug = 'my-repo'
  AND state = 'OPEN'
ORDER BY created_on DESC
LIMIT 20;
```

Merged PR cycle times:

```sql
SELECT id, title, author_nickname, created_on, updated_on
FROM bitbucket.pull_requests
WHERE workspace = 'my-workspace'
  AND repo_slug = 'my-repo'
  AND state = 'MERGED'
ORDER BY updated_on DESC
LIMIT 50;
```

Failed pipeline runs (filtered locally on `state_result_name`):

```sql
SELECT build_number, creator_nickname, created_on, completed_on
FROM bitbucket.pipelines
WHERE workspace = 'my-workspace'
  AND repo_slug = 'my-repo'
  AND state_result_name = 'FAILED'
ORDER BY created_on DESC
LIMIT 20;
```

Successful pipeline runs:

```sql
SELECT build_number, state_name, state_result_name, created_on, completed_on
FROM bitbucket.pipelines
WHERE workspace = 'my-workspace'
  AND repo_slug = 'my-repo'
  AND state_result_name = 'SUCCESSFUL'
ORDER BY created_on DESC
LIMIT 20;
```

Private repositories in a workspace:

```sql
SELECT slug, name, language, updated_on
FROM bitbucket.repositories
WHERE workspace = 'my-workspace'
  AND is_private = true
ORDER BY updated_on DESC
LIMIT 20;
```

Recent pipeline runs alongside open pull requests for the same repository:

```sql
SELECT
  pr.id                  AS pr_id,
  pr.title               AS pr_title,
  pr.author_nickname,
  pr.created_on          AS pr_opened,
  pi.build_number,
  pi.state_name,
  pi.state_result_name,
  pi.completed_on        AS pipeline_completed
FROM bitbucket.pull_requests pr
JOIN bitbucket.pipelines pi
  ON pr.workspace = pi.workspace
  AND pr.repo_slug = pi.repo_slug
WHERE pr.workspace = 'my-workspace'
  AND pr.repo_slug = 'my-repo'
  AND pr.state = 'OPEN'
ORDER BY pi.build_number DESC
LIMIT 20;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/bitbucket/manifest.yaml
```

Add the source:

```sh
export BITBUCKET_CLIENT_ID="<your-consumer-key>"
export BITBUCKET_CLIENT_SECRET="<your-consumer-secret>"
cargo run -p coral-cli -- source add --file sources/community/bitbucket/manifest.yaml
```

Validate each table with your real workspace and repo slug. Replace
`your-workspace` and `your-repo` with values from your Bitbucket account:

```sh
# repositories — requires only workspace
cargo run -p coral-cli -- sql "SELECT slug, name, is_private FROM bitbucket.repositories WHERE workspace = 'your-workspace' LIMIT 5"

# pull_requests — requires workspace and repo_slug; optional state pushdown
cargo run -p coral-cli -- sql "SELECT id, title, state, author_nickname, created_on FROM bitbucket.pull_requests WHERE workspace = 'your-workspace' AND repo_slug = 'your-repo' AND state = 'OPEN' LIMIT 5"

# pipelines — requires workspace and repo_slug
cargo run -p coral-cli -- sql "SELECT build_number, state_name, state_result_name, creator_nickname, created_on, completed_on FROM bitbucket.pipelines WHERE workspace = 'your-workspace' AND repo_slug = 'your-repo' LIMIT 5"
```

> **Note:** `pull_requests` and `pipelines` require real `workspace` and
> `repo_slug` path parameters. The `repositories` table only needs `workspace`
> and is the recommended first validation step.

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'bitbucket'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'bitbucket' ORDER BY table_name, ordinal_position"
```

## Notes

- **Supported Bearer tokens:** this source accepts any Bitbucket Bearer token
  — OAuth access tokens issued by the authorization-code flow, or Bitbucket
  repository access tokens (also Bearer). Bitbucket App passwords use HTTP
  Basic auth and will not work here.
- **OAuth requires a confidential client:** Bitbucket Cloud requires
  `client_secret` for the authorization-code flow; PKCE public-client
  flows are not currently supported.
- **Token refresh:** Coral handles OAuth token refresh automatically using
  the stored refresh token. OAuth access tokens expire after one hour;
  no manual intervention is required for refresh.
- **Fixed redirect URI:** the OAuth consumer must be registered with the
  exact redirect URI `http://127.0.0.1:53682/oauth/callback`.
- **No workspace discovery:** Bitbucket Cloud does not expose a simple
  workspace list without admin scopes. Pass the workspace slug directly
  as a required filter.
- **Page-based pagination:** all tables paginate by `page` and `pagelen`.
  Always use `LIMIT` on large workspaces or repositories.
- **`account` scope not required:** only `repository`, `pullrequest`, and
  `pipeline` read scopes are needed.

## Out of scope for v1

- Workspace discovery (requires admin scopes)
- Commits table
- Branches table
- Pipeline steps and log output
- Write operations of any kind
