# GitLab Connector

**API Version:** v4
**Source:** OpenAPI-generated from GitLab's REST API v4 spec
**Backend:** HTTP
**Tables:** 216
**Base URL:** `https://gitlab.com` (override with `GITLAB_API_BASE` variable for self-hosted instances)

## Authentication

Requires a `GITLAB_TOKEN` credential. You can connect with GitLab OAuth for
GitLab.com, or paste a token.

### OAuth

Add the source interactively and choose `Connect with GitLab.com OAuth`:

```bash
coral source add --interactive gitlab
```

To use your own GitLab OAuth application, configure it with:

- Redirect URI: `http://127.0.0.1:53682/oauth/callback`
- Scopes: `read_api`
- Leave `Confidential` unchecked

Then enter its Application ID when prompted for `GITLAB_OAUTH_CLIENT_ID`.

The bundled OAuth flow targets GitLab.com. For self-hosted GitLab instances,
use a token with `GITLAB_API_BASE` set to your instance URL.

GitLab OAuth access tokens expire after two hours. Until Coral supports
automatic refresh, rerun `coral source add --interactive gitlab` to refresh the
stored token. See the
[GitLab OAuth provider docs](https://docs.gitlab.com/integration/oauth_provider/).

### Token

Add the source and provide your token when prompted:

```bash
coral source add --interactive gitlab
```

To rotate or update your token, run the same command again. Non-interactive
setup still works with environment variables:

```bash
GITLAB_TOKEN=glpat-... coral source add gitlab
```

### Token scopes

| Scope | Coverage | Notes |
|---|---|---|
| `read_api` | Most tables | Recommended minimum scope |
| `api` | All tables | Full read/write access |

### Rate limiting

GitLab allows 2,000 API requests per minute for authenticated users (may vary on self-hosted instances).

## Table categories

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| No filter | 35 | `SELECT * FROM gitlab.all_projects` |
| `id` (project or group) | 92 | `WHERE id = '12345'` |
| `id` + `merge_request_iid` | 10 | `WHERE id = '123' AND merge_request_iid = '45'` |
| `id` + `issue_iid` | 6 | `WHERE id = '123' AND issue_iid = '67'` |
| `id` + `sha` | 6 | `WHERE id = '123' AND sha = 'abc123'` |
| `id` + `pipeline_id` | 3 | `WHERE id = '123' AND pipeline_id = '789'` |
| `id` + `package_name` | 7 | `WHERE id = '123' AND package_name = 'my-pkg'` |
| Other compound filters | 57 | Various combinations |

### No filter required (35 tables)

| Table | Description |
|---|---|
| `all_projects` | All visible projects for authenticated user |
| `all_merge_requests` | All merge requests |
| `all_issues` | Authenticated user's issues |
| `all_events` | Authenticated user's events |
| `all_runners` | All available runners |
| `all_deploy_tokens` | All deploy tokens |
| `groups` | All visible groups |
| `namespaces` | All namespaces |
| `snippets` | Authenticated user's snippets |
| `personal_access_tokens` | User's personal access tokens |
| `version` | GitLab server version |
| `metadata` | GitLab instance metadata |
| `topics` | Instance-wide topics |
| `broadcast_messages` | Active broadcast messages |

### Project tables (115 tables)

Core data:

| Table | Notes |
|---|---|
| `projects` | User or runner projects (requires `user_id` or `id`) |
| `branches` | Project branches |
| `commits` | Commit history |
| `merge_requests` | Project merge requests |
| `issues` | Project/group issues |
| `labels` | Project labels |
| `milestones` | Project milestones |
| `releases` | Project releases |
| `wikis` | Project wiki pages |
| `members` | Project/group members |
| `environments` | Deployment environments |

CI/CD:

| Table | Notes |
|---|---|
| `pipelines` | Project pipelines |
| `jobs` | Pipeline jobs |
| `deployments` | Project deployments |
| `runners` | Project/group runners |
| `triggers` | Pipeline triggers |
| `variables` | Pipeline variables |
| `bridges` | Pipeline bridge jobs |
| `test_report` | Pipeline test report |

Code review:

| Table | Notes |
|---|---|
| `approvals` | MR approvals |
| `approval_state` | MR approval state |
| `changes` | MR diff changes |
| `comments` | MR comments |
| `notes` | Issue/MR notes |
| `draft_notes` | MR draft notes |

### Group tables (41 tables)

| Table | Notes |
|---|---|
| `groups` | All visible groups |
| `subgroups` | Group subgroups |
| `billable_members` | Billable group members |
| `audit_events` | Group/project audit events |
| `epics` | Group epics |
| `iterations` | Group iterations |
| `invitations` | Pending group invitations |

### Package & registry tables (12 tables)

| Table | Notes |
|---|---|
| `packages` | Project packages |
| `package_files` | Package file listings |
| `registry_repositories` | Container registry repositories |

## Example queries

```sql
-- List all visible projects
SELECT id, name, web_url
FROM gitlab.all_projects;

-- Open merge requests in a project
SELECT title, author, created_at, web_url
FROM gitlab.merge_requests
WHERE id = '12345' AND state = 'opened';

-- Recent pipeline failures
SELECT id, status, ref, created_at
FROM gitlab.pipelines
WHERE id = '12345' AND status = 'failed';

-- Issues in a group
SELECT title, state, labels, due_date
FROM gitlab.issues
WHERE id = '67890';

-- Project branches
SELECT name, merged, protected
FROM gitlab.branches
WHERE id = '12345';

-- Deployment history
SELECT environment, status, created_at
FROM gitlab.deployments
WHERE id = '12345';
```

## Quick start

```bash
# Add the source
coral source add gitlab

# Discover tables
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'gitlab'"

# Find required filters
coral sql \
  "SELECT table_name, column_name FROM coral.columns \
   WHERE schema_name = 'gitlab' AND is_required_filter = true \
   ORDER BY table_name"

# Query
coral sql \
  "SELECT name, web_url FROM gitlab.all_projects LIMIT 10"
```
