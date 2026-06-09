# GitHub MCP Connector

**Version:** 0.1.0
**Source:** GitHub official remote MCP server
**Backend:** MCP (Streamable HTTP, native)
**Server URL:** `https://api.githubcopilot.com/mcp/x/all/readonly`
**Surface:** 64 tables + 16 functions wrapping GitHub's read-only MCP tools

This connector exposes GitHub's official remote MCP server as a Coral source.
It is separate from the bundled `github` HTTP source. GitHub MCP tools that
return JSON expose typed SQL columns plus a `raw` JSON column for the full row.
Stable GitHub resources are modeled as tables with `WHERE` filters. Search,
scanning, report, and resource-content operations remain table functions.
Tools that return Markdown, diff text, reports, or embedded resources expose a
single `result` text column.

The bundled `github` source is the better fit when you need broad, stable REST
API coverage with typed table schemas. This MCP source follows GitHub's
agent-oriented MCP tool surface, including tools that do not have an equivalent
in the bundled HTTP source.

## GitHub token setup

GitHub's remote MCP server requires a valid GitHub access token in the
`Authorization` header. The MCP server itself does not issue tokens, so Coral
obtains or stores a GitHub token through the source credential setup.

Register the source interactively:

```bash
coral source add --file sources/community/github_mcp/manifest.yaml --interactive
```

When prompted for `GITHUB_MCP_ACCESS_TOKEN`, choose one of:

- **Connect with GitHub device code**: sign in with GitHub using device-code
  OAuth. This does not require you to create an OAuth app.
- **Connect with GitHub OAuth app**: use your own GitHub OAuth app. Register
  the callback URL `http://127.0.0.1:53682/oauth/callback`, then provide
  `GITHUB_MCP_OAUTH_CLIENT_ID` and `GITHUB_MCP_OAUTH_CLIENT_SECRET`.
- **Paste GitHub token**: paste a personal access token or the output of
  `gh auth token`.

The OAuth methods request these scopes:

```text
repo read:org read:user user:email gist notifications read:project security_events
```

The source uses GitHub's hosted all-toolsets read-only MCP URL:

```text
https://api.githubcopilot.com/mcp/x/all/readonly
```

GitHub documents that `/readonly` disables write tools even when enabled
toolsets contain mutation tools. The manifest exposes the live read-only tools
returned by the all-toolsets endpoint at authoring time. Individual queries can
still fail if the token, organization, repository, or account does not have
access to the corresponding GitHub feature.

## Tables

Use tables for stable GitHub resources. Required inputs are SQL `WHERE` filters; optional filters map to optional MCP tool arguments. Wrapper-style MCP tools are split into tables with fixed hidden method arguments where possible.

| Table | MCP tool | Required filters | Description |
|---|---|---|---|
| `me` | `get_me` | none | Authenticated GitHub user profile returned by the get_me tool. |
| `teams` | `get_teams` | none | Get teams for a GitHub user, defaulting to the authenticated user. |
| `team_members` | `get_team_members` | `org`, `team_slug` | Get members of a team in a GitHub organization. |
| `issue` | `issue_read` | `owner`, `repo`, `issue_number` | Read issue details, comments, sub-issues, or labels. |
| `issues` | `list_issues` | `owner`, `repo` | List issues in a repository. |
| `pull_requests` | `list_pull_requests` | `owner`, `repo` | List pull requests in a repository. |
| `repository_tree` | `get_repository_tree` | `owner`, `repo` | Get a repository tree, optionally filtered by path prefix. |
| `commit` | `get_commit` | `owner`, `repo`, `sha` | Get details for a repository commit. |
| `branches` | `list_branches` | `owner`, `repo` | List branches in a repository. |
| `commits` | `list_commits` | `owner`, `repo` | List repository commits. |
| `releases` | `list_releases` | `owner`, `repo` | List repository releases. |
| `latest_release` | `get_latest_release` | `owner`, `repo` | Get the latest release for a repository. |
| `release_by_tag` | `get_release_by_tag` | `owner`, `repo`, `tag` | Get a repository release by tag. |
| `tags` | `list_tags` | `owner`, `repo` | List repository tags. |
| `code_scanning_alerts` | `list_code_scanning_alerts` | `owner`, `repo` | List code scanning alerts in a repository. |
| `code_scanning_alert` | `get_code_scanning_alert` | `owner`, `repo`, `alert_number` | Get details for one code scanning alert. |
| `dependabot_alerts` | `list_dependabot_alerts` | `owner`, `repo` | List Dependabot alerts in a repository. |
| `dependabot_alert` | `get_dependabot_alert` | `owner`, `repo`, `alert_number` | Get details for one Dependabot alert. |
| `secret_scanning_alerts` | `list_secret_scanning_alerts` | `owner`, `repo` | List secret scanning alerts in a repository. |
| `secret_scanning_alert` | `get_secret_scanning_alert` | `owner`, `repo`, `alert_number` | Get details for one secret scanning alert. |
| `global_security_advisories` | `list_global_security_advisories` | none | List global GitHub security advisories. |
| `global_security_advisory` | `get_global_security_advisory` | `ghsa_id` | Get one global GitHub security advisory. |
| `repository_security_advisories` | `list_repository_security_advisories` | `owner`, `repo` | List security advisories for a repository. |
| `org_repository_security_advisories` | `list_org_repository_security_advisories` | `org` | List repository security advisories for an organization. |
| `discussions` | `list_discussions` | `owner` | List GitHub Discussions for a repository or organization. |
| `discussion` | `get_discussion` | `owner`, `repo`, `discussion_number` | Get one GitHub Discussion. |
| `discussion_comments` | `get_discussion_comments` | `owner`, `repo`, `discussion_number` | Get comments for one GitHub Discussion. |
| `discussion_categories` | `list_discussion_categories` | `owner` | List GitHub Discussion categories for a repository or organization. |
| `notifications` | `list_notifications` | none | List GitHub notifications for the authenticated user. |
| `notification_details` | `get_notification_details` | `notification_id` | Get details for one GitHub notification. |
| `gists` | `list_gists` | none | List gists for a user or the authenticated user. |
| `gist` | `get_gist` | `gist_id` | Get one GitHub gist by ID. |
| `labels` | `list_label` | `owner`, `repo` | List labels in a repository. |
| `label` | `get_label` | `owner`, `repo`, `name` | Get one label in a repository. |
| `issue_types` | `list_issue_types` | `owner` | List issue types for an organization. |
| `repository_collaborators` | `list_repository_collaborators` | `owner`, `repo` | List collaborators in a repository. |
| `starred_repositories` | `list_starred_repositories` | none | List starred repositories for a user or the authenticated user. |
| `tag` | `get_tag` | `owner`, `repo`, `tag` | Get details for one repository tag. |
| `copilot_spaces` | `list_copilot_spaces` | none | List Copilot Spaces accessible to the authenticated user. |
| `copilot_space` | `get_copilot_space` | `owner`, `name` | Get the contents of one Copilot Space. |
| `copilot_job_status` | `get_copilot_job_status` | `owner`, `repo`, `id` | Get status for a GitHub Copilot coding agent job. |
| `pull_request` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read pull request details, diff, files, comments, reviews, or checks. Fixed to method=get. |
| `pull_request_files` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read pull request details, diff, files, comments, reviews, or checks. Fixed to method=get_files. |
| `pull_request_review_comments` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read pull request details, diff, files, comments, reviews, or checks. Fixed to method=get_review_comments. |
| `pull_request_reviews` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read pull request details, diff, files, comments, reviews, or checks. Fixed to method=get_reviews. |
| `pull_request_comments` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read pull request details, diff, files, comments, reviews, or checks. Fixed to method=get_comments. |
| `pull_request_check_runs` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read pull request details, diff, files, comments, reviews, or checks. Fixed to method=get_check_runs. |
| `workflows` | `actions_list` | `owner`, `repo` | List GitHub Actions workflows, runs, jobs, or artifacts. Fixed to method=list_workflows. |
| `workflow_runs` | `actions_list` | `owner`, `repo` | List GitHub Actions workflows, runs, jobs, or artifacts. Fixed to method=list_workflow_runs. |
| `workflow_jobs` | `actions_list` | `owner`, `repo`, `resource_id` | List GitHub Actions workflows, runs, jobs, or artifacts. Fixed to method=list_workflow_jobs. |
| `workflow_run_artifacts` | `actions_list` | `owner`, `repo`, `resource_id` | List GitHub Actions workflows, runs, jobs, or artifacts. Fixed to method=list_workflow_run_artifacts. |
| `workflow` | `actions_get` | `owner`, `repo`, `resource_id` | Get one GitHub Actions workflow, run, job, artifact, or logs URL. Fixed to method=get_workflow. |
| `workflow_run` | `actions_get` | `owner`, `repo`, `resource_id` | Get one GitHub Actions workflow, run, job, artifact, or logs URL. Fixed to method=get_workflow_run. |
| `workflow_job` | `actions_get` | `owner`, `repo`, `resource_id` | Get one GitHub Actions workflow, run, job, artifact, or logs URL. Fixed to method=get_workflow_job. |
| `workflow_run_usage` | `actions_get` | `owner`, `repo`, `resource_id` | Get one GitHub Actions workflow, run, job, artifact, or logs URL. Fixed to method=get_workflow_run_usage. |
| `workflow_run_logs_url` | `actions_get` | `owner`, `repo`, `resource_id` | Get one GitHub Actions workflow, run, job, artifact, or logs URL. Fixed to method=get_workflow_run_logs_url. |
| `projects` | `projects_list` | `owner` | List GitHub Projects, project fields, items, or status updates. Fixed to method=list_projects. |
| `project_fields` | `projects_list` | `owner`, `project_number` | List GitHub Projects, project fields, items, or status updates. Fixed to method=list_project_fields. |
| `project_items` | `projects_list` | `owner`, `project_number` | List GitHub Projects, project fields, items, or status updates. Fixed to method=list_project_items. |
| `project_status_updates` | `projects_list` | `owner`, `project_number` | List GitHub Projects, project fields, items, or status updates. Fixed to method=list_project_status_updates. |
| `project` | `projects_get` | `owner`, `project_number` | Get one GitHub Project, field, item, or status update. Fixed to method=get_project. |
| `project_field` | `projects_get` | `field_id` | Get one GitHub Project, field, item, or status update. Fixed to method=get_project_field. |
| `project_item` | `projects_get` | `item_id` | Get one GitHub Project, field, item, or status update. Fixed to method=get_project_item. |
| `project_status_update` | `projects_get` | `status_update_id` | Get one GitHub Project, field, item, or status update. Fixed to method=get_project_status_update. |

## Functions

Use functions for search-style tools and operations whose input is naturally a
call payload, plus Markdown, report, or resource-content tools. Text-oriented
functions expose only `result`.

| Function | MCP tool | Required args | Description |
|---|---|---|---|
| `search_issues` | `search_issues` | `query` | Search issues using GitHub issue search syntax. |
| `search_pull_requests` | `search_pull_requests` | `query` | Search pull requests using GitHub pull request search syntax. |
| `search_code` | `search_code` | `query` | Search code using GitHub code search syntax. |
| `search_commits` | `search_commits` | `query` | Search commits using GitHub commit search syntax. |
| `search_repositories` | `search_repositories` | `query` | Search repositories using GitHub repository search syntax. |
| `search_users` | `search_users` | `query` | Search users using GitHub user search syntax. |
| `search_orgs` | `search_orgs` | `query` | Search organizations using GitHub organization search syntax. |
| `semantic_issues_search` | `semantic_issues_search` | `query` | Search issues semantically with natural language. |
| `semantic_issue_similarity_search` | `semantic_issue_similarity_search` | `owner`, `repo`, `issue_number` | Find issues semantically similar to one issue. |
| `web_search` | `web_search` | `query` | Run GitHub MCP's AI-powered web search. |
| `pull_request_diff` | `pull_request_read` | `owner`, `repo`, `pull_number` | Read a pull request unified diff. |
| `get_file_contents` | `get_file_contents` | `owner`, `repo` | Get file or directory contents from a repository. |
| `check_dependency_vulnerabilities` | `check_dependency_vulnerabilities` | `owner`, `repo`, `dependencies` | Check dependencies against the GitHub Security Advisory Database. |
| `github_support_docs_search` | `github_support_docs_search` | `query` | Search GitHub support documentation through the GitHub MCP server. |
| `run_secret_scanning` | `run_secret_scanning` | `owner`, `repo`, `files` | Scan raw content strings or diff hunks for secrets with GitHub MCP. |
| `get_job_logs` | `get_job_logs` | `owner`, `repo` | Get logs for one GitHub Actions job or failed jobs in a run. |

## Examples

```sql
SELECT login, name, public_repos
FROM github_mcp.me;

SELECT number, title, state, html_url
FROM github_mcp.issues
WHERE owner = 'withcoral'
  AND repo = 'coral'
  AND state = 'OPEN'
LIMIT 10;

SELECT result
FROM github_mcp.pull_request_diff(
  owner => 'withcoral',
  repo => 'coral',
  pull_number => 1183
);

SELECT name, path, state
FROM github_mcp.workflows
WHERE owner = 'withcoral'
  AND repo = 'coral'
LIMIT 5;

SELECT ghsa_id, summary, severity, published_at
FROM github_mcp.global_security_advisories
WHERE ecosystem = 'rust'
  AND severity = 'critical';

SELECT result
FROM github_mcp.github_support_docs_search(
  query => 'GitHub Actions reusable workflows'
);

SELECT num_bytes_scanned, blobs_scanned, secrets
FROM github_mcp.run_secret_scanning(
  owner => 'withcoral',
  repo => 'coral',
  files => 'Cargo.toml content without secrets'
);
```

## Notes

GitHub's MCP server is optimized for agent workflows. Its read-oriented API
tools generally return JSON serialized as MCP text content, so this source
projects stable JSON fields into typed columns and keeps the full row in `raw`.
Text-oriented relations keep only `result`.

Coral currently sends MCP function string arguments as strings, numbers as JSON
numbers, and booleans as JSON booleans. MCP table filters are declared with
explicit SQL types where numeric or boolean values matter. GitHub MCP arguments
that require JSON arrays or objects are intentionally exposed in the catalog
but limited until Coral grows explicit structured function-argument support.
This affects required structured arguments such as
`check_dependency_vulnerabilities` `dependencies`.

This source intentionally uses GitHub's read-only MCP endpoint. The official
MCP server also includes write-capable tools, but those are not modeled here
because SQL queries should not unexpectedly mutate GitHub state.

GitHub's official MCP documentation:
https://github.com/github/github-mcp-server
