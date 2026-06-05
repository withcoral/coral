# GitHub MCP Connector

**Version:** 0.1.0
**Source:** GitHub official remote MCP server
**Backend:** MCP (Streamable HTTP, native)
**Server URL:** `https://api.githubcopilot.com/mcp/x/all/readonly`
**Surface:** 1 table + 60 functions wrapping GitHub's read-only MCP tools

This connector exposes GitHub's official remote MCP server as a Coral source.
It is separate from the bundled `github` HTTP source and keeps MCP output
intact: each table or function returns a `result` text column plus a `raw` JSON
column.

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

## Table

| Table | MCP tool | Description |
|---|---|---|
| `me` | `get_me` | Authenticated GitHub user profile |

## Functions

All functions require named arguments.

| Function | MCP tool | Required args | Description |
|---|---|---|---|
| `get_teams` | `get_teams` | none | Get teams for a GitHub user, defaulting to the authenticated user. |
| `get_team_members` | `get_team_members` | `org`, `team_slug` | Get members of a team in a GitHub organization. |
| `issue_read` | `issue_read` | `owner`, `repo`, `issue_number`, `method` | Read issue details, comments, sub-issues, or labels. |
| `list_issues` | `list_issues` | `owner`, `repo` | List issues in a repository. |
| `search_issues` | `search_issues` | `query` | Search issues using GitHub issue search syntax. |
| `list_pull_requests` | `list_pull_requests` | `owner`, `repo` | List pull requests in a repository. |
| `pull_request_read` | `pull_request_read` | `owner`, `repo`, `pull_number`, `method` | Read pull request details, diff, files, comments, reviews, or checks. |
| `search_pull_requests` | `search_pull_requests` | `query` | Search pull requests using GitHub pull request search syntax. |
| `get_repository_tree` | `get_repository_tree` | `owner`, `repo` | Get a repository tree, optionally filtered by path prefix. |
| `get_commit` | `get_commit` | `owner`, `repo`, `sha` | Get details for a repository commit. |
| `get_file_contents` | `get_file_contents` | `owner`, `repo` | Get file or directory contents from a repository. |
| `list_branches` | `list_branches` | `owner`, `repo` | List branches in a repository. |
| `list_commits` | `list_commits` | `owner`, `repo` | List repository commits. |
| `list_releases` | `list_releases` | `owner`, `repo` | List repository releases. |
| `get_latest_release` | `get_latest_release` | `owner`, `repo` | Get the latest release for a repository. |
| `get_release_by_tag` | `get_release_by_tag` | `owner`, `repo`, `tag` | Get a repository release by tag. |
| `list_tags` | `list_tags` | `owner`, `repo` | List repository tags. |
| `search_code` | `search_code` | `query` | Search code using GitHub code search syntax. |
| `search_commits` | `search_commits` | `query` | Search commits using GitHub commit search syntax. |
| `search_repositories` | `search_repositories` | `query` | Search repositories using GitHub repository search syntax. |
| `search_users` | `search_users` | `query` | Search users using GitHub user search syntax. |
| `actions_list` | `actions_list` | `method`, `owner`, `repo` | List GitHub Actions workflows, runs, jobs, or artifacts. |
| `actions_get` | `actions_get` | `method`, `owner`, `repo`, `resource_id` | Get one GitHub Actions workflow, run, job, artifact, or logs URL. |
| `get_job_logs` | `get_job_logs` | `owner`, `repo` | Get logs for one GitHub Actions job or failed jobs in a run. |
| `list_code_scanning_alerts` | `list_code_scanning_alerts` | `owner`, `repo` | List code scanning alerts in a repository. |
| `get_code_scanning_alert` | `get_code_scanning_alert` | `owner`, `repo`, `alert_number` | Get details for one code scanning alert. |
| `list_dependabot_alerts` | `list_dependabot_alerts` | `owner`, `repo` | List Dependabot alerts in a repository. |
| `get_dependabot_alert` | `get_dependabot_alert` | `owner`, `repo`, `alert_number` | Get details for one Dependabot alert. |
| `list_secret_scanning_alerts` | `list_secret_scanning_alerts` | `owner`, `repo` | List secret scanning alerts in a repository. |
| `get_secret_scanning_alert` | `get_secret_scanning_alert` | `owner`, `repo`, `alert_number` | Get details for one secret scanning alert. |
| `check_dependency_vulnerabilities` | `check_dependency_vulnerabilities` | `owner`, `repo`, `dependencies` | Check dependencies against the GitHub Security Advisory Database. |
| `list_global_security_advisories` | `list_global_security_advisories` | none | List global GitHub security advisories. |
| `get_global_security_advisory` | `get_global_security_advisory` | `ghsa_id` | Get one global GitHub security advisory. |
| `list_repository_security_advisories` | `list_repository_security_advisories` | `owner`, `repo` | List security advisories for a repository. |
| `list_org_repository_security_advisories` | `list_org_repository_security_advisories` | `org` | List repository security advisories for an organization. |
| `list_discussions` | `list_discussions` | `owner` | List GitHub Discussions for a repository or organization. |
| `get_discussion` | `get_discussion` | `owner`, `repo`, `discussion_number` | Get one GitHub Discussion. |
| `get_discussion_comments` | `get_discussion_comments` | `owner`, `repo`, `discussion_number` | Get comments for one GitHub Discussion. |
| `list_discussion_categories` | `list_discussion_categories` | `owner` | List GitHub Discussion categories for a repository or organization. |
| `list_notifications` | `list_notifications` | none | List GitHub notifications for the authenticated user. |
| `get_notification_details` | `get_notification_details` | `notification_id` | Get details for one GitHub notification. |
| `projects_list` | `projects_list` | `method`, `owner` | List GitHub Projects, project fields, items, or status updates. |
| `projects_get` | `projects_get` | `method` | Get one GitHub Project, field, item, or status update. |
| `list_gists` | `list_gists` | none | List gists for a user or the authenticated user. |
| `get_gist` | `get_gist` | `gist_id` | Get one GitHub gist by ID. |
| `list_label` | `list_label` | `owner`, `repo` | List labels in a repository. |
| `get_label` | `get_label` | `owner`, `repo`, `name` | Get one label in a repository. |
| `list_issue_types` | `list_issue_types` | `owner` | List issue types for an organization. |
| `list_repository_collaborators` | `list_repository_collaborators` | `owner`, `repo` | List collaborators in a repository. |
| `list_starred_repositories` | `list_starred_repositories` | none | List starred repositories for a user or the authenticated user. |
| `get_tag` | `get_tag` | `owner`, `repo`, `tag` | Get details for one repository tag. |
| `search_orgs` | `search_orgs` | `query` | Search organizations using GitHub organization search syntax. |
| `semantic_issues_search` | `semantic_issues_search` | `query` | Search issues semantically with natural language. |
| `semantic_issue_similarity_search` | `semantic_issue_similarity_search` | `owner`, `repo`, `issue_number` | Find issues semantically similar to one issue. |
| `github_support_docs_search` | `github_support_docs_search` | `query` | Search GitHub support documentation through the GitHub MCP server. |
| `web_search` | `web_search` | `query` | Run GitHub MCP's AI-powered web search. |
| `list_copilot_spaces` | `list_copilot_spaces` | none | List Copilot Spaces accessible to the authenticated user. |
| `get_copilot_space` | `get_copilot_space` | `owner`, `name` | Get the contents of one Copilot Space. |
| `get_copilot_job_status` | `get_copilot_job_status` | `owner`, `repo`, `id` | Get status for a GitHub Copilot coding agent job. |
| `run_secret_scanning` | `run_secret_scanning` | `owner`, `repo`, `files` | Scan raw content strings or diff hunks for secrets with GitHub MCP. |

## Examples

```sql
SELECT result
FROM github_mcp.me;

SELECT result
FROM github_mcp.search_issues(
  query => 'repo:withcoral/coral is:open label:bug'
);

SELECT result
FROM github_mcp.pull_request_read(
  owner => 'withcoral',
  repo => 'coral',
  pull_number => 1183,
  method => 'get_files'
);

SELECT result
FROM github_mcp.actions_list(
  method => 'list_workflows',
  owner => 'withcoral',
  repo => 'coral',
  per_page => 5
);

SELECT result
FROM github_mcp.list_global_security_advisories(
  ecosystem => 'rust',
  severity => 'critical'
);

SELECT result
FROM github_mcp.github_support_docs_search(
  query => 'GitHub Actions reusable workflows'
);

SELECT result
FROM github_mcp.run_secret_scanning(
  owner => 'withcoral',
  repo => 'coral',
  files => 'Cargo.toml content without secrets'
);
```

## Notes

GitHub's MCP server is optimized for agent workflows. Responses may be
structured JSON or human-readable text. Use `result` for the rendered response
and `raw` when the server returns structured data.

Coral currently sends MCP function string arguments as strings, numbers as JSON
numbers, and booleans as JSON booleans. GitHub MCP arguments that require JSON
arrays or objects are intentionally exposed in the catalog but limited until
Coral grows explicit structured function-argument support. This affects
required structured arguments such as `check_dependency_vulnerabilities`
`dependencies`, and optional structured filters such as `actions_list`
`workflow_runs_filter`.

This source intentionally uses GitHub's read-only MCP endpoint. The official
MCP server also includes write-capable tools, but those are not modeled here
because SQL queries should not unexpectedly mutate GitHub state.

GitHub's official MCP documentation:
https://github.com/github/github-mcp-server
