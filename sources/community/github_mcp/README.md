# GitHub MCP Connector

**Version:** 0.1.0
**Source:** GitHub official remote MCP server
**Backend:** MCP (Streamable HTTP, native)
**Server URL:** `https://api.githubcopilot.com/mcp/x/all/readonly`
**Surface:** 1 table + 21 functions wrapping read-oriented MCP tools

This connector exposes GitHub's official remote MCP server as a Coral source.
It is separate from the bundled `github` HTTP source and keeps MCP output
intact: each table or function returns a `result` text column plus a `raw` JSON
column.

The bundled `github` source is the better fit when you need broad, stable REST
API coverage with typed table schemas. This MCP source is focused on the
agent-oriented GitHub MCP tool surface.

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
repo read:org read:user user:email
```

The source uses GitHub's hosted all-toolsets read-only MCP URL:

```text
https://api.githubcopilot.com/mcp/x/all/readonly
```

GitHub documents that `/readonly` disables write tools even when enabled
toolsets contain mutation tools. The manifest only exposes a focused subset of
read-oriented tools.

## Table

| Table | MCP tool | Description |
|---|---|---|
| `me` | `get_me` | Authenticated GitHub user profile |

## Functions

All functions require named arguments.

| Function | MCP tool | Required args | Description |
|---|---|---|---|
| `get_teams` | `get_teams` | none | Teams for a user, defaulting to the authenticated user |
| `get_team_members` | `get_team_members` | `org`, `team_slug` | Members of a GitHub team |
| `issue_read` | `issue_read` | `owner`, `repo`, `issue_number`, `method` | Read one issue, comments, sub-issues, labels, or events |
| `list_issues` | `list_issues` | `owner`, `repo` | List repository issues |
| `search_issues` | `search_issues` | `query` | Search issues |
| `list_pull_requests` | `list_pull_requests` | `owner`, `repo` | List repository pull requests |
| `pull_request_read` | `pull_request_read` | `owner`, `repo`, `pull_number`, `method` | Read one pull request, diff, files, comments, reviews, or checks |
| `search_pull_requests` | `search_pull_requests` | `query` | Search pull requests |
| `get_repository_tree` | `get_repository_tree` | `owner`, `repo` | Get a repository tree |
| `get_commit` | `get_commit` | `owner`, `repo`, `sha` | Get commit details |
| `get_file_contents` | `get_file_contents` | `owner`, `repo` | Get file or directory contents |
| `list_branches` | `list_branches` | `owner`, `repo` | List repository branches |
| `list_commits` | `list_commits` | `owner`, `repo` | List repository commits |
| `list_releases` | `list_releases` | `owner`, `repo` | List repository releases |
| `get_latest_release` | `get_latest_release` | `owner`, `repo` | Get the latest release |
| `get_release_by_tag` | `get_release_by_tag` | `owner`, `repo`, `tag` | Get a release by tag |
| `list_tags` | `list_tags` | `owner`, `repo` | List repository tags |
| `search_code` | `search_code` | `query` | Search code |
| `search_commits` | `search_commits` | `query` | Search commits |
| `search_repositories` | `search_repositories` | `query` | Search repositories |
| `search_users` | `search_users` | `query` | Search users |

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
FROM github_mcp.get_file_contents(
  owner => 'withcoral',
  repo => 'coral',
  path => 'README.md',
  ref => 'refs/heads/main'
);
```

## Notes

GitHub's MCP server is optimized for agent workflows. Responses may be
structured JSON or human-readable text. Use `result` for the rendered response
and `raw` when the server returns structured data.

This source intentionally uses GitHub's read-only MCP endpoint and exposes only
read-oriented tools. The official MCP server also includes write-capable tools,
but those are not modeled here because SQL queries should not unexpectedly
mutate GitHub state.

GitHub's official MCP documentation:
https://github.com/github/github-mcp-server
