# Jira Connector

**API Version:** v3  
**Backend:** HTTP  
**Tables:** 11  
**Base URL:** set via `JIRA_BASE_URL` (e.g. `https://acme.atlassian.net`)

## Authentication

Requires:

- `JIRA_BASE_URL`: your Jira Cloud site URL, for example `https://acme.atlassian.net`
- `JIRA_USERNAME`: your Jira Cloud email address
- `JIRA_API_TOKEN`: your Jira Cloud API token

Create a token at [Atlassian API token settings](https://id.atlassian.com/manage-profile/security/api-tokens). Coral sends `JIRA_USERNAME` and `JIRA_API_TOKEN` as HTTP Basic auth credentials for Jira Cloud.

## Quick start

```bash
coral source add jira
coral source test jira
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'jira' ORDER BY table_name"
```

## Tables

| Table | Notes |
|---|---|
| `projects` | Visible Jira projects |
| `issues` | Issue search; requires `jql` |
| `issue_comments` | Comments for one issue; requires `issue_id_or_key` |
| `issue_worklogs` | Worklogs for one issue; requires `issue_id_or_key` |
| `project_versions` | Versions for one project; requires `project_id_or_key` |
| `project_components` | Components for one project; requires `project_id_or_key` |
| `myself` | Authenticated Jira user |
| `issue_types` | Visible issue types |
| `priorities` | Jira priorities |
| `project_categories` | Jira project categories |
| `issue_link_types` | Issue link relationship types |

## Example queries

```sql
SELECT id, key, name
FROM jira.projects
ORDER BY name;

SELECT key, summary, status_name
FROM jira.issues
WHERE jql = 'project = SCRUM ORDER BY created DESC'
LIMIT 25;

SELECT id, author_display_name, created
FROM jira.issue_comments
WHERE issue_id_or_key = 'SCRUM-1'
ORDER BY created DESC;
```

## Notes

- `jira.issues` intentionally requires bounded JQL because Jira rejects unbounded enhanced search queries.
- `issue_comments.body` and worklog comment content are exposed as raw Atlassian document JSON text today.
