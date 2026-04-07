# Jira Connector

**API Version:** v3  
**Backend:** HTTP  
**Tables:** 11  
**Base URL:** `https://your-domain.atlassian.net` (override with `JIRA_BASE_URL`)

## Authentication

Requires `JIRA_BASIC_AUTH`, which is the Base64 form of `email:api_token` for Jira Cloud.

Helper script:

```bash
./sources/jira/jira-auth.sh you@example.com
```

The script prints the value Coral expects and also accepts the token as a second argument or through `JIRA_API_TOKEN`.

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
