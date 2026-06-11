# YouTrack Connector

**Version:** 0.2.0
**Source:** Hand-authored from YouTrack REST API
**Backend:** HTTP
**Tables:** 11
**Base URL:** `{{input.YOUTRACK_BASE_URL}}/api`

## Authentication

Requires `YOUTRACK_BASE_URL` and `YOUTRACK_TOKEN` environment variables, or saved credentials via `coral source add`.

```bash
coral source add youtrack --file sources/community/youtrack/manifest.yaml
```

To rotate or update your token, run the same command again.

### Token Setup

Generate a permanent token in your YouTrack instance under **Profile** -> **Account Security** -> **Tokens**. Ensure the token has the necessary read scopes for the data you intend to query (e.g., issues, projects, users).

Your `YOUTRACK_BASE_URL` should be the base path of your instance (e.g. `https://example.myjetbrains.com/youtrack`), without the `/api` suffix or a trailing slash.

## Table categories

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| No filter | 9 | `SELECT * FROM youtrack.users` |
| `issue_id` | 1 | `WHERE issue_id = '81-12'` |
| `agile_id` | 1 | `WHERE agile_id = '104-3'` |

#### No filter required (9 tables)

| Table | Description |
|---|---|
| `users` | Users in the YouTrack instance. |
| `projects` | Projects in the YouTrack instance. |
| `issues` | Issues across projects (supports `query` pushdown). |
| `tags` | Issue tags defined globally. |
| `agile_boards` | Agile boards configured in YouTrack. |
| `groups` | User permission groups. |
| `roles` | System roles. |
| `saved_searches` | User-defined search queries. |
| `custom_fields` | Global custom field definitions. |

#### Issue tables (requires `issue_id`)

| Table | Description |
|---|---|
| `comments` | Discussion threads attached to specific issues. |

#### Agile board tables (requires `agile_id`)

| Table | Description |
|---|---|
| `sprints` | Sprints/iterations within a specific agile board. |

## Quick start

```bash
# Setup (ensure you do not use surrounding quotes in Windows cmd)
export YOUTRACK_BASE_URL=https://yourinstance.myjetbrains.com/youtrack
export YOUTRACK_TOKEN=perm:your_token

coral source add youtrack --file sources/community/youtrack/manifest.yaml

# Query examples
coral sql \
  "SELECT login, name, email FROM youtrack.users LIMIT 10;"

coral sql \
  "SELECT id, summary FROM youtrack.issues WHERE query = '#Unresolved' LIMIT 5;"

coral sql \
  "SELECT id, text FROM youtrack.comments WHERE issue_id = '81-12' LIMIT 5;"
```
