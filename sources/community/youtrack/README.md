# YouTrack Connector

**Version:** 0.2.0
**Source:** Hand-authored from YouTrack REST API
**Backend:** HTTP
**Tables:** 11
**Base URL:** `{{input.YOUTRACK_BASE_URL}}/api`

## Authentication

Requires `YOUTRACK_BASE_URL` and `YOUTRACK_TOKEN` environment variables, or saved credentials via `coral source add`.

```bash
coral source add --file sources/community/youtrack/manifest.yaml
```

To rotate or update your token, run the same command again.

### Token Setup

Generate a permanent token in your YouTrack instance under **Profile** -> **Account Security** -> **Tokens**.

Permanent tokens are constrained by both the selected token scope(s) and the permissions
of the user who created them. When generating your token, you must select the scope(s)
needed for the tables you plan to query, and your account must have the matching permissions:

* **Standard Access (`YouTrack` scope)**: Required for core tables like `issues`, `comments`, `tags`, and `agile_boards`. Note that `saved_searches` will only return searches visible to the authenticated user.
* **Admin/Access Management (`YouTrack Administration` scope)**: Required for administrative and access-management tables, including `projects` (via `/api/admin/projects`), `custom_fields`, `users`, `groups`, and `roles`. Your account must also have corresponding project/access-management privileges (such as *Update Organization*, *Admin Read App*, or *Low-level Admin Read*).

*(Note: The `users`, `groups`, and `roles` tables rely on the YouTrack REST API introduced in YouTrack 2026.1+)*

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

coral source add --file sources/community/youtrack/manifest.yaml
```

```text
Added source youtrack
```

Run tests to verify connectivity:

```bash
coral source test youtrack
```

```text
  ✓ youtrack connected successfully

    youtrack (11 tables)
    ├─ agile_boards
    ├─ comments
    ├─ custom_fields
    ├─ groups
    ├─ issues
    ├─ projects
    ├─ roles
    ├─ saved_searches
    ├─ sprints
    ├─ tags
    └─ users
    Query tests
    4 declared · 4 passed · 0 failed

    ✓ SELECT id, summary FROM youtrack.issues LIMIT 5
      4 rows

    ✓ SELECT id, name FROM youtrack.agile_boards LIMIT 5
      0 rows

    ✓ SELECT id, name FROM youtrack.tags LIMIT 5
      2 rows

    ✓ SELECT id, name, query FROM youtrack.saved_searches LIMIT 5
      1 row
```

Query examples:

```bash
coral sql "SELECT login, name, email FROM youtrack.users LIMIT 3;"
```

```text
+--------------+----------------+-------------------+
| login        | name           | email             |
+--------------+----------------+-------------------+
| admin        | admin          | admin@example.com |
| guest        | guest          |                   |
| <user_login> | <display_name> | <email>           |
+--------------+----------------+-------------------+
```

```bash
coral sql "SELECT id, summary FROM youtrack.issues WHERE query = '#Unresolved' LIMIT 5;"
```

```text
+------+-----------------------------------------------------+
| id   | summary                                             |
+------+-----------------------------------------------------+
| 3-22 | Docs: Update README with setup instructions         |
| 3-21 | Security: Upgrade Jackson databind library          |
| 3-20 | Feature Request: Add caching to InventoryController |
| 3-19 | Bug: NullPointerException in PaymentController      |
+------+-----------------------------------------------------+
```

```bash
coral sql "SELECT id, name FROM youtrack.groups LIMIT 3;"
```

```text
+-----+---------------------+
| id  | name                |
+-----+---------------------+
| 4-0 | demo-project admins |
| 5-1 | demo-project Team   |
| 6-0 | Registered Users    |
+-----+---------------------+
```
