# Google Tasks Community Source

Exposes user task lists and individual task items as standard SQL tables
using the official Google Tasks REST API v1.

## Authentication

This source uses **OAuth 2.0 authorization-code flow with PKCE** over a
random loopback redirect port, matching the Google native-app OAuth
pattern. You need a Google Cloud project with the Google Tasks API enabled.

### Required inputs

| Input | Kind | Description |
|---|---|---|
| `GOOGLE_TASKS_OAUTH_CLIENT_ID` | variable | OAuth 2.0 Client ID (Desktop app) from Google Cloud Console |
| `GOOGLE_TASKS_OAUTH_CLIENT_SECRET` | secret | OAuth 2.0 Client Secret from Google Cloud Console |
| `GOOGLE_TASKS_ACCESS_TOKEN` | secret | OAuth access token — Coral opens a browser flow automatically |

### Setup steps

1. Go to [Google Cloud Console](https://console.cloud.google.com/) →
   **APIs & Services** → **Enabled APIs** and enable the **Google Tasks API**.
2. Go to **APIs & Services** → **Credentials** → **Create Credentials** →
   **OAuth 2.0 Client ID**.
3. Choose **Desktop app** as the application type.
4. Copy the **Client ID** and **Client Secret**.
5. Add the source interactively — Coral opens a browser window for the OAuth consent flow:

```sh
coral source add --interactive --file sources/community/google_tasks/manifest.yaml
```

Coral uses authorization-code flow with PKCE and a random loopback
redirect port (`http://127.0.0.1:{random}/oauth/callback`). The
authorization URL includes `access_type=offline&prompt=consent` so
Google issues a refresh token on first consent.

If the browser cannot open (e.g. headless/WSL), Coral prints the
authorization URL. Open it manually, complete consent, then paste the
redirect URL back into the terminal.

### Required scope

| Scope | Tables |
|---|---|
| `https://www.googleapis.com/auth/tasks.readonly` | `google_tasks.task_lists`, `google_tasks.tasks` |

## Tables

### `google_tasks.task_lists`

Lists metadata for all task lists owned by or shared with the authenticated
user. Entry-point table; no required filters.

### `google_tasks.tasks`

Lists individual task items from a specific task list.

**`tasklist_id` is required** — obtain it from `google_tasks.task_lists`.

**API-level pushdown filters** (sent as query parameters to Google's API):

| Filter column | Type | API default | Description |
|---|---|---|---|
| `show_completed` | Boolean | **true** | Include completed tasks. Google returns them by default; set `false` to exclude. |
| `show_hidden` | Boolean | false | Include hidden tasks (tasks completed in Google first-party clients). Required alongside `show_completed` for full completion history. |
| `show_deleted` | Boolean | false | Include deleted tasks |
| `show_assigned` | Boolean | false | Include tasks assigned from Google Docs or Google Chat |
| `due_min` | String | — | Lower bound for due date (RFC 3339) |
| `due_max` | String | — | Upper bound for due date (RFC 3339) |
| `completed_min` | String | — | Lower bound for completion date (RFC 3339) |
| `completed_max` | String | — | Upper bound for completion date (RFC 3339) |
| `updated_min` | String | — | Lower bound for last modification date (RFC 3339) |

These filters are exposed as virtual columns so they can be used in
`WHERE` clauses.

### Assignment metadata

Tasks assigned from Google Docs or Google Chat carry `assignmentInfo`
with origin details. The following columns expose this data:

| Column | Description |
|---|---|
| `assignment_surface_type` | `DOCUMENT` (Google Docs) or `SPACE` (Google Chat) |
| `assignment_link_to_task` | Absolute link to the task in its origin surface |
| `assignment_drive_file_id` | Drive file ID when surface is DOCUMENT |
| `assignment_space` | Chat space identifier (`spaces/{space}`) when surface is SPACE |
| `assignment_info` | Full raw `assignmentInfo` JSON object |

## Rate limits

Google Tasks enforces a courtesy quota of **50,000 queries per day** per
project (see [Google Tasks usage limits](https://developers.google.com/workspace/tasks/limits)).
Both tables default to `fetch_limit_default: 100` rows per query. Use
`LIMIT` and date-range filters to keep individual queries bounded on
large task lists.

## Example queries

### 1. Discover your task list IDs

```sql
SELECT id, title, updated FROM google_tasks.task_lists;
```

### 2. Exclude completed tasks (only incomplete tasks)

Use the `show_completed = false` pushdown filter to have the API return
only incomplete tasks. Without this filter, Google returns completed tasks
by default.

```sql
SELECT id, title, due
FROM google_tasks.tasks
WHERE tasklist_id = 'YOUR_LIST_ID'
  AND show_completed = false;
```

### 3. Full completion history (visible + hidden completed tasks)

Tasks completed in first-party Google clients (web UI, mobile) become
*hidden*. To see all completed tasks, set both filters:

```sql
SELECT id, title, completed
FROM google_tasks.tasks
WHERE tasklist_id = 'YOUR_LIST_ID'
  AND show_completed = true
  AND show_hidden = true;
```

### 4. Completed tasks with list metadata join

```sql
SELECT
    l.title AS list_name,
    t.title AS task_name,
    t.completed
FROM google_tasks.tasks t
JOIN google_tasks.task_lists l ON t.tasklist_id = l.id
WHERE t.tasklist_id = 'YOUR_LIST_ID'
  AND t.show_completed = true
  AND t.show_hidden = true
  AND t.status = 'completed';
```

### 5. Tasks assigned from Google Docs or Chat

```sql
SELECT
    id,
    title,
    assignment_surface_type,
    assignment_link_to_task,
    assignment_drive_file_id
FROM google_tasks.tasks
WHERE tasklist_id = 'YOUR_LIST_ID'
  AND show_assigned = true;
```

### 6. Filter tasks by due date range

```sql
SELECT title, due
FROM google_tasks.tasks
WHERE tasklist_id = 'YOUR_LIST_ID'
  AND due_min = '2024-01-01T00:00:00Z'
  AND due_max = '2024-01-31T23:59:59Z';
```

## API reference

- [Google Tasks REST API v1](https://developers.google.com/tasks/reference/rest/v1)
- [tasks.tasklists.list](https://developers.google.com/workspace/tasks/reference/rest/v1/tasklists/list)
- [tasks.tasks.list](https://developers.google.com/workspace/tasks/reference/rest/v1/tasks/list)
- [Task resource (assignmentInfo)](https://developers.google.com/workspace/tasks/reference/rest/v1/tasks)
- [OAuth for installed apps](https://developers.google.com/identity/protocols/oauth2/native-app)
- [Usage limits](https://developers.google.com/workspace/tasks/limits)
