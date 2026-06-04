# Shortcut (Community)

**Version:** 0.1.0
**Backend:** HTTP (Shortcut REST API v3)
**Tables:** 7
**Base URL:** `https://api.app.shortcut.com/api/v3`

Query members, workflows, epic states, epics, stories, iterations, and
objectives from Shortcut via SQL. Designed for engineering project analytics:
story cycle times, sprint velocity, epic progress, and cross-source joins
with the bundled **Linear**, **GitHub**, and **Jira** sources.

## Setup

### 1. Generate a Shortcut API token

1. Go to [https://app.shortcut.com/settings/account/api-tokens](https://app.shortcut.com/settings/account/api-tokens)
2. Click **Generate Token**, give it a name, and copy the token.

> **Security note:** Shortcut API tokens provide **complete workspace access**
> for the user who created them — read and write access to all workspace data.
> Treat the token like a password: store it in an environment variable or
> secrets manager, never in source code. For read-only access, generate the
> token as a member with the **Observer role**, which limits write permissions
> while retaining broad workspace visibility.
> See [The Observer Role](https://help.shortcut.com/hc/en-us/articles/360000413023-The-Observer-Role)
> and [API Tokens](https://help.shortcut.com/hc/en-us/articles/205701199-Shortcut-API-Tokens).

### 2. Set your token

```sh
export SHORTCUT_TOKEN="<your-api-token>"
```

### 3. Add the source

```sh
coral source add --file sources/community/shortcut/manifest.yaml
```

### 4. Verify

```sh
coral sql "SELECT id, name FROM shortcut.members LIMIT 5"
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `shortcut.members` | Workspace members | — | — |
| `shortcut.workflows` | Workspace workflows | — | — |
| `shortcut.epic_states` | Epic workflow states — join to epics.epic_state_id | — | — |
| `shortcut.epics` | Epics in the workspace | — | — |
| `shortcut.stories` | Stories via Shortcut Search API — first page only (up to 250 records, explicitly requested via page_size=250) | `query` | — |
| `shortcut.iterations` | Iterations (sprints) | — | — |
| `shortcut.objectives` | Objectives (replaces deprecated milestones) | — | — |

All tables are read-only. This source does not create, modify, or delete any
Shortcut data.

### `members`

Lists all workspace members. `email` is sourced from the nested
`profile.email_address` field. `mention_name` is the @-handle used in
Shortcut comments and descriptions.

### `workflows`

Lists all workflows in the workspace. Use `id` to join with
`shortcut.stories` on `workflow_id`. Workflow states are nested inside
each workflow object in the API response and cannot be unnested into a
separate SQL table in this version — see Out of scope for v1.

### `epic_states`

Exposes the epic workflow states from `GET /epic-workflow`. Join `id` to
`shortcut.epics.epic_state_id` to resolve epic state names. This is the
current model — prefer this over the deprecated `epics.state` string field.

| Column | Description |
|---|---|
| `id` | Epic state ID — join to `epics.epic_state_id` |
| `name` | State display name |
| `type` | State type as returned by the API: `Unstarted`, `Started`, or `Done` |

### `epics`

Lists all epics. Two state fields are available:

| Column | Status | Usage |
|---|---|---|
| `epic_state_id` | Current | Join to `epic_states.id` for state name and type |
| `state` | **Deprecated by Shortcut** | Legacy string field — may be removed in a future API version |

Use `epic_state_id` joined to `epic_states` for new queries.

### `stories`

Stories are discovered via the Shortcut Search API. The `query` filter is
required and is pushed down to the API using Shortcut search operators.

| Example | Meaning |
|---|---|
| `is:started` | Stories currently in progress |
| `type:bug` | Stories of type bug |
| `type:feature` | Stories of type feature |
| `is:completed` | Completed stories |
| `epic:my-epic` | Stories in a specific epic |
| `iteration:current` | Stories in the current iteration |

`cycle_time` is returned in seconds from story start to completion.

**Pagination limitation:** Shortcut's `StorySearchResults.next` field is a
full URL string, not a bare cursor token. Coral cannot extract a bare token
from a full URL with `cursor_query`, so `stories` returns the first page
only. The source explicitly sends `page_size=250` (the API maximum) to
maximise records per request. Full multi-page pagination is out of scope for v1.

### `iterations`

Lists all iterations (sprints). Use `status` to filter locally:
`unstarted`, `started`, or `done`.

### `objectives`

Lists all objectives. Shortcut deprecated `GET /milestones` in favour of
`GET /objectives`. Use `state` to filter locally: `to do`, `in progress`,
or `done`.

## Example queries

In-progress epics with state name (current model):

```sql
SELECT e.id, e.name, es.name AS state_name, e.started_at
FROM shortcut.epics e
JOIN shortcut.epic_states es ON e.epic_state_id = es.id
WHERE es.type = 'Started'
ORDER BY e.started_at
LIMIT 20;
```

Stories joined to their epic with epic state:

```sql
SELECT
  s.id,
  s.name,
  s.story_type,
  e.name AS epic_name,
  es.name AS epic_state
FROM shortcut.stories s
LEFT JOIN shortcut.epics e ON s.epic_id = e.id
LEFT JOIN shortcut.epic_states es ON e.epic_state_id = es.id
WHERE s.query = 'is:started'
LIMIT 20;
```

Bug stories in the current iteration:

```sql
SELECT id, name, story_type, workflow_state_id, estimate, created_at
FROM shortcut.stories
WHERE query = 'type:bug iteration:current'
ORDER BY created_at DESC
LIMIT 20;
```

All stories with cycle time for completed work:

```sql
SELECT id, name, story_type, cycle_time, completed_at
FROM shortcut.stories
WHERE query = 'is:completed'
  AND completed = true
ORDER BY completed_at DESC
LIMIT 50;
```

Current iteration stories with iteration name:

```sql
SELECT s.id, s.name, s.story_type, i.name AS iteration_name, i.status
FROM shortcut.stories s
LEFT JOIN shortcut.iterations i ON s.iteration_id = i.id
WHERE s.query = 'iteration:current'
ORDER BY s.id
LIMIT 20;
```

Cross-source: Shortcut members alongside Linear users:

```sql
SELECT sc.name AS shortcut_name, sc.email, l.name AS linear_name
FROM shortcut.members sc
LEFT JOIN linear.users l ON LOWER(sc.email) = LOWER(l.email)
WHERE sc.email IS NOT NULL
ORDER BY sc.name
LIMIT 20;
```

## Validation

Lint the manifest:

```sh
coral source lint sources/community/shortcut/manifest.yaml
```

Add the source and validate each table:

```sh
export SHORTCUT_TOKEN="<your-api-token>"
coral source add --file sources/community/shortcut/manifest.yaml

coral sql "SELECT id, name, email, role FROM shortcut.members LIMIT 5"
coral sql "SELECT id, name FROM shortcut.workflows LIMIT 5"
coral sql "SELECT id, name, type FROM shortcut.epic_states LIMIT 5"
coral sql "SELECT id, name, epic_state_id FROM shortcut.epics LIMIT 5"
coral sql "SELECT id, name, story_type, workflow_state_id FROM shortcut.stories WHERE query = 'type:bug' LIMIT 5"
coral sql "SELECT id, name, status, start_date, end_date FROM shortcut.iterations LIMIT 5"
coral sql "SELECT id, name, state, created_at FROM shortcut.objectives LIMIT 5"
```

Inspect registered tables and columns:

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'shortcut'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'shortcut' ORDER BY table_name, ordinal_position"
```

## Notes

- **Token security:** Shortcut tokens provide complete workspace access for
  the creating user. Store them in environment variables or a secrets manager.
  Use an Observer-role member to limit write exposure.
- **Rate limits:** 200 requests per minute per token; retry on 429.
- **`detail=full`** on `/search/stories` ensures `cycle_time`, `estimate`,
  and workflow metadata are included.
- **Auth header:** this source uses `Shortcut-Token: <token>` not
  `Authorization: Bearer`.
- **`email` field:** sourced from `profile.email_address` in the API
  response, not a top-level field.
- **`epic_states.type`:** values returned by the API are `Unstarted`,
  `Started`, and `Done` — use these exact strings in WHERE clauses.
- **`epics.state`:** deprecated by Shortcut — use `epic_state_id` joined
  to `epic_states` instead.
- **`stories` pagination:** first page only (up to 250 records, explicitly
  requested via `page_size=250`). Use a narrow `query` to stay within one page.
- **`query` filter on stories:** required — this table uses Shortcut's Search
  API which does not support unfiltered listing. Accepts search operators such
  as `is:started`, `type:bug`, `epic:my-epic`, and `iteration:current`.
- **`cycle_time`** is in seconds — divide by 3600 for hours or 86400 for days.
- **Workflow states:** the `GET /workflows` response embeds states as a nested
  array per workflow object. Coral cannot unnest per-parent arrays into a
  separate table, so `workflow_states` is not available in this version. Use
  `workflow_state_id` on stories to identify the state ID.

## Out of scope for v1

- `workflow_states` table (Coral cannot unnest per-parent nested arrays from a single endpoint)
- Multi-page pagination for `stories` (blocked by Shortcut returning `next` as a full URL)
- Labels table
- Groups table
- Story comments
- Write operations of any kind
