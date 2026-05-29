# Shortcut (Community)

**Version:** 0.1.0
**Backend:** HTTP (Shortcut REST API v3)
**Tables:** 6
**Base URL:** `https://api.app.shortcut.com/api/v3`

Query members, workflows, epics, stories, iterations, and milestones from
Shortcut via SQL. Designed for engineering project analytics: story cycle
times, sprint velocity, epic progress, and cross-source joins with the
bundled **Linear**, **GitHub**, and **Jira** sources.

## Setup

### 1. Generate a Shortcut API token

1. Go to https://app.shortcut.com/settings/account/api-tokens
2. Click **Generate Token**, give it a name, and copy the token.

Shortcut API tokens inherit the full permissions of the user who created
them. Depending on the associated Shortcut role, the token may allow read
or write access across the workspace outside Coral.

This source itself is read-only and never performs write operations, but
the token can still be used independently against the Shortcut API.

For least-privilege access, consider creating a dedicated Shortcut Observer
(read-only) user for Coral integrations when possible.

### 2. Set your token

```sh
export SHORTCUT_TOKEN="<your-api-token>"
```

### 3. Add the source

```sh
cargo run -p coral-cli -- source add --file sources/community/shortcut/manifest.yaml
```

### 4. Verify

```sh
cargo run -p coral-cli -- sql "SELECT id, name FROM shortcut.members LIMIT 5"
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `shortcut.members` | Workspace members | — | — |
| `shortcut.workflows` | Workspace workflows | — | — |
| `shortcut.epics` | Epics in the workspace | — | — |
| `shortcut.stories` | Stories discovered via search API | `query` | — |
| `shortcut.iterations` | Iterations (sprints) | — | — |
| `shortcut.milestones` | Milestones | — | — |

All tables are read-only. This source does not create, modify, or delete any
Shortcut data.

### `members`

Lists all workspace members. `email` is sourced from the nested
`profile.email_address` field. `mention_name` is the @-handle used in
Shortcut comments and descriptions.

### `workflows`

Lists all workflows in the workspace. Use `id` to join with
`shortcut.stories` on `workflow_id`.

### `epics`

Lists all epics in the workspace. Use `state` to filter locally:

| Value | Meaning |
|---|---|
| `to do` | Epic not yet started |
| `in progress` | Epic is in progress |
| `done` | Epic is completed |

### `stories`

Lists stories discovered via the Shortcut search API.

The required `query` filter is pushed down directly to Shortcut and accepts
Shortcut search operators:

| Example             | Meaning                          |
| ------------------- | -------------------------------- |
| `is:started`        | Stories currently in progress    |
| `type:bug`          | Stories of type bug              |
| `type:feature`      | Stories of type feature          |
| `is:completed`      | Completed stories                |
| `epic:my-epic`      | Stories in a specific epic       |
| `iteration:current` | Stories in the current iteration |

Example:

```sql
SELECT id, name, story_type
FROM shortcut.stories
WHERE query = 'type:bug iteration:current'
LIMIT 20;
```

`cycle_time` is returned in seconds from story start to completion.

Current limitation: this source currently returns only the first page of
Shortcut search results because cursor pagination is not yet implemented.
Use restrictive search queries to reduce result size.

### `iterations`

Lists all iterations (sprints). Use `status` to filter locally:

| Value | Meaning |
|---|---|
| `unstarted` | Iteration not yet started |
| `started` | Iteration is in progress |
| `done` | Iteration is completed |

### `milestones`

Lists all milestones. Use `state` to filter locally: `to do`, `in progress`,
or `done`.

## Example queries

List workspace members with email:

```sql
SELECT id, name, email, mention_name, role
FROM shortcut.members
ORDER BY name
LIMIT 20;
```

List all workflows:

```sql
SELECT id, name, description
FROM shortcut.workflows
LIMIT 10;
```

In-progress epics:

```sql
SELECT id, name, state, started_at, updated_at
FROM shortcut.epics
WHERE state = 'in progress'
ORDER BY started_at
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

Stories joined to their epic:

```sql
SELECT s.id, s.name, s.story_type, e.name AS epic_name, e.state AS epic_state
FROM shortcut.stories s
LEFT JOIN shortcut.epics e ON s.epic_id = e.id
WHERE query = 'is:started'
ORDER BY e.name, s.id
LIMIT 20;
```

Current iteration stories with member names:

```sql
SELECT s.id, s.name, s.story_type, i.name AS iteration_name, i.status
FROM shortcut.stories s
LEFT JOIN shortcut.iterations i ON s.iteration_id = i.id
WHERE query = 'iteration:current'
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
cargo run -p coral-cli -- source lint sources/community/shortcut/manifest.yaml
```

Add the source and validate each table:

```sh
export SHORTCUT_TOKEN="<your-api-token>"
cargo run -p coral-cli -- source add --file sources/community/shortcut/manifest.yaml

# members — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, email, role FROM shortcut.members LIMIT 5"

# workflows — no required filters
cargo run -p coral-cli -- sql "SELECT id, name FROM shortcut.workflows LIMIT 5"

# epics — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, state, created_at FROM shortcut.epics LIMIT 5"

# stories — required query pushdown

cargo run -p coral-cli -- sql "SELECT id, name, story_type, epic_id, created_at FROM shortcut.stories WHERE query = 'is:started' LIMIT 5"

# iterations — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, status, start_date, end_date FROM shortcut.iterations LIMIT 5"

# milestones — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, state, created_at FROM shortcut.milestones LIMIT 5"
```

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'shortcut'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'shortcut' ORDER BY table_name, ordinal_position"
```

## Notes

- **Auth header:** this source uses `Shortcut-Token: <token>` not
  `Authorization: Bearer`. Generate the token at
  https://app.shortcut.com/settings/account/api-tokens.
- **`email` field:** sourced from `profile.email_address` in the API
  response, not a top-level field.
- **`stories` pagination:** this source currently returns only the first
  page of Shortcut search results because cursor pagination is not yet
  implemented in Coral for this endpoint.
- **`query` filter on stories:** required for `shortcut.stories` because
  the underlying Shortcut `/search/stories` endpoint requires a search
  query. The filter accepts Shortcut search operators. See
  https://help.shortcut.com/hc/en-us/articles/360000046646-Search-Operators
  for the full list of operators.
- **`cycle_time`** is returned in seconds from story start to completion.
  Divide by 3600 for hours or 86400 for days.
- **`members`, `workflows`, `epics`, `iterations`, `milestones`** return
  the full workspace list with no pagination — all results are returned
  in a single response.

## Out of scope for v1

- Labels table
- Groups table
- Story comments
- Write operations of any kind
