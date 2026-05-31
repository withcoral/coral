# monday.com (Community)

**Version:** 0.1.0
**Backend:** HTTP (monday.com GraphQL API v2)
**Tables:** 2 · **Functions:** 2
**Base URL:** `https://api.monday.com`

Query monday.com boards, items, columns, and users via the GraphQL API v2.
Join with GitHub PRs, Linear issues, and PagerDuty incidents for cross-source
project management and engineering intelligence.

## Install

```bash
coral source add --file sources/community/monday/manifest.yaml
```

## Authentication and setup

Requires a monday.com personal API token.

1. In monday.com, go to **your avatar → Developers → My Access Tokens**.
2. Copy the token value.

```bash
export MONDAY_API_TOKEN=eyJhbGciOiJIUzI1NiJ9...
coral source add --file sources/community/monday/manifest.yaml
```

The token is passed directly as the `Authorization` header value — no `Bearer`
prefix.

## Tables and functions

### Tables

| Table | Description |
| --- | --- |
| `boards` | All active boards with ID, name, kind, state, and workspace |
| `users` | All non-guest account users with email, title, and role flags |

### Table functions

| Function | Required args | Description |
| --- | --- | --- |
| `board_items(board_id)` | `board_id` | Items (rows/tasks) for one board, cursor-paginated across all pages |
| `board_columns(board_id)` | `board_id` | Column definitions for one board |

Obtain `board_id` values from `monday.boards`.

## Example queries

### List all active boards

```sql
SELECT id, name, board_kind, workspace_name
FROM monday.boards
ORDER BY name
LIMIT 20;
```

### Items in a board

```sql
SELECT id, name, state, group_title, created_at
FROM monday.board_items('1234567890')
WHERE state = 'active'
ORDER BY created_at DESC
LIMIT 50;
```

### Column definitions for a board

```sql
SELECT id, title, type
FROM monday.board_columns('1234567890')
ORDER BY title;
```

### All users

```sql
SELECT id, name, email, title, is_admin, time_zone
FROM monday.users
ORDER BY name;
```

## Cross-source examples

### Engineers with open GitHub PRs

```sql
WITH open_prs AS (
  SELECT user__login, number
  FROM github.pulls
  WHERE owner = 'your-org' AND repo = 'your-repo' AND state = 'open'
)
SELECT u.name, u.email, COUNT(DISTINCT pr.number) AS open_prs
FROM monday.users u
LEFT JOIN open_prs pr
  ON LOWER(pr.user__login) = LOWER(SPLIT_PART(u.email, '@', 1))
GROUP BY u.name, u.email
ORDER BY open_prs DESC
LIMIT 20;
```

The bundled GitHub source exposes `github.pulls` (requires `owner` and `repo` filters) and the author column `user__login`.

### monday.com active items matched against Linear issues

```sql
SELECT mi.name AS monday_item, li.title AS linear_issue, li.state_type
FROM monday.board_items('BOARD_ID') mi
JOIN linear.issues li
  ON LOWER(li.title) LIKE '%' || LOWER(mi.name) || '%'
WHERE mi.state = 'active'
  AND li.state_type != 'completed'
LIMIT 20;
```

### PagerDuty incidents versus board item creation on the same day

```sql
SELECT pd.title AS incident, pd.created_at,
       COUNT(mi.id) AS items_created_same_day
FROM pagerduty.incidents pd
LEFT JOIN monday.board_items('BOARD_ID') mi
  ON SUBSTR(mi.created_at, 1, 10) = SUBSTR(pd.created_at, 1, 10)
WHERE pd.status = 'triggered'
GROUP BY pd.title, pd.created_at
ORDER BY pd.created_at DESC
LIMIT 10;
```

## Verify your install

```bash
coral source lint sources/community/monday/manifest.yaml
MONDAY_API_TOKEN=eyJhbGciOiJIUzI1NiJ9... \
  coral source add --file sources/community/monday/manifest.yaml
coral source test monday
```

A successful `coral source test monday` lists the `boards` and `users` tables
and passes the declared test queries. To exercise every surface against your
own account:

```bash
coral sql "SELECT id, name, board_kind, workspace_name FROM monday.boards LIMIT 5"
coral sql "SELECT id, name, email, is_admin FROM monday.users LIMIT 5"
coral sql "SELECT id, title, type FROM monday.board_columns('<board_id>') LIMIT 10"
coral sql "SELECT id, name, state, group_title FROM monday.board_items('<board_id>') LIMIT 10"
```

## Limitations

- **GraphQL POST only** — all requests are POST to `https://api.monday.com/v2`;
  monday.com has no REST API.
- **Page size** — `board_items` fetches 100 items per request by default (max
  500) and follows monday's `items_page` cursor automatically, so all items on
  a board are returned across pages.
- **No column values** — item column values (status labels, dates, assignees)
  are not decoded in v1; use the `raw` column and DataFusion JSON functions to
  extract them.
- **Active boards only** — the `boards` table returns `state: active` boards
  by default; archived boards are excluded.
- **Rate limits** — monday.com enforces complexity-based rate limits; avoid
  scanning large boards in tight loops.
- **API-Version header** — pinned to `2026-04`; update when a new stable
  version is released.
- Community sources are maintained separately from bundled core sources.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the linked issue
first, sign the CLA if this is your first contribution, run `make lint-sources`,
and open a focused PR titled
`feat(sources/community/monday): add monday.com community source`.
