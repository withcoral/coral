# Hacker News (hn)

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 8
**Base URL:** `https://hn.algolia.com/api/v1`

Query Hacker News stories, comments, users, and items via the public [Algolia HN API](https://hn.algolia.com/api). No authentication required.

```bash
coral source add --file sources/community/hn/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `search` | Full-text search ranked by relevance | `query`, `tags`, `numeric_filters` |
| `search_by_date` | Full-text search ordered newest-first | `query`, `tags`, `numeric_filters` |
| `items` | Fetch a single item by ID with full comment thread | `id` (required) |
| `users` | Fetch a user profile by username | `username` (required) |
| `front_page` | Fetch stories currently on the front page | `query`, `numeric_filters` |
| `ask_hn` | Fetch Ask HN posts (newest first) | `query`, `numeric_filters` |
| `show_hn` | Fetch Show HN posts (newest first) | `query`, `numeric_filters` |
| `jobs` | Fetch Job posts (newest first) | `query`, `numeric_filters` |

---

### `search` and `search_by_date`

Both tables expose the same columns and filters. Use `search` for relevance-ranked results and `search_by_date` for chronological (newest-first) results.

#### Filters

| Filter | Type | Description |
|---|---|---|
| `query` | string | Full-text keyword search (e.g. `rust`, `openai`) |
| `tags` | string | Algolia tag scoping — filter by type, author, or story (see below) |
| `numeric_filters` | string | Numeric constraints on points or time (e.g. `points>100`, `created_at_i>1700000000`) |

#### Tag values

| Tag | Matches |
|---|---|
| `story` | Stories only |
| `comment` | Comments only |
| `poll` | Polls only |
| `job` | Job posts only |
| `author_<username>` | Items by a specific author (e.g. `author_pg`) |
| `story_<id>` | Items under a specific story (includes the story itself — combine with `comment` for comments only) |
| `(story,poll)` | OR logic — stories or polls |

#### Columns

| Column | Type | Description |
|---|---|---|
| `object_id` | Utf8 | HN item ID (Algolia primary key) |
| `author` | Utf8 | HN username of submitter or commenter |
| `title` | Utf8 | Story title (null for comments) |
| `url` | Utf8 | Story URL (null for comments and Ask HN posts) |
| `story_text` | Utf8 | HTML body for self-posts (Ask HN, Show HN) |
| `comment_text` | Utf8 | HTML body for comment hits |
| `story_title` | Utf8 | Parent story title (for comment hits) |
| `story_url` | Utf8 | Parent story URL (for comment hits) |
| `story_id` | Int64 | Parent story ID |
| `parent_id` | Int64 | Direct parent item ID (for comment hits) |
| `points` | Int64 | Story score (null for comments) |
| `num_comments` | Int64 | Total descendant comments |
| `created_at_i` | Int64 | Creation time as Unix epoch seconds |
| `created_at` | Timestamp | Creation time as UTC timestamp |
| `tags` | Utf8 | Comma-joined Algolia tags (e.g. `story,author_pg,story_8863`) |
| `query` | Utf8 | Echoes the `query` filter used |
| `tags_filter` | Utf8 | Echoes the `tags` filter used |
| `numeric_filters` | Utf8 | Echoes the `numeric_filters` filter used |

---

### `items`

Fetch a single HN item (story, comment, poll, or job) by ID. Returns the full nested comment thread under the `children` column as JSON.

#### Filters

| Filter | Type | Required | Description |
|---|---|---|---|
| `id` | Int64 | Yes | HN item ID to fetch |

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | HN item ID |
| `type` | Utf8 | One of `story`, `comment`, `poll`, `pollopt`, `job` |
| `author` | Utf8 | Submitter username |
| `title` | Utf8 | Story title (null for comments) |
| `url` | Utf8 | Story URL |
| `text` | Utf8 | Comment or self-post HTML body |
| `points` | Int64 | Story score (null for comments) |
| `parent_id` | Int64 | Parent item ID for comments |
| `story_id` | Int64 | Top-level story ID this item belongs to |
| `children` | Json | Full nested comment thread |
| `created_at_i` | Int64 | Creation time as Unix epoch seconds |
| `created_at` | Timestamp | Creation time as UTC timestamp |

---

### `users`

Fetch a Hacker News user profile by username.

#### Filters

| Filter | Type | Required | Description |
|---|---|---|---|
| `username` | string | Yes | Exact HN username (case-sensitive) |

#### Columns

| Column | Type | Description |
|---|---|---|
| `username` | Utf8 | HN username |
| `karma` | Int64 | User karma score |
| `about` | Utf8 | Profile bio HTML, if set |

---

## Quick start

```bash
# Confirm connectivity
coral sql "SELECT * FROM hn.search_by_date WHERE query = 'rust' AND tags = 'story' LIMIT 1"

# Recent stories about a topic
coral sql "
  SELECT title, author, points, url, created_at
  FROM hn.search_by_date
  WHERE query = 'rust'
    AND tags = 'story'
  LIMIT 10
"

# High-scoring stories (relevance-ranked)
coral sql "
  SELECT title, author, points, num_comments, url
  FROM hn.search
  WHERE query = 'machine learning'
    AND tags = 'story'
    AND numeric_filters = 'points>200'
  LIMIT 10
"

# Front page stories
coral sql "
  SELECT title, author, points, url
  FROM hn.front_page
  LIMIT 10
"

# Latest Ask HN posts
coral sql "
  SELECT title, author, points, created_at
  FROM hn.ask_hn
  LIMIT 5
"

# Recent stories since a date (compute the epoch cutoff for your window)
# For example, 30 days ago: date -j -v-30d +%s  (macOS) or date -d '30 days ago' +%s (Linux)
coral sql "
  SELECT title, author, points, created_at
  FROM hn.search_by_date
  WHERE query = 'postgres'
    AND tags = 'story'
    AND numeric_filters = 'created_at_i>$(date -d "30 days ago" +%s)'
  LIMIT 10
"

# Comments on a specific story (comma = AND, so 'comment,story_8863' returns only comments)
coral sql "
  SELECT author, comment_text, created_at
  FROM hn.search_by_date
  WHERE tags = 'comment,story_8863'
  LIMIT 20
"

# Stories by a specific author (comma = AND in Algolia tag syntax)
coral sql "
  SELECT title, points, created_at, url
  FROM hn.search
  WHERE tags = 'story,author_pg'
  LIMIT 10
"

# Fetch a single item with its comment thread
coral sql "
  SELECT id, title, author, points, children
  FROM hn.items
  WHERE id = 8863
"

# Look up a user profile
coral sql "
  SELECT username, karma, about
  FROM hn.users
  WHERE username = 'pg'
"
```

## Notes

- `search` ranks by Algolia relevance score. `search_by_date` ranks by newest first. For time-bounded queries, prefer `search_by_date` with a `numeric_filters` time constraint.
- The `tags` filter uses Algolia tag syntax. Use `(story,poll)` for OR logic across types.
- `created_at_i` is Unix epoch seconds. Use it in `numeric_filters` for time-range queries (e.g. `created_at_i>1700000000`).
- The `children` column in `items` is raw JSON containing the full nested comment thread.
- `username` in `users` is case-sensitive — `pg` and `PG` are different lookups.
- No authentication or API key is required.
