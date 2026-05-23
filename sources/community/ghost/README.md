# Ghost

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `{{input.GHOST_SITE_URL}}/ghost/api/content` (default: `https://demo.ghost.io/ghost/api/content`)

> **Note:** `GHOST_SITE_URL` should be the admin/API origin of your Ghost
> instance — **not** necessarily the public domain. For Ghost(Pro) sites they
> are usually the same (e.g. `https://demo.ghost.io`). For self-hosted
> installs, use the origin visible in your Ghost Admin URL bar (e.g.
> `https://admin.your-site.com`). Do **not** include `/ghost/api/content` or a
> trailing slash.

Query public posts, pages, tags, and authors from any Ghost CMS site via the Ghost Content API.

## Authentication

Requires a Content API key (`GHOST_CONTENT_API_KEY`). You can generate one from your Ghost Admin dashboard:
1. Go to **Settings → Integrations → Custom Integration**.
2. Click **Add custom integration**.
3. Copy the **Content API Key**.

For testing, you can use the public Ghost demo credentials:
* **GHOST_SITE_URL**: `https://demo.ghost.io`
* **GHOST_CONTENT_API_KEY**: `22444f78447824223cefc48062`

```bash
GHOST_CONTENT_API_KEY=22444f78447824223cefc48062 \
  coral source add --file sources/community/ghost/manifest.yaml
```

Run the command from the repository root. Or set `GHOST_SITE_URL` for custom self-hosted sites:

```bash
GHOST_SITE_URL=https://admin.your-site.com GHOST_CONTENT_API_KEY=your_key_here \
  coral source add --file sources/community/ghost/manifest.yaml
```

To add it interactively:

```bash
coral source add --file sources/community/ghost/manifest.yaml --interactive
```

## Tables

| Table | Description | Filters | Default ordering | Relation data included |
|---|---|---|---|---|
| `posts` | Public posts from the Ghost site. | `nql_filter`, `nql_order` | `published_at DESC` | `tags`, `authors` |
| `pages` | Public pages from the Ghost site. | `nql_filter`, `nql_order` | `title ASC` | `tags`, `authors` |
| `tags` | Public tags with at least one published post. Internal tags excluded. | — | — | — |
| `authors` | Authors who have published content on the site. | — | — | — |

> **Filters:** `posts` and `pages` support optional `nql_filter` and `nql_order` parameters. Use `nql_filter` with Ghost NQL syntax to narrow results server-side (e.g., `WHERE nql_filter = 'tag:news'`). Use `nql_order` to change the sort order (e.g., `WHERE nql_order = 'published_at ASC'`). These map to Ghost's `filter` and `order` API query parameters.

## Quick Start

```sql
-- Retrieve the latest 5 posts with their publication date
coral sql "SELECT title, published_at, url FROM ghost.posts ORDER BY published_at DESC LIMIT 5"

-- Retrieve all static pages
coral sql "SELECT title, url FROM ghost.pages"

-- Find the most popular tags by post count
coral sql "SELECT name, post_count FROM ghost.tags ORDER BY post_count DESC"

-- List all authors and their public post count
coral sql "SELECT name, location, post_count FROM ghost.authors ORDER BY post_count DESC"
```

## Advanced Queries

### Server-side NQL Filtering

Ghost's Content API supports a powerful NQL (Node Query Language) filter syntax. Coral passes `nql_filter` and `nql_order` values directly to the Ghost API so filtering happens server-side.

```sql
-- Posts tagged "getting-started"
coral sql "SELECT title, url FROM ghost.posts WHERE nql_filter = 'tag:getting-started' LIMIT 5"

-- Featured posts only
coral sql "SELECT title, featured, url FROM ghost.posts WHERE nql_filter = 'featured:true' LIMIT 5"

-- Posts ordered by oldest first
coral sql "SELECT title, published_at FROM ghost.posts WHERE nql_order = 'published_at ASC' LIMIT 5"

-- Combine filter and order
coral sql "SELECT title, published_at FROM ghost.posts WHERE nql_filter = 'featured:true' AND nql_order = 'published_at ASC' LIMIT 5"
```

### Querying Nested Relations

Ghost `posts` and `pages` return nested lists of tags and authors as structured JSON. You can query or extract them:

```sql
-- List posts and extract their first tag name
coral sql "
  SELECT 
    title,
    json_get_str(tags, 0, 'name') as primary_tag 
  FROM ghost.posts 
  LIMIT 5
"
```

### Full JSON Raw Payload

Every table includes a `raw` JSON column that contains the complete response payload from the Ghost API, allowing access to any fields not explicitly mapped to top-level columns:

```sql
-- Access custom fields or other API metadata
coral sql "SELECT json_get_str(raw, 'comment_id') as comment_id FROM ghost.posts LIMIT 5"
```
