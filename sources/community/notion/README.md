# Notion (Community)

**Version:** 0.1.0
**Backend:** HTTP (Notion REST API v1)
**Tables:** 4
**Base URL:** `https://api.notion.com/v1`

Query workspace users, pages, databases, and block content from Notion via
SQL. Designed for knowledge-base analytics, content audits, and cross-source
joins with the bundled **Linear**, **GitHub**, and **Jira** sources.

## Setup

Two authentication methods are supported. An internal integration token is
the simplest option for personal or team use.

### Option 1 — Internal integration token (recommended for most users)

1. Go to [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations)
   and click **New integration**.
2. Give it a name, select your workspace, and set the capabilities to
   **Read content** and **Read user information without email** (or with email
   if you need the `email` column on `notion.users`).
3. Copy the **Internal Integration Token** (`secret_xxx`).
4. **Share your pages and databases with the integration** — go to each
   Notion page or database, click **Share**, and invite the integration by
   name. Only shared content will appear in query results.

```sh
export NOTION_TOKEN="secret_xxx"
cargo run -p coral-cli -- source add --file sources/community/notion/manifest.yaml
```

When prompted, choose **Paste internal integration token** and enter the token.

### Option 2 — OAuth (for public integrations)

1. Go to [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations)
   and create a **Public integration**.
2. Add `http://127.0.0.1:53682/oauth/callback` as an allowed redirect URI.
3. Copy the **OAuth Client ID** and **OAuth Client Secret**.

```sh
export NOTION_CLIENT_ID="<your-client-id>"
export NOTION_CLIENT_SECRET="<your-client-secret>"
cargo run -p coral-cli -- source add --file sources/community/notion/manifest.yaml
```

When prompted, choose **Connect with Notion** to complete the browser OAuth
flow.

### Verify

```sh
cargo run -p coral-cli -- sql "SELECT id, name, type FROM notion.users LIMIT 5"
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `notion.users` | Workspace members (people and bots) | — |
| `notion.databases` | Databases shared with the integration | — |
| `notion.pages` | Pages shared with the integration | — |
| `notion.blocks` | Child blocks of a page or block | `block_id` |

All tables are read-only. This source does not create, modify, or delete any
Notion content.

### `users`

Lists all workspace members visible to the integration. `type` is either
`person` or `bot`. `email` is only populated for person users when the
integration has the **Read user information with email** capability enabled.

### `databases`

Lists all databases shared with the integration, discovered via the Notion
search API. `title` is extracted from the first element of the rich text
title array in the API response. Use `id` as a reference when building
downstream queries.

### `pages`

Lists all pages shared with the integration, discovered via the Notion search
API. Use `id` as `block_id` in `notion.blocks` to retrieve page content.
`created_by_id` and `last_edited_by_id` join to `notion.users.id`.

### `blocks`

Lists direct child blocks of a page or block. Requires `block_id` — pass a
page ID from `notion.pages` or any block ID. Use `has_children` to identify
blocks with nested content; query again with that block's `id` as `block_id`
to retrieve deeper levels.

Block types include: `paragraph`, `heading_1`, `heading_2`, `heading_3`,
`to_do`, `code`, `bulleted_list_item`, `numbered_list_item`, `image`,
`divider`, and others.

## Example queries

List workspace members:

```sql
SELECT id, name, type, email
FROM notion.users
ORDER BY name
LIMIT 20;
```

List all databases the integration can see:

```sql
SELECT id, title, created_time, last_edited_time, url
FROM notion.databases
ORDER BY last_edited_time DESC
LIMIT 20;
```

Recently edited pages:

```sql
SELECT id, url, created_time, last_edited_time, archived
FROM notion.pages
WHERE archived = false
ORDER BY last_edited_time DESC
LIMIT 20;
```

Page content blocks for a known page:

```sql
SELECT id, type, has_children, created_time
FROM notion.blocks
WHERE block_id = 'your-page-id'
ORDER BY created_time
LIMIT 50;
```

Join pages with the user who last edited them:

```sql
SELECT p.url, p.last_edited_time, u.name AS last_edited_by
FROM notion.pages p
LEFT JOIN notion.users u ON p.last_edited_by_id = u.id
WHERE p.archived = false
ORDER BY p.last_edited_time DESC
LIMIT 20;
```

Cross-source: Notion users alongside Linear users:

```sql
SELECT n.name AS notion_name, n.email, l.name AS linear_name
FROM notion.users n
LEFT JOIN linear.users l ON LOWER(n.email) = LOWER(l.email)
WHERE n.type = 'person'
ORDER BY n.name
LIMIT 20;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/notion/manifest.yaml
```

Add the source and validate each table:

```sh
export NOTION_TOKEN="secret_xxx"
cargo run -p coral-cli -- source add --file sources/community/notion/manifest.yaml

# users — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, type FROM notion.users LIMIT 5"

# databases — no required filters
cargo run -p coral-cli -- sql "SELECT id, title, last_edited_time FROM notion.databases LIMIT 5"

# pages — no required filters
cargo run -p coral-cli -- sql "SELECT id, url, last_edited_time FROM notion.pages LIMIT 5"

# blocks — requires block_id (use a real page ID from notion.pages above)
cargo run -p coral-cli -- sql "SELECT id, type, has_children FROM notion.blocks WHERE block_id = 'your-page-id' LIMIT 5"
```

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'notion'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'notion' ORDER BY table_name, ordinal_position"
```

## Notes

- **Integration permissions:** Notion requires pages and databases to be
  explicitly shared with the integration. Content not shared will not appear
  in query results.
- **Internal token vs OAuth:** internal integration tokens are simpler for
  personal use; OAuth is preferred for multi-user or public integrations.
  Both use `Authorization: Bearer` and are compatible with this source.
- **`email` column:** only populated for person users when the integration
  has the **Read user information with email** capability enabled.
- **`title` on databases:** extracted from `title[0].plain_text` in the
  Notion API response. Databases without a title return null.
- **`blocks` depth:** the blocks table returns only direct children of the
  given `block_id`. For nested content, query recursively using child block
  IDs with `has_children = true`.
- **Notion-Version header:** this source uses API version `2022-06-28`,
  the current stable version.
- **Rate limits:** the Notion API enforces rate limits of approximately 3
  requests per second per integration. Reduce query frequency if you hit
  limits.

## Out of scope for v1

- Query database rows (`POST /databases/{id}/query`)
- Page properties beyond timestamps and audit fields
- Comments (`GET /comments`)
- Write operations of any kind
