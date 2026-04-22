# Confluence Connector

**API Version:** v2  
**Backend:** HTTP  
**Tables:** 10  
**Base URL:** `https://your-domain.atlassian.net/wiki` (override with `CONFLUENCE_BASE_URL`)

## Authentication

Requires `CONFLUENCE_BASIC_AUTH`, which is the Base64 form of `email:api_token` for Confluence Cloud.

Helper script:

```bash
./sources/confluence/confluence-auth.sh you@example.com
```

The script prints the value Coral expects and also accepts the token as a second argument or through `CONFLUENCE_API_TOKEN`.

## Quick start

```bash
coral source add confluence
coral source test confluence
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'confluence' ORDER BY table_name"
```

## Tables

| Table | Notes |
|---|---|
| `spaces` | Visible Confluence spaces |
| `pages` | Pages list; optional `space_id`, `status`, `title`, `body_format` |
| `page` | Single page by ID; requires `id` |
| `blog_posts` | Blog posts list; optional `space_id`, `status`, `body_format` |
| `page_footer_comments` | Footer comments for one page; requires `page_id` |
| `page_inline_comments` | Inline comments for one page; requires `page_id` |
| `blog_post_footer_comments` | Footer comments for one blog post; requires `blog_post_id` |
| `blog_post_inline_comments` | Inline comments for one blog post; requires `blog_post_id` |
| `page_attachments` | Attachments for one page; requires `page_id` |
| `labels` | Labels defined in Confluence |

## Body formats

`pages`, `page`, `blog_posts`, and every comment table accept an optional `body_format` filter. When set, the corresponding `body_*` column is populated; when unset, body columns are null.

| `body_format`       | Column populated          |
|---------------------|---------------------------|
| `storage`           | `body_storage` (XHTML)    |
| `atlas_doc_format`  | `body_atlas_doc_format` (ADF JSON) |
| `view`              | `body_view` (rendered HTML, `page` only) |

## Example queries

```sql
SELECT id, key, name, type
FROM confluence.spaces
ORDER BY name;

SELECT id, title, space_id, created_at
FROM confluence.pages
WHERE space_id = '123456'
ORDER BY created_at DESC
LIMIT 25;

SELECT id, page_id, version_created_at, body_storage
FROM confluence.page_footer_comments
WHERE page_id = '987654' AND body_format = 'storage'
ORDER BY version_created_at DESC;
```
