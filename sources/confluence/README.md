# Confluence Connector

**API Version:** v2  
**Backend:** HTTP  
**Tables:** 13  
**Base URL:** set via `CONFLUENCE_BASE_URL` (e.g. `https://acme.atlassian.net`)

## Authentication

Requires `CONFLUENCE_BASIC_AUTH`, which is the Base64 form of `email:api_token` for Confluence Cloud. Create an API token at [Atlassian API token settings](https://id.atlassian.com/manage-profile/security/api-tokens), then encode:

```bash
printf '%s' 'you@example.com:YOUR_CONFLUENCE_API_TOKEN' | base64
```

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
| `page` | Single page by ID; requires `id`; optional `body_format` |
| `blog_posts` | Blog posts list; optional `space_id`, `status`, `body_format` |
| `blog_post` | Single blog post by ID; requires `id`; optional `body_format` |
| `page_footer_comments` | Footer comments for one page; requires `page_id` |
| `page_inline_comments` | Inline comments for one page; requires `page_id` |
| `blog_post_footer_comments` | Footer comments for one blog post; requires `blog_post_id` |
| `blog_post_inline_comments` | Inline comments for one blog post; requires `blog_post_id` |
| `page_attachments` | Attachments for one page; requires `page_id` |
| `blog_post_attachments` | Attachments for one blog post; requires `blog_post_id` |
| `attachment` | Single attachment by ID; requires `id` |
| `labels` | Labels defined in Confluence |

## Body formats

`pages`, `page`, `blog_posts`, `blog_post`, and every comment table accept an optional `body_format` filter. When set, the corresponding `body_*` column is populated; when unset, body columns are null.

| `body_format`       | Column populated          |
|---------------------|---------------------------|
| `storage`           | `body_storage` (XHTML)    |
| `atlas_doc_format`  | `body_atlas_doc_format` (ADF JSON) |
| `view`              | `body_view` (rendered HTML, `page` and `blog_post` only) |

## Notes

- `confluence.blog_posts` (`GET /api/v2/blogposts`) accepts `status` values `current`, `deleted`, and `trashed`.
- `confluence.blog_posts` with `status = 'draft'` returns `400 INVALID_REQUEST_PARAMETER`.
- Use `confluence.blog_post` with a known `id` to fetch a specific draft blog post.

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
