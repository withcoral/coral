# Medium Community Source

**Version:** 0.1.0  
**Backend:** HTTP  
**Tables:** 1  
**Base URL:** `https://medium.com`

Query publicly available Medium RSS feed articles and metadata through Coral.

```bash
coral source add --file sources/community/medium/manifest.yaml
```

## Tables

| Table | Description |
|---|---|
| `feed_articles` | Fetch publicly available Medium RSS feed article metadata |

---

## `feed_articles`

Fetch Medium RSS feed article metadata.

### Columns

| Column | Type | Description |
|---|---|---|
| `title` | Utf8 | Medium article title |
| `author` | Utf8 | Author name |
| `url` | Utf8 | Medium article URL |
| `publication` | Utf8 | Medium publication or category |
| `published_at` | Utf8 | Article publication timestamp |

---

## Quick start

```bash
# Fetch Medium RSS feed articles
coral sql "
  SELECT title, author, url
  FROM medium.feed_articles
  LIMIT 10
"
```

## Notes

- This source uses publicly available Medium RSS feeds.
- It does not use Medium's authenticated publishing API.
- Generic platform-wide article discovery/search is not supported.
- No authentication is required for RSS feed access.
- Useful for feed ingestion, article tracking, and content monitoring workflows.

## Limitations

- RSS feed scoped retrieval only
- Depends on publicly available Medium feeds
- Does not support authenticated publishing workflows
- Does not support unrestricted article search across Medium

## References

- https://github.com/Medium/medium-api-docs
- https://help.medium.com/hc/en-us/articles/213480228-API-Importing
