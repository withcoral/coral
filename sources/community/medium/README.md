# Medium Community Source

**Version:** 0.1.0  
**Backend:** HTTP  
**Tables:** 1  
**Base URL:** `https://medium.com`

Query and discover Medium articles, publications, and author metadata.

```bash
coral source add --file sources/community/medium/manifest.yaml
```

## Tables

| Table | Description |
|---|---|
| `articles` | Fetch Medium article metadata and discovery information |

---

## `articles`

Fetch Medium article related metadata.

### Columns

| Column | Type | Description |
|---|---|---|
| `title` | Utf8 | Medium article title |
| `author` | Utf8 | Author name |
| `url` | Utf8 | Medium article URL |
| `publication` | Utf8 | Medium publication name |

---

## Quick start

```bash
# Fetch Medium article data
coral sql "
  SELECT title, author, url
  FROM medium.articles
  LIMIT 10
"
```

## Notes

- Medium is widely used for technical blogs and developer content.
- Useful for article discovery and content indexing workflows.
- No authentication required.