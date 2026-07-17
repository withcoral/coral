# xkcd

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** `https://xkcd.com`

Query webcomics from [xkcd](https://xkcd.com/).

```bash
coral source add --file sources/community/xkcd/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `comics` | Fetch the latest xkcd comic, or a specific comic by ID | `comic_id` (optional) |

---

### `comics`

Fetch a webcomic from xkcd. By default, it fetches the latest comic. If `comic_id` is provided, it fetches that specific comic.

#### Filters

| Filter | Type | Required | Description |
|---|---|---|---|
| `comic_id` | Int64 | No | The ID of a specific comic to fetch. |

#### Columns

| Column | Type | Description |
|---|---|---|
| `comic_id` | Int64 | The comic ID |
| `title` | Utf8 | The title of the comic |
| `safe_title` | Utf8 | A web-safe version of the title |
| `alt` | Utf8 | The hover/alt text for the comic |
| `transcript` | Utf8 | Text transcript of the comic, if available |
| `img` | Utf8 | URL to the comic image |
| `year` | Utf8 | Publication year |
| `month` | Utf8 | Publication month |
| `day` | Utf8 | Publication day |
| `link` | Utf8 | Additional link |
| `news` | Utf8 | Associated news text |

---

## Quick start

```bash
# Fetch the latest comic
coral sql "
  SELECT comic_id, title, img, year, month, day
  FROM xkcd.comics
  LIMIT 1
"

/* Sample output captured on 2026-05-26:
+----------+-------------+----------------------------------------------+------+-------+-----+
| comic_id | title       | img                                          | year | month | day |
+----------+-------------+----------------------------------------------+------+-------+-----+
| 3250     | Flag Design | https://imgs.xkcd.com/comics/flag_design.png | 2026 | 5     | 25  |
+----------+-------------+----------------------------------------------+------+-------+-----+
*/

# Fetch a specific comic by ID
coral sql "
  SELECT comic_id, title, safe_title, alt
  FROM xkcd.comics
  WHERE comic_id = 614
"

/*
+----------+------------+------------+-----------------------------------------------------------------------------------------+
| comic_id | title      | safe_title | alt                                                                                     |
+----------+------------+------------+-----------------------------------------------------------------------------------------+
| 614      | Woodpecker | Woodpecker | If you don't have an extension cord I can get that too.  Because we're friends!  Right? |
+----------+------------+------------+-----------------------------------------------------------------------------------------+
*/
```

## Links

- [xkcd API documentation](https://xkcd.com/json.html)

## Local Testing

```bash
coral source add --file sources/community/xkcd/manifest.yaml
# Added source xkcd
# 
#   ✓ xkcd connected successfully
# 
#     xkcd (1 table)
#     └─ comics
#     Query tests
#     2 declared · 2 passed · 0 failed
# 
#     ✓ SELECT * FROM xkcd.comics LIMIT 1
#       1 row
# 
#     ✓ SELECT * FROM xkcd.comics WHERE comic_id = 614 LIMIT 1
#       1 row

coral source test xkcd
#   ✓ xkcd connected successfully
# 
#     xkcd (1 table)
#     └─ comics
#     Query tests
#     2 declared · 2 passed · 0 failed
# 
#     ✓ SELECT * FROM xkcd.comics LIMIT 1
#       1 row
# 
#     ✓ SELECT * FROM xkcd.comics WHERE comic_id = 614 LIMIT 1
#       1 row

coral sql "SELECT comic_id, title FROM xkcd.comics WHERE comic_id = 614"
# +----------+------------+
# | comic_id | title      |
# +----------+------------+
# | 614      | Woodpecker |
# +----------+------------+
```
