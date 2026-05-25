# Wikipedia

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://en.wikipedia.org`

Query Wikipedia articles via the public MediaWiki and REST APIs. Search articles by keyword, look up page summaries by title, and discover random articles. No authentication required.

## Authentication

No authentication required. Wikipedia exposes public read-only APIs.

```bash
coral source add --file sources/community/wikipedia/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `search` | Search Wikipedia articles by keyword | `query` (required), `limit` |
| `page` | Fetch a single article summary by exact title | `title` (required) |
| `random` | Fetch a random article summary | None |

## Quick start

```bash
# Search for articles about a topic
coral sql "
  SELECT title, snippet, wordcount
  FROM wikipedia.search
  WHERE query = 'Rust programming language'
  LIMIT 5
"

# Look up a specific article by title
coral sql "
  SELECT title, description, extract, content_url
  FROM wikipedia.page
  WHERE title = 'Rust (programming language)'
"

# Get a random article
coral sql "
  SELECT title, description, extract
  FROM wikipedia.random
  LIMIT 1
"

# Find articles and join with their full summaries
coral sql "
  SELECT s.title, p.description, p.extract
  FROM wikipedia.search s
  JOIN wikipedia.page p ON s.title = p.title
  WHERE s.query = 'Machine learning'
  LIMIT 5
"

# Search with a larger result set
coral sql "
  SELECT title, wordcount, timestamp
  FROM wikipedia.search
  WHERE query = 'Artificial intelligence' AND limit = '20'
  LIMIT 10
"

# Find the longest articles on a topic
coral sql "
  SELECT title, wordcount, size
  FROM wikipedia.search
  WHERE query = 'Python'
  ORDER BY wordcount DESC
  LIMIT 5
"
```

## Column reference

### `search`

| Column | Type | Description |
|---|---|---|
| `ns` | Int64 | MediaWiki namespace ID (0 = articles) |
| `title` | Utf8 | Article title |
| `pageid` | Int64 | Unique Wikipedia page ID |
| `size` | Int64 | Page size in bytes |
| `wordcount` | Int64 | Approximate word count |
| `snippet` | Utf8 | HTML snippet matching the query |
| `timestamp` | Timestamp | Last edit timestamp |
| `query` | Utf8 | Echoes the search query used |

### `page`

| Column | Type | Description |
|---|---|---|
| `type` | Utf8 | Page type (standard, disambiguation, no-extract) |
| `title` | Utf8 | Article title |
| `displaytitle` | Utf8 | Formatted display title |
| `pageid` | Int64 | Wikipedia page ID |
| `lang` | Utf8 | Language code (e.g. en) |
| `dir` | Utf8 | Text direction (ltr or rtl) |
| `revision` | Utf8 | Current revision ID |
| `timestamp` | Timestamp | Last modified timestamp |
| `description` | Utf8 | Short topic description |
| `extract` | Utf8 | Plain-text summary |
| `extract_html` | Utf8 | HTML summary |
| `thumbnail_source` | Utf8 | Thumbnail image URL |
| `thumbnail_width` | Int64 | Thumbnail width in pixels |
| `thumbnail_height` | Int64 | Thumbnail height in pixels |
| `content_url` | Utf8 | Desktop article URL |
| `mobile_url` | Utf8 | Mobile article URL |
| `wikibase_item` | Utf8 | Wikidata item ID (e.g. Q12345) |

### `random`

Same columns as `page`.

## Validation

```bash
coral source lint sources/community/wikipedia/manifest.yaml
# Manifest is valid

coral source add --file sources/community/wikipedia/manifest.yaml
```

Output:
```text
Added source wikipedia

  ✓ wikipedia connected successfully

    wikipedia (3 tables)
    ├─ page
    ├─ random
    └─ search
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT title, snippet FROM wikipedia.search WHERE query = 'Rust' LIMIT 1
      1 row

    ✓ SELECT title, description, extract FROM wikipedia.page WHERE title = 'Rust' LIMIT 1
      1 row

    ✓ SELECT title, description, extract FROM wikipedia.random LIMIT 1
      1 row
```

```bash
coral source test wikipedia
```

Output:
```text
  ✓ wikipedia connected successfully

    wikipedia (3 tables)
    ├─ page
    ├─ random
    └─ search
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT title, snippet FROM wikipedia.search WHERE query = 'Rust' LIMIT 1
      1 row

    ✓ SELECT title, description, extract FROM wikipedia.page WHERE title = 'Rust' LIMIT 1
      1 row

    ✓ SELECT title, description, extract FROM wikipedia.random LIMIT 1
      1 row
```

```bash
coral sql "SELECT title, snippet FROM wikipedia.search WHERE query = 'Rust' LIMIT 1"
```

Output:
```text
+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| title | snippet                                                                                                                                                                                  |
+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Rust  | <span class="searchmatch">Rust</span> is an iron oxide, a usually reddish-brown oxide formed by the reaction of iron and oxygen in the catalytic presence of water or air moisture. Rust |
+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

```bash
coral sql "SELECT title, description, extract FROM wikipedia.page WHERE title = 'Rust' LIMIT 1"
```

Output:
```text
+-------+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| title | description        | extract                                                                                                                                                                                                                                                              |
+-------+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Rust  | Type of iron oxide | Rust is an iron oxide, a usually reddish-brown oxide formed by the reaction of iron and oxygen in the catalytic presence of water or air moisture. Rust consists of hydrous iron(III) oxides (Fe2O3·nH2O) and iron(III) oxide-hydroxide (FeO(OH), Fe(OH)3), and is typically associated with the corrosion of refined iron. |
+-------+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

```bash
coral sql "SELECT title, description, extract FROM wikipedia.random LIMIT 1"
```

Output:
```text
+-------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| title             | description        | extract                                                                                                                                                                                                                               |
+-------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Pseudocaeciliidae | Family of booklice | Pseudocaeciliidae is a family of Psocodea belonging to the suborder Psocomorpha. The name stems from a superficial resemblance to the distantly related family Caeciliusidae. The family is closely related to the family Philotarsidae, both within the infraorder Philotarsetae. |
+-------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

## Notes

- The `search` table uses the MediaWiki search API. Results are ranked by relevance. The `snippet` column contains HTML highlighting; use `extract` from the `page` table for clean text.
- The `page` table requires an exact title match. Use the `search` table first to find the correct title if unsure.
- The `random` table returns one random article per request. Use `LIMIT` to control how many random articles to fetch.
- All APIs are rate-limited by Wikipedia. Add explicit `LIMIT` clauses to keep requests reasonable.
- This source targets the English Wikipedia (`en.wikipedia.org`). To query other language editions, fork the source and change `base_url`.
