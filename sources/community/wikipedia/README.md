# Wikipedia

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://en.wikipedia.org`

Query Wikipedia articles via the public MediaWiki and REST APIs. Search articles by keyword, look up page summaries by title, and discover random articles. No authentication required.

## Authentication

No authentication required. Wikipedia exposes public read-only APIs. Wikimedia recommends that all clients identify themselves via a `User-Agent` header (or the browser-friendly `Api-User-Agent` alternative); Coral sets `Api-User-Agent` automatically on every request.

```bash
coral source add --file sources/community/wikipedia/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `search` | Search Wikipedia articles by keyword | `query` (required) |
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
```
```text
+------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| title                                    | snippet                                                                                                                                                                                                                                                  | wordcount |
+------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| Rust (programming language)              | <span class="searchmatch">Rust</span> is a general-purpose <span class="searchmatch">programming</span> <span class="searchmatch">language</span> which emphasizes performance, type safety, concurrency, and memory safety. <span class="searchmatch">Rust</span> supports multiple programming | 10559     |
| Outline of the Rust programming language | topical guide to <span class="searchmatch">Rust</span>: <span class="searchmatch">Rust</span> is a multi-paradigm <span class="searchmatch">programming</span> <span class="searchmatch">language</span> emphasizing performance, memory safety, and concurrency. <span class="searchmatch">Rust</span> was initially developed | 991       |
| List of programming languages by type    | Raku Red Ruby <span class="searchmatch">Rust</span> Scheme SequenceL Smalltalk Source TREE-META Wolfram Mathematica (Wolfram <span class="searchmatch">language</span>) Zig Modular <span class="searchmatch">programming</span> is a <span class="searchmatch">programming</span> paradigm of | 6802      |
| Rust syntax                              | functional <span class="searchmatch">programming</span> <span class="searchmatch">languages</span> such as OCaml. Although <span class="searchmatch">Rust</span> syntax is heavily influenced by the syntaxes of C and C++, the syntax of <span class="searchmatch">Rust</span> is far more | 4777      |
| Functional programming                   | functional <span class="searchmatch">programming</span> is a <span class="searchmatch">programming</span> paradigm where <span class="searchmatch">programs</span> are constructed by applying and composing functions. It is a declarative <span class="searchmatch">programming</span> paradigm | 8758      |
+------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
```

```bash
# Look up a specific article by title
coral sql "
SELECT title, description, extract, content_url
FROM wikipedia.page
WHERE title = 'Rust (programming language)'
"
```
```text
+-----------------------------+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------+
| title                       | description                          | extract                                                                                                                   | content_url                                               |
+-----------------------------+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------+
| Rust (programming language) | General-purpose programming language | Rust is a general-purpose programming language which emphasizes performance, type safety, concurrency, and memory safety. | https://en.wikipedia.org/wiki/Rust_(programming_language) |
+-----------------------------+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------+
```

```bash
# Get a random article
coral sql "
SELECT title, description, extract
FROM wikipedia.random
"
```
```text
+---------------------------------------+------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| title                                 | description                        | extract                                                                                                                                                                                                                                                                                                                                                                            |
+---------------------------------------+------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Upper Madawaska River Provincial Park | Provincial park in Ontario, Canada | Upper Madawaska River Provincial Park is a waterway-class provincial park on the Madawaska River in the municipality of South Algonquin in Nipissing District, Ontario, Canada. The park consists of a strip of land along both shores of the Madawaska River from the communities of Whitney to Madawaska. It is upstream and north of the Lower Madawaska River Provincial Park. |
+---------------------------------------+------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

```bash
# Step 1 — find the exact article title by searching
coral sql "
SELECT title
FROM wikipedia.search
WHERE query = 'Machine learning'
LIMIT 1
"
```
```text
+------------------+
| title            |
+------------------+
| Machine learning |
+------------------+
```

```bash
# Step 2 — use that exact title to fetch the full summary
coral sql "
SELECT titles_normalized, description, extract, content_url
FROM wikipedia.page
WHERE title = 'Machine learning'
"
```
```text
+-------------------+-----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+
| titles_normalized | description                       | extract                                                                                                                                                                                                                                                                                                                                                                                                                                  | content_url                                    |
+-------------------+-----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+
| Machine learning  | Subset of artificial intelligence | Machine learning (ML) is a field of study in artificial intelligence concerned with the development and study of statistical algorithms that can learn from data and generalize to unseen data, and thus perform tasks without being explicitly programmed. Advances in the field of deep learning have allowed neural networks, a class of statistical algorithms, to surpass many previous machine learning approaches in performance. | https://en.wikipedia.org/wiki/Machine_learning |
+-------------------+-----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+
```

```bash
# Search with a larger result set
coral sql "
SELECT title, wordcount, timestamp
FROM wikipedia.search
WHERE query = 'Artificial intelligence'
LIMIT 20
"
```
```text
+------------------------------------------------------------+-----------+----------------------+
| title                                                      | wordcount | timestamp            |
+------------------------------------------------------------+-----------+----------------------+
| Artificial intelligence                                    | 26884     | 2026-05-21T22:32:25Z |
| A.I. Artificial Intelligence                               | 6396      | 2026-05-23T14:21:39Z |
| Artificial general intelligence                            | 13511     | 2026-05-23T17:54:54Z |
| History of artificial intelligence                         | 19454     | 2026-05-03T14:26:13Z |
| Hallucination (artificial intelligence)                    | 9732      | 2026-05-21T21:31:50Z |
| Generative AI                                              | 11906     | 2026-05-25T03:39:54Z |
| Applications of artificial intelligence                    | 17707     | 2026-05-25T15:03:25Z |
| Glossary of artificial intelligence                        | 30748     | 2026-05-15T05:11:54Z |
| Association for the Advancement of Artificial Intelligence | 881       | 2026-04-25T20:00:33Z |
| Existential risk from artificial intelligence              | 12630     | 2026-05-24T01:01:09Z |
| Artificial Intelligence Act                                | 5149      | 2026-03-23T06:52:57Z |
| Artificial intelligence controversies                      | 4569      | 2026-05-19T19:37:45Z |
| Progress in artificial intelligence                        | 5976      | 2026-05-19T00:52:18Z |
| List of artificial intelligence companies                  | 224       | 2026-05-25T03:27:49Z |
| Artificial intelligence in healthcare                      | 14967     | 2026-05-24T02:42:35Z |
| Artificial intelligence in video games                     | 7819      | 2026-05-18T15:08:36Z |
| 2025 in artificial intelligence                            | 785       | 2026-05-11T20:12:18Z |
| Large language model                                       | 14094     | 2026-05-21T02:19:21Z |
| Ethics of artificial intelligence                          | 16204     | 2026-05-21T22:00:35Z |
| Artificial intelligence in music                           | 6910      | 2026-05-18T08:35:05Z |
+------------------------------------------------------------+-----------+----------------------+
```

```bash
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
| `timestamp` | Utf8 | Last edit timestamp |
| `query` | Utf8 | Echoes the search query used |

### `page`

| Column | Type | Description |
|---|---|---|
| `type` | Utf8 | Page type (standard, disambiguation, no-extract) |
| `title` | Utf8 | Article title (compatibility alias; prefer `titles_normalized`) |
| `displaytitle` | Utf8 | Formatted display title (compatibility alias; prefer `titles_display`) |
| `titles_canonical` | Utf8 | Canonical title with underscores (e.g. `Rust_(programming_language)`) |
| `titles_normalized` | Utf8 | Normalized title with spaces (e.g. `Rust (programming language)`) |
| `titles_display` | Utf8 | Formatted display title, may include HTML (e.g. italics) |
| `pageid` | Int64 | Wikipedia page ID |
| `lang` | Utf8 | Language code (e.g. en) |
| `dir` | Utf8 | Text direction (ltr or rtl) |
| `revision` | Utf8 | Current revision ID |
| `timestamp` | Utf8 | Last modified timestamp |
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
- The `random` table returns exactly **one** random article per query. The underlying REST endpoint (`/api/rest_v1/page/random/summary`) does not support fetching multiple random articles in a single request; run multiple queries to get multiple random articles.
- All APIs are rate-limited by Wikipedia. Add explicit `LIMIT` clauses to keep requests reasonable.
- This source targets the English Wikipedia (`en.wikipedia.org`). To query other language editions, fork the source and change `base_url`.
- The `title` and `displaytitle` columns in `page` and `random` are kept for backwards compatibility but are considered deprecated by the Wikipedia REST API. Prefer `titles_normalized` (plain text with spaces), `titles_canonical` (with underscores), and `titles_display` (may include HTML formatting).
