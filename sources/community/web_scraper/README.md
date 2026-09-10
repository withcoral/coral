# Web Scraper

**Version:** 0.1.0
**Backend:** File (JSONL)
**Tables:** 2

Query scraped web pages and discovered links from local JSONL files. Extract page content, metadata, and link graphs from any website without an API key.

## Installation

1. Run the scraper to generate JSONL files:

```bash
python3 sources/community/web_scraper/scripts/scrape.py https://example.com https://example.com/about
```

2. Install the source:

```bash
coral source add --file sources/community/web_scraper/manifest.yaml
```

## Prerequisites

**Required:**

```bash
pip install requests beautifulsoup4 lxml
```

**Optional (for JS-rendered pages):**

```bash
pip install playwright && playwright install chromium
```

| Dependency | Purpose |
|------------|---------|
| `requests` | HTTP requests with sessions, cookies, redirects |
| `beautifulsoup4` | HTML parsing and content extraction |
| `lxml` | Fast, robust HTML parser backend for BeautifulSoup |
| `playwright` | (Optional) JavaScript rendering for SPAs via `--js` flag |

## Quick Start

```sql
-- List all scraped pages
SELECT url, title, status_code, scraped_at
FROM web_scraper.pages;

-- Get page text content
SELECT url, title, text
FROM web_scraper.pages
WHERE status_code = 200;

-- Find external links
SELECT source_url, href, text
FROM web_scraper.links
WHERE is_external = true
LIMIT 10;

-- Count links per page
SELECT source_url, COUNT(*) as link_count
FROM web_scraper.links
GROUP BY source_url
ORDER BY link_count DESC;

-- Find all links to a specific domain
SELECT source_url, href, text
FROM web_scraper.links
WHERE href LIKE '%github.com%';
```

## Scraper Usage

```bash
# Scrape specific URLs
python3 sources/community/web_scraper/scripts/scrape.py https://example.com https://example.com/about

# Scrape URLs from a file (one per line)
python3 sources/community/web_scraper/scripts/scrape.py --file urls.txt

# JS-rendered pages (requires Playwright)
python3 sources/community/web_scraper/scripts/scrape.py --js https://spa-site.com

# Set request timeout (default 30s)
python3 sources/community/web_scraper/scripts/scrape.py --timeout 60 https://example.com
```

Default output directory: `~/.coral/web_scraper/`

The scraper writes to temporary files and atomically replaces the output only after a full successful run. If any URL fails, the scraper exits with a non-zero status code.

**Note:** The `--output` flag changes where JSONL files are written, but the manifest hardcodes `file://~/.coral/web_scraper/`. If you use a custom output path, update `source.location` in the manifest to match.

The scraper auto-prepends `https://` if no scheme is provided.

## Tables

### `pages`

Scraped web pages with extracted text, title, description, and metadata. One row per URL.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `url` | Utf8 | Original URL that was requested |
| `final_url` | Utf8 | Final URL after redirects |
| `title` | Utf8 | Page title from HTML |
| `description` | Utf8 | Page description from meta tag |
| `text` | Utf8 | Extracted plain text content (scripts and styles removed) |
| `status_code` | Int64 | HTTP status code |
| `content_type` | Utf8 | Content-Type header value |
| `language` | Utf8 | Language from the HTML lang attribute |
| `scraped_at` | Utf8 | Timestamp when the page was scraped (ISO 8601 UTC) |

---

### `links`

Links discovered on scraped pages. One row per anchor tag with an href attribute.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `source_url` | Utf8 | URL of the page where the link was found |
| `href` | Utf8 | Absolute URL the link points to |
| `text` | Utf8 | Anchor text of the link |
| `is_external` | Boolean | Whether the link points to a different domain |

## Source scope

- File-backed source reading from `~/.coral/web_scraper/pages.jsonl` and `~/.coral/web_scraper/links.jsonl`.
- No API key, no credentials, no rate limits.
- The scraper uses `requests` + `beautifulsoup4` + `lxml` for robust HTML parsing.
- Optional `--js` flag uses Playwright for JavaScript-rendered pages.
- Data is static — re-run the scraper to refresh.
- The scraper strips `<script>`, `<style>`, and `<noscript>` tags before extracting text.
- Links are resolved to absolute URLs. Fragment-only, `javascript:`, `mailto:`, and `tel:` hrefs are excluded.
- 2 declared test queries (pages + links) are source-independent.

## Limitations

- Without `--js`, the scraper does not execute JavaScript. JS-rendered SPAs will return empty or partial content. Use `--js` for JS-heavy sites (requires Playwright), or use the Firecrawl source.
- No built-in crawling — you must provide the list of URLs to scrape. The scraper does not follow links automatically.
- No proxy or anti-bot evasion. Sites with aggressive bot detection may block requests.
- The `text` column may contain navigation, header, and footer text. The scraper does not isolate main content.
- Duplicate links are not deduplicated — if a page has the same link twice, it appears twice in `links.jsonl`.

## Live validation output

Validated by scraping `https://withcoral.com` and `https://withcoral.com/docs`.

```bash
$ python3 sources/community/web_scraper/scripts/scrape.py https://withcoral.com https://withcoral.com/docs
  → https://withcoral.com
  → https://withcoral.com/docs

  ✓ 2 pages → ~/.coral/web_scraper/pages.jsonl
  ✓ 70 links → ~/.coral/web_scraper/links.jsonl
```

```bash
$ coral source lint sources/community/web_scraper/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/web_scraper/manifest.yaml
Added source web_scraper

  ✓ web_scraper connected successfully

    web_scraper (2 tables)
    ├─ links
    └─ pages
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT url, title, status_code FROM web_scraper.pages LIMIT 3
      2 rows

    ✓ SELECT source_url, href, is_external FROM web_scraper.links LIMIT 5
      5 rows
```

**Table introspection:**

```sql
SELECT table_name, description
FROM coral.tables
WHERE schema_name = 'web_scraper'
ORDER BY table_name;
```

```text
+------------+-------------------------------------------------------------------------------------------+
| table_name | description                                                                               |
+------------+-------------------------------------------------------------------------------------------+
| links      | Links discovered on scraped pages. One row per anchor tag with an href attribute.         |
| pages      | Scraped web pages with extracted text, title, description, and metadata. One row per URL. |
+------------+-------------------------------------------------------------------------------------------+
```

**Live pages proof:**

```sql
SELECT url, title, status_code, language, scraped_at
FROM web_scraper.pages;
```

```text
+----------------------------+-------------------------------------------+-------------+----------+----------------------------------+
| url                        | title                                     | status_code | language | scraped_at                       |
+----------------------------+-------------------------------------------+-------------+----------+----------------------------------+
| https://withcoral.com      | Coral — The data engine for enterprise AI | 200         | en       | 2026-07-01T21:43:04.544563+00:00 |
| https://withcoral.com/docs | Introduction to Coral - Coral Docs        | 200         | en       | 2026-07-01T21:43:05.519822+00:00 |
+----------------------------+-------------------------------------------+-------------+----------+----------------------------------+
```

**Live external links proof:**

```sql
SELECT source_url, href, text, is_external
FROM web_scraper.links
WHERE is_external = true
LIMIT 5;
```

```text
+-----------------------+----------------------------------------+------------------+-------------+
| source_url            | href                                   | text             | is_external |
+-----------------------+----------------------------------------+------------------+-------------+
| https://withcoral.com | https://github.com/withcoral/coral     | GitHub      5.1K | true        |
| https://withcoral.com | https://github.com/withcoral/coral     | GitHub      5.1K | true        |
| https://withcoral.com | https://github.com/withcoral/coral     | GitHub           | true        |
| https://withcoral.com | https://github.com/withcoral/coral     | GitHub           | true        |
| https://withcoral.com | https://linkedin.com/company/withcoral | LinkedIn         | true        |
+-----------------------+----------------------------------------+------------------+-------------+
```
