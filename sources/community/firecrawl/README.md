# Firecrawl

**Version:** 0.1.0
**Backend:** HTTP
**Functions:** 3

Query web content, search results, and site URL maps from Firecrawl. Scrape any URL for clean markdown, search the web for ranked results, and discover all URLs on a website.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/firecrawl/manifest.yaml
```

## Credentials

To use this source, you will need a Firecrawl API key.

1. Sign up at [firecrawl.dev](https://www.firecrawl.dev).
2. Navigate to [API Keys](https://www.firecrawl.dev/app/api-keys).
3. Copy your API key (starts with `fc-`).
4. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export FIRECRAWL_API_KEY="fc-your-api-key"
```

## Live request costs

Each function call performs one live `POST` request to `https://api.firecrawl.dev/v2`. Firecrawl charges per credit; refer to [firecrawl.dev/pricing](https://www.firecrawl.dev/pricing) for current rates. `scrape` costs 1 credit per page. `search` costs 2 credits per 10 results. `map` costs 1 credit per page. SQL `LIMIT` is pushed to the Firecrawl request via the page-size mechanism, controlling how many results the API returns.

## Quick Start

```sql
-- Search the web
SELECT url, title, description
FROM firecrawl.search(q => 'Coral SQL')
LIMIT 5;

-- Scrape a page and get markdown content
SELECT title, status_code, language
FROM firecrawl.scrape(url => 'https://withcoral.com');

-- Discover all URLs on a website
SELECT url, title
FROM firecrawl.map(url => 'https://withcoral.com')
LIMIT 10;

-- Search with country filter
SELECT url, title, description
FROM firecrawl.search(q => 'web scraping API', country => 'US')
LIMIT 3;

-- Map with search filter to find specific pages
SELECT url, title
FROM firecrawl.map(url => 'https://withcoral.com', search => 'blog')
LIMIT 5;
```

## Functions

### `firecrawl.search`

Search the web using Firecrawl. Pass the query as a named argument with `q => '<query>'`. Returns ranked web results.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `q` | Utf8 | (Required) Search query |
| `tbs` | Utf8 | Time-based search filter (`qdr:h`, `qdr:d`, `qdr:w`, `qdr:m`, `qdr:y`) |
| `country` | Utf8 | ISO country code for geo-targeting (default `US`) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `url` | Utf8 | URL of the search result |
| `title` | Utf8 | Title of the search result |
| `description` | Utf8 | Description snippet of the search result |

---

### `firecrawl.scrape`

Scrape a single URL and return its content as clean markdown with page metadata.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `url` | Utf8 | (Required) URL to scrape |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `markdown` | Utf8 | Page content converted to clean markdown |
| `title` | Utf8 | Page title from HTML metadata |
| `description` | Utf8 | Page description from HTML metadata |
| `source_url` | Utf8 | Original URL that was requested |
| `url` | Utf8 | Final URL of the page after redirects |
| `status_code` | Int64 | HTTP status code of the page |
| `language` | Utf8 | Language of the page from HTML metadata |

---

### `firecrawl.map`

Discover all URLs on a website. Returns discovered pages with titles and descriptions when available.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `url` | Utf8 | (Required) Base URL to discover pages from |
| `search` | Utf8 | Filter discovered URLs by relevance to this query |
| `include_subdomains` | Boolean | Include subdomains (default true) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `url` | Utf8 | Discovered URL on the website |
| `title` | Utf8 | Title of the page, if available |
| `description` | Utf8 | Description of the page, if available |

## Source scope

- Targets the Firecrawl hosted API at `https://api.firecrawl.dev/v2`.
- Requires `FIRECRAWL_API_KEY` authentication as a Bearer token.
- The `q` argument is required for `search`. The `url` argument is required for `scrape` and `map`.
- `search` defaults to 5 results via `fetch_limit_default`. SQL `LIMIT N` pushes into the Firecrawl request body via page-size (API default 10, max 100).
- `map` defaults to 100 results via `fetch_limit_default`. SQL `LIMIT N` pushes into the Firecrawl request body via page-size (default 100, max 100000).
- `scrape` always returns exactly one row per URL.
- No pagination — each function makes a single API call per SQL query.
- 1 declared test query (`search`) is source-independent.

## Limitations

- The source models `POST /scrape`, `POST /search`, and `POST /map` only. The crawl endpoint (`POST /crawl`) is async and requires polling, so it is intentionally out of scope.
- The interact endpoint (`POST /scrape/{scrapeId}/interact`) requires a session from a prior scrape and is out of scope.
- `scrape` returns markdown by default. Other output formats (HTML, screenshots, structured JSON extraction) are not exposed in this version.
- `search` returns web results only. Image and news result sources are not exposed in this version.
- `map` returns URLs discovered via sitemap and link crawling. Results depend on the target site's structure.
- Rate limits apply based on your Firecrawl plan. Free plan includes 1,000 credits/month.

## Provider docs

- Firecrawl introduction: https://docs.firecrawl.dev/introduction
- Scrape API reference: https://docs.firecrawl.dev/api-reference/endpoint/scrape
- Search API reference: https://docs.firecrawl.dev/api-reference/endpoint/search
- Map API reference: https://docs.firecrawl.dev/api-reference/endpoint/map
- API keys: https://www.firecrawl.dev/app/api-keys

## Live validation output

Validated against a live Firecrawl account with a valid `FIRECRAWL_API_KEY`.

```bash
$ coral source lint sources/community/firecrawl/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/firecrawl/manifest.yaml
Added source firecrawl

  ✓ firecrawl connected successfully

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT url, title, description FROM firecrawl.search(q => 'Coral SQL') LIMIT 2
      2 rows
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json, result_columns_json
FROM coral.table_functions
WHERE schema_name = 'firecrawl'
ORDER BY function_name;
```

```text
+---------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| function_name | kind   | arguments_json                                                                                                                                         | result_columns_json                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
+---------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| map           | table  | [{"name":"url","required":true,"values":[]},{"name":"search","required":false,"values":[]},{"name":"include_subdomains","required":false,"values":[]}] | [{"name":"url","type":"Utf8","nullable":false,"description":"Discovered URL on the website."},{"name":"title","type":"Utf8","nullable":true,"description":"Title of the page, if available."},{"name":"description","type":"Utf8","nullable":true,"description":"Description of the page, if available."}]                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| scrape        | table  | [{"name":"url","required":true,"values":[]}]                                                                                                           | [{"name":"markdown","type":"Utf8","nullable":true,"description":"Page content converted to clean markdown."},{"name":"title","type":"Utf8","nullable":true,"description":"Page title from HTML metadata."},{"name":"description","type":"Utf8","nullable":true,"description":"Page description from HTML metadata."},{"name":"source_url","type":"Utf8","nullable":true,"description":"Original URL that was requested."},{"name":"url","type":"Utf8","nullable":true,"description":"Final URL of the page after redirects."},{"name":"status_code","type":"Int64","nullable":true,"description":"HTTP status code of the page."},{"name":"language","type":"Utf8","nullable":true,"description":"Language of the page from HTML metadata."}] |
| search        | search | [{"name":"q","required":true,"values":[]},{"name":"tbs","required":false,"values":[]},{"name":"country","required":false,"values":[]}]                 | [{"name":"url","type":"Utf8","nullable":false,"description":"URL of the search result."},{"name":"title","type":"Utf8","nullable":true,"description":"Title of the search result."},{"name":"description","type":"Utf8","nullable":true,"description":"Description snippet of the search result."}]                                                                                                                                                                                                                                                                                                                                                                                                                                           |
+---------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'firecrawl'
ORDER BY key;
```

```text
+-------------------+--------+----------+--------+
| key               | kind   | required | is_set |
+-------------------+--------+----------+--------+
| FIRECRAWL_API_KEY | secret | true     | true   |
+-------------------+--------+----------+--------+
```

```bash
$ coral source test firecrawl
  ✓ firecrawl connected successfully
  Secrets: keychain
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT url, title, description FROM firecrawl.search(q => 'Coral SQL') LIMIT 2
      2 rows
```

**Live search proof:**

```sql
SELECT url, title, description
FROM firecrawl.search(q => 'Coral SQL')
LIMIT 3;
```

```text
+------------------------------------+---------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| url                                | title                                                                     | description                                                                                                                                                        |
+------------------------------------+---------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| https://github.com/withcoral/coral | withcoral/coral: One SQL interface over APIs, files, and live sources ... | Coral gives agents a local-first SQL runtime over APIs, files, and other data sources. Query it from the CLI, inspect schemas and tables, or ...                   |
| https://withcoral.com/             | Coral — The data engine for enterprise AI                                 | Coral is the most token efficient and accurate way for any agent to retrieve data from APIs, databases and internal systems. brew.                                 |
| https://github.com/linkedin/coral  | linkedin/coral: Coral is a translation, analysis, and query rewrite ...   | With multiple SQL dialects supported, Coral can be used to translate SQL statements and views defined in one dialect to equivalent ones in another dialect. It ... |
+------------------------------------+---------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

**Live scrape proof:**

```sql
SELECT title, source_url, url, status_code, language
FROM firecrawl.scrape(url => 'https://withcoral.com');
```

```text
+-------------------------------------------+-----------------------+------------------------+-------------+----------+
| title                                     | source_url            | url                    | status_code | language |
+-------------------------------------------+-----------------------+------------------------+-------------+----------+
| Coral — The data engine for enterprise AI | https://withcoral.com | https://withcoral.com/ | 200         | en       |
+-------------------------------------------+-----------------------+------------------------+-------------+----------+
```

**Live map proof:**

```sql
SELECT url, title, description
FROM firecrawl.map(url => 'https://withcoral.com')
LIMIT 5;
```

```text
+----------------------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| url                                                            | title                                                           | description                                                                                                                                                    |
+----------------------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| https://withcoral.com                                          | Coral — The data engine for enterprise AI                       | One SQL connection for your agents to get governed access to data across all of your SaaS and internal systems.                                                |
| https://withcoral.com/benchmark-results                        |                                                                 |                                                                                                                                                                |
| https://withcoral.com/blog                                     | Blog - Coral                                                    | Build an AI SRE Agent with Coral. A read-only AI SRE agent you own and control. It runs on your infrastructure, queries your telemetry, code, and incident ... |
| https://withcoral.com/blog/benchmarks                          | Benchmarking Coding Agent Data Retrieval: Claude Code is 31 ... | Coral enables AI agents and applications to query data across any API, database or file system with SQL. Today, AI agents commonly use data ...                |
| https://withcoral.com/blog/building-an-ai-sre-agent-with-coral |                                                                 |                                                                                                                                                                |
+----------------------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
