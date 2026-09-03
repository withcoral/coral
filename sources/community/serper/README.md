# Serper

**Version:** 0.1.0
**Backend:** HTTP
**Functions:** 2

Query Google Search results and news from Serper. Returns ranked web results with titles, URLs, snippets, and positions, plus news articles with sources and dates.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/serper/manifest.yaml
```

## Credentials

To use this source, you will need a Serper API key.

1. Sign up at [serper.dev](https://serper.dev) (free, 2500 queries included).
2. Copy your API key from the dashboard.
3. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export SERPER_API_KEY="your-api-key"
```

## Live request costs

Each function call performs one live `POST` request to `https://google.serper.dev`. Serper charges per search credit; refer to [serper.dev/pricing](https://serper.dev/#pricing) for current rates. `search` and `news` each cost 1 credit per call. SQL `LIMIT` is pushed to the Serper request via `num` body param, controlling how many results the API returns.

## Quick Start

```sql
-- Google web search
SELECT title, link, snippet, position
FROM serper.search(q => 'Coral SQL')
LIMIT 5;

-- Google news search
SELECT title, link, source, date
FROM serper.news(q => 'AI agents')
LIMIT 5;

-- Search with country and language filters
SELECT title, link, snippet
FROM serper.search(q => 'machine learning', gl => 'us', hl => 'en')
LIMIT 5;

-- Search with time filter (past week)
SELECT title, link, snippet
FROM serper.search(q => 'AI news', tbs => 'qdr:w')
LIMIT 5;
```

## Functions

### `serper.search`

Search the web using Google via Serper. Returns ranked organic results.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `q` | Utf8 | (Required) Search query |
| `gl` | Utf8 | Country code for results (e.g. `us`, `gb`, `in`) |
| `hl` | Utf8 | Language code (e.g. `en`, `es`, `fr`) |
| `tbs` | Utf8 | Time filter (`qdr:h` hour, `qdr:d` day, `qdr:w` week, `qdr:m` month, `qdr:y` year) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `title` | Utf8 | Title of the search result |
| `link` | Utf8 | URL of the search result |
| `snippet` | Utf8 | Text snippet from the result |
| `position` | Int64 | Position in search results (1-indexed) |
| `date` | Utf8 | Date of the result, if available |
| `sitelinks` | Json | Sitelinks for the result (JSON array) |

---

### `serper.news`

Search Google News via Serper. Returns news articles with sources and dates.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `q` | Utf8 | (Required) News search query |
| `gl` | Utf8 | Country code for results (e.g. `us`, `gb`, `in`) |
| `hl` | Utf8 | Language code (e.g. `en`, `es`, `fr`) |
| `tbs` | Utf8 | Time filter (`qdr:h` hour, `qdr:d` day, `qdr:w` week, `qdr:m` month, `qdr:y` year) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `title` | Utf8 | Title of the news article |
| `link` | Utf8 | URL of the news article |
| `snippet` | Utf8 | Text snippet from the article |
| `date` | Utf8 | Relative date (e.g. "2 days ago") |
| `source` | Utf8 | Name of the news source |
| `image_url` | Utf8 | URL of the article's thumbnail image |
| `position` | Int64 | Position in news results (1-indexed) |

## Source scope

- Targets the Serper API at `https://google.serper.dev`.
- Requires `SERPER_API_KEY` authentication via `X-API-KEY` header.
- `search` returns Google organic web results. `news` returns Google News results.
- SQL `LIMIT N` pushes into the Serper request body via `num` param (default 10, max 100).
- Supports `gl` (country), `hl` (language), and `tbs` (time filter) arguments for geo-targeted and time-filtered results.
- No pagination — each function makes a single API call per SQL query.
- 1 declared test query (`search`) requires no filters.
- Provides read-only access to Google Search results through Serper's API.

## Limitations

- The source models `POST /search` and `POST /news` only. Image search (`POST /images`), places, maps, and scholar endpoints are not exposed in this version.
- No pagination — Serper returns a single page of results per call (max 100).
- The `date` column in search results is only populated for some results.
- News dates are relative strings (e.g. "2 days ago"), not absolute timestamps.
- Rate limits and credit usage apply based on your Serper plan. Free tier includes 2500 queries.

## Provider docs

- Serper API: https://serper.dev
- Search endpoint: https://serper.dev/playground
- API playground: https://serper.dev/playground

## Live validation output

Validated against a live Serper account with a valid `SERPER_API_KEY`.

```bash
$ coral source lint sources/community/serper/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/serper/manifest.yaml
Added source serper

  ✓ serper connected successfully

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT title, link, snippet FROM serper.search(q => 'Coral SQL') LIMIT 3
      3 rows
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json
FROM coral.table_functions
WHERE schema_name = 'serper';
```

```text
+---------------+--------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| function_name | kind   | arguments_json                                                                                                                                                               |
+---------------+--------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| news          | search | [{"name":"q","required":true,"values":[]},{"name":"gl","required":false,"values":[]},{"name":"hl","required":false,"values":[]},{"name":"tbs","required":false,"values":[]}] |
| search        | search | [{"name":"q","required":true,"values":[]},{"name":"gl","required":false,"values":[]},{"name":"hl","required":false,"values":[]},{"name":"tbs","required":false,"values":[]}] |
+---------------+--------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

**Live search proof:**

```sql
SELECT title, link, position
FROM serper.search(q => 'Coral SQL') LIMIT 3;
```

```text
+---------------------------------------------------------------------------+------------------------------------+----------+
| title                                                                     | link                               | position |
+---------------------------------------------------------------------------+------------------------------------+----------+
| withcoral/coral: One SQL interface over APIs, files, and live sources ... | https://github.com/withcoral/coral | 1        |
| Coral — The data engine for enterprise AI                                 | https://withcoral.com/             | 2        |
| GitHub - linkedin/coral: Coral is a translation, analysis, and query ...  | https://github.com/linkedin/coral  | 3        |
+---------------------------------------------------------------------------+------------------------------------+----------+
```

**Live news proof:**

```sql
SELECT title, link, source, date
FROM serper.news(q => 'AI agents') LIMIT 3;
```

```text
+-------------------------------------------------------------------------------+----------------------------------------------+---------------+--------------+
| title                                                                         | link                                         | source        | date         |
+-------------------------------------------------------------------------------+----------------------------------------------+---------------+--------------+
| An AI agent startup just let its agent run its $100M fundraise                | https://techcrunch.com/2026/07/09/...        | TechCrunch    | 2 days ago   |
| All Rise: Internet Court For AI Agents Is In Session                          | https://www.forbes.com/sites/...             | Forbes        | 1 day ago    |
| Microsoft joins Google in backing Go for AI agents                            | https://thenewstack.io/...                   | The New Stack | 18 hours ago |
+-------------------------------------------------------------------------------+----------------------------------------------+---------------+--------------+
```
