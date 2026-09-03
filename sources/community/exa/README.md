# Exa

**Version:** 0.1.0
**Backend:** HTTP
**Functions:** 1

Query AI-powered web search results from Exa. Returns ranked results with titles, URLs, published dates, and authors.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/exa/manifest.yaml
```

## Credentials

To use this source, you will need an Exa API key.

1. Sign up at [exa.ai](https://exa.ai).
2. Navigate to [API Keys](https://dashboard.exa.ai/api-keys).
3. Copy your API key.
4. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export EXA_API_KEY="your-api-key"
```

## Live request costs

Each function call performs one live `POST` request to `https://api.exa.ai`. Exa charges per request; refer to [exa.ai/pricing](https://exa.ai/pricing) for current rates. SQL `LIMIT` is pushed to the Exa request via the page-size mechanism (`numResults`), controlling how many results the API returns.

## Quick Start

```sql
-- Search the web
SELECT url, title, published_date
FROM exa.search(q => 'Coral SQL')
LIMIT 5;

-- Search for research papers
SELECT url, title, author, published_date
FROM exa.search(q => 'large language models', category => 'research paper')
LIMIT 5;

-- Search with instant mode for lowest latency
SELECT url, title
FROM exa.search(q => 'Coral SQL', type => 'instant')
LIMIT 3;
```

## Functions

### `exa.search`

Search the web using Exa's AI-powered search engine. Pass the query as a named argument with `q => '<query>'`. Returns ranked results.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `q` | Utf8 | (Required) Search query |
| `type` | Utf8 | Search mode: `auto` (default), `instant`, `fast`, `deep-lite`, `deep`, `deep-reasoning` |
| `category` | Utf8 | Category filter: `company`, `research paper`, `news`, `personal site`, `financial report`, `people` |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `url` | Utf8 | URL of the search result |
| `title` | Utf8 | Title of the search result |
| `id` | Utf8 | Unique identifier for the result |
| `published_date` | Utf8 | Published date of the page (ISO 8601) |
| `author` | Utf8 | Author of the page |

## Source scope

- Targets the Exa hosted API at `https://api.exa.ai`.
- Requires `EXA_API_KEY` authentication via `x-api-key` header.
- The `q` argument is required for `search`.
- `search` defaults to 5 results via `fetch_limit_default`. SQL `LIMIT N` pushes into the Exa request body via page-size (API default 10, max 100).
- No pagination — each function makes a single API call per SQL query.
- 1 declared test query (`search`) is source-independent.

## Limitations

- The source models `POST /search` only. The contents endpoint (`POST /contents`), answer endpoint (`POST /answer`), agent API, and websets API are intentionally out of scope.
- `search` returns basic result metadata only. Page text, highlights, and summaries require the `contents` option which is not exposed in this version.
- The `people` and `company` categories have limited filter support — `startPublishedDate`, `endPublishedDate`, `startCrawlDate`, `endCrawlDate`, and `excludeDomains` are not supported for these categories.
- Rate limits apply based on your Exa plan.

## Provider docs

- Exa introduction: https://exa.ai/docs/reference/getting-started
- Search API reference: https://exa.ai/docs/reference/search
- API keys: https://dashboard.exa.ai/api-keys

## Live validation output

Validated against a live Exa account with a valid `EXA_API_KEY`.

```bash
$ coral source lint sources/community/exa/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/exa/manifest.yaml
Added source exa

  ✓ exa connected successfully

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT url, title, published_date FROM exa.search(q => 'Coral SQL') LIMIT 2
      2 rows
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json
FROM coral.table_functions
WHERE schema_name = 'exa'
ORDER BY function_name;
```

```text
+---------------+--------+------------------------------------------------------------------------------------------------------------------------------------------+
| function_name | kind   | arguments_json                                                                                                                           |
+---------------+--------+------------------------------------------------------------------------------------------------------------------------------------------+
| search        | search | [{"name":"q","required":true,"values":[]},{"name":"type","required":false,"values":[]},{"name":"category","required":false,"values":[]}] |
+---------------+--------+------------------------------------------------------------------------------------------------------------------------------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'exa'
ORDER BY key;
```

```text
+-------------+--------+----------+--------+
| key         | kind   | required | is_set |
+-------------+--------+----------+--------+
| EXA_API_KEY | secret | true     | true   |
+-------------+--------+----------+--------+
```

```bash
$ coral source test exa
  ✓ exa connected successfully
  Secrets: keychain
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT url, title, published_date FROM exa.search(q => 'Coral SQL') LIMIT 2
      2 rows
```

**Live search proof:**

```sql
SELECT url, title, published_date
FROM exa.search(q => 'Coral SQL')
LIMIT 3;
```

```text
+------------------------------------+---------------------------------------------------------------------------+--------------------------+
| url                                | title                                                                     | published_date           |
+------------------------------------+---------------------------------------------------------------------------+--------------------------+
| https://github.com/withcoral/coral | withcoral/coral: One SQL interface over APIs, files, and live sources ... | 2026-06-27T00:28:42.026Z |
| https://withcoral.com/             | Coral — The data engine for enterprise AI                                 |                          |
| https://withcoral.com/docs         | Introduction to Coral - Coral Docs                                        |                          |
+------------------------------------+---------------------------------------------------------------------------+--------------------------+
```
