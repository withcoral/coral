# Tavily

**Version:** 0.1.0
**Backend:** HTTP
**Functions:** 1

Query web search results from Tavily. The source provides a provider-native search function that returns ranked results with titles, URLs, content snippets, and relevance scores optimized for LLM consumption.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/tavily/manifest.yaml
```

## Credentials

To use this source, you will need a Tavily API key.

1. Register at [app.tavily.com](https://app.tavily.com).
2. Copy your API key (starts with `tvly-`).
3. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export TAVILY_API_KEY="tvly-your-api-key"
```

## Quick Start

```sql
-- Basic web search with provider-native ranking
SELECT url, title, score
FROM tavily.search(q => 'Coral SQL')
LIMIT 5;

-- Search with advanced depth for more comprehensive results
SELECT url, title, content, score
FROM tavily.search(q => 'Coral SQL', search_depth => 'advanced')
LIMIT 3;

-- Filter by news topic within a specific time range
SELECT url, title, published_date, score
FROM tavily.search(q => 'Coral SQL', topic => 'news', time_range => 'week')
LIMIT 5;

-- Search with favicons enabled
SELECT url, title, favicon
FROM tavily.search(q => 'Coral SQL', include_favicon => true)
LIMIT 3;
```

## Functions

### `tavily.search`
Provider-native search for the web. Pass the query as a named argument with `q => '<query>'`.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `q` | Utf8 | (Required) Search query |
| `max_results` | Int64 | Maximum number of results (default 5, max 20) |
| `search_depth` | Utf8 | Search depth: `basic`, `advanced`, `fast`, or `ultra-fast` |
| `topic` | Utf8 | Topic: `general`, `news`, or `finance` |
| `time_range` | Utf8 | Time range: `day`, `week`, `month`, `year` |
| `include_images` | Boolean | Set to `true` to include images |
| `include_raw_content` | Utf8 | Set to `true`, `'markdown'`, or `'text'` to include raw HTML content |
| `include_favicon` | Boolean | Set to `true` to include favicon URLs |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `url` | Utf8 | URL of the search result |
| `title` | Utf8 | Title of the search result |
| `content` | Utf8 | Most query-related content extracted from the source |
| `score` | Float64 | Relevance score of the search result (0 to 1) |
| `raw_content` | Utf8 | Parsed and cleaned HTML content (requires `include_raw_content => true`) |
| `published_date` | Utf8 | Publication date (only for news topic searches) |
| `images` | Json | Images extracted from the result (requires `include_images => true`) |
| `favicon` | Utf8 | Favicon URL for the result (requires `include_favicon => true`) |

## Live request costs

Calling `tavily.search` performs one live `POST /search` call per SQL query. Tavily charges per search credit; refer to <https://docs.tavily.com/documentation/api-credits> for current rates. Pass `max_results => N` to control how many results Tavily returns (default 5, max 20). `LIMIT` only caps rows after Coral receives the response.

## Source scope

- Targets Tavily's hosted API at `https://api.tavily.com`.
- Requires `TAVILY_API_KEY` authentication as a Bearer token.
- The `q` argument is required.
- `fetch_limit_default: 5` matches the Tavily API's default `max_results` of 5.
- `include_raw_content` accepts `true`, `'markdown'`, or `'text'`. `include_images` and `include_favicon` accept `true` or `false`.
- The `score` column is a relevance score between 0 and 1.
- `search_depth` supports `basic`, `advanced`, `fast`, and `ultra-fast`.

## Limitations

- The source models the `POST /search` endpoint only. Other Tavily endpoints are intentionally out of scope.
- `raw_content` is only available when `include_raw_content => true` is passed.
- `published_date` is only populated for news topic searches.
- `favicon` is only available when `include_favicon => true` is passed.
- Pagination is not supported; Tavily returns a single page of results per call (max 20).

## Notes

- **Rate Limits:** Rate limits apply based on your Tavily plan. Refer to Tavily's pricing page for details.
- **Nullable Fields:** `raw_content`, `published_date`, `images`, and `favicon` may be `NULL` depending on the arguments passed and the results returned.

## Provider docs

- Search API reference: https://docs.tavily.com/documentation/api-reference/endpoint/search
- API keys: https://app.tavily.com

## Live validation output

Validated against a live Tavily account with a valid `TAVILY_API_KEY`.

```bash
$ coral source lint sources/community/tavily/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/tavily/manifest.yaml
Added source tavily (secrets: file (plaintext))

  ✓ tavily connected successfully
  Secrets: file (plaintext)
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT url, title, score FROM tavily.search(q => 'Coral SQL') LIMIT 2
      2 rows
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json, result_columns_json
FROM coral.table_functions
WHERE schema_name = 'tavily';
```

```text
+---------------+--------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| function_name | kind   | arguments_json                                                                                                                                                                                                                                                                                                                                                                                                                  | result_columns_json                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
+---------------+--------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| search        | search | [{"name":"q","required":true,"values":[]},{"name":"max_results","required":false,"values":[]},{"name":"search_depth","required":false,"values":[]},{"name":"topic","required":false,"values":[]},{"name":"time_range","required":false,"values":[]},{"name":"include_images","required":false,"values":[]},{"name":"include_raw_content","required":false,"values":[]},{"name":"include_favicon","required":false,"values":[]}] | [{"name":"url","type":"Utf8","nullable":false,"description":"URL of the search result."},{"name":"title","type":"Utf8","nullable":true,"description":"Title of the search result."},{"name":"content","type":"Utf8","nullable":true,"description":"Most query-related content extracted from the source. With advanced search depth, this contains concatenated relevant chunks."},{"name":"score","type":"Float64","nullable":false,"description":"Relevance score of the search result (0 to 1)."},{"name":"raw_content","type":"Utf8","nullable":true,"description":"Parsed and cleaned HTML content. Only available when include_raw_content is enabled."},{"name":"published_date","type":"Utf8","nullable":true,"description":"Publication date of the source. Only available for news topic searches."},{"name":"images","type":"Json","nullable":true,"description":"Images extracted from this search result. Only included when include_images is enabled."},{"name":"favicon","type":"Utf8","nullable":true,"description":"Favicon URL for the search result. Only included when include_favicon is enabled."}] |
+---------------+--------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'tavily'
ORDER BY key;
```

```text
+----------------+--------+----------+--------+
| key            | kind   | required | is_set |
+----------------+--------+----------+--------+
| TAVILY_API_KEY | secret | true     | true   |
+----------------+--------+----------+--------+
```

```bash
$ coral source test tavily
  ✓ tavily connected successfully
  Secrets: file (plaintext)
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT url, title, score FROM tavily.search(q => 'Coral SQL') LIMIT 2
      2 rows
```

**Live bounded search proof with favicons:**

```sql
SELECT url, title, favicon
FROM tavily.search(q => 'GitHub withcoral coral', include_favicon => true)
LIMIT 3;
```

```text
+-----------------------------------------------+-----------------------------------------------------------------+-------------------------------------------------------------------+
| url                                           | title                                                           | favicon                                                           |
+-----------------------------------------------+-----------------------------------------------------------------+-------------------------------------------------------------------+
| https://ossinsight.io/analyze/withcoral/coral | Analyze withcoral/coral | OSSInsight                            | https://ossinsight.io/favicon.png                                 |
| https://trendshift.io/repositories/31444      | withcoral/coral — GitHub trending stats & insights | Trendshift | https://trendshift.io/apple-icon.png?apple-icon.09zgcp6ds_~s_.png |
| https://withcoral.com/docs                    | Introduction to Coral - Coral Docs                              | https://withcoral.com/favicon.svg                                 |
+-----------------------------------------------+-----------------------------------------------------------------+-------------------------------------------------------------------+
```
