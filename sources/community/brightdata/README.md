# Bright Data community source

Query your Bright Data account through Coral SQL: balance, account status,
zones, dataset catalog, zone usage costs, Web Unlocker fetches, and SERP
results. This source adds the Bright Data customer API to the community
catalog so users and agents can monitor usage, inspect their zones, and scrape
public pages or run search-engine result lookups without leaving SQL.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Table functions:** 4
**Base URL (default):** `https://api.brightdata.com`

## Why this source

Bright Data operates the largest residential/ISP/datacenter proxy network plus
scraping APIs (Web Unlocker, SERP, DCA, Datasets). Coral did not have a Bright
Data source yet, so this community spec gives the reef a focused read/query
surface for:

- Checking account balance and credit in SQL before running large jobs.
- Verifying account status and the proxy exit IP.
- Listing the zones on the account and their type (unlocker, browser API).
- Browsing the public dataset catalog by size.
- Fetching any public URL through the Web Unlocker as HTML or markdown.
- Running search-engine result lookups through the SERP API.

The v1 surface is intentionally narrow and read-oriented. It proves Coral can
authenticate against the Bright Data customer API with a zone API token and
map the account, zone, unlocker, and SERP endpoints into verifiable tables.
Async operations (dataset pipelines, Discover task polling, DCA crawls) have
no request/response mapping in the current HTTP backend and are out of scope
for this first version.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral
repository and add the manifest from this directory:

```bash
coral source add --file sources/community/brightdata/manifest.yaml
```

You can also copy `manifest.yaml` into another workspace and pass that path to
`coral source add --file`.

## Authentication

Bright Data requests are authenticated with a zone API token sent as a Bearer
token. Create one from the dashboard:

1. Log in to the Bright Data dashboard at <https://brightdata.com/>.
2. Go to **Account** → **API access** (https://brightdata.com/api).
3. Click **Create API token**, choose **Zone API token**, select the zone, and
   copy the token. Use the **Admin** permission level: this source calls both
   billing endpoints (balance, zone cost) and proxy endpoints (Web Unlocker,
   SERP), and only Admin covers both with a single token (Finance covers
   billing only; Ops and User cover proxy access only).

Set the token as `BRIGHT_DATA_API_KEY` before adding or testing the source:

```bash
export BRIGHT_DATA_API_KEY="your_zone_api_token"
coral source add --file sources/community/brightdata/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/brightdata/manifest.yaml
```

The optional `BRIGHT_DATA_ZONE` input (default `unlocker`) selects the zone
used by `web_unlocker` and `serp_search`; every function that consumes it also
accepts a `zone` argument to override it per call. Table functions take a
plain `api` (top-level `Authorization` header) token and require no proxy
credentials. This source reads data from the Bright Data API directly with
your token; it does not route requests through Bright Data proxy zones.

## Provider docs

- Bright Data API: https://docs.brightdata.com
- Bright Data API access/tokens: https://brightdata.com/api
- Web Unlocker: https://docs.brightdata.com/scraping-methods/web-unlocker
- SERP API: https://docs.brightdata.com/scraping-methods/serp

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `brightdata.customer_balance` | Current account balance, credit, prepayment, and pending costs. | None |
| `brightdata.customer_status` | Account status, exit IP, and proxy request health. | None |
| `brightdata.zones` | Zones on the account and their type. | None |
| `brightdata.datasets` | Public Bright Data dataset catalog (id, name, row count). | None |

### `brightdata.customer_balance`

Returns the account balance snapshot from `GET /customer/balance`.

```sql
SELECT balance, credit, prepayment, pending_costs
FROM brightdata.customer_balance;
```

### `brightdata.customer_status`

Returns the account status from `GET /status`. `can_make_requests` reflects
proxy/zone health; a `false` value with `auth_fail_reason` set to
`zone_not_found` means the zone is not provisioned even though the API token is
valid (see Limitations).

```sql
SELECT status, customer, can_make_requests, ip
FROM brightdata.customer_status;
```

### `brightdata.zones`

Lists the zones on the account via `GET /zone/get_active_zones`. `type` is one
of `unblocker`, `browser_api`, etc.

```sql
SELECT name, type FROM brightdata.zones;
```

### `brightdata.datasets`

Lists the public dataset catalog from `GET /datasets/list`. `size` is the
dataset row count; use it to find large prebuilt datasets.

```sql
SELECT id, name, size
FROM brightdata.datasets
WHERE size > 1000000
LIMIT 10;
```

## Table functions

| Function | Kind | Description |
| --- | --- | --- |
| `brightdata.zone_info(zone)` | table | Zone configuration and plan details for one zone. |
| `brightdata.zone_cost(zone, from?, to?)` | table | Usage cost and bandwidth by billing period for one zone. |
| `brightdata.web_unlocker(url, zone?, country?, locale?, data_format?)` | table | Fetch a public URL through the Web Unlocker as HTML or markdown. |
| `brightdata.serp_search(url, zone?)` | search | Search-engine result pages through the SERP API. |

### `brightdata.zone_info`

Looks up one zone via `GET /zone?zone=<name>`. The `perm` column reports the
zone permission mode (`country`), and plan columns carry plan type, product,
and VIP pool. Zone passwords are intentionally not exposed.

```sql
SELECT zone, created, perm, plan_type
FROM brightdata.zone_info(zone => 'unlocker');
```

### `brightdata.zone_cost`

Returns usage cost and bandwidth for one zone from `GET /zone/cost`. One row
per customer; columns report the current calendar month (`month_*`) and
current day (`day_*`) buckets, with `*_range` carrying the period bounds.

```sql
SELECT customer_id, month_cost, month_bw, month_reqs_serp,
       month_reqs_unblocker
FROM brightdata.zone_cost(zone => 'unlocker');
```

### `brightdata.web_unlocker`

Fetches any public URL through the Web Unlocker via `POST /request`. Returns
`status_code`, response `headers` (Json), and `body` as HTML by default. Request
markdown output with `data_format => 'markdown'`, or choose a country/locale
with `country => 'us'` / `locale => 'en-US'`.

```sql
SELECT status_code, body
FROM brightdata.web_unlocker(url => 'https://example.com');

SELECT status_code, body
FROM brightdata.web_unlocker(
  url => 'https://en.wikipedia.org/wiki/Coral',
  data_format => 'markdown'
);
```

### `brightdata.serp_search`

Runs a search-engine lookup through the SERP API via `POST /request`. Because
the Coral template engine does not URL-encode template values, `serp_search`
takes a full, pre-encoded search URL as its `url` argument. Build it from a
search engine endpoint and the `brd_json=1` parameter so Bright Data returns
JSON:

```sql
SELECT link, title, rank
FROM brightdata.serp_search(
  url => 'https://www.google.com/search?q=coral+sql&brd_json=1'
)
LIMIT 10;
```

`serp_search` is a search-kind function: it maps the `organic` result list and
applies the declared `default_top_k` (10) limit, with a maximum of 50 top-k
rows and one call per query. To page, add `start=N` to the URL. Use
`gl=CC`/`hl=lang` parameters for country/language, and swap the host to
`https://www.bing.com/search` for Bing results. Google and Bing are the
validated engines; other engines are not covered by this source.

## Validation

Run the source-level checks with a valid `BRIGHT_DATA_API_KEY` before opening
or updating a PR. The API key is required for `source add`, `source test`, and
live SQL queries, but it should never be printed or committed.

```bash
coral source lint sources/community/brightdata/manifest.yaml

export BRIGHT_DATA_API_KEY="your_zone_api_token"
coral source add --file sources/community/brightdata/manifest.yaml
coral source test brightdata
```

The declared test queries cover account balance, zone listing, and a SERP
lookup:

```sql
SELECT balance, credit FROM brightdata.customer_balance;

SELECT name, type FROM brightdata.zones;

SELECT link, title
FROM brightdata.serp_search(url => 'https://www.google.com/search?q=coral+sql&brd_json=1')
LIMIT 2;
```

### Live validation output

The following output was captured from a live validation run using a real
Bright Data zone API token.

#### Manifest lint

Command:

```bash
coral source lint sources/community/brightdata/manifest.yaml
```

Output:

```text
Manifest is valid
```

#### Add source and run declared tests

Command:

```bash
coral source add --file sources/community/brightdata/manifest.yaml
```

Output:

```text
    brightdata (4 tables)
    ├─ customer_balance
    ├─ customer_status
    ├─ datasets
    └─ zones

    brightdata (4 table functions)
    ├─ serp_search
    ├─ web_unlocker
    ├─ zone_cost
    └─ zone_info
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT balance, credit FROM brightdata.customer_balance
      1 row

    ✓ SELECT name, type FROM brightdata.zones
      2 rows

    ✓ SELECT link, title FROM brightdata.serp_search(url => 'https://www.google.com/search?q=coral+sql&brd_json=1') LIMIT 2
      2 rows
```

#### Confirm table discovery

Command:

```bash
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'brightdata' ORDER BY table_name"
```

Output:

```text
+------------------+
| table_name       |
+------------------+
| customer_balance |
| customer_status  |
| datasets         |
| zones            |
+------------------+
```

#### Confirm column discovery

Command:

```bash
coral sql "SELECT table_name, column_name, data_type, is_nullable FROM coral.columns WHERE schema_name = 'brightdata' ORDER BY table_name, ordinal_position"
```

Output:

```text
+------------------+-------------------+-----------+-------------+
| table_name       | column_name       | data_type | is_nullable |
+------------------+-------------------+-----------+-------------+
| customer_balance | balance           | Float64   | true        |
| customer_balance | credit            | Float64   | true        |
| customer_balance | prepayment        | Float64   | true        |
| customer_balance | pending_costs     | Float64   | true        |
| customer_status  | status            | Utf8      | true        |
| customer_status  | customer          | Utf8      | true        |
| customer_status  | can_make_requests | Boolean   | true        |
| customer_status  | auth_fail_reason  | Utf8      | true        |
| customer_status  | ip                | Utf8      | true        |
| datasets         | id                | Utf8      | false       |
| datasets         | name              | Utf8      | false       |
| datasets         | size              | Int64     | true        |
| zones            | name              | Utf8      | false       |
| zones            | type              | Utf8      | false       |
+------------------+-------------------+-----------+-------------+
```

#### Confirm input discovery

Command:

```bash
coral sql "SELECT key, kind, required FROM coral.inputs WHERE schema_name = 'brightdata' ORDER BY key"
```

Output:

```text
+---------------------+----------+----------+
| key                 | kind     | required |
+---------------------+----------+----------+
| BRIGHT_DATA_API_KEY | secret   | true     |
| BRIGHT_DATA_ZONE    | variable | false    |
+---------------------+----------+----------+
```

#### Run a live balance query

Command:

```bash
coral sql "SELECT balance, credit, prepayment, pending_costs FROM brightdata.customer_balance"
```

Output:

```text
+---------+--------+------------+---------------+
| balance | credit | prepayment | pending_costs |
+---------+--------+------------+---------------+
| 2.0     | 0.0    | 0.0        | 0.0           |
+---------+--------+------------+---------------+
```

#### Run a live status query

Command:

```bash
coral sql "SELECT status, customer, can_make_requests, ip FROM brightdata.customer_status"
```

Output:

```text
+--------+-------------+-------------------+----------------+
| status | customer    | can_make_requests | ip             |
+--------+-------------+-------------------+----------------+
| active | hl_12bc31c4 | false             | 43.230.107.97  |
+--------+-------------+-------------------+----------------+
```

#### Run a live zones query

Command:

```bash
coral sql "SELECT name, type FROM brightdata.zones"
```

Output:

```text
+--------------+-------------+
| name         | type        |
+--------------+-------------+
| cli_unlocker | unblocker   |
| cli_browser  | browser_api |
+--------------+-------------+
```

#### Run a live zone_info query

Command:

```bash
coral sql "SELECT zone, created, perm, plan_type, plan_product, plan_vips_type FROM brightdata.zone_info(zone => 'cli_unlocker')"
```

Output:

```text
+--------------+--------------------------+---------+-----------+--------------+----------------+
| zone         | created                  | perm    | plan_type | plan_product | plan_vips_type |
+--------------+--------------------------+---------+-----------+--------------+----------------+
| cli_unlocker | 2026-08-11T15:21:21.629Z | country | unblocker | unblocker    | shared         |
+--------------+--------------------------+---------+-----------+--------------+----------------+
```

#### Run a live zone_cost query

Command:

```bash
coral sql "SELECT customer_id, month_cost, month_bw, month_reqs_serp, month_reqs_unblocker, month_range FROM brightdata.zone_cost(zone => 'cli_unlocker')"
```

Output:

```text
+-------------+------------+----------+-----------------+----------------------+-------------------------------------+
| customer_id | month_cost | month_bw | month_reqs_serp | month_reqs_unblocker | month_range                         |
+-------------+------------+----------+-----------------+----------------------+-------------------------------------+
| hl_12bc31c4 | 0.0195     | 888308   | 5               | 8                    | {"from":"Aug-2026","to":"Sep-2026"} |
+-------------+------------+----------+-----------------+----------------------+-------------------------------------+
```

#### Run a live web_unlocker query

Command:

```bash
coral sql "SELECT url, status_code, octet_length(body) AS body_bytes FROM brightdata.web_unlocker(url => 'https://example.com')"
```

Output:

```text
+---------------------+-------------+------------+
| url                 | status_code | body_bytes |
+---------------------+-------------+------------+
| https://example.com | 200         | 559        |
+---------------------+-------------+------------+
```

#### Run a live SERP search query

Command:

```bash
coral sql "SELECT link, title, rank FROM brightdata.serp_search(url => 'https://www.google.com/search?q=coral+sql&brd_json=1') LIMIT 3"
```

Output:

```text
+------------------------------------+-------------------------------------------------------------------+------+
| link                               | title                                                             | rank |
+------------------------------------+-------------------------------------------------------------------+------+
| https://withcoral.com/             | Coral — The data engine for enterprise AI                         | 1    |
| https://github.com/withcoral/coral | withcoral/coral: One SQL interface over APIs, files, and live ... | 2    |
| https://github.com/linkedin/coral  | linkedin/coral: Coral is a translation, analysis, and query ...   | 3    |
+------------------------------------+-------------------------------------------------------------------+------+
```

## Limitations

- This source is read/query oriented. It does not create or manage zones,
  datasets, or proxy credentials; the Bright Data dashboard remains the
  management surface.
- `customer_status.can_make_requests` reflects proxy/zone health, not API token
  validity. On a free-trial account the API endpoints work while the report may
  show `false` with `auth_fail_reason = zone_not_found` because the proxy zone
  is not provisioned.
- Async operations are not exposed: dataset sync (`POST /datasets/v3/scrape`),
  Discover (`POST /discover` with task polling), and DCA crawls
  (`POST /dca/crawl`) all require follow-up polling that the current HTTP
  backend has no mapping for. Only the read-only dataset catalog is included.
- `serp_search` requires a pre-URL-encoded `url` because Coral's template
  engine does not URL-encode values; build the URL with `+`/`%20` and
  `brd_json=1` as described above.
- The Web Unlocker `data_format` options `markdown` and `screenshot` are
  accepted; screenshots are returned as a binary buffer and are not rendered
  as an image column, so prefer `markdown` or raw HTML in SQL.
- Zone passwords are deliberately excluded from `zone_info` output.
- Balance, usage costs, dataset catalog contents, and error responses depend
  on the Bright Data account, its plan/trial, and the current provider API.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md), keep the manifest focused,
and include the validation commands plus proof output in the PR description.
