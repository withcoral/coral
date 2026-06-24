# Google Search Console

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 9
**Base URL:** `https://www.googleapis.com/webmasters/v3`

Query Google Search Console for verified properties, submitted sitemaps, and
search performance metrics (clicks, impressions, CTR, position) filtered by
date range and grouped by dimensions such as date, query, page, country,
device, or Search appearance. The performance tables require an
`encoded_site_url`, `start_date`, and `end_date` filter, making them useful
for combining with other analytics sources or extracting specific SEO
snapshots.

## Tables

| Table | Description | Required filters | Optional filters |
| :--- | :--- | :--- | :--- |
| `sites` | Lists all verified properties accessible to the user. | | |
| `sitemaps` | Lists submitted sitemap entries for a property. | `encoded_site_url` | `sitemap_index` |
| `search_performance` | Aggregate metrics without dimension grouping. | `encoded_site_url`, `start_date`, `end_date` | `type` |
| `performance_by_date` | Metrics grouped by date. | `encoded_site_url`, `start_date`, `end_date` | `type` |
| `performance_by_query` | Metrics grouped by search query (keyword). | `encoded_site_url`, `start_date`, `end_date` | `type` |
| `performance_by_page` | Metrics grouped by landing page URL. | `encoded_site_url`, `start_date`, `end_date` | `type` |
| `performance_by_country` | Metrics grouped by country (lowercase ISO 3166-1 alpha-3). | `encoded_site_url`, `start_date`, `end_date` | `type` |
| `performance_by_device` | Metrics grouped by device type (MOBILE, DESKTOP, TABLET). | `encoded_site_url`, `start_date`, `end_date` | `type` |
| `performance_by_search_appearance` | Metrics grouped by Search appearance feature. | `encoded_site_url`, `start_date`, `end_date` | `type` |

## Authentication

Google Search Console requires an OAuth 2.0 access token with the
`https://www.googleapis.com/auth/webmasters.readonly` scope.

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project or select an existing one.
3. Enable the **Google Search Console API** in the API library.
4. Navigate to **APIs & Services > Credentials** and click **Create Credentials > OAuth client ID**.
5. Select **Desktop app** as the application type.
6. Note down the **Client ID** and **Client Secret**.
7. In Coral, run the interactive setup command below and provide the Client ID and Secret when prompted.

## Install

Add the source interactively:
```sh
coral source add --interactive --file sources/community/google_search_console/manifest.yaml
```

Lint the manifest to verify syntax:
```sh
coral source lint sources/community/google_search_console/manifest.yaml
```

Run test queries defined in the manifest:
```sh
coral source test google_search_console
```

## Example Queries

### List available properties
Start by retrieving the `encoded_site_url` values, which are required for performance queries.

```sql
SELECT site_url, encoded_site_url, permission_level
FROM google_search_console.sites;
```

### List submitted sitemaps
Inspect sitemap metadata for one property.

```sql
SELECT path, type, last_submitted, last_downloaded, warnings, errors
FROM google_search_console.sitemaps
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
LIMIT 20;
```

### Total search performance for a month
Fetch the aggregate clicks, impressions, CTR, and average position for January 2025.

```sql
SELECT clicks, impressions, ctr, position
FROM google_search_console.search_performance
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31'
  AND type = 'web';
```

### Top 50 web search queries by clicks
Find the Google Search keywords driving the most traffic.

```sql
SELECT query, clicks, impressions, position
FROM google_search_console.performance_by_query
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31'
  AND type = 'web'
ORDER BY clicks DESC
LIMIT 50;
```

### Daily performance trends
Retrieve daily metrics to plot traffic trends.

```sql
SELECT date, clicks, impressions, ctr
FROM google_search_console.performance_by_date
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31'
ORDER BY date ASC;
```

### Traffic by device type
Break down search performance by Mobile, Desktop, and Tablet.

```sql
SELECT device, clicks, impressions, ctr
FROM google_search_console.performance_by_device
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31';
```

### Traffic by Search appearance
Discover Search appearance feature values returned for a property.

```sql
SELECT search_appearance, clicks, impressions, ctr
FROM google_search_console.performance_by_search_appearance
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31'
LIMIT 50;
```

## Notes

* The `encoded_site_url` filter value must be URL-encoded (e.g.,
  `https%3A%2F%2Fexample.com%2F` or `sc-domain%3Aexample.com`). The
  `sites.encoded_site_url` column is a convenience value for common
  `https://.../` and `sc-domain:...` properties; source-spec expressions only
  escape `/` and `:` here. Manually percent-encode or copy-check unusual
  URL-prefix properties with path segments, query strings, fragments, spaces,
  or other reserved characters before using them in filters.
* The optional Search Analytics `type` filter defaults to Google's `web` result
  type when omitted. Valid values include `web`, `image`, `video`, `news`,
  `discover`, and `googleNews`, but Google does not support every dimension or
  metric for every type. In particular, query and position-oriented workflows
  should use Search result types such as `web`, `image`, `video`, or `news`;
  `discover` and `googleNews` are safest for aggregate/date-style clicks,
  impressions, and CTR checks. Unsupported type/dimension combinations return
  Google API errors.
* The Search Analytics API returns up to 25,000 rows per request. This source
  uses a conservative provider `rowLimit` of 1,000 rows for dimension tables
  because Search Console's `startRow` pagination value belongs in the JSON
  request body, while Coral's offset pagination currently emits offsets as
  query parameters. Use narrower date ranges or dimensions if more than 1,000
  rows are needed.
* The API may omit rows where data is zero (e.g., a date with no clicks or impressions).
* Start and end dates must be provided in `YYYY-MM-DD` format.
* Country dimension values are returned as lowercase three-letter ISO 3166-1 alpha-3 codes such as `usa` and `gbr`.

## Rate limits

Search Analytics has load limits as well as QPS/QPM quotas. Google calls out
page/query grouping, long date ranges, and repeated re-queries as higher-load
patterns. Keep date ranges narrow for expensive dimensions such as `query` and
`page`, and wait before retrying if Google returns quota errors.

## References

* [Sites API](https://developers.google.com/webmaster-tools/v1/sites/list)
* [Sitemaps API](https://developers.google.com/webmaster-tools/v1/sitemaps)
* [Search Analytics API](https://developers.google.com/webmaster-tools/v1/searchanalytics/query)
* [Search Console API usage limits](https://developers.google.com/webmaster-tools/limits)

## Limitations

* This v1 intentionally exposes common one-dimension Search Analytics groupings. Multi-dimension requests such as date plus query are out of scope for this source structure.
* URL Inspection uses a separate Search Console API base URL and is not included in this v1 source.
* This source supports read-only operations. Modifying sitemaps or managing property verification is not supported.
