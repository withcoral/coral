# Plausible

**Version:** 1.0.0
**Backend:** HTTP
**Base URL:** `https://plausible.io`

Query Plausible Analytics web stats as SQL tables. Get aggregate metrics, time-series data, and traffic breakdowns by page and source for any site in your Plausible account. Join with GitHub activity, Linear issues, or Notion pages for cross-source product intelligence.

## Tables

| Table | Description | Required filters | Optional filters |
|-------|-------------|-----------------|-----------------|
| `plausible.aggregate` | Single-row aggregate metrics for a site and period | `site_id` | `period`, `date` |
| `plausible.timeseries` | Time-bucketed metrics — one row per day or month | `site_id` | `period`, `interval`, `date` |
| `plausible.pages` | Top pages ranked by visitors | `site_id` | `period`, `date` |
| `plausible.sources` | Traffic sources ranked by visitors | `site_id` | `period`, `date` |

## Source-Scoped Table Functions

| Function | Description |
|----------|-------------|
| `plausible.breakdown(site_id => '...', property => '...')` | Break down stats by any Plausible property — page, source, country, device, browser, OS, UTM |

## Authentication

Requires a `PLAUSIBLE_API_KEY`.

**To get your API key:**

1. Log in to [plausible.io](https://plausible.io)
2. Go to **Settings** (top right) → **API keys**
3. Click **New API key**, give it a name, click **Create API key**
4. Copy the key — it will not be shown again

## Install

```bash
coral source lint manifest.yaml
coral source add --file manifest.yaml
coral source test plausible
```

Or with the key inline:

```bash
PLAUSIBLE_API_KEY=your-key coral source add --file manifest.yaml
```

## The `period` filter

All tables accept a `period` filter. Valid values:

| Value | Meaning |
|-------|---------|
| `12mo` | Last 12 calendar months |
| `6mo` | Last 6 calendar months |
| `month` | Current calendar month |
| `30d` | Last 30 days (default when omitted) |
| `7d` | Last 7 days |
| `day` | Today |
| `custom` | Custom range — requires `date = 'YYYY-MM-DD,YYYY-MM-DD'` |

## Example Queries

Aggregate stats for the last 30 days:

```sql
SELECT visitors, pageviews, bounce_rate, visit_duration
FROM plausible.aggregate
WHERE site_id = 'yourdomain.com'
  AND period = '30d';
```

Daily visitor trend over the last 7 days:

```sql
SELECT date, visitors, pageviews
FROM plausible.timeseries
WHERE site_id = 'yourdomain.com'
  AND period = '7d'
ORDER BY date ASC;
```

Top 10 pages this month:

```sql
SELECT page, visitors, pageviews, bounce_rate
FROM plausible.pages
WHERE site_id = 'yourdomain.com'
  AND period = 'month'
ORDER BY visitors DESC
LIMIT 10;
```

Top traffic sources last 30 days:

```sql
SELECT source, visitors, bounce_rate
FROM plausible.sources
WHERE site_id = 'yourdomain.com'
  AND period = '30d'
ORDER BY visitors DESC
LIMIT 10;
```

Country breakdown using the table function:

```sql
SELECT country, visitors, pageviews
FROM plausible.breakdown(
    site_id   => 'yourdomain.com',
    property  => 'visit:country',
    period    => '30d'
)
ORDER BY visitors DESC
LIMIT 20;
```

Custom date range:

```sql
SELECT date, visitors
FROM plausible.timeseries
WHERE site_id = 'yourdomain.com'
  AND period = 'custom'
  AND date = '2026-01-01,2026-01-31'
ORDER BY date ASC;
```

## Cross-Source JOIN Example

Traffic trend alongside GitHub deploy activity (requires `github` source installed):

```sql
WITH web AS (
    SELECT date, visitors, pageviews
    FROM plausible.timeseries
    WHERE site_id = 'yourdomain.com'
      AND period = '30d'
),
deploys AS (
    SELECT SUBSTR(merged_at, 1, 10) AS deploy_date,
           COUNT(*) AS prs_merged
    FROM github.pulls
    WHERE owner = 'your-org'
      AND repo = 'your-repo'
    GROUP BY SUBSTR(merged_at, 1, 10)
)
SELECT w.date, w.visitors, w.pageviews, COALESCE(d.prs_merged, 0) AS deploys
FROM web w
LEFT JOIN deploys d ON d.deploy_date = w.date
ORDER BY w.date DESC;
```

## The `breakdown` Table Function

`plausible.breakdown` supports any Plausible property. The dimension column matching your property is populated; others are NULL.

Supported `property` values:

| Property | Populated column |
|----------|-----------------|
| `event:page` | `page` |
| `visit:source` | `source` |
| `visit:country` | `country` |
| `visit:device` | `device` |
| `visit:browser` | `browser` |
| `visit:os` | `os` |
| `visit:utm_source` | `utm_source` |
| `visit:utm_campaign` | `utm_campaign` |

## Notes

- `site_id` is the domain you registered in Plausible (e.g. `yourdomain.com`, not a URL).
- All tables are strictly read-only.
- `period` defaults to `30d` when omitted — Plausible's API default.
- `plausible.aggregate` returns exactly one row. `row_strategy: direct` is used since the API returns a single results object, not an array.
- `plausible.timeseries` returns all date buckets at once — no pagination needed.
- `plausible.pages` and `plausible.sources` paginate via Plausible's `page`/`limit` params (up to 1000 rows per page).
- Rate limit is 600 requests per hour. Handled automatically via `rate_limit` config in the manifest.
