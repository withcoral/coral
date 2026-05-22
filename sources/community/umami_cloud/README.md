# Umami Cloud

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://api.umami.is/v1` (override with `UMAMI_BASE_URL` env var)

Query Umami analytics data — websites, pageviews, events, sessions, and
summary statistics — directly through SQL using Coral. Supports both
[Umami Cloud](https://cloud.umami.is) and self-hosted Umami deployments.

## Authentication

Requires a `UMAMI_API_KEY` environment variable. Coral sends it via the
`x-umami-api-key` header on every request.

### Umami Cloud

1. Log in to [cloud.umami.is](https://cloud.umami.is)
2. Go to **Settings -> API keys** and click **Create key**
3. Copy the generated key — you will not be able to see it again

```bash
export UMAMI_API_KEY="api_..."
coral source add --file sources/community/umami_cloud/manifest.yaml
```

### Self-hosted Umami

Set `UMAMI_BASE_URL` to your instance's API root and supply your API key:

```bash
export UMAMI_API_KEY="<your-key>"
export UMAMI_BASE_URL="https://umami.example.com/api"
coral source add --file sources/community/umami_cloud/manifest.yaml
```

### Rate limiting

Umami Cloud limits API keys to 50 requests per 15 seconds. The connector
respects the `Retry-After` response header when the limit is hit. Use bounded
`LIMIT` clauses and narrow time ranges to stay within the quota.

## Inputs

| Input | Kind | Default | Description |
|---|---|---|---|
| `UMAMI_API_KEY` | secret | — | API key sent via `x-umami-api-key` header (required) |
| `UMAMI_BASE_URL` | variable | `https://api.umami.is/v1` | Base URL for the Umami API |

## Tables

| Table | Required filters | Description |
|---|---|---|
| `websites` | — | All websites in the account. Start here to get `website_id` values. |
| `pageviews` | `website_id`, `start_at`, `end_at` | Raw pageview and custom event rows for a website and time range. |
| `sessions` | `website_id`, `start_at`, `end_at` | Visitor sessions with device, location, and engagement metadata. |
| `stats` | `website_id`, `start_at`, `end_at` | Single-row aggregated summary (pageviews, visitors, bounces, total time). |
| `metrics` | `website_id`, `start_at`, `end_at`, `type` | Ranked breakdown of one dimension (browser, country, OS, URL, etc.). |

### Timestamp format

`start_at` and `end_at` are Unix epoch **milliseconds** (not seconds).

```text
2024-01-01 00:00:00 UTC  =  1704067200000
2024-01-31 23:59:59 UTC  =  1706745599000
```

### `metrics` type values

| Value | Description |
|---|---|
| `url` | Page URL paths |
| `referrer` | Referrer domains |
| `browser` | Browser names |
| `os` | Operating systems |
| `device` | Device categories (desktop, mobile, tablet) |
| `country` | ISO 3166-1 country codes |
| `language` | Browser language tags |
| `event` | Custom event names |
| `hostname` | Tracked hostnames |
| `region` | Region or state codes |
| `city` | City names |
| `query` | URL query strings |
| `title` | Page titles |
| `channel` | UTM channels |
| `domain` | Referrer domains (deduplicated) |
| `screen` | Screen resolutions |
| `tag` | Custom tags |

## Cascading queries

Most tables require a `website_id` from the `websites` table. Use this
discovery order:

```text
websites
  → id (website_id)
    → pageviews  (website_id, start_at, end_at)
    → sessions   (website_id, start_at, end_at)
    → stats      (website_id, start_at, end_at)
    → metrics    (website_id, start_at, end_at, type)
```

## Quick start

```bash
# Add the source
export UMAMI_API_KEY="api_..."
coral source add --file sources/community/umami_cloud/manifest.yaml

# Validate
coral source test umami_cloud

# Discover your websites
coral sql "SELECT id, name, domain FROM umami_cloud.websites"

# Find required filters
coral sql "
  SELECT table_name, column_name
  FROM coral.columns
  WHERE schema_name = 'umami_cloud' AND is_required_filter = true
  ORDER BY table_name
"
```

## Example queries

Discover websites:

```sql
SELECT id, name, domain
FROM umami_cloud.websites
```

Top pages by traffic:

```sql
SELECT url_path, COUNT(*) AS views
FROM umami_cloud.pageviews
WHERE website_id = '<uuid>'
  AND start_at = 1704067200000
  AND end_at   = 1706745599000
  AND event_type = 1
GROUP BY url_path
ORDER BY views DESC
LIMIT 20
```

Most common custom events:

```sql
SELECT event_name, COUNT(*) AS total
FROM umami_cloud.pageviews
WHERE website_id = '<uuid>'
  AND start_at = 1704067200000
  AND end_at   = 1706745599000
  AND event_type = 2
GROUP BY event_name
ORDER BY total DESC
```

Traffic by country:

```sql
SELECT x AS country, y AS visitors
FROM umami_cloud.metrics
WHERE website_id = '<uuid>'
  AND start_at = 1704067200000
  AND end_at   = 1706745599000
  AND type     = 'country'
ORDER BY visitors DESC
LIMIT 20
```

Browser breakdown:

```sql
SELECT x AS browser, y AS visitors
FROM umami_cloud.metrics
WHERE website_id = '<uuid>'
  AND start_at = 1704067200000
  AND end_at   = 1706745599000
  AND type     = 'browser'
ORDER BY visitors DESC
```

Summary statistics:

```sql
SELECT pageviews, visitors, visits, bounces, total_time
FROM umami_cloud.stats
WHERE website_id = '<uuid>'
  AND start_at = 1704067200000
  AND end_at   = 1706745599000
```

Recent sessions by country:

```sql
SELECT id, country, browser, os, device, visits, views, first_at, last_at
FROM umami_cloud.sessions
WHERE website_id = '<uuid>'
  AND start_at = 1704067200000
  AND end_at   = 1706745599000
ORDER BY last_at DESC
LIMIT 50
```

## Limitations

- Read-only. Write operations (creating websites, resetting data) are not supported.
- `stats` and `metrics` return aggregated data with no pagination. Use narrow
  time ranges for large deployments.
- Self-hosted deployments using session-based auth (not API key auth) require
  a token refresh workflow outside Coral.
