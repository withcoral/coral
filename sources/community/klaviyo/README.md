# Klaviyo

**Version:** 1.0.0
**Backend:** HTTP
**Base URL:** `https://a.klaviyo.com`

Query Klaviyo email marketing data as SQL tables. Inspect subscriber lists, campaigns, automation flows, and event metric definitions. Join with Shopify orders or Chargebee subscriptions for e-commerce revenue intelligence.

## Tables

| Table | Description | Required filters | Optional filters |
|-------|-------------|-----------------|-----------------|
| `klaviyo.lists` | Subscriber lists with opt-in configuration | — | `filter` |
| `klaviyo.campaigns` | Email and SMS campaigns with status and send time | `channel` | `filter` |
| `klaviyo.flows` | Automation flow definitions with status | — | `filter` |
| `klaviyo.metrics` | Event metric definitions with integration source | — | `filter` |

## Authentication

Requires `KLAVIYO_API_KEY`.

**To get your API key:**

1. Log in to your Klaviyo dashboard
2. Go to **Settings** → **API Keys**
3. Click **Create Private API Key**
4. Grant `lists:read`, `campaigns:read`, `flows:read`, and `metrics:read` to
   query every table exposed by this source

The connector uses an `Authorization: Klaviyo-API-Key {key}` header and the API revision `2026-04-15`.

## Install

```bash
coral source lint manifest.yaml
coral source add --file manifest.yaml
coral source test klaviyo
```

Or with the key inline:

```bash
KLAVIYO_API_KEY=your-key coral source add --file manifest.yaml
```

## Example Queries

### Lists

First page of subscriber lists (up to 10):

```sql
SELECT id, name, opt_in_process, created
FROM klaviyo.lists
ORDER BY created DESC;
```

Match a list name exactly (API-level filter):

```sql
SELECT id, name, opt_in_process, created
FROM klaviyo.lists
WHERE filter = 'equals(name,"Newsletter")'
ORDER BY created DESC;
```

Lists created after a date (API-level filter):

```sql
SELECT id, name, opt_in_process, created
FROM klaviyo.lists
WHERE filter = 'greater-than(created,"2024-01-01T00:00:00+00:00")'
ORDER BY created DESC;
```

### Campaigns

`channel` is required by the Klaviyo API. Valid values: `email`, `sms`, `mobile_push`.

Email campaigns (first page of up to 100, most recently updated):

```sql
SELECT id, name, status, channel, send_time
FROM klaviyo.campaigns
WHERE channel = 'email'
ORDER BY send_time DESC;
```

SMS campaigns:

```sql
SELECT id, name, status, send_time
FROM klaviyo.campaigns
WHERE channel = 'sms'
ORDER BY send_time DESC;
```

Sent email campaigns only (additional API-level filter):

```sql
SELECT id, name, send_time
FROM klaviyo.campaigns
WHERE channel = 'email'
  AND filter = 'equals(status,"Sent")'
ORDER BY send_time DESC;
```

Email campaigns updated after a date (additional API-level filter):

```sql
SELECT id, name, status, send_time
FROM klaviyo.campaigns
WHERE channel = 'email'
  AND filter = 'greater-or-equal(updated_at,"2026-01-01T00:00:00+00:00")'
ORDER BY send_time DESC;
```

### Flows

First page of automation flows (up to 50):

```sql
SELECT id, name, status, created
FROM klaviyo.flows
ORDER BY created DESC;
```

Live flows only (API-level filter — efficient for large accounts):

```sql
SELECT id, name, status, created
FROM klaviyo.flows
WHERE filter = 'equals(status,"live")'
ORDER BY created DESC;
```

Flows created after a date:

```sql
SELECT id, name, status, created
FROM klaviyo.flows
WHERE filter = 'greater-than(created,"2024-01-01T00:00:00+00:00")'
ORDER BY created DESC;
```

### Metrics

First page of event metrics (up to 200):

```sql
SELECT id, name, integration_name, integration_category
FROM klaviyo.metrics
ORDER BY integration_name ASC, name ASC;
```

Metrics from Shopify only:

```sql
SELECT id, name, integration_name, integration_category
FROM klaviyo.metrics
WHERE filter = 'equals(integration.name,"Shopify")'
ORDER BY name ASC;
```

## Cross-Source JOIN Example

Campaign send volume versus support ticket volume by day — detect whether email campaigns correlate with support spikes (requires `freshdesk` source installed):

```sql
WITH campaign_days AS (
    SELECT
        SUBSTR(send_time, 1, 10) AS day,
        COUNT(*)                 AS campaigns_sent
    FROM klaviyo.campaigns
    WHERE channel = 'email'
      AND status = 'Sent'
    GROUP BY SUBSTR(send_time, 1, 10)
),
ticket_days AS (
    SELECT
        SUBSTR(created_at, 1, 10) AS day,
        COUNT(*)                  AS tickets_opened
    FROM freshdesk.tickets
    GROUP BY SUBSTR(created_at, 1, 10)
)
SELECT
    COALESCE(c.day, t.day)        AS date,
    COALESCE(c.campaigns_sent, 0) AS campaigns_sent,
    COALESCE(t.tickets_opened, 0) AS tickets_opened
FROM campaign_days c
FULL OUTER JOIN ticket_days t ON t.day = c.day
ORDER BY date DESC;
```

## Klaviyo Filter Syntax

All four tables accept an optional `filter` SQL parameter whose value is a
Klaviyo filter expression. For `klaviyo.campaigns`, the source combines this
expression with the required channel filter. String values must be double-quoted
inside the expression; wrap the whole expression in single quotes in SQL.

| Pattern | Example |
|---------|---------|
| Equality | `equals(field,"value")` |
| Contains | `contains(field,"value")` |
| Greater-than | `greater-than(field,"value")` |
| Less-than | `less-than(field,"value")` |
| Combine | `and(expr1,expr2)` |

Each endpoint permits only specific filter fields and operators. See Klaviyo's
[API reference](https://developers.klaviyo.com/en/reference/api-overview#filtering)
before composing a filter. For example, lists support `equals(name,...)` but
not `contains(name,...)`.

`klaviyo.campaigns` also takes a required plain `channel` filter (`email`,
`sms`, or `mobile_push`); the connector automatically constructs and combines
the required `messages.channel` expression.

## Status Reference

### Campaign status

| Value | Meaning |
|-------|---------|
| `Draft` | Not yet sent or scheduled |
| `Scheduled` | Queued for future delivery |
| `Sending` | Currently being delivered |
| `Sent` | Delivery complete |
| `Cancelled` | Sending was cancelled |

### Flow status

| Value | Meaning |
|-------|---------|
| `draft` | Not yet live |
| `live` | Active and triggering |
| `manual` | Paused for manual review |
| `archived` | Archived, no longer triggering |

## Notes

- All tables are strictly read-only.
- Klaviyo returns cursor links as full URLs in the JSON response body, which
  Coral's HTTP backend cannot currently follow. Each query therefore returns
  the first API page: up to 10 lists, 100 campaigns, 50 flows, or 200 metrics.
  Use provider-level `filter` expressions to retrieve narrow, reliable subsets.
- `klaviyo.campaigns` requires a `channel` value (`email`, `sms`, or
  `mobile_push`) and accepts an optional additional provider filter. The source
  automatically constructs and combines the required
  `equals(messages.channel,"<value>")` expression.
- The `revision: 2026-04-15` header is sent automatically with every request.
- All timestamp fields use ISO 8601 format with timezone offset (e.g. `2024-01-15T10:30:00+00:00`).
- Rate limit handling: `429` responses are retried automatically via `Retry-After`.
