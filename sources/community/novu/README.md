# Novu

**Version:** 1.0.0
**Backend:** HTTP
**Base URL:** `https://api.novu.co` (configurable for self-hosted)

Query Novu notification workflows, subscribers, topics, and activity as SQL tables. Inspect workflow definitions, browse subscriber rosters, explore topic segments, and audit the full notification activity feed. Join with Freshdesk, Linear, or GitHub for cross-source product and support intelligence.

## Tables

| Table | Description | Required filters | Optional filters |
|-------|-------------|-----------------|-----------------|
| `novu.workflows` | Notification workflow templates with status and tags | — | — |
| `novu.subscribers` | Subscribers with contact details and online status | — | — |
| `novu.topics` | Named subscriber segments used for bulk delivery | — | — |
| `novu.notifications` | Full notification activity feed — every event triggered | — | — |

## Authentication

Requires `NOVU_API_KEY`.

**To get your API key:**

1. Log in to the [Novu dashboard](https://dashboard.novu.co)
2. Go to **Settings** → **API Keys**
3. Copy your secret key

The connector uses an `Authorization: ApiKey {key}` header as documented by the Novu API.

## Install

```bash
coral source lint manifest.yaml
coral source add --file manifest.yaml
coral source test novu
```

Or with the key inline:

```bash
NOVU_API_KEY=your-key coral source add --file manifest.yaml
```

For a self-hosted instance:

```bash
NOVU_API_KEY=your-key NOVU_BASE_URL=https://novu.yourdomain.com coral source add --file manifest.yaml
```

## Example Queries

All active workflows:

```sql
SELECT id, name, tags, created_at
FROM novu.workflows
WHERE active = true
ORDER BY name ASC;
```

Recent notification activity:

```sql
SELECT transaction_id, template_id, subscriber_id, channels, created_at
FROM novu.notifications
ORDER BY created_at DESC
LIMIT 100;
```

Notification volume by workflow:

```sql
SELECT template_id, COUNT(*) AS total_sent
FROM novu.notifications
GROUP BY template_id
ORDER BY total_sent DESC;
```

All subscribers with email:

```sql
SELECT subscriber_id, first_name, last_name, email
FROM novu.subscribers
WHERE email IS NOT NULL
ORDER BY created_at DESC;
```

All topics:

```sql
SELECT key, name
FROM novu.topics
ORDER BY name ASC;
```

Join workflows with notification counts:

```sql
SELECT
    w.name             AS workflow_name,
    w.active,
    COUNT(n.id)        AS notifications_sent
FROM novu.workflows w
LEFT JOIN novu.notifications n ON n.template_id = w.id
GROUP BY w.id, w.name, w.active
ORDER BY notifications_sent DESC;
```

## Cross-Source JOIN Example

Notification activity alongside Freshdesk support tickets — correlate notification bursts with ticket spikes (requires `freshdesk` source installed):

```sql
WITH daily_notifications AS (
    SELECT SUBSTR(created_at, 1, 10) AS date,
           COUNT(*)                  AS notifications_sent
    FROM novu.notifications
    GROUP BY SUBSTR(created_at, 1, 10)
),
daily_tickets AS (
    SELECT SUBSTR(created_at, 1, 10) AS date,
           COUNT(*)                  AS tickets_opened
    FROM freshdesk.tickets
    GROUP BY SUBSTR(created_at, 1, 10)
)
SELECT
    COALESCE(n.date, t.date) AS date,
    COALESCE(n.notifications_sent, 0) AS notifications_sent,
    COALESCE(t.tickets_opened, 0)     AS tickets_opened
FROM daily_notifications n
FULL OUTER JOIN daily_tickets t ON t.date = n.date
ORDER BY date DESC;
```

## Notes

- All tables are strictly read-only.
- `novu.workflows`, `novu.subscribers`, and `novu.notifications` use 0-indexed page pagination. `novu.topics` uses the Novu v2 cursor API (`GET /v2/topics`, `after`/`limit` params) — Coral handles all pagination automatically.
- `novu.notifications` returns Novu's internal `_subscriberId` in `subscriber_id` — join to `novu.subscribers.id` (not `subscriber_id`) to get contact details.
- `novu.notifications.template_id` joins to `novu.workflows.id`.
- The `channels` column is a comma-separated string. Valid values: `email`, `sms`, `push`, `in_app`, `chat`.
- `NOVU_BASE_URL` defaults to `https://api.novu.co` for Novu Cloud. Override for self-hosted deployments.
- Rate limit handling: `429` responses are retried automatically via `Retry-After`.
