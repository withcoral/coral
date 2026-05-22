# razorpay

A Coral community source that exposes your Razorpay account as queryable SQL tables — payments, orders, refunds, settlements, plans, subscriptions, and payment links. Built for Indian startups and SaaS teams that want to run AI-powered revenue analysis without writing custom API glue.

## Tables

| Table | API endpoint | Description |
|---|---|---|
| `razorpay.payments` | `GET /v1/payments` | Every payment attempt — captured, authorized, failed, or refunded |
| `razorpay.orders` | `GET /v1/orders` | Merchant-created order sessions before customer checkout |
| `razorpay.refunds` | `GET /v1/refunds` | All refunds issued across all payments |
| `razorpay.settlements` | `GET /v1/settlements` | Settlement batches transferred to your bank account |
| `razorpay.plans` | `GET /v1/plans` | Recurring billing plan definitions |
| `razorpay.subscriptions` | `GET /v1/subscriptions` | Active and historical recurring subscriptions |
| `razorpay.payment_links` | `GET /v1/payment_links` | Payment links created for no-code payment collection |

## Prerequisites

- A Razorpay account (free to sign up at [razorpay.com](https://razorpay.com))
- API credentials from **Dashboard → Settings → API Keys**
  - `Key ID` — starts with `rzp_live_` (production) or `rzp_test_` (test mode)
  - `Key Secret` — shown once on creation; store it somewhere safe

## Add this source

```bash
# Interactive — Coral will prompt for your Key ID and Key Secret
coral source add --interactive --file ./manifest.yaml

# Non-interactive — pass credentials as environment variables
RAZORPAY_KEY_ID=rzp_live_xxxx \
RAZORPAY_KEY_SECRET=your_secret \
coral source add --file ./manifest.yaml
```

## Validate

```bash
coral source test razorpay
```

Coral runs the declared `test_queries` and confirms the tested tables return rows. If you see authentication errors, re-check your Key ID and Secret.

## Example queries

### Revenue snapshot — last 30 days

```sql
SELECT
  currency,
  COUNT(*)                              AS total_payments,
  SUM(amount) / 100.0                   AS gross_revenue_inr,
  SUM(amount_refunded) / 100.0          AS total_refunded_inr,
  SUM(fee) / 100.0                      AS total_fees_inr,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status = 'captured')
    / NULLIF(COUNT(*), 0),
    2
  )                                     AS capture_rate_pct
FROM razorpay.payments
WHERE
  from = EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days')::INT
  AND to = EXTRACT(EPOCH FROM NOW())::INT
GROUP BY currency
ORDER BY gross_revenue_inr DESC;
```

### Failed payment breakdown by error reason

```sql
SELECT
  error_code,
  error_reason,
  error_source,
  COUNT(*)              AS failures,
  SUM(amount) / 100.0  AS lost_revenue_inr
FROM razorpay.payments
WHERE status = 'failed'
GROUP BY error_code, error_reason, error_source
ORDER BY failures DESC
LIMIT 20;
```

### Refund rate by payment method

```sql
SELECT
  p.method,
  COUNT(DISTINCT p.id)                  AS payments,
  COUNT(DISTINCT r.id)                  AS refunds,
  ROUND(
    100.0 * COUNT(DISTINCT r.id)
    / NULLIF(COUNT(DISTINCT p.id), 0),
    2
  )                                     AS refund_rate_pct,
  SUM(r.amount) / 100.0                 AS total_refunded_inr
FROM razorpay.payments p
LEFT JOIN razorpay.refunds r ON r.payment_id = p.id
WHERE p.status = 'captured'
GROUP BY p.method
ORDER BY refund_rate_pct DESC;
```

### Subscription health — active vs churned

```sql
SELECT
  s.status,
  COUNT(*)                    AS count,
  p.item__name                AS plan_name,
  p.item__amount / 100.0      AS plan_amount_inr,
  p.period                    AS billing_period
FROM razorpay.subscriptions s
JOIN razorpay.plans p ON p.id = s.plan_id
GROUP BY s.status, p.item__name, p.item__amount, p.period
ORDER BY count DESC;
```

### Monthly recurring revenue (MRR) estimate

```sql
SELECT
  p.item__name                                      AS plan_name,
  p.period,
  p.item__amount / 100.0                            AS unit_price_inr,
  COUNT(*) FILTER (WHERE s.status = 'active')       AS active_subscribers,
  -- normalise to monthly
  CASE p.period
    WHEN 'daily'   THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0) * 30
    WHEN 'weekly'  THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0) * 4
    WHEN 'monthly' THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0)
    WHEN 'yearly'  THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0) / 12
  END                                               AS estimated_mrr_inr
FROM razorpay.subscriptions s
JOIN razorpay.plans p ON p.id = s.plan_id
GROUP BY p.item__name, p.period, p.item__amount
ORDER BY estimated_mrr_inr DESC NULLS LAST;
```

### Settlement reconciliation — fees vs gross

```sql
SELECT
  DATE_TRUNC('week', created_at)   AS week,
  COUNT(*)                         AS settlements,
  SUM(amount) / 100.0              AS net_settled_inr,
  SUM(fees) / 100.0                AS total_fees_inr,
  SUM(tax) / 100.0                 AS total_gst_inr,
  ROUND(100.0 * SUM(fees) / NULLIF(SUM(amount + fees), 0), 3) AS effective_fee_rate_pct
FROM razorpay.settlements
GROUP BY week
ORDER BY week DESC;
```

### Payment links — conversion funnel

```sql
SELECT
  status,
  COUNT(*)               AS links,
  SUM(amount) / 100.0    AS total_amount_inr,
  SUM(amount_paid) / 100.0 AS total_collected_inr
FROM razorpay.payment_links
GROUP BY status
ORDER BY links DESC;
```

### UPI vs card — volume and average order value

```sql
SELECT
  method,
  COUNT(*)                        AS payment_count,
  ROUND(AVG(amount) / 100.0, 2)   AS avg_amount_inr,
  SUM(amount) / 100.0             AS total_volume_inr
FROM razorpay.payments
WHERE status = 'captured'
GROUP BY method
ORDER BY total_volume_inr DESC;
```

## Filters

All tables that support time-range filtering accept `from` and `to` as Unix epoch seconds in the `WHERE` clause:

```sql
-- Payments in January 2025
SELECT id, amount, status
FROM razorpay.payments
WHERE from = 1735689600 AND to = 1738367999;
```

The `razorpay.subscriptions` table also accepts a `plan_id` filter:

```sql
SELECT id, status, paid_count
FROM razorpay.subscriptions
WHERE plan_id = 'plan_XXXXXXXXXXXXXXXX';
```

## Cross-source joins

Because Coral normalises every source into SQL tables, you can join Razorpay data with other installed sources in a single query. For example, if you have the GitHub source installed:

```sql
-- Correlate your deployment activity with payment spikes
SELECT
  DATE_TRUNC('day', p.created_at)  AS day,
  COUNT(p.id)                      AS payments,
  SUM(p.amount) / 100.0            AS revenue_inr,
  COUNT(pr.number)                 AS merged_prs
FROM razorpay.payments p
LEFT JOIN github.pulls pr
  ON DATE_TRUNC('day', pr.merged_at) = DATE_TRUNC('day', p.created_at)
  AND pr.owner = 'your-org'
  AND pr.repo  = 'your-repo'
  AND pr.state = 'closed'
WHERE p.status = 'captured'
GROUP BY day
ORDER BY day DESC;
```

## Amounts and currency

All monetary values are returned by Razorpay in the **smallest currency unit** — paise for INR, cents for USD. Divide by 100 to get the display amount:

```sql
SELECT id, amount / 100.0 AS amount_inr FROM razorpay.payments LIMIT 5;
```

## Test mode vs live mode

Razorpay uses the same API endpoint for both modes. The credentials determine which data you see:

- `rzp_test_*` keys → test/sandbox data only
- `rzp_live_*` keys → real production data

Use test keys to validate queries before running them against live data.

## Rate limits

Razorpay enforces API rate limits per key. Coral respects `Retry-After` and `X-RateLimit-*` response headers automatically and retries on 429 responses. For large date ranges, prefer narrow `from`/`to` windows to avoid hitting per-request result caps.

## Supported Coral version

Requires Coral `dsl_version: 3` (available from v0.2.0 onwards).

## Contributing

Issues and pull requests are welcome in the [withcoral/coral](https://github.com/withcoral/coral) repository. If you find an endpoint or column missing, open an issue or submit a PR against `sources/community/razorpay/`.