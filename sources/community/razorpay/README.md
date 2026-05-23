# razorpay

A Coral community source that exposes your Razorpay account as queryable SQL
tables — payments, orders, refunds, settlements, plans, subscriptions, and
payment links. Built for Indian startups and SaaS teams that want to run
AI-powered revenue analysis without writing custom API glue.

> **v0.2.0** — fixes lint errors, corrects the `authorized` filter type,
> removes unsupported pagination from `payment_links`, and adds conservative
> `fetch_limit_default` on ledger tables. See [Changelog](#changelog).

---

## Tables

| Table | Endpoint | Paginated | Time filter |
|---|---|---|---|
| `razorpay.payments` | `GET /v1/payments` | ✅ offset | ✅ `from` / `to` |
| `razorpay.orders` | `GET /v1/orders` | ✅ offset | ✅ `from` / `to` |
| `razorpay.refunds` | `GET /v1/refunds` | ✅ offset | ✅ `from` / `to` |
| `razorpay.settlements` | `GET /v1/settlements` | ✅ offset | ✅ `from` / `to` |
| `razorpay.plans` | `GET /v1/plans` | ✅ offset | — |
| `razorpay.subscriptions` | `GET /v1/subscriptions` | ✅ offset | ✅ `from` / `to` |
| `razorpay.payment_links` | `GET /v1/payment_links/` | ❌ none | — |

> **payment_links note:** Razorpay's list endpoint for Payment Links does not
> support `count`/`skip` or time-range parameters. Coral fetches the full
> list in one call. Use `payment_id` or `reference_id` filters to narrow
> results when you need a specific link.

---

## Prerequisites

- A Razorpay account — sign up at [razorpay.com](https://razorpay.com)
- API credentials from **Dashboard → Settings → API Keys**
  - `Key ID` — starts with `rzp_live_` (production) or `rzp_test_` (test mode)
  - `Key Secret` — shown once on creation; store it in a password manager

---

## Install

```bash
# Interactive — Coral prompts for Key ID and Key Secret
coral source add --interactive --file ./manifest.yaml

# Non-interactive — pass credentials as environment variables
RAZORPAY_KEY_ID=rzp_live_xxxx \
RAZORPAY_KEY_SECRET=your_secret \
coral source add --file ./manifest.yaml
```

### Validate

```bash
coral source test razorpay
```

Coral runs the built-in `test_queries` and confirms all three tables return
rows. Authentication errors mean a Key ID / Secret mismatch — regenerate
from the Dashboard and re-add.

---

## Filters

Filters are declared at the table level and map to Razorpay query parameters.
Pass them in `WHERE` clauses:

```sql
-- Payments in a specific window (Unix epoch seconds)
SELECT id, amount, status
FROM razorpay.payments
WHERE from = 1735689600 AND to = 1738367999;

-- Only authorized orders (integer 1 or 0 — not boolean)
SELECT id, amount, status
FROM razorpay.orders
WHERE authorized = 1;

-- Subscriptions for a specific plan
SELECT id, status, paid_count
FROM razorpay.subscriptions
WHERE plan_id = 'plan_XXXXXXXXXXXXXXXX';

-- Payment link by reference number
SELECT id, status, short_url
FROM razorpay.payment_links
WHERE reference_id = 'INV-2025-001';
```

### Safe defaults on ledger tables

Payments, orders, refunds, and settlements default to pages of **25 rows**
without a time filter. For accounts with large transaction volumes always add
a `from`/`to` window to keep query times predictable.

---

## Example queries

All monetary values are in the **smallest currency unit** — paise for INR,
cents for USD. Divide by 100 for display amounts.

### Revenue snapshot — last 30 days

```sql
SELECT
  currency,
  COUNT(*)                                                    AS total_payments,
  SUM(amount) / 100.0                                         AS gross_revenue,
  SUM(amount_refunded) / 100.0                                AS total_refunded,
  SUM(fee) / 100.0                                            AS total_fees,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status = 'captured')
    / NULLIF(COUNT(*), 0), 2
  )                                                           AS capture_rate_pct
FROM razorpay.payments
WHERE
  from = EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days')::INT
  AND to = EXTRACT(EPOCH FROM NOW())::INT
GROUP BY currency
ORDER BY gross_revenue DESC;
```

### Failed payment breakdown by error reason

```sql
SELECT
  error_code,
  error_reason,
  error_source,
  COUNT(*)              AS failures,
  SUM(amount) / 100.0  AS lost_revenue
FROM razorpay.payments
WHERE
  status = 'failed'
  AND from = EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days')::INT
  AND to   = EXTRACT(EPOCH FROM NOW())::INT
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
    / NULLIF(COUNT(DISTINCT p.id), 0), 2
  )                                     AS refund_rate_pct,
  SUM(r.amount) / 100.0                 AS total_refunded
FROM razorpay.payments p
LEFT JOIN razorpay.refunds r ON r.payment_id = p.id
WHERE
  p.status = 'captured'
  AND p.from = EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')::INT
  AND p.to   = EXTRACT(EPOCH FROM NOW())::INT
GROUP BY p.method
ORDER BY refund_rate_pct DESC;
```

### Subscription health — active vs churned

```sql
SELECT
  s.status,
  COUNT(*)                    AS count,
  p.item__name                AS plan_name,
  p.item__amount / 100.0      AS plan_price,
  p.period                    AS billing_period
FROM razorpay.subscriptions s
JOIN razorpay.plans p ON p.id = s.plan_id
GROUP BY s.status, p.item__name, p.item__amount, p.period
ORDER BY count DESC;
```

### Monthly recurring revenue (MRR) estimate

```sql
SELECT
  p.item__name                                        AS plan_name,
  p.period,
  p.item__amount / 100.0                              AS unit_price,
  COUNT(*) FILTER (WHERE s.status = 'active')         AS active_subscribers,
  CASE p.period
    WHEN 'daily'   THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0) * 30
    WHEN 'weekly'  THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0) * 4
    WHEN 'monthly' THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0)
    WHEN 'yearly'  THEN COUNT(*) FILTER (WHERE s.status = 'active') * (p.item__amount / 100.0) / 12
  END                                                 AS estimated_mrr
FROM razorpay.subscriptions s
JOIN razorpay.plans p ON p.id = s.plan_id
GROUP BY p.item__name, p.period, p.item__amount
ORDER BY estimated_mrr DESC NULLS LAST;
```

### Settlement reconciliation — fees vs gross

```sql
SELECT
  DATE_TRUNC('week', created_at)                                  AS week,
  COUNT(*)                                                        AS settlements,
  SUM(amount) / 100.0                                             AS net_settled,
  SUM(fees) / 100.0                                               AS total_fees,
  SUM(tax) / 100.0                                                AS total_gst,
  ROUND(100.0 * SUM(fees) / NULLIF(SUM(amount + fees), 0), 3)    AS effective_fee_rate_pct
FROM razorpay.settlements
WHERE
  from = EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')::INT
  AND to = EXTRACT(EPOCH FROM NOW())::INT
GROUP BY week
ORDER BY week DESC;
```

### Payment link conversion funnel

```sql
SELECT
  status,
  COUNT(*)                  AS links,
  SUM(amount) / 100.0       AS total_amount,
  SUM(amount_paid) / 100.0  AS total_collected
FROM razorpay.payment_links
GROUP BY status
ORDER BY links DESC;
```

### UPI vs card — volume and average order value

```sql
SELECT
  method,
  COUNT(*)                         AS payment_count,
  ROUND(AVG(amount) / 100.0, 2)   AS avg_amount,
  SUM(amount) / 100.0              AS total_volume
FROM razorpay.payments
WHERE
  status = 'captured'
  AND from = EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days')::INT
  AND to   = EXTRACT(EPOCH FROM NOW())::INT
GROUP BY method
ORDER BY total_volume DESC;
```

---

## Cross-source joins

Coral normalises every source into SQL, so you can join Razorpay data with
other installed sources in a single query:

```sql
-- Correlate deployment activity with payment spikes (requires GitHub source)
SELECT
  DATE_TRUNC('day', p.created_at)  AS day,
  COUNT(p.id)                      AS payments,
  SUM(p.amount) / 100.0            AS revenue,
  COUNT(pr.number)                 AS merged_prs
FROM razorpay.payments p
LEFT JOIN github.pulls pr
  ON DATE_TRUNC('day', pr.merged_at) = DATE_TRUNC('day', p.created_at)
  AND pr.owner = 'your-org'
  AND pr.repo  = 'your-repo'
  AND pr.state = 'closed'
WHERE
  p.status = 'captured'
  AND p.from = EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days')::INT
  AND p.to   = EXTRACT(EPOCH FROM NOW())::INT
GROUP BY day
ORDER BY day DESC;
```

---

## Test mode vs live mode

Razorpay uses the same API base URL for both environments. Your key prefix
determines which dataset is returned:

- `rzp_test_*` → sandbox / test data
- `rzp_live_*` → real production data

Use test credentials to validate your queries before switching to live keys.

---

## Rate limits

Razorpay enforces per-key API rate limits. Coral automatically respects
`Retry-After` and `X-RateLimit-*` response headers and backs off on 429
responses. For large accounts always use `from`/`to` time windows to keep
per-query page counts manageable.

---

## Supported Coral version

Requires `dsl_version: 3` (available from Coral v0.2.0 onwards).

---

## Changelog

### v0.2.0
- **Fix #1** — Moved all `required`/optional filter declarations from
  `request.query` into table-level `filters:` blocks. `request.query` entries
  now only map filters to provider parameters, which is what `coral source lint`
  expects.
- **Fix #2** — Changed `orders.authorized` from `from: filter_bool` to
  `from: filter_int`. Razorpay documents this param as integer `1` or `0`;
  `filter_bool` was incorrectly sending `true` / `false`.
- **Fix #3** — `payment_links` no longer declares `from`/`to` filters or
  `count`/`skip` pagination. Razorpay's list endpoint for Payment Links only
  accepts `payment_id` and `reference_id`. Pagination mode set to `none` to
  prevent duplicate rows. Response key corrected to `payment_links` (not
  `items`).
- **Fix #4** — `payments`, `orders`, `refunds`, and `settlements` now default
  to page size 25 (`fetch_limit_default: 25`) to avoid runaway fetches on
  large accounts when no time filter is provided. README examples updated to
  include `from`/`to` on all ledger queries.

### v0.1.0
- Initial release: 7 tables covering payments, orders, refunds, settlements,
  plans, subscriptions, and payment links.

---

## Contributing

Issues and pull requests are welcome in the
[withcoral/coral](https://github.com/withcoral/coral) repository under
`sources/community/razorpay/`.