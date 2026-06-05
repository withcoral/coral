# razorpay

A Coral community source that exposes your Razorpay account as queryable SQL
tables — payments, orders, refunds, settlements, plans, subscriptions, and
payment links. Built for Indian startups and SaaS teams that want to run
AI-powered revenue analysis without writing custom API glue.

> **v0.3.0** — fixes YAML parse error on `speed_processed` description,
> removes hard-coded `count` from all query blocks (now owned exclusively by
> pagination), corrects `from`/`to`/`authorized` filter types to `Int64`,
> exposes `payment_links` nullable-timestamp fields as raw `Int64` (Razorpay
> returns `0` for unset), removes undocumented `X-RateLimit-*` header claims,
> and adds required links to provider API docs. See [Changelog](#changelog).

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
- **Authentication docs:** https://razorpay.com/docs/api/authentication/

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

## Live-test evidence

> **Maintainer:** Replace the placeholder block below with sanitized output
> from a real `rzp_test_*` account before submitting for review. The three
> commands shown are the minimum required by Coral's contribution guidelines.

```
# coral source add (sanitized — Key Secret redacted)
$ RAZORPAY_KEY_ID=rzp_test_xxxxxxxxxxxx \
  RAZORPAY_KEY_SECRET=*** \
  coral source add --file ./manifest.yaml

✓ Source "razorpay" added (id: src_xxxxxxxx)

# coral source test
$ coral source test razorpay

Running test queries…
  ✓ SELECT id, status, amount, currency FROM razorpay.payments LIMIT 1  (1 row, 42 ms)
  ✓ SELECT id, status, amount FROM razorpay.orders LIMIT 1              (1 row, 38 ms)
  ✓ SELECT id, status, amount FROM razorpay.refunds LIMIT 1             (1 row, 35 ms)
All 3 test queries passed.

# coral query spot-check
$ coral query "SELECT id, amount, status FROM razorpay.payments LIMIT 3"

 id                   | amount | status
----------------------+--------+----------
 pay_XXXXXXXXXXXXXXXX |  49900 | captured
 pay_YYYYYYYYYYYYYYYY |  99900 | captured
 pay_ZZZZZZZZZZZZZZZZ |   9900 | failed
(3 rows)
```

---

## Filters

Filters are declared at the table level and map to Razorpay query parameters.
Pass them in `WHERE` clauses. **`from` and `to` are integer Unix epoch
seconds** — not ISO strings.

```sql
-- Payments in a specific window (Unix epoch seconds, integer)
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

### payment_links — zero timestamps

`cancelled_at`, `expire_by`, and `expired_at` are `Int64` columns (raw Unix
epoch seconds). Razorpay returns `0` — not `null` — when these are unset.
Filter accordingly:

```sql
-- Only links with a real expiry set
SELECT id, short_url, expire_by
FROM razorpay.payment_links
WHERE expire_by > 0;
```

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

Razorpay enforces per-key API rate limits and returns HTTP `429` when a limit
is exceeded. Coral is configured to respect the `Retry-After` response header
and will back off automatically on `429` responses.

For large accounts, always scope ledger queries (`payments`, `orders`,
`refunds`, `settlements`) with `from`/`to` time windows to reduce the number
of paginated API calls per query.

- **Rate limit docs:** https://razorpay.com/docs/api/understand/?preferred-country=IN

---

## API reference

| Resource | Razorpay docs |
|---|---|
| Authentication | https://razorpay.com/docs/api/authentication/ |
| Payments | https://razorpay.com/docs/api/payments/fetch-all-payments/ |
| Orders | https://razorpay.com/docs/api/orders/fetch-all/ |
| Refunds | https://razorpay.com/docs/api/refunds/fetch-all/ |
| Settlements | https://razorpay.com/docs/api/settlements/fetch-all/ |
| Plans | https://razorpay.com/docs/api/payments/route/plans/fetch-all/ |
| Subscriptions | https://razorpay.com/docs/api/payments/subscriptions/fetch-all/ |
| Payment Links | https://razorpay.com/docs/api/payments/payment-links/fetch-all-standard/ |
| Rate limits | https://razorpay.com/docs/api/understand/?preferred-country=IN |

---

## Supported Coral version

Requires `dsl_version: 3` (available from Coral v0.2.0 onwards).

---

## Changelog

### v0.3.0
- **Fix #1 (High)** — Quoted the `speed_processed` description
  (`"Actual refund speed used: normal or optimum (instant)"`) so the manifest
  is valid YAML and passes `coral source lint`.
- **Fix #2 (High)** — Removed hard-coded `count: 100` from all `request.query`
  blocks (`payments`, `orders`, `refunds`, `settlements`, `plans`,
  `subscriptions`). `count` is now owned exclusively by each table's
  `pagination.page_size.query_param`, which lets Coral lower the provider page
  size when a SQL `LIMIT` is smaller than the max, eliminating duplicate /
  conflicting `count` parameters.
- **Fix #3 (High)** — Added a **Live-test evidence** section with a sanitized
  template covering `coral source add`, `coral source test`, and `coral query`.
  Maintainers must replace the placeholder with real output before merging.
- **Fix #4 (Medium)** — Changed `from` and `to` filter types from `Utf8` to
  `Int64` on all tables that accept them (`payments`, `orders`, `refunds`,
  `settlements`, `subscriptions`). The `authorized` filter on `orders` was
  already typed correctly as integer (`filter_int`); its filter declaration is
  now `Int64` to match.
- **Fix #5 (Medium)** — `payment_links.cancelled_at`, `expire_by`, and
  `expired_at` are now `Int64` (raw Unix epoch seconds) instead of `Timestamp`.
  Razorpay returns `0` for unset values; mapping them as `Timestamp` would
  have surfaced `1970-01-01` as a misleading "cancelled" date. Consumers
  should treat `0` as not-set; the README includes a filter example.
- **Fix #6 (Medium)** — Removed the undocumented `X-RateLimit-Remaining` and
  `X-RateLimit-Reset` headers from `rate_limit:` (Razorpay does not publish
  these). Only `Retry-After` is retained, which Razorpay does honour on 429
  responses. Added a dedicated **API reference** table with direct links to
  Razorpay docs for auth, each endpoint, and rate limits.

### v0.2.0
- **Fix #1** — Moved all `required`/optional filter declarations from
  `request.query` into table-level `filters:` blocks.
- **Fix #2** — Changed `orders.authorized` from `from: filter_bool` to
  `from: filter_int`.
- **Fix #3** — `payment_links` no longer declares `from`/`to` filters or
  `count`/`skip` pagination. Pagination mode set to `none`. Response key
  corrected to `payment_links`.
- **Fix #4** — `payments`, `orders`, `refunds`, and `settlements` default to
  page size 25 to avoid runaway fetches on large accounts.

### v0.1.0
- Initial release: 7 tables covering payments, orders, refunds, settlements,
  plans, subscriptions, and payment links.

---

## Contributing

Issues and pull requests are welcome in the
[withcoral/coral](https://github.com/withcoral/coral) repository under
`sources/community/razorpay/`.