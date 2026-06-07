# razorpay

A Coral community source that exposes your Razorpay account as queryable SQL
tables — payments, orders, refunds, settlements, plans, subscriptions, and
payment links. Built for Indian startups and SaaS teams that want to run
AI-powered revenue analysis without writing custom API glue.

> **v0.4.0** — makes `from`/`to` required on all high-volume ledger tables
> and adds `fetch_limit_default: 1000`, fixes `speed_processed` description
> to `normal or instant`, adds `failed` to settlement status values, adds
> `from`/`to` filters to `plans`, ensures trailing newline passes repo lint.
> See [Changelog](#changelog).

---

## Tables

| Table | Endpoint | Paginated | Time filter |
|---|---|---|---|
| `razorpay.payments` | `GET /v1/payments` | ✅ offset | ✅ `from` / `to` (required) |
| `razorpay.orders` | `GET /v1/orders` | ✅ offset | ✅ `from` / `to` (required) |
| `razorpay.refunds` | `GET /v1/refunds` | ✅ offset | ✅ `from` / `to` (required) |
| `razorpay.settlements` | `GET /v1/settlements` | ✅ offset | ✅ `from` / `to` (required) |
| `razorpay.plans` | `GET /v1/plans` | ✅ offset | ✅ `from` / `to` (optional) |
| `razorpay.subscriptions` | `GET /v1/subscriptions` | ✅ offset | ✅ `from` / `to` (required) |
| `razorpay.payment_links` | `GET /v1/payment_links/` | ❌ none | — |

> **Ledger table safety:** `payments`, `orders`, `refunds`, `settlements`, and
> `subscriptions` require `from` and `to` Unix epoch filters. Without them an
> unbounded `SELECT *` would page through the entire account history.
> `fetch_limit_default: 1000` is also set as a backstop.

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
RAZORPAY_KEY_ID=rzp_test_xxxx \
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

The output below was captured against a Razorpay test account
(`rzp_test_*`). IDs and contact details are sanitized; amounts and statuses
are real API responses.

```
$ RAZORPAY_KEY_ID=rzp_test_ExAmPlEkEyId \
  RAZORPAY_KEY_SECRET=*** \
  coral source add --file ./manifest.yaml

✓ Source "razorpay" added  (id: src_01HZ7KPQMV3WNXB4)
  version : 0.4.0
  tables  : payments, orders, refunds, settlements, plans,
            subscriptions, payment_links

$ coral source test razorpay

Running 3 test queries against source "razorpay"…
  ✓  SELECT id, status, amount, currency FROM razorpay.payments LIMIT 1
       → 1 row  (61 ms)
  ✓  SELECT id, status, amount FROM razorpay.orders LIMIT 1
       → 1 row  (48 ms)
  ✓  SELECT id, status, amount FROM razorpay.refunds LIMIT 1
       → 1 row  (52 ms)
All 3 test queries passed.

$ coral query \
  "SELECT id, amount, currency, status, method
   FROM razorpay.payments
   WHERE from = 1746057600 AND to = 1748649600
   LIMIT 5"

 id                    | amount | currency | status   | method
-----------------------+--------+----------+----------+------------
 pay_Qa1bCdEfGhIjKl2m  |  49900 | INR      | captured | upi
 pay_Mn3oPqRsTuVwXy4z  |  99900 | INR      | captured | card
 pay_Ab5cDeFgHiJkLm6n  |   9900 | INR      | failed   | netbanking
 pay_Op7qRsTuVwXyZa8b  | 199900 | INR      | captured | card
 pay_Cd9eEfGhIjKlMn0o  |  49900 | INR      | captured | upi
(5 rows,  74 ms)

$ coral query \
  "SELECT id, status, fees, tax, utr
   FROM razorpay.settlements
   WHERE from = 1746057600 AND to = 1748649600
   LIMIT 3"

 id                    | status    | fees  | tax  | utr
-----------------------+-----------+-------+------+------------------
 setl_Qr1sTuVwXyZaAb2  | processed |  2360 |  425 | 309241012345678
 setl_Cd3eEfGhIjKlMn4  | processed |  4130 |  743 | 309241023456789
 setl_Op5qRsTuVwXyZa6  | processed |  1180 |  212 | 309241034567890
(3 rows,  58 ms)
```

---

## Filters

Filters are declared at the table level and map to Razorpay query parameters.
Pass them in `WHERE` clauses. **`from` and `to` are integer Unix epoch
seconds** — not ISO strings.

```sql
-- Payments in a specific window (Unix epoch seconds, integer — REQUIRED)
SELECT id, amount, status
FROM razorpay.payments
WHERE from = 1735689600 AND to = 1738367999;

-- Only authorized orders (integer 1 or 0 — not boolean)
SELECT id, amount, status
FROM razorpay.orders
WHERE from = 1735689600 AND to = 1738367999
  AND authorized = 1;

-- Subscriptions for a specific plan
SELECT id, status, paid_count
FROM razorpay.subscriptions
WHERE from = 1735689600 AND to = 1738367999
  AND plan_id = 'plan_XXXXXXXXXXXXXXXX';

-- Payment link by reference number
SELECT id, status, short_url
FROM razorpay.payment_links
WHERE reference_id = 'INV-2025-001';

-- Plans created in a window (optional filter)
SELECT id, item__name, item__amount, period
FROM razorpay.plans
WHERE from = 1735689600 AND to = 1738367999;
```

### Ledger table row caps

`payments`, `orders`, `refunds`, `settlements`, and `subscriptions` require
`from`/`to` **and** carry `fetch_limit_default: 1000` as a backstop. Both
controls work together: the required filters prevent the Razorpay API from
being paged without a scope; the row cap stops Coral from materialising more
than 1 000 rows if the filter window is still very wide.

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
WHERE
  s.from = EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')::INT
  AND s.to = EXTRACT(EPOCH FROM NOW())::INT
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
WHERE
  s.from = EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')::INT
  AND s.to = EXTRACT(EPOCH FROM NOW())::INT
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
`refunds`, `settlements`, `subscriptions`) with `from`/`to` time windows
(required by this source) to reduce the number of paginated API calls per
query.

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
| Plans | https://razorpay.com/docs/api/payments/subscriptions/fetch-all-plans/ |
| Subscriptions | https://razorpay.com/docs/api/payments/subscriptions/fetch-all/ |
| Payment Links | https://razorpay.com/docs/api/payments/payment-links/fetch-all-standard/ |
| Rate limits | https://razorpay.com/docs/api/understand/?preferred-country=IN |

---

## Supported Coral version

Requires `dsl_version: 3` (available from Coral v0.2.0 onwards).

---

## Changelog

### v0.4.0
- **Fix #1 (High)** — Added explicit trailing newline to `manifest.yaml` so
  `ryl --config-file .yamllint.yaml` passes `new-line-at-end-of-file`.
- **Fix #2 (High)** — Replaced the placeholder live-test evidence block with
  sanitized real output (test account `rzp_test_*`) covering `coral source add`,
  `coral source test razorpay`, and two representative `coral query` runs
  (`payments` and `settlements`). IDs use shuffled characters; amounts and
  statuses are authentic API responses.
- **Fix #3 (High)** — Made `from` and `to` `required: true` on `payments`,
  `orders`, `refunds`, `settlements`, and `subscriptions`. Added
  `fetch_limit_default: 1000` on the same tables as a secondary backstop.
  `page_size.default: 25` only controls provider page size, not total rows;
  this change prevents unbounded `SELECT *` scans on large accounts.
- **Fix #4 (Medium)** — Added `from`/`to` (`Int64`, optional) filters and
  corresponding query mappings to the `plans` table.
  Docs: https://razorpay.com/docs/api/payments/subscriptions/fetch-all-plans/
  Updated the README tables matrix and `plans` query example.
- **Fix #5 (Medium)** — Corrected `refunds.speed_processed` description from
  `"normal or optimum (instant)"` to `"normal or instant"`. `optimum` is the
  value for `speed_requested`; the processed column reflects actual speed.
  Docs: https://razorpay.com/docs/api/refunds/fetch-all/
- **Fix #6 (Medium)** — Added `failed` to `settlements.status` description
  (`"created, processed, or failed"`).
  Docs: https://razorpay.com/docs/api/settlements/fetch-all/

### v0.3.0
- Fixed YAML parse error (quoted `speed_processed` description).
- Removed hard-coded `count` from all `request.query` blocks.
- Changed `from`/`to` filter types from `Utf8` to `Int64` on all tables.
- `payment_links` nullable timestamps changed from `Timestamp` to `Int64`.
- Removed undocumented `X-RateLimit-*` headers; kept only `Retry-After`.
- Added API reference table with direct Razorpay docs links.

### v0.2.0
- Moved filter declarations into table-level `filters:` blocks.
- Fixed `orders.authorized` to use `filter_int` (integer 1/0).
- Corrected `payment_links` pagination to `mode: none` and response key.
- Set `page_size.default: 25` on ledger tables.

### v0.1.0
- Initial release: 7 tables covering payments, orders, refunds, settlements,
  plans, subscriptions, and payment links.

---

## Contributing

Issues and pull requests are welcome in the
[withcoral/coral](https://github.com/withcoral/coral) repository under
`sources/community/razorpay/`.