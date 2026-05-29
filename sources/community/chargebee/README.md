# Chargebee (Community)

**Version:** 0.1.0
**Backend:** HTTP (Chargebee REST API v2)
**Tables:** 5
**Base URL:** `https://{your-site}.chargebee.com/api/v2`

Query customers, subscriptions, invoices, plans, and transactions from
[Chargebee](https://www.chargebee.com/) using SQL. Designed for subscription
billing analytics: MRR tracking, churn analysis, failed payment audits, and
revenue reporting. Pairs naturally with the bundled **Stripe** source for
cross-platform payment coverage.

## Setup

### 1. Get your Chargebee API key

1. Log in to your Chargebee dashboard
2. Go to **Settings → API Keys & Webhooks**
3. Click **Add API Key** — use a read-only key where possible
4. Note your **site name** from the URL (e.g. `yoursite` from `yoursite.chargebee.com`)

> **Note:** Chargebee uses HTTP Basic Auth. The API key is sent as the
> username with a blank password. Both live and test site API keys are
> supported — use your test site key during development.

### 2. Set your credentials

```sh
export CHARGEBEE_SITE="https://yoursite.chargebee.com"
export CHARGEBEE_API_KEY="your_api_key_here"
```

### 3. Add the source

```sh
cargo run -p coral-cli -- source add --file sources/community/chargebee/manifest.yaml
```

Or interactively:

```sh
cargo run -p coral-cli -- source add --interactive --file sources/community/chargebee/manifest.yaml
```

### 4. Verify

```sh
cargo run -p coral-cli -- sql "SELECT id, email, status FROM chargebee.customers LIMIT 5"
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `chargebee.customers` | Customers in the account | — | — |
| `chargebee.subscriptions` | Active and historical subscriptions | — | — |
| `chargebee.invoices` | Invoices with status and payment details | — | — |
| `chargebee.plans` | Plans defined in Product Catalog v1 | — | — |
| `chargebee.transactions` | Payment transactions and refunds | — | — |

All tables are read-only. This source does not create, modify, or delete any
Chargebee data.

### `customers`

Lists all customers in the Chargebee account. The `id` column is the foreign
key used in `subscriptions`, `invoices`, and `transactions`.

### `subscriptions`

Lists all subscriptions. `status` holds the lifecycle state:

| Value | Meaning |
|---|---|
| `active` | Subscription is active and renewing |
| `in_trial` | Subscription is in a trial period |
| `non_renewing` | Subscription will not renew at term end |
| `paused` | Subscription is paused |
| `cancelled` | Subscription has been cancelled |

### `invoices`

Lists all invoices. `status` holds the payment state:

| Value | Meaning |
|---|---|
| `paid` | Invoice has been fully paid |
| `posted` | Invoice is awaiting payment |
| `payment_due` | Payment is due |
| `not_paid` | Payment was not collected |
| `voided` | Invoice has been voided |
| `pending` | Invoice is pending close |

`subscription_id` is null when consolidated invoicing is enabled and the
invoice spans multiple subscriptions.

### `plans`

Lists all plans using the Product Catalog v1 `/plans` endpoint. If your site
uses Product Catalog v2, the `item_prices` endpoint is the equivalent — this
is not yet included in this source.

### `transactions`

Lists all payment transactions. `type` holds the transaction kind:

| Value | Meaning |
|---|---|
| `payment` | A payment charge |
| `refund` | A refund |
| `authorization` | An authorization hold |
| `payment_reversal` | A reversal of a payment |

## Example queries

List active subscriptions with customer email:

```sql
SELECT
  s.id,
  s.status,
  s.plan_id,
  s.current_term_end,
  c.email,
  c.company
FROM chargebee.subscriptions s
JOIN chargebee.customers c ON c.id = s.customer_id
WHERE s.status = 'active'
ORDER BY s.current_term_end ASC
LIMIT 20;
```

Find unpaid invoices and their customers:

```sql
SELECT
  i.id,
  i.amount_due,
  i.currency_code,
  i.due_date,
  c.email,
  c.company
FROM chargebee.invoices i
JOIN chargebee.customers c ON c.id = i.customer_id
WHERE i.status = 'not_paid'
ORDER BY i.due_date ASC
LIMIT 20;
```

Monthly recurring revenue by plan:

```sql
SELECT
  p.name             AS plan_name,
  COUNT(s.id)        AS active_subscriptions,
  SUM(s.mrr)         AS total_mrr
FROM chargebee.subscriptions s
JOIN chargebee.plans p ON p.id = s.plan_id
WHERE s.status = 'active'
GROUP BY p.name
ORDER BY total_mrr DESC;
```

Recent failed transactions with customer email:

```sql
SELECT
  t.id,
  t.amount,
  t.currency_code,
  t.payment_method,
  t.date,
  c.email
FROM chargebee.transactions t
JOIN chargebee.customers c ON c.id = t.customer_id
WHERE t.status = 'failure'
ORDER BY t.date DESC
LIMIT 20;
```

Cancelled subscriptions in the last 30 days (churn report):

```sql
SELECT
  s.id,
  s.plan_id,
  s.mrr,
  s.cancelled_at,
  c.email,
  c.company
FROM chargebee.subscriptions s
JOIN chargebee.customers c ON c.id = s.customer_id
WHERE s.status = 'cancelled'
ORDER BY s.cancelled_at DESC
LIMIT 50;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/chargebee/manifest.yaml
```

Add the source:

```sh
export CHARGEBEE_SITE="https://yoursite.chargebee.com"
export CHARGEBEE_API_KEY="your_api_key_here"
cargo run -p coral-cli -- source add --file sources/community/chargebee/manifest.yaml
```

Validate each table. Replace `yoursite` with your Chargebee site name:

```sh
# customers
cargo run -p coral-cli -- sql "SELECT id, email, status FROM chargebee.customers LIMIT 5"

# subscriptions
cargo run -p coral-cli -- sql "SELECT id, status, plan_id, customer_id FROM chargebee.subscriptions LIMIT 5"

# invoices
cargo run -p coral-cli -- sql "SELECT id, status, amount_due, currency_code FROM chargebee.invoices LIMIT 5"

# plans
cargo run -p coral-cli -- sql "SELECT id, name, price, period_unit, status FROM chargebee.plans LIMIT 5"

# transactions
cargo run -p coral-cli -- sql "SELECT id, type, status, amount, currency_code FROM chargebee.transactions LIMIT 5"
```

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'chargebee'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'chargebee' ORDER BY table_name, ordinal_position"
```

## Notes

- **Currency amounts.** `amount_due`, `amount_paid`, `amount`, `price`, and
  `mrr` are all returned in the **smallest currency unit** (e.g. cents for
  USD). Divide by 100 in your query for display values.
- **Timestamps.** Chargebee returns timestamps as Unix epoch integers.
  Coral converts these to `Timestamp` columns automatically.
- **Cursor pagination.** Chargebee uses offset-based cursor pagination via
  `next_offset`. Coral handles this automatically — no manual pagination
  is needed.
- **Product Catalog v1 only.** The `plans` table uses the PC v1 `/plans`
  endpoint. Sites on Product Catalog v2 should use `item_prices` instead —
  deferred to a follow-up.
- **Rate limits.** Chargebee enforces per-site rate limits. Use `LIMIT`
  clauses during development to avoid hitting them on large accounts.
- **Test vs live sites.** Chargebee provides separate test and live sites
  with separate API keys. Set `CHARGEBEE_SITE` to your test site URL
  (`https://yoursite-test.chargebee.com`) during development.

## Out of scope for v1

- Product Catalog v2 `item_prices` table
- `addons` and `coupons` tables
- `orders` table
- Write operations of any kind
