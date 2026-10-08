# Dodo Payments

**Version:** 0.1.0
**Backend:** HTTP (Dodo Payments REST API v1.105.4)
**Tables:** 6
**Base URL:** `https://live.dodopayments.com` (override with `DODO_BASE_URL`)

Query Dodo Payments transactions, customers, subscriptions, refunds, disputes,
and payouts through Coral SQL. Provides read-only access to payment and billing
data for SaaS and AI products using Dodo Payments as their merchant of record.

## Installation

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/dodopayments/manifest.yaml
```

## Credentials

Dodo Payments uses **Bearer token authentication** with an API key.

1. Sign in to the [Dodo Payments Dashboard](https://app.dodopayments.com).
2. Navigate to **Developer → API Keys**.
3. Click **Add API Key**.
4. Provide a descriptive name and **uncheck "Enable write access"** for
   read-only access (recommended for Coral).
5. Copy the generated key immediately.

> **Note:** Keys prefixed with `dodo_test_` are for **test mode** at
> `test.dodopayments.com`; `dodo_live_` are for **live mode** at
> `live.dodopayments.com`.

Provide the values as environment variables or when prompted by
`coral source add`:

```bash
export DODO_API_KEY="dodo_test_your_key_here"
export DODO_BASE_URL="https://test.dodopayments.com"
```

For live mode, omit `DODO_BASE_URL` (it defaults to `https://live.dodopayments.com`):

```bash
export DODO_API_KEY="dodo_live_your_key_here"
coral source add --file sources/community/dodopayments/manifest.yaml
```

## Quick Start

```sql
-- Recent succeeded payments with customer info
SELECT payment_id, status, total_amount, currency,
       customer_id, customer_name, customer_email,
       refund_status, dispute_status, created_at
FROM dodopayments.payments
WHERE status = 'succeeded'
ORDER BY created_at DESC
LIMIT 10;

-- Payments with open disputes (no join to disputes table required)
SELECT payment_id, customer_email, total_amount, currency,
       dispute_status, created_at
FROM dodopayments.payments
WHERE dispute_status NOT IN ('dispute_won', 'dispute_lost',
                             'dispute_cancelled', 'dispute_expired')
  AND dispute_status IS NOT NULL
ORDER BY created_at DESC
LIMIT 10;

-- Active subscriptions with next billing date
SELECT subscription_id, customer_id, product_id, status,
       next_billing_date, recurring_pre_tax_amount, currency
FROM dodopayments.subscriptions
WHERE status = 'active'
ORDER BY next_billing_date ASC
LIMIT 10;

-- Customer directory
SELECT customer_id, name, email, phone_number, created_at
FROM dodopayments.customers
ORDER BY created_at DESC
LIMIT 20;

-- Recent refunds
SELECT refund_id, payment_id, status, amount, currency,
       is_partial, reason, created_at
FROM dodopayments.refunds
ORDER BY created_at DESC
LIMIT 10;

-- Open disputes
SELECT dispute_id, payment_id, amount, currency,
       dispute_status, dispute_stage, created_at
FROM dodopayments.disputes
WHERE dispute_status NOT IN ('dispute_won', 'dispute_lost',
                             'dispute_cancelled', 'dispute_expired')
ORDER BY created_at DESC
LIMIT 10;

-- Payout settlement summary
SELECT payout_id, status, amount, currency, fee, tax,
       refunds, chargebacks, created_at, updated_at
FROM dodopayments.payouts
ORDER BY created_at DESC
LIMIT 10;
```

## Tables

### `payments`

Payments processed through Dodo Payments, including one-time purchases and
subscription renewals. Join to `customers` via `customer_id` or
`customer_email`.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `created_at_gte` | Utf8 | | Payments created on or after this timestamp (ISO 8601) |
| `created_at_lte` | Utf8 | | Payments created on or before this timestamp (ISO 8601) |
| `status` | Utf8 | | Payment status: `succeeded`, `failed`, `cancelled`, `processing`, `requires_customer_action`, `requires_merchant_action`, `requires_payment_method`, `requires_confirmation`, `requires_capture`, `partially_captured`, `partially_captured_and_capturable` |
| `customer_id` | Utf8 | | Filter by customer ID |
| `subscription_id` | Utf8 | | Filter by subscription ID |
| `brand_id` | Utf8 | | Filter by brand ID |
| `product_id` | Utf8 | | Filter by product ID |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `payment_id` | Utf8 | Unique payment identifier |
| `status` | Utf8 | Payment intent status |
| `total_amount` | Int64 | Total amount in currency's smallest unit (e.g. cents for USD) |
| `currency` | Utf8 | ISO 4217 currency code |
| `customer_id` | Utf8 | Customer identifier |
| `customer_name` | Utf8 | Customer name |
| `customer_email` | Utf8 | Customer email (primary join key for cross-source queries) |
| `created_at` | Utf8 | Payment creation timestamp (ISO 8601) |
| `subscription_id` | Utf8 | Subscription ID if this is a subscription payment |
| `invoice_id` | Utf8 | Invoice identifier (India-specific if available) |
| `invoice_url` | Utf8 | URL to download the invoice PDF for this payment |
| `payment_provider` | Utf8 | Processor — `stripe`, `adyen`, or `dodo` |
| `payment_method` | Utf8 | Payment method e.g. `card`, `bank_transfer` |
| `payment_method_type` | Utf8 | Specific type e.g. `visa`, `mastercard` |
| `card_last_four` | Utf8 | Last four digits of the card |
| `card_network` | Utf8 | Card network e.g. `VISA`, `MASTERCARD` |
| `brand_id` | Utf8 | Brand identifier this payment belongs to |
| `digital_products_delivered` | Boolean | Whether digital products have been delivered |
| `has_license_key` | Boolean | Whether this payment includes a license key |
| `refund_status` | Utf8 | `partial`, `full`, or null if no refunds |
| `dispute_status` | Utf8 | Most recent dispute status for this payment, or null if none (`dispute_opened`, `dispute_won`, etc.) |
| `metadata` | Json | Additional metadata as a JSON object |

---

### `customers`

Customers in your Dodo Payments account. Join to `payments` and
`subscriptions` via `customer_id`. For `refunds` and `disputes`, use
`WHERE customer_id = ...` to push the API filter, or join through
`payments.payment_id` when you need customer fields on each row.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `email` | Utf8 | | Filter by customer email |
| `name` | Utf8 | | Filter by customer name (partial match, case-insensitive) |
| `created_at_gte` | Utf8 | | Customers created on or after this timestamp |
| `created_at_lte` | Utf8 | | Customers created on or before this timestamp |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | Utf8 | Unique customer identifier |
| `business_id` | Utf8 | Business identifier |
| `name` | Utf8 | Customer full name |
| `email` | Utf8 | Customer email address |
| `phone_number` | Utf8 | Customer phone number |
| `created_at` | Utf8 | Creation timestamp (ISO 8601) |
| `metadata` | Json | Additional metadata as a JSON object |

---

### `refunds`

Refunds issued for payments. Join to `payments` via `payment_id`.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `created_at_gte` | Utf8 | | Refunds created on or after this timestamp |
| `created_at_lte` | Utf8 | | Refunds created on or before this timestamp |
| `status` | Utf8 | | Refund status: `succeeded`, `failed`, `pending`, `review` |
| `subscription_id` | Utf8 | | Filter by subscription ID |
| `customer_id` | Utf8 | | Filter by customer ID (query param only; not returned on list rows) |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `refund_id` | Utf8 | Unique refund identifier |
| `payment_id` | Utf8 | Associated payment identifier |
| `business_id` | Utf8 | Business identifier |
| `status` | Utf8 | Refund status |
| `amount` | Int64 | Refunded amount in currency's smallest unit |
| `currency` | Utf8 | ISO 4217 currency code |
| `is_partial` | Boolean | Whether this is a partial refund |
| `reason` | Utf8 | Reason provided for the refund |
| `created_at` | Utf8 | Refund creation timestamp (ISO 8601) |
| `subscription_id` | Utf8 | Filter-only virtual column (query param; not on list rows) |
| `customer_id` | Utf8 | Filter-only virtual column (query param; not on list rows) |

---

### `disputes`

Payment disputes (chargebacks). Join to `payments` via `payment_id`.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `created_at_gte` | Utf8 | | Disputes created on or after this timestamp |
| `created_at_lte` | Utf8 | | Disputes created on or before this timestamp |
| `dispute_status` | Utf8 | | `dispute_opened`, `dispute_expired`, `dispute_accepted`, `dispute_cancelled`, `dispute_challenged`, `dispute_won`, `dispute_lost` |
| `dispute_stage` | Utf8 | | `pre_dispute`, `dispute`, `pre_arbitration` |
| `customer_id` | Utf8 | | Filter by customer ID (query param only; not returned on list rows) |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `dispute_id` | Utf8 | Unique dispute identifier |
| `payment_id` | Utf8 | Associated payment identifier |
| `business_id` | Utf8 | Business identifier |
| `amount` | Utf8 | Disputed amount as a string for precision |
| `currency` | Utf8 | ISO 4217 currency code |
| `dispute_status` | Utf8 | Current dispute status |
| `dispute_stage` | Utf8 | Current dispute stage |
| `is_resolved_by_rdr` | Boolean | Whether resolved by Rapid Dispute Resolution |
| `payment_provider` | Utf8 | Processor — `stripe`, `adyen`, or `dodo` |
| `created_at` | Utf8 | Dispute creation timestamp (ISO 8601) |
| `customer_id` | Utf8 | Filter-only virtual column (query param; not on list rows) |

---

### `payouts`

Settlement payouts to your connected bank account.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `created_at_gte` | Utf8 | | Payouts created on or after this timestamp |
| `created_at_lte` | Utf8 | | Payouts created on or before this timestamp |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `payout_id` | Utf8 | Unique payout identifier |
| `business_id` | Utf8 | Business identifier |
| `status` | Utf8 | Payout status: `not_initiated`, `in_progress`, `on_hold`, `failed`, `success` |
| `amount` | Int64 | Total payout amount in currency's smallest unit |
| `currency` | Utf8 | ISO 4217 currency code |
| `payment_method` | Utf8 | Payment method used for the payout |
| `name` | Utf8 | Payout recipient name or purpose |
| `remarks` | Utf8 | Additional remarks |
| `fee` | Int64 | Processing fee |
| `tax` | Int64 | Tax applied to the payout (deprecated; prefer v3 breakup endpoints) |
| `refunds` | Int64 | Total refund value associated with the payout (deprecated) |
| `chargebacks` | Int64 | Total chargeback value associated with the payout (deprecated) |
| `payout_document_url` | Utf8 | URL to download payout document |
| `created_at` | Utf8 | Creation timestamp (ISO 8601) |
| `updated_at` | Utf8 | Last update timestamp (ISO 8601) |

---

### `subscriptions`

Active and historical subscriptions. Join to `customers` via `customer_id`
and to `payments` via `subscription_id`.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `status` | Utf8 | | Subscription status: `pending`, `active`, `on_hold`, `cancelled`, `failed`, `expired` |
| `customer_id` | Utf8 | | Filter by customer ID |
| `product_id` | Utf8 | | Filter by product ID |
| `brand_id` | Utf8 | | Filter by brand ID |
| `created_at_gte` | Utf8 | | Subscriptions created on or after this timestamp |
| `created_at_lte` | Utf8 | | Subscriptions created on or before this timestamp |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `subscription_id` | Utf8 | Unique subscription identifier |
| `customer_id` | Utf8 | Customer identifier |
| `customer_name` | Utf8 | Customer name |
| `customer_email` | Utf8 | Customer email |
| `product_id` | Utf8 | Product identifier |
| `product_name` | Utf8 | Product name (when returned by the list endpoint) |
| `brand_id` | Utf8 | Filter-only virtual column (query param; not on list rows) |
| `status` | Utf8 | Subscription status |
| `quantity` | Int64 | Number of units |
| `recurring_pre_tax_amount` | Int64 | Recurring charge before tax (smallest currency unit) |
| `currency` | Utf8 | ISO 4217 currency code |
| `tax_inclusive` | Boolean | Whether the recurring amount is tax inclusive |
| `trial_period_days` | Int64 | Trial period in days (0 if none) |
| `subscription_period_interval` | Utf8 | Billing interval — `Day`, `Week`, `Month`, `Year` |
| `subscription_period_count` | Int64 | Number of intervals in subscription period |
| `payment_frequency_interval` | Utf8 | Payment interval — `Day`, `Week`, `Month`, `Year` |
| `payment_frequency_count` | Int64 | Number of intervals in payment frequency |
| `next_billing_date` | Utf8 | Next scheduled billing timestamp (ISO 8601) |
| `previous_billing_date` | Utf8 | Previous billing timestamp (ISO 8601) |
| `cancel_at_next_billing_date` | Boolean | Whether subscription cancels at next billing |
| `on_demand` | Boolean | Whether this is an on-demand subscription |
| `customer_business_name` | Utf8 | Business / legal name for B2B tax invoices |
| `payment_method_id` | Utf8 | Saved payment method used for recurring charges |
| `tax_id` | Utf8 | Tax identifier for this subscription |
| `discounts` | Json | Applied discounts as a JSON array |
| `metadata` | Json | Additional metadata as a JSON object |
| `billing_country` | Utf8 | Billing address country (ISO 3166-1 alpha-2) |
| `billing_city` | Utf8 | Billing address city |
| `billing_state` | Utf8 | Billing address state |
| `billing_street` | Utf8 | Billing street address |
| `billing_zipcode` | Utf8 | Billing postal code |
| `created_at` | Utf8 | Creation timestamp (ISO 8601) |
| `cancelled_at` | Utf8 | Cancellation timestamp if cancelled |
| `scheduled_change` | Json | Scheduled plan change details as JSON |

## Source scope

- Targets the Dodo Payments REST API at `https://test.dodopayments.com` or
  `https://live.dodopayments.com`.
- Requires `DODO_API_KEY` (`kind: secret`) for Bearer token authentication and
  `DODO_BASE_URL` (`kind: variable`, defaulting to live mode).
- Covers read-only access: payments, customers, refunds, disputes, payouts,
  and subscriptions.
- Page-based pagination (`page_number` starting at 0, `page_size` up to 100).
- 7 filters on `payments`, 4 on `customers`, 5 on `refunds`, 5 on `disputes`,
  2 on `payouts`, and 6 on `subscriptions`.
- All 6 declared `test_queries` are source-independent (use `LIMIT` and work
  on any account with data).
- Column definitions are a selected subset of the Dodo Payments OpenAPI
  list-response schemas linked in [Provider docs](#provider-docs), focused on
  billing and reconciliation fields users query most often.

## Limitations

- Read-only access. Payment creation, refunds, subscription management,
  product configuration, and other write operations are intentionally out of
  scope.
- No `products`, `addons`, `discounts`, `license_keys`, `entitlements`,
  `meters`, or `usage_events` tables yet.
- No credit-based billing or wallet tables.
- The `payments` list endpoint does not return `updated_at`; use `created_at`
  for recency filters.
- The `payouts` table exposes deprecated `tax`, `refunds`, and `chargebacks`
  aggregations from the list endpoint. Use the v3 payout breakup endpoints for
  detailed breakdowns (not yet exposed as Coral tables).
- Dodo Payments rate limits apply: 40 req/s burst, 240 req/min sustained
  (Tier 0). Use date-range filters to reduce API calls on large datasets.
- Test mode accounts may return empty results for some tables until test data
  is created through the dashboard, API, or the
  [Dodo CLI](https://docs.dodopayments.com/developer-resources/sdks/cli)
  (`dodo customers create`, `dodo checkout new`, etc.).

## Provider docs

- Dodo Payments API reference: https://docs.dodopayments.com/api-reference/introduction
- Dodo Payments dashboard: https://app.dodopayments.com
- Dodo CLI (manage resources, seed test data): https://docs.dodopayments.com/developer-resources/sdks/cli
- Authentication: https://docs.dodopayments.com/api-reference/introduction
- Test mode vs live mode: https://docs.dodopayments.com/miscellaneous/test-mode-vs-live-mode

## Cross-source JOIN examples

These examples use bundled core sources (`intercom`, `linear`) or community
sources that must be installed separately (for example `hubspot`). Dodo
Payments timestamp columns are `Utf8` ISO 8601 strings — prefer the
`created_at_gte` / `created_at_lte` API filters or fixed ISO literals instead
of comparing against `NOW() - INTERVAL ...` directly.

### HubSpot + Dodo Payments

Requires `coral source add --file sources/community/hubspot/manifest.yaml`
in addition to this source. Revenue by customer lifecycle stage:

```sql
SELECT
  h.lifecyclestage,
  COUNT(DISTINCT d.customer_id) AS paying_customers,
  SUM(d.total_amount) / 100.0 AS revenue
FROM dodopayments.payments d
JOIN hubspot.contacts h
  ON LOWER(h.email) = LOWER(d.customer_email)
WHERE d.status = 'succeeded'
  AND d.created_at >= '2026-01-01T00:00:00Z'
GROUP BY 1
ORDER BY revenue DESC;
```

Alternatively, push the date filter to the API:

```sql
SELECT
  h.lifecyclestage,
  COUNT(DISTINCT d.customer_id) AS paying_customers,
  SUM(d.total_amount) / 100.0 AS revenue
FROM dodopayments.payments d
JOIN hubspot.contacts h
  ON LOWER(h.email) = LOWER(d.customer_email)
WHERE d.status = 'succeeded'
  AND d.created_at_gte = '2026-01-01T00:00:00Z'
GROUP BY 1
ORDER BY revenue DESC;
```

### Intercom + Dodo Payments

Paying customers matched to Intercom contacts:

```sql
SELECT
  d.customer_email,
  i.name AS intercom_name,
  SUM(d.total_amount) / 100.0 AS revenue
FROM dodopayments.payments d
JOIN intercom.contacts i
  ON LOWER(i.email) = LOWER(d.customer_email)
WHERE d.status = 'succeeded'
  AND d.created_at >= '2026-01-01T00:00:00Z'
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 20;
```

### Linear + Dodo Payments

Payments from Linear workspace members:

```sql
SELECT
  u.email,
  u.name AS linear_name,
  COUNT(*) AS payment_count,
  SUM(d.total_amount) / 100.0 AS revenue
FROM dodopayments.payments d
JOIN linear.users u
  ON LOWER(u.email) = LOWER(d.customer_email)
WHERE d.status = 'succeeded'
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 20;
```

### Customers with refunds (via payments)

For row-level customer fields on refunds, join through `payments` (the
`customer_id` column on `refunds` is filter-only and echoes the query param):

```sql
SELECT
  c.customer_id,
  c.name,
  c.email,
  r.refund_id,
  r.amount,
  r.currency
FROM dodopayments.customers c
JOIN dodopayments.payments p ON p.customer_id = c.customer_id
JOIN dodopayments.refunds r ON r.payment_id = p.payment_id
ORDER BY r.created_at DESC
LIMIT 20;
```

## Live validation commands

```bash
# YAML style (requires: cargo install ryl --locked)
make lint-sources

# Manifest structure and smoke queries (requires Coral CLI)
coral source lint sources/community/dodopayments/manifest.yaml

export DODO_API_KEY=dodo_test_...
export DODO_BASE_URL=https://test.dodopayments.com
coral source add --file sources/community/dodopayments/manifest.yaml

coral source test dodopayments
```

To validate this source against a live Dodo Payments account, run the
commands above.

### Live validation output

Re-run the commands above locally after manifest changes. Example output from
Dodo Payments test mode with a read-only API key after seeding test data via
the [Dodo CLI](https://docs.dodopayments.com/developer-resources/sdks/cli) or
API (`POST /customers`, `POST /products`, etc.):

```bash
$ coral source lint sources/community/dodopayments/manifest.yaml
Manifest is valid
```

```bash
$ export DODO_API_KEY=dodo_test_...
$ export DODO_BASE_URL=https://test.dodopayments.com
$ coral source add --file sources/community/dodopayments/manifest.yaml
Added source dodopayments (secrets: keychain)

  ✓ dodopayments connected successfully
  Secrets: keychain

    dodopayments (6 tables)
    ├─ customers
    ├─ disputes
    ├─ payments
    ├─ payouts
    ├─ refunds
    └─ subscriptions
    Query tests
    6 declared · 6 passed · 0 failed

    ✓ SELECT payment_id, status, total_amount, currency, refund_status, dispute_status FROM dodopayments.payments LIMIT 5
      2 rows

    ✓ SELECT customer_id, name, email FROM dodopayments.customers LIMIT 5
      1 row

    ✓ SELECT refund_id, payment_id, status, amount, is_partial FROM dodopayments.refunds LIMIT 5
      0 rows

    ✓ SELECT dispute_id, payment_id, dispute_status, amount, dispute_stage FROM dodopayments.disputes LIMIT 5
      0 rows

    ✓ SELECT payout_id, status, amount, currency, fee, refunds, chargebacks FROM dodopayments.payouts LIMIT 5
      0 rows

    ✓ SELECT subscription_id, customer_id, status FROM dodopayments.subscriptions LIMIT 5
      1 row
```

> **Note:** Empty tables (`refunds`, `disputes`, `payouts`) are expected until
> those events occur in the account. The non-empty results above exercise list
> row paths and column mapping for `customers`, `payments`, and
> `subscriptions`. Accounts with no data still confirm authentication when all
> queries return 0 rows.

```bash
$ coral source test dodopayments
  ✓ dodopayments connected successfully
  Secrets: keychain

    dodopayments (6 tables)
    ├─ customers
    ├─ disputes
    ├─ payments
    ├─ payouts
    ├─ refunds
    └─ subscriptions
    Query tests
    6 declared · 6 passed · 0 failed
```
