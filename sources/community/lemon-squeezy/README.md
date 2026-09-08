# Lemon Squeezy

Query your [Lemon Squeezy](https://www.lemonsqueezy.com/) store data - orders,
subscriptions, customers, products, prices, license keys, discounts, and more.

## Authentication

Requires a LEMON_SQUEEZY_API_KEY

1. Log in to [Lemon Squeezy](https://app.lemonsqueezy.com).
2. Go to **Settings → API**.
3. Click **+ Add key**, give it a name, and copy the generated key.

## Installation

```sh
LEMON_SQUEEZY_API_KEY=your_api_key coral source add --file sources/community/lemon-squeezy/manifest.yaml
```

```sh
LEMON_SQUEEZY_API_KEY=your_api_key coral source add --file sources/community/lemon-squeezy/manifest.yaml --interactive
```

Validate the source after adding it:

```sh
coral source test lemon_squeezy
```

## Tables

| Table                   | Description                                                                                                     |
| ----------------------- | --------------------------------------------------------------------------------------------------------------- |
| `stores`                | Stores owned by the authenticated account, with revenue totals and 30-day summaries                             |
| `products`              | Products listed in your stores                                                                                  |
| `variants`              | Legacy variant metadata and back-compat pricing/billing defaults - current price records live in `prices`       |
| `prices`                | Current price records for variants, including billing scheme, tiers, trials, and price history                  |
| `customers`             | Customers who have made purchases, with MRR and cumulative revenue                                              |
| `orders`                | Individual purchase transactions with full price and tax breakdowns in the order currency and USD               |
| `order_items`           | Line items within each order, with price and quantity per variant purchased (in the order currency)             |
| `subscriptions`         | Active and historical subscriptions with billing status, renewal dates, and payment method                      |
| `subscription_invoices` | Invoices generated per subscription billing cycle, amounts in the invoice currency and USD                      |
| `discounts`             | Coupon codes with validity windows, redemption limits, and subscription discount duration settings              |
| `discount_redemptions`  | Individual records of each discount code being applied to an order - use this to count redemptions per discount |
| `license_keys`          | License keys issued with purchases, tracking activation limit and current activation count                      |

All monetary amounts are in **cents**. The currency a given amount is expressed
in depends on which column you are reading:

**Per-row transactional currency** — these columns reflect the currency of the
individual transaction, which may differ from the store's default currency when
a customer pays in a different currency:

- `orders`: `subtotal`, `setup_fee`, `discount_total`, `tax`, `total`,
  `refunded_amount`, and their `_formatted` counterparts are in the
  **order currency** (`orders.currency`).
- `order_items`: `price` is in the **order currency** of the parent order
  (the `order_items` table does not carry its own `currency` column; join to
  `orders` on `order_id` to get the currency code).
- `subscription_invoices`: `subtotal`, `discount_total`, `tax`, `total`,
  `refunded_amount`, and their `_formatted` counterparts are in the
  **invoice currency** (`subscription_invoices.currency`).
- `discount_redemptions`: `amount` is the saving applied to that specific
  order, expressed in the **order currency** of the parent order.

**Always USD cents** — the following aggregate fields are always in USD cents
regardless of per-row transaction currency: `stores.total_revenue`,
`stores.thirty_day_revenue`, `customers.total_revenue_currency`, and
`customers.mrr`.

**Explicit USD columns** — columns suffixed `_usd` (e.g. `orders.total_usd`,
`subscription_invoices.total_usd`) are always USD cents. Use these for
cross-order aggregations so you do not accidentally mix currencies.

Divide any cent value by 100 to get the decimal amount.

Most price columns have a `_formatted` counterpart (e.g. `total_formatted`,
`mrr_formatted`, `subtotal_formatted`) that returns a pre-formatted
human-readable string in the row's own currency (e.g. `"$9.99"` or
`"£9.99"`). Use the raw integer columns for arithmetic and aggregation; use
the `_formatted` columns for display only.

> **Aggregating across multiple currencies:** Because `orders`, `order_items`,
> `subscription_invoices`, and `discount_redemptions` can each contain rows in
> different currencies, never `SUM` their non-`_usd` columns without grouping
> or filtering by currency first. Use the `_usd` columns for multi-currency
> aggregations, or `GROUP BY currency` when you need per-currency totals.

> **Note on fetch limits:** High-cardinality tables (`orders`, `order_items`,
> `customers`, `subscriptions`, `subscription_invoices`, `license_keys`,
> `discount_redemptions`) default to fetching up to 500 rows per unfiltered
> query to avoid exhausting the 300 req/min rate limit.
> Use filters to scope queries to a specific store, subscription, or date range
> where possible.

## Filters

Most tables accept optional filters that are pushed to the API as query
parameters, reducing data transfer.

| Table                   | Filterable columns                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------- |
| `products`              | `store_id`                                                                                  |
| `prices`                | `variant_id`                                                                                |
| `variants`              | `product_id`, `status`                                                                      |
| `customers`             | `store_id`, `email`                                                                         |
| `orders`                | `store_id`, `user_email`, `order_number`                                                    |
| `order_items`           | `order_id`, `product_id`, `variant_id`                                                      |
| `subscriptions`         | `store_id`, `order_id`, `order_item_id`, `product_id`, `variant_id`, `user_email`, `status` |
| `subscription_invoices` | `store_id`, `subscription_id`, `status`, `refunded`                                         |
| `discounts`             | `store_id`                                                                                  |
| `discount_redemptions`  | `discount_id`, `order_id`                                                                   |
| `license_keys`          | `store_id`, `order_id`, `order_item_id`, `product_id`, `status`                             |

## Example queries

### Store overview

```sql
SELECT
  name,
  currency,
  total_sales,
  ROUND(total_revenue / 100.0, 2)       AS total_revenue_usd,
  ROUND(thirty_day_revenue / 100.0, 2)  AS revenue_last_30d_usd
FROM lemon_squeezy.stores
ORDER BY total_revenue DESC;
```

### Monthly revenue from paid orders (use \_usd columns to safely sum across currencies)

```sql
SELECT
  DATE_TRUNC('month', created_at)       AS month,
  COUNT(*)                               AS orders,
  ROUND(SUM(total_usd) / 100.0, 2)      AS revenue_usd
FROM lemon_squeezy.orders
WHERE status = 'paid'
GROUP BY 1
ORDER BY 1 DESC;
```

### Monthly revenue from paid orders, broken out by order currency

```sql
SELECT
  DATE_TRUNC('month', created_at)       AS month,
  currency,
  COUNT(*)                               AS orders,
  ROUND(SUM(total) / 100.0, 2)          AS revenue
FROM lemon_squeezy.orders
WHERE status = 'paid'
GROUP BY 1, 2
ORDER BY 1 DESC, revenue DESC;
```

### Active subscriptions by product

```sql
SELECT
  product_name,
  variant_name,
  COUNT(*) AS active_subscriptions
FROM lemon_squeezy.subscriptions
WHERE status = 'active'
GROUP BY 1, 2
ORDER BY active_subscriptions DESC;
```

### Customers with highest MRR

```sql
SELECT
  name,
  email,
  ROUND(mrr / 100.0, 2)                   AS mrr_usd,
  ROUND(total_revenue_currency / 100.0, 2) AS lifetime_revenue_usd
FROM lemon_squeezy.customers
ORDER BY mrr DESC
LIMIT 20;
```

### Revenue by product including quantity, per order currency

```sql
SELECT
  oi.product_name,
  oi.variant_name,
  o.currency,
  SUM(oi.quantity)                                      AS units_sold,
  ROUND(SUM(oi.price * oi.quantity) / 100.0, 2)        AS gross_revenue
FROM lemon_squeezy.order_items oi
JOIN lemon_squeezy.orders o ON o.id = CAST(oi.order_id AS VARCHAR)
WHERE o.status = 'paid'
GROUP BY 1, 2, 3
ORDER BY gross_revenue DESC;
```

### License key usage vs limit

```sql
SELECT
  user_email,
  key_short,
  status,
  instances_count,
  activation_limit,
  CASE
    WHEN activation_limit IS NULL THEN 'unlimited'
    ELSE CAST(activation_limit - instances_count AS VARCHAR) || ' remaining'
  END AS headroom
FROM lemon_squeezy.license_keys
WHERE status = 'active'
ORDER BY instances_count DESC
LIMIT 50;
```

### Discount code performance

```sql
SELECT
  d.code,
  d.amount_type,
  CASE d.amount_type
    WHEN 'percent' THEN CAST(d.amount AS VARCHAR) || '%'
    ELSE CAST(ROUND(d.amount / 100.0, 2) AS VARCHAR)
  END AS discount_value,
  o.currency,
  COUNT(dr.id) AS redemptions,
  ROUND(COALESCE(SUM(dr.amount), 0) / 100.0, 2) AS total_savings
FROM lemon_squeezy.discounts d
LEFT JOIN lemon_squeezy.discount_redemptions dr
  ON dr.discount_id = CAST(d.id AS BIGINT)
LEFT JOIN lemon_squeezy.orders o
  ON o.id = CAST(dr.order_id AS VARCHAR)
WHERE d.status = 'published'
GROUP BY d.id, d.code, d.amount_type, d.amount, o.currency
ORDER BY redemptions DESC;
```

### Subscription invoices - recent void and refunded invoices

```sql
SELECT
  i.id            AS invoice_id,
  i.subscription_id,
  i.currency,
  i.total_formatted,
  i.status,
  i.created_at
FROM lemon_squeezy.subscription_invoices i
WHERE i.status IN ('void', 'refunded')
ORDER BY i.created_at DESC
LIMIT 25;
```

### Cross-source: join orders with Linear issues to track refund investigations

```sql
-- Covers both fully refunded orders (refunded = true) and partial refunds
-- (status = 'partial_refund'). refunded_at is only populated for full refunds,
-- so COALESCE falls back to updated_at for ordering partial-refund rows.
SELECT
  o.order_number,
  o.user_email,
  o.currency,
  o.total_formatted,
  o.status,
  o.refunded_amount_formatted,
  o.refunded_at,
  i.identifier                  AS linear_issue,
  i.title                       AS issue_title
FROM lemon_squeezy.orders o
JOIN linear.issues i
  ON i.title ILIKE '%refund%' || CAST(o.order_number AS VARCHAR) || '%'
WHERE o.refunded = true
   OR o.status = 'partial_refund'
ORDER BY COALESCE(o.refunded_at, o.updated_at) DESC
LIMIT 10;
```

## Notes

- The Lemon Squeezy API has a rate limit of **300 requests per minute**. Coral
  handles back-off automatically when the limit is reached.
- All responses use the JSON:API format. Coral flattens the `attributes` object
  so each field is a direct SQL column.
- **Currency of transactional columns:** `orders`, `order_items`,
  `subscription_invoices`, and `discount_redemptions` each carry amounts in the
  currency of the individual transaction, not a fixed store currency. Always
  check the `currency` column on the parent row before aggregating, or use the
  `_usd` columns to obtain a normalised USD value. Specifically:
  - `orders.*` amounts (e.g. `subtotal`, `tax`, `total`, `refunded_amount`)
    are in `orders.currency`.
  - `order_items.price` is in the order currency of the parent order. The
    `order_items` table does not have its own `currency` column; join to
    `orders` on `order_id` to retrieve it.
  - `subscription_invoices.*` amounts (e.g. `subtotal`, `tax`, `total`,
    `refunded_amount`) are in `subscription_invoices.currency`.
  - `discount_redemptions.amount` is the saving applied to a specific order,
    expressed in the order currency of that order. Join to `orders` on
    `order_id` to retrieve the currency.
- Monetary values are almost always integers (cents). The only exceptions are
  `unit_price_decimal` and the `unit_price_decimal` field nested inside
  `prices.tiers` objects - both are string representations of the price
  **in cents** as a decimal (e.g. `"999.0"` for $9.99). When usage-based
  billing is enabled (`usage_aggregation` is non-null), `prices.unit_price`
  is null and `unit_price_decimal` is the authoritative price; cast it to a
  numeric type for arithmetic (e.g. `CAST(unit_price_decimal AS DECIMAL) / 100`).
  The same applies to `unit_price_decimal` inside tier objects. For standard
  pricing where `unit_price` is non-null, prefer that integer column for
  `SUM`, `AVG`, and other aggregations.
- Most price columns have a `_formatted` counterpart returning a
  human-readable string (e.g. `total_formatted`, `setup_fee_formatted`,
  `mrr_formatted`). These are for display only — always use the raw integer
  columns for `SUM`, `AVG`, and other aggregations.
- `order_items.quantity` records how many units were purchased per line item.
  Multiply `price × quantity` to get the true line-item revenue; using `price`
  alone undercounts multi-unit purchases.
- `subscription_invoices.urls__invoice_url` is the signed PDF download URL for
  an invoice. It is null for invoices in pending status.
- `tax_inclusive` on both `orders` and `subscription_invoices` indicates
  whether the total was calculated with tax already included in the displayed
  price.
- `orders.refunded` and `subscription_invoices.refunded` are true **only for
  full refunds**. Partial refunds set `status = 'partial_refund'` and populate
  `refunded_amount` / `refunded_amount_usd` with the partial amount; `refunded`
  remains false and `refunded_at` remains null in that case. Always combine
  both signals when investigating any kind of refund activity:
  `WHERE refunded = true OR status = 'partial_refund'`.
- The `license_keys.instances_count` column is the live activation count
  returned by the API. It may briefly lag behind actual activation events.
- Test-mode and live-mode stores are completely separate. You must re-add the
  source with a live-mode key to query production data.

## Links

- [Lemon Squeezy API reference](https://docs.lemonsqueezy.com/api)
- [API authentication](https://docs.lemonsqueezy.com/guides/developer-guide/getting-started)
- [Pagination](https://docs.lemonsqueezy.com/api/getting-started/requests#pagination)