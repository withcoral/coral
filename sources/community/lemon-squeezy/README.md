# Lemon Squeezy

Query your [Lemon Squeezy](https://www.lemonsqueezy.com/) store data — orders,
subscriptions, customers, products, license keys, discounts, and more.

## Prerequisites

You need a Lemon Squeezy API key.

1. Log in to [Lemon Squeezy](https://app.lemonsqueezy.com).
2. Go to **Settings → API**.
3. Click **+ Add key**, give it a name, and copy the generated key.

Use a **test-mode key** while developing. Switch to a live-mode key for
production data. Test-mode and live-mode keys access separate datasets.

## Installation

```sh
LEMON_SQUEEZY_API_KEY=your_api_key coral source add --file manifest.yaml
```

Or add it interactively and paste the key when prompted:

```sh
coral source add --file manifest.yaml --interactive
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
| `variants`              | Variants of each product, controlling price, billing interval, licensing, and trial settings                    |
| `customers`             | Customers who have made purchases, with MRR and cumulative revenue                                              |
| `orders`                | Individual purchase transactions with full price and tax breakdowns in store currency and USD                   |
| `order_items`           | Line items within each order                                                                                    |
| `subscriptions`         | Active and historical subscriptions with billing status, renewal dates, and payment method                      |
| `subscription_invoices` | Invoices generated per subscription billing cycle                                                               |
| `discounts`             | Coupon codes with validity windows, redemption limits, and subscription discount duration settings              |
| `discount_redemptions`  | Individual records of each discount code being applied to an order — use this to count redemptions per discount |
| `license_keys`          | License keys issued with purchases, tracking activation limit and current activation count                      |

All monetary amounts are in **cents**. Most transactional columns
(`subtotal`, `tax`, `total`, etc.) use the store's configured currency. The
following aggregate fields are always in **USD cents** regardless of the
store's currency: `stores.total_revenue`, `stores.thirty_day_revenue`,
`customers.total_revenue_currency`, and `customers.mrr`. Columns explicitly
suffixed `_usd` are also USD cents. Divide any cent value by 100 to get the
decimal amount.

> **Note on fetch limits:** High-cardinality tables (`orders`, `customers`,
> `subscriptions`, `subscription_invoices`, `license_keys`,
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

### Monthly revenue from paid orders

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

### Active subscriptions by product

```sql
SELECT
  product_name,
  variant_name,
  COUNT(*)                AS active_count,
  ROUND(SUM(
    CASE WHEN cancelled THEN 0 ELSE 1 END
  ) * 100.0 / COUNT(*), 1) AS retention_pct
FROM lemon_squeezy.subscriptions
WHERE status = 'active'
GROUP BY 1, 2
ORDER BY active_count DESC;
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
  END                               AS discount_value,
  s.currency                        AS store_currency,
  COUNT(dr.id)                      AS redemptions,
  ROUND(SUM(dr.amount) / 100.0, 2) AS total_savings,
  d.status
FROM lemon_squeezy.discounts d
LEFT JOIN lemon_squeezy.discount_redemptions dr
  ON dr.discount_id = CAST(d.id AS BIGINT)
JOIN lemon_squeezy.stores s
  ON s.id = CAST(d.store_id AS VARCHAR)
WHERE d.status = 'published'
GROUP BY d.id, d.code, d.amount_type, d.amount, d.status, s.currency
ORDER BY redemptions DESC;
```

### Subscription invoices — recent failed payments

```sql
SELECT
  i.id            AS invoice_id,
  i.subscription_id,
  i.currency,
  ROUND(i.total / 100.0, 2) AS total,
  i.status,
  i.created_at
FROM lemon_squeezy.subscription_invoices i
WHERE i.status IN ('void', 'refunded')
ORDER BY i.created_at DESC
LIMIT 25;
```

### Cross-source: join orders with Linear issues to track refund investigations

```sql
SELECT
  o.order_number,
  o.user_email,
  ROUND(o.total_usd / 100.0, 2) AS total_usd,
  o.refunded_at,
  i.identifier                  AS linear_issue,
  i.title                       AS issue_title
FROM lemon_squeezy.orders o
JOIN linear.issues i
  ON i.title ILIKE '%refund%' || CAST(o.order_number AS VARCHAR) || '%'
WHERE o.refunded = true
ORDER BY o.refunded_at DESC
LIMIT 10;
```

## Notes

- The Lemon Squeezy API has a rate limit of **300 requests per minute**. Coral
  handles back-off automatically when the limit is reached.
- All responses use the JSON:API format. Coral flattens the `attributes` object
  so each field is a direct SQL column.
- Monetary values are always integers (cents). There are no decimal money
  columns in this source.
- `subscription_invoices.urls__invoice_url` is the signed PDF download URL for
  an invoice. It is null for invoices in pending status.
- The `license_keys.instances_count` column is the live activation count
  returned by the API. It may briefly lag behind actual activation events.
- Test-mode and live-mode stores are completely separate. You must re-add the
  source with a live-mode key to query production data.

## Links

- [Lemon Squeezy API reference](https://docs.lemonsqueezy.com/api)
- [API authentication](https://docs.lemonsqueezy.com/guides/developer-guide/getting-started)
- [Pagination](https://docs.lemonsqueezy.com/api/getting-started/requests#pagination)