# WooCommerce

Query WooCommerce store status, products, orders, and customers from a
self-hosted WordPress + WooCommerce store through the WooCommerce v3 REST
API.

## Setup

### Requirements

- A WooCommerce store reachable over **HTTPS** (WooCommerce's REST API only
  enables Basic auth over TLS — see *Notes* for a local-HTTP workaround).
- A WooCommerce REST API key pair (consumer key + consumer secret) with
  at least `Read` permission.

### Add the Source

Set the inputs as environment variables, then add the source from this
manifest:

```bash
export WOOCOMMERCE_URL=https://your-store.example.com
export WOOCOMMERCE_CONSUMER_KEY=ck_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export WOOCOMMERCE_CONSUMER_SECRET=cs_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
coral source add --file sources/community/woocommerce/manifest.yaml
```

Inputs:

- `WOOCOMMERCE_URL` — base URL of the store including scheme and host,
  e.g. `https://your-store.example.com`. No trailing slash.
- `WOOCOMMERCE_CONSUMER_KEY` — REST API consumer key (starts with `ck_`).
- `WOOCOMMERCE_CONSUMER_SECRET` — REST API consumer secret (starts with
  `cs_`). Create both together in *WooCommerce → Settings → Advanced →
  REST API*.

## Tables

### `system_status`
Single-row store status from `/wp-json/wc/v3/system_status`.

**Useful for:**
- Connectivity and version reporting (WC / WP / PHP / MySQL)
- Currency and HPOS configuration audit

### `products`
Catalog from `/wp-json/wc/v3/products`.

**Useful for:**
- Inventory and pricing review (`stock_status`, `stock_quantity`, `price`)
- Best-seller analysis via `total_sales`
- Sale and rating audits

### `orders`
Orders from `/wp-json/wc/v3/orders`.

**Useful for:**
- Recent order monitoring by `status`, `date_created`, `total`
- Payment method and channel attribution (`payment_method`, `created_via`)
- Per-customer order aggregation (join with `customers` on `customer_id`)

### `customers`
Registered customers from `/wp-json/wc/v3/customers`.

**Useful for:**
- Customer inventory and country breakdowns
- Identifying paying vs non-paying accounts

Guest checkouts do not create customer rows; query `orders` with
`customer_id = 0` and `billing__email` to find guest order activity.

## Authentication

Uses the standard WooCommerce REST API key pair over HTTP Basic:

```text
Authorization: Basic base64(WOOCOMMERCE_CONSUMER_KEY:WOOCOMMERCE_CONSUMER_SECRET)
```

A `Read`-permission key is enough for every table.

## Limits

- This source is **read-only**. It exposes catalog/order/customer/status
  endpoints only — no creating, updating, or deleting data, and no
  refund/coupon/tax mutations.
- Price and amount fields are decimal strings as WooCommerce returns them
  (e.g. `"12.50"`). Use `CAST(price AS DOUBLE)` for arithmetic.
- Timestamps use the `_gmt` (UTC) variants from the API and are parsed into
  real `Timestamp` columns.
- Nested arrays (`categories`, `tags`, `line_items`, `refunds`) are exposed
  as `Json` columns; use JSON accessor functions.
- `orders_count` and `total_spent` on `customers` were dropped in
  WooCommerce 10. Derive them by aggregating `orders` by `customer_id`.
- Pagination: each list table fetches up to **100 rows per page** and
  follows pages until the API returns empty.

## Example Queries

### Store environment

```sql
SELECT wc_version, wp_version, php_version, mysql_version, currency,
       hpos_enabled
FROM woocommerce.system_status
```

### Low-stock products

```sql
SELECT name, sku, stock_quantity
FROM woocommerce.products
WHERE manage_stock = TRUE
  AND stock_quantity IS NOT NULL
  AND stock_quantity < 10
ORDER BY stock_quantity ASC
```

### Orders awaiting fulfilment

```sql
SELECT id, number, status, total, customer_id, date_created
FROM woocommerce.orders
WHERE status = 'processing'
ORDER BY date_created ASC
```

### Top customers by spend (derived)

```sql
SELECT customer_id,
       COUNT(*) AS orders,
       SUM(CAST(total AS DOUBLE)) AS total_spent
FROM woocommerce.orders
WHERE customer_id <> 0 AND status = 'completed'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 20
```

## Notes

- Verified against WooCommerce 10.7 on WordPress 6.9. The
  `system_status`, `products`, `orders`, and `customers` endpoints are
  stable across WooCommerce 7.x+.
- WooCommerce gates Basic auth (and consumer-key query params) behind an
  `is_ssl()` check, so this source targets HTTPS stores. For local Docker
  testing over plain HTTP, drop a one-line mu-plugin into the WordPress
  install that sets `$_SERVER["HTTPS"] = "on"` — see the PR description
  for the exact command.
- `orders_count` and `total_spent` columns are intentionally omitted from
  `customers` because WooCommerce 10 removed them from the default
  response. Aggregate `orders` instead.
