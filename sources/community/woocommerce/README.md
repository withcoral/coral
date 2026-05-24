# WooCommerce

Query WooCommerce store status, products, product variations, orders, and
customers from a self-hosted WordPress + WooCommerce store through the
WooCommerce v3 REST API.

## Setup

### Requirements

- A WooCommerce store reachable over **HTTPS** (WooCommerce's REST API
  only enables Basic auth over TLS — see *Troubleshooting* for a local-
  HTTP workaround).
- A WooCommerce REST API key pair (consumer key + consumer secret).
  `Read` is the right key permission level, but the key inherits the
  WordPress user's capabilities — generate it against an **Administrator
  or Shop Manager** account so it can read orders and customers as well
  as the catalog.

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
  REST API*, against an Administrator or Shop Manager user.

## Tables

### `system_status`
Single-row store status from `/wp-json/wc/v3/system_status`.

**Useful for:** version reporting (WC / WP / PHP / MySQL), currency, HPOS.

### `products`
Catalog from `/wp-json/wc/v3/products`.

**Useful for:** catalog inventory, pricing audit, best-seller analysis.

Predicates pushed to the API: `type`, `status`, `sku`, `stock_status`,
`search`, `after`, `before`, `modified_after`, `modified_before`.
Filtered queries return
**all** matching rows; bounding is the caller's choice via a narrowing
predicate or an explicit `LIMIT N`.

### `product_variations`
Variations of a variable product from
`/wp-json/wc/v3/products/<product_id>/variations`.

**Requires:** `WHERE product_id = <id>` (the parent product ID).

For variable products WooCommerce keeps variation-level SKU, price, stock
state, and attributes here — `products.sku` / `products.stock_quantity`
are empty for variable parents by design.

### `orders`
Orders from `/wp-json/wc/v3/orders`.

**Useful for:** fulfilment monitoring, per-customer aggregation, channel
attribution.

Predicates pushed to the API: `status`, `customer_id`, `product`,
`search`, `after`, `before`, `modified_after`, `modified_before`.
Filtered queries return
**all** matching rows; use a narrowing predicate or explicit `LIMIT N`
for performance on long order histories.

### `customers`
Registered customers from `/wp-json/wc/v3/customers`.

**Useful for:** customer inventory, country breakdowns, paying-vs-not.

Predicates pushed to the API: `email`, `role`, `search`. Filtered
queries return **all** matching rows; use a narrowing predicate or
explicit `LIMIT N` for performance on large customer tables. Guest
checkouts do not create customer rows; query `orders` with
`customer_id = 0` and `billing__email` for guest order activity.

## Authentication

Standard WooCommerce REST API key pair over HTTP Basic:

```text
Authorization: Basic base64(WOOCOMMERCE_CONSUMER_KEY:WOOCOMMERCE_CONSUMER_SECRET)
```

The key authenticates as the **WordPress user it was created for**, and
the API permission check uses *both* the key permission level and that
user's capabilities. A `Read` key on an Administrator/Shop Manager user
can read every table here. A `Read` key on a low-privilege user (e.g.
Subscriber) will fail with HTTP 401 `woocommerce_rest_cannot_view`
("Sorry, you cannot list resources.") on those tables even though the
key itself is valid — recreate it against a higher-privileged user.
See *Troubleshooting* below.

## Known limitations

- **`products`, `orders`, and `customers` fetch every matching row** —
  Coral keeps paging the API until it returns empty. Filtered queries
  preserve completeness (no silent cap). On large stores, narrow the
  result set deliberately:
  - **Filter** with a pushdown predicate (`type`, `status`,
    `customer_id`, `after`, etc.) — server-side, fast.
  - **Or** add an explicit `LIMIT N` — Coral stops paging at `N`.
  Unfiltered `SELECT *` on a list table on a busy store can iterate
  many pages; that's a performance choice, not a correctness one.
- **Variable products and variations live in two tables.** A variable
  product's parent row in `products` has no real SKU / price / stock —
  those are on the variation rows in `product_variations`, which
  requires `WHERE product_id = <id>`. Use the new `type` pushdown to
  list variable parents (`WHERE type = 'variable'`), then loop.
- `manage_stock` on `product_variations` is `Utf8`, not `Boolean`: the
  v3 API documents three values — `true`, `false`, and `parent`
  (inherit from the parent product). A Boolean column would silently
  drop the `parent` value.
- `orders_count` and `total_spent` on `customers` were removed in
  WooCommerce 10. Derive them by aggregating `orders` by `customer_id`
  — see the *Top customers by spend* example below.
- Price and amount fields (`price`, `total`, etc.) are decimal strings
  from the API (e.g. `"12.50"`). `CAST(... AS DOUBLE)` for arithmetic.
- Timestamps use the `_gmt` (UTC) variants and are parsed into real
  `Timestamp` columns.
- Each list table fetches up to **100 rows per page** from the API
  (the WC maximum) when paging up to the fetch cap or an explicit
  `LIMIT`.

## Example Queries

### Store environment

```sql
SELECT wc_version, wp_version, php_version, mysql_version, currency, hpos_enabled
FROM woocommerce.system_status
```

### Server-side filter pushdown — orders awaiting fulfilment

```sql
SELECT id, number, status, total, customer_id, date_created
FROM woocommerce.orders
WHERE status = 'processing'        -- pushed to ?status=
ORDER BY date_created ASC
```

### Search across products

```sql
SELECT id, name, sku, price, stock_status
FROM woocommerce.products
WHERE search = 'mug'               -- pushed to ?search=
```

### Recently modified orders

```sql
SELECT id, status, total, date_modified
FROM woocommerce.orders
WHERE modified_after = '2026-05-01T00:00:00'   -- pushed to ?modified_after=
ORDER BY date_modified DESC
```

### Inventory for variable products (two-step, server-filtered)

`product_variations` requires a constant `product_id`, so this is a
two-step pattern. First list variable parents — `type = 'variable'` is
pushed down to `/products?type=variable`, so the catalog isn't scanned
locally:

```sql
SELECT id, name FROM woocommerce.products WHERE type = 'variable'
```

Then query variations for each ID:

```sql
SELECT sku, regular_price, stock_quantity, stock_status, manage_stock,
       attributes
FROM woocommerce.product_variations
WHERE product_id = 11
```

An agent typically loops over the variable-product IDs from the first
query and issues one `product_variations` query per ID.

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

## Troubleshooting

### "Consumer key is missing" (HTTP 401)

WooCommerce reads the `Authorization` header to authenticate REST calls.
On Apache + mod_php and many fcgi setups, the header can be stripped
before PHP sees it, and WooCommerce then reports *"Consumer key is
missing"* even though the request sent one. Fix it at the host:

**Apache (`.htaccess` at the WP root):**

```apache
RewriteEngine On
RewriteCond %{HTTP:Authorization} ^(.+)$
RewriteRule .* - [E=HTTP_AUTHORIZATION:%1]
SetEnvIf Authorization "(.*)" HTTP_AUTHORIZATION=$1
```

**Nginx (in the WordPress server block):**

```nginx
fastcgi_pass_header Authorization;
```

Reload the web server after the change. Reference:
<https://developer.woocommerce.com/docs/apis/rest-api/authentication/>

### "Sorry, you cannot list resources." (HTTP 401)

The credentials are valid but the underlying WordPress user lacks the
capability for that resource — see *Authentication*. Recreate the API
key against an Administrator or Shop Manager user.

### Local-HTTP development

WooCommerce gates Basic auth (and consumer-key query params) behind
`is_ssl()`. The shipped source targets HTTPS stores. For local Docker
testing over plain HTTP only, a one-line WordPress mu-plugin lifts the
gate — see the PR description for the exact snippet.

## Notes

- Verified against WooCommerce 10.7 on WordPress 6.9. The `system_status`,
  `products`, `product_variations`, `orders`, and `customers` endpoints
  are stable across WooCommerce 7.x+.
