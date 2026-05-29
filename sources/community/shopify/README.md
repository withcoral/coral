# Shopify Community Source

Query Shopify shop metadata, products, product variants, collections, orders,
customers, locations, and inventory items through Coral SQL using the Shopify
Admin GraphQL API.

## Setup

### 1. Create an Admin API access token

Create or use a Shopify custom app or public app with an Admin API access
token. Coral sends the token in the `X-Shopify-Access-Token` header.

Grant only the read scopes needed for the tables you plan to query:

| Table | Minimum scope |
|---|---|
| `shopify.shop` | Valid Admin API token |
| `shopify.products` | `read_products` |
| `shopify.product_variants` | `read_products` |
| `shopify.collections` | `read_products` |
| `shopify.orders` | `read_orders` |
| `shopify.customers` | `read_customers` |
| `shopify.locations` | `read_locations` |
| `shopify.inventory_items` | `read_inventory` or `read_products` |

Historical order access beyond Shopify's default recent-orders window requires
approved `read_all_orders` in addition to `read_orders`.

### 2. Add the source

Set the shop domain without `https://` and without a trailing slash:

```bash
export SHOPIFY_SHOP_DOMAIN="example.myshopify.com"
export SHOPIFY_ADMIN_ACCESS_TOKEN="shpat_..."
coral source add --file sources/community/shopify/manifest.yaml
```

The source defaults `SHOPIFY_API_VERSION` to `2026-04`. Override it only after
validating this source against another supported Shopify Admin GraphQL version:

```bash
export SHOPIFY_API_VERSION="2026-04"
```

### 3. Verify

```bash
coral source test shopify
```

The built-in test queries read `shopify.shop` and `shopify.products`, so the
token needs access to shop metadata and products.

## Tables

| Table | Description | Optional filters | Pagination |
|---|---|---|---|
| `shopify.shop` | Shop metadata for the authenticated Admin API token | none | none |
| `shopify.products` | Products in the Shopify catalog | `query` | cursor |
| `shopify.product_variants` | Product variants, SKUs, pricing, inventory item metadata | `query` | cursor |
| `shopify.collections` | Product collections and collection metadata | `query` | cursor |
| `shopify.orders` | Orders, status, totals, tags, and customer summary fields | `query` | cursor |
| `shopify.customers` | Customer profiles, order counts, spend, tags, and contact fields | `query` | cursor |
| `shopify.locations` | Inventory and fulfillment locations | `query`, `include_inactive`, `include_legacy` | cursor |
| `shopify.inventory_items` | Inventory item SKUs, tracking, shipping, origin, and location counts | `query` | cursor |

The `query` filters use Shopify Admin API search syntax for the corresponding
GraphQL connection. Examples include `status:active`, `sku:ABC-123`,
`updated_at:>2026-01-01`, and `financial_status:paid`.

Money amount columns expose both forms where arithmetic is common: the natural
amount column is `Float64` for aggregation, and the matching `*_decimal` column
keeps Shopify's exact decimal string.

## Example Queries

```sql
-- Confirm which shop the token belongs to
SELECT id, name, myshopify_domain, currency_code
FROM shopify.shop
LIMIT 1;

-- Active products updated recently
SELECT id, title, status, vendor, total_inventory, updated_at
FROM shopify.products
WHERE query = 'status:active updated_at:>2026-01-01'
LIMIT 25;

-- Find variants by SKU prefix
SELECT id, product_title, sku, price, inventory_quantity
FROM shopify.product_variants
WHERE query = 'sku:ABC*'
LIMIT 25;

-- Collections and product counts
SELECT id, title, handle, products_count
FROM shopify.collections
ORDER BY title
LIMIT 50;

-- Paid orders that are not fulfilled
SELECT id, name, created_at, display_financial_status,
       display_fulfillment_status, current_total_price_amount
FROM shopify.orders
WHERE query = 'financial_status:paid fulfillment_status:unfulfilled'
LIMIT 25;

-- Total paid unfulfilled order value
SELECT SUM(current_total_price_amount) AS total_unfulfilled_value
FROM shopify.orders
WHERE query = 'financial_status:paid fulfillment_status:unfulfilled';

-- Highest-spend customers returned by a targeted Shopify search
SELECT id, display_name, default_email, number_of_orders, amount_spent
FROM shopify.customers
WHERE query = 'orders_count:>0'
LIMIT 25;

-- Active fulfillment locations
SELECT id, name, is_active, fulfills_online_orders, address__city
FROM shopify.locations
WHERE include_inactive IS FALSE
LIMIT 25;

-- Tracked inventory items by SKU
SELECT id, sku, tracked, requires_shipping, locations_count
FROM shopify.inventory_items
WHERE query = 'sku:ABC*'
LIMIT 25;
```

## Validation

```bash
export SHOPIFY_SHOP_DOMAIN="example.myshopify.com"
export SHOPIFY_ADMIN_ACCESS_TOKEN="shpat_..."
coral source lint sources/community/shopify/manifest.yaml
coral source add --file sources/community/shopify/manifest.yaml
coral source test shopify
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'shopify'"
coral sql "SELECT table_name, column_name FROM coral.columns WHERE schema_name = 'shopify' ORDER BY table_name, ordinal_position"
coral sql "SELECT id, title, status FROM shopify.products LIMIT 10"
```

## Notes

- This source uses Shopify Admin GraphQL, not REST Admin API. Shopify marks REST
  Admin API as legacy and requires new public apps to use GraphQL Admin API.
- The default `SHOPIFY_API_VERSION` should be reviewed when Shopify API
  support windows move. Shopify releases stable API versions quarterly and
  supports each stable version for at least 12 months. Before bumping the
  default, check Shopify's API versioning release schedule:
  https://shopify.dev/docs/api/usage/versioning#release-schedule. Version
  bumps can require GraphQL query updates for renamed, deprecated, or removed
  fields and enum values.
- GraphQL connections are paginated with `first`, `after`, and
  `pageInfo.endCursor`. Coral requests up to 100 rows per page by default.
- Shopify GraphQL IDs are exposed as `Utf8` global IDs such as
  `gid://shopify/Product/...`.
- Shopify money amounts are returned by the API as decimal strings. This source
  keeps those exact values in `*_decimal` columns and exposes `Float64` amount
  columns for arithmetic.
- Shopify `DateTime` values are exposed as `Timestamp` columns for date-range
  filtering and ordering.
- Nested or repeatable values such as tags, selected options, and smart
  collection rule sets are exposed as `Json`.
- Order and customer data can include protected customer data. Shopify may
  restrict access unless the app meets Shopify's protected customer data
  requirements.

## Out of scope for v1

- Product, variant, inventory, customer, order, or fulfillment mutations
- Fulfillment orders and inventory levels by location
- Metafields as standalone query surfaces
- Discounts, markets, returns, analytics, pages, blogs, themes, and webhooks
- Bulk operations
- Storefront API data
- Write operations of any kind
