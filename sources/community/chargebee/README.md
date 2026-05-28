# Chargebee

Query customers, subscriptions, invoices, and transactions from Chargebee.

## Setup

### Get Your API Key

Create a Chargebee API key with read access to customers, subscriptions,
invoices, and transactions. Use a least-privilege key for production sites.

### Add the Source

```bash
export CHARGEBEE_SITE="https://example.chargebee.com"
export CHARGEBEE_API_KEY="test_..."
coral source add --file sources/community/chargebee/manifest.yaml
```

`CHARGEBEE_SITE` should be the site URL without a trailing slash. The source
adds `/api/v2`.

## Tables

### `customers`

Lists Chargebee customer records. Start here for customer identity and join
`customers.id` to `subscriptions.customer_id`, `invoices.customer_id`, and
`transactions.customer_id`.

### `subscriptions`

Lists Chargebee subscription records. Use `customer_id` to join back to
customers and `status` to narrow active, paused, cancelled, or non-renewing
subscriptions.

### `invoices`

Lists Chargebee invoice records. Use `customer_id` and `subscription_id` to
connect invoices to customers and subscriptions. Amount columns are returned in
the smallest currency unit.

### `transactions`

Lists Chargebee transaction records. Use this table for payments, refunds, and
gateway transaction metadata. Amount columns are returned in the smallest
currency unit.

## Authentication

Chargebee API v2 uses HTTP Basic Auth. This source sends `CHARGEBEE_API_KEY`
as the Basic Auth username with a blank password, matching Chargebee's API key
authentication pattern.

## Pagination

Chargebee list endpoints return an opaque `next_offset` token. This source uses
Coral cursor query pagination with `offset` as the request cursor parameter and
`next_offset` as the response cursor path.

## Example Queries

### Recent Customers

```sql
SELECT id, email, first_name, last_name, company, created_at
FROM chargebee.customers
LIMIT 20;
```

### Active Subscriptions

```sql
SELECT id, customer_id, status, current_term_end, next_billing_at
FROM chargebee.subscriptions
WHERE status = 'active'
LIMIT 20;
```

### Open Invoices

```sql
SELECT id, customer_id, subscription_id, status, amount_due, due_date
FROM chargebee.invoices
WHERE status = 'payment_due'
LIMIT 20;
```

### Transactions for a Customer

```sql
SELECT id, type, status, amount, currency_code, date
FROM chargebee.transactions
WHERE customer_id = 'customer_id'
LIMIT 20;
```

## Validation

```bash
coral source lint sources/community/chargebee/manifest.yaml
export CHARGEBEE_SITE="https://example.chargebee.com"
export CHARGEBEE_API_KEY="test_..."
coral source add --file sources/community/chargebee/manifest.yaml
coral source test chargebee
```

## Limitations

- Read-only v1 focused on customers, subscriptions, invoices, and transactions.
- Product Catalog resources such as plans, items, item prices, addons, and
  coupons are intentionally out of scope for the first version.
- The source exposes a conservative scalar field set. Custom fields and nested
  resource arrays can be added later after live payload validation.
- Live validation requires a Chargebee test or production site and API key.
