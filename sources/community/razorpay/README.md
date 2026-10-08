# Razorpay Source

Query Razorpay payments, orders, refunds, customers, invoices, payment links, and settlements through Coral SQL.

## Summary

This source lets Coral query read-only Razorpay Payment Gateway inventory: payment rows, order rows, refund rows, customers, invoices, standard payment links, and settlements. It also exposes `razorpay.payments_summary` for collection-level connectivity checks when a test account has no payments yet.

## Provider docs

- Authentication: https://razorpay.com/docs/api/authentication/
- Pagination and rate limits: https://razorpay.com/docs/api/pagination/
- Payments: https://razorpay.com/docs/api/payments/fetch-all-payments/
- Orders: https://razorpay.com/docs/api/orders/
- Refunds: https://razorpay.com/docs/api/refunds/fetch-all/
- Customers: https://razorpay.com/docs/api/customers/fetch-all/
- Invoices: https://razorpay.com/docs/api/payments/invoices/fetch-all/
- Payment Links: https://razorpay.com/docs/api/payments/payment-links/fetch-all-standard/
- Settlements: https://razorpay.com/docs/api/settlements/fetch-all/

## Authentication

Razorpay APIs use HTTP Basic Auth with a Key ID and Key Secret.

Generate test or live keys in the Razorpay Dashboard under Account & Settings -> API Keys, then add the community source:

```bash
RAZORPAY_API_KEY=rzp_test_... \
RAZORPAY_KEY_SECRET=... \
coral source add --file sources/community/razorpay/manifest.yaml
```

Use test mode keys for validation. Use live keys only when you intentionally want Coral queries to read live Razorpay account data.

## Request limits

This source performs live read-only Razorpay API requests. List tables use Razorpay's `count` and `skip` pagination with a maximum page size of 100. Razorpay applies API rate limits; retry or reduce query volume if the API returns HTTP 429.

## Source shape

The source exposes eight tables:

- `razorpay.payments_summary` returns top-level metadata and raw items from `GET /v1/payments`.
- `razorpay.payments` returns one row per payment from `GET /v1/payments`.
- `razorpay.orders` returns one row per order from `GET /v1/orders`.
- `razorpay.refunds` returns one row per refund from `GET /v1/refunds`.
- `razorpay.customers` returns one row per customer from `GET /v1/customers`.
- `razorpay.invoices` returns one row per invoice from `GET /v1/invoices`.
- `razorpay.payment_links` returns one row per standard payment link from `GET /v1/payment_links`.
- `razorpay.settlements` returns one row per settlement from `GET /v1/settlements`.

## Source scope

- Targets Razorpay Payment Gateway APIs through `RAZORPAY_BASE_URL`, defaulting to `https://api.razorpay.com`.
- Requires `RAZORPAY_API_KEY` and `RAZORPAY_KEY_SECRET`.
- Keeps the first version read-only.
- SQL columns named `from_timestamp` and `to_timestamp` map to Razorpay's `from` and `to` API query parameters.
- Mutating operations such as payment capture, order creation, refund creation, customer updates, invoice creation, payment-link creation/cancellation, and settlement actions are intentionally omitted.
- RazorpayX payout APIs are intentionally omitted from this Payment Gateway-focused first version.

## Limitations

- Razorpay API keys are account-mode specific. Test keys read test-mode data, and live keys read live-mode data.
- A fresh test account can validly return zero rows for the list tables. `razorpay.payments_summary` is included so source add/test can still prove authenticated connectivity.
- Payouts, fund accounts, contacts, and other RazorpayX APIs are outside this first version.
- The source does not expand nested payment card/EMI/offer details; it preserves nested objects such as `notes`, `acquirer_data`, `customer`, and `line_items` as JSON.

## Tables

### `razorpay.payments_summary`

Returns the top-level payments collection response.

```sql
SELECT entity, count, substr(CAST(items AS VARCHAR), 1, 80) AS items_preview
FROM razorpay.payments_summary
LIMIT 1;
```

### `razorpay.payments`

Returns one row per Razorpay payment.

```sql
SELECT id, amount, currency, status, method, order_id, created_at
FROM razorpay.payments
LIMIT 10;
```

### `razorpay.orders`

Returns one row per Razorpay order.

```sql
SELECT id, amount, amount_paid, amount_due, currency, status, receipt, created_at
FROM razorpay.orders
LIMIT 10;
```

### `razorpay.refunds`

Returns one row per Razorpay refund.

```sql
SELECT id, payment_id, amount, currency, status, speed_processed, created_at
FROM razorpay.refunds
LIMIT 10;
```

### `razorpay.customers`

Returns one row per Razorpay customer.

```sql
SELECT id, name, email, contact, created_at
FROM razorpay.customers
LIMIT 10;
```

### `razorpay.invoices`

Returns one row per Razorpay invoice.

```sql
SELECT id, type, invoice_number, status, customer_id, amount, currency, created_at
FROM razorpay.invoices
LIMIT 10;
```

### `razorpay.payment_links`

Returns one row per standard Razorpay payment link.

```sql
SELECT id, amount, currency, status, reference_id, short_url, created_at
FROM razorpay.payment_links
LIMIT 10;
```

`payment_id` and `reference_id` filters are pushed down to Razorpay.

### `razorpay.settlements`

Returns one row per Razorpay settlement.

```sql
SELECT id, amount, status, fees, tax, utr, created_at
FROM razorpay.settlements
LIMIT 10;
```

## Live validation output

Run these checks after setting `RAZORPAY_API_KEY` and `RAZORPAY_KEY_SECRET`.

```bash
$ coral source lint sources/community/razorpay/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/razorpay/manifest.yaml
Added source razorpay

  PASS razorpay connected successfully

    razorpay (8 tables)
    - customers
    - invoices
    - orders
    - payment_links
    - payments
    - payments_summary
    - refunds
    - settlements
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT entity, count FROM razorpay.payments_summary LIMIT 1
      1 row
```

```bash
$ coral source test razorpay
  PASS razorpay connected successfully

    razorpay (8 tables)
    - customers
    - invoices
    - orders
    - payment_links
    - payments
    - payments_summary
    - refunds
    - settlements
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT entity, count FROM razorpay.payments_summary LIMIT 1
      1 row
```

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'razorpay'
ORDER BY table_name;
```

```text
+------------------+
| table_name       |
+------------------+
| customers        |
| invoices         |
| orders           |
| payment_links    |
| payments         |
| payments_summary |
| refunds          |
| settlements      |
+------------------+
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'razorpay'
ORDER BY key;
```

```text
+---------------------+----------+----------+
| key                 | kind     | required |
+---------------------+----------+----------+
| RAZORPAY_API_KEY    | secret   | true     |
| RAZORPAY_BASE_URL   | variable | false    |
| RAZORPAY_KEY_SECRET | secret   | true     |
+---------------------+----------+----------+
```

```sql
SELECT table_name, column_name, data_type
FROM coral.columns
WHERE schema_name = 'razorpay'
ORDER BY table_name, ordinal_position;
```

```text
+------------------+--------------------+-----------+
| table_name       | column_name        | data_type |
+------------------+--------------------+-----------+
| customers        | id                 | Utf8      |
| customers        | entity             | Utf8      |
| customers        | name               | Utf8      |
| customers        | email              | Utf8      |
| customers        | contact            | Utf8      |
| customers        | gstin              | Utf8      |
| customers        | created_at         | Timestamp |
| customers        | notes              | Json      |
| customers        | shipping_address   | Json      |
| invoices         | type_filter        | Utf8      |
| invoices         | payment_id_filter  | Utf8      |
| invoices         | receipt_filter     | Utf8      |
| invoices         | customer_id_filter | Utf8      |
| invoices         | id                 | Utf8      |
| invoices         | entity             | Utf8      |
| invoices         | type               | Utf8      |
| invoices         | invoice_number     | Utf8      |
| invoices         | receipt            | Utf8      |
| invoices         | status             | Utf8      |
| invoices         | customer_id        | Utf8      |
| invoices         | order_id           | Utf8      |
| invoices         | payment_id         | Utf8      |
| invoices         | amount             | Int64     |
| invoices         | currency           | Utf8      |
| invoices         | created_at         | Timestamp |
| invoices         | customer_details   | Json      |
| invoices         | line_items         | Json      |
| invoices         | notes              | Json      |
| orders           | from_timestamp     | Int64     |
| orders           | to_timestamp       | Int64     |
| orders           | receipt_filter     | Utf8      |
| orders           | id                 | Utf8      |
| orders           | entity             | Utf8      |
| orders           | amount             | Int64     |
| orders           | amount_paid        | Int64     |
| orders           | amount_due         | Int64     |
| orders           | currency           | Utf8      |
| orders           | receipt            | Utf8      |
| orders           | status             | Utf8      |
| orders           | attempts           | Int64     |
| orders           | created_at         | Timestamp |
| orders           | notes              | Json      |
| payment_links    | payment_id         | Utf8      |
| payment_links    | id                 | Utf8      |
| payment_links    | entity             | Utf8      |
| payment_links    | amount             | Int64     |
| payment_links    | currency           | Utf8      |
| payment_links    | status             | Utf8      |
| payment_links    | description        | Utf8      |
| payment_links    | reference_id       | Utf8      |
| payment_links    | short_url          | Utf8      |
| payment_links    | customer           | Json      |
| payment_links    | notify             | Json      |
| payment_links    | notes              | Json      |
| payment_links    | created_at         | Timestamp |
| payments         | from_timestamp     | Int64     |
| payments         | to_timestamp       | Int64     |
| payments         | id                 | Utf8      |
| payments         | entity             | Utf8      |
| payments         | amount             | Int64     |
| payments         | currency           | Utf8      |
| payments         | status             | Utf8      |
| payments         | method             | Utf8      |
| payments         | order_id           | Utf8      |
| payments         | invoice_id         | Utf8      |
| payments         | captured           | Boolean   |
| payments         | amount_refunded    | Int64     |
| payments         | refund_status      | Utf8      |
| payments         | email              | Utf8      |
| payments         | contact            | Utf8      |
| payments         | created_at         | Timestamp |
| payments         | notes              | Json      |
| payments         | acquirer_data      | Json      |
| payments         | error_code         | Utf8      |
| payments         | error_description  | Utf8      |
| payments_summary | from_timestamp     | Int64     |
| payments_summary | to_timestamp       | Int64     |
| payments_summary | entity             | Utf8      |
| payments_summary | count              | Int64     |
| payments_summary | items              | Json      |
| refunds          | from_timestamp     | Int64     |
| refunds          | to_timestamp       | Int64     |
| refunds          | payment_id_filter  | Utf8      |
| refunds          | id                 | Utf8      |
| refunds          | entity             | Utf8      |
| refunds          | payment_id         | Utf8      |
| refunds          | amount             | Int64     |
| refunds          | currency           | Utf8      |
| refunds          | status             | Utf8      |
| refunds          | receipt            | Utf8      |
| refunds          | speed_requested    | Utf8      |
| refunds          | speed_processed    | Utf8      |
| refunds          | created_at         | Timestamp |
| refunds          | notes              | Json      |
| settlements      | from_timestamp     | Int64     |
| settlements      | to_timestamp       | Int64     |
| settlements      | id                 | Utf8      |
| settlements      | entity             | Utf8      |
| settlements      | amount             | Int64     |
| settlements      | status             | Utf8      |
| settlements      | fees               | Int64     |
| settlements      | tax                | Int64     |
| settlements      | utr                | Utf8      |
| settlements      | created_at         | Timestamp |
+------------------+--------------------+-----------+
```

```sql
SELECT entity, count, substr(CAST(items AS VARCHAR), 1, 80) AS items_preview
FROM razorpay.payments_summary
LIMIT 1;
```

```text
+------------+-------+---------------+
| entity     | count | items_preview |
+------------+-------+---------------+
| collection | 0     | []            |
+------------+-------+---------------+
```

```sql
SELECT entity, count, from_timestamp, to_timestamp
FROM razorpay.payments_summary
WHERE from_timestamp = 1704067200
  AND to_timestamp = 1893456000
LIMIT 1;
```

```text
+------------+-------+----------------+--------------+
| entity     | count | from_timestamp | to_timestamp |
+------------+-------+----------------+--------------+
| collection | 0     | 1704067200     | 1893456000   |
+------------+-------+----------------+--------------+
```

```sql
SELECT id, amount, currency, status, method, order_id, created_at
FROM razorpay.payments
LIMIT 10;
```

```text
+----+--------+----------+--------+--------+----------+------------+
| id | amount | currency | status | method | order_id | created_at |
+----+--------+----------+--------+--------+----------+------------+
+----+--------+----------+--------+--------+----------+------------+
```

```sql
SELECT id, amount, amount_paid, amount_due, currency, status, receipt, created_at
FROM razorpay.orders
LIMIT 10;
```

```text
+----+--------+-------------+------------+----------+--------+---------+------------+
| id | amount | amount_paid | amount_due | currency | status | receipt | created_at |
+----+--------+-------------+------------+----------+--------+---------+------------+
+----+--------+-------------+------------+----------+--------+---------+------------+
```

```sql
SELECT id, payment_id, amount, currency, status, speed_processed, created_at
FROM razorpay.refunds
LIMIT 10;
```

```text
+----+------------+--------+----------+--------+-----------------+------------+
| id | payment_id | amount | currency | status | speed_processed | created_at |
+----+------------+--------+----------+--------+-----------------+------------+
+----+------------+--------+----------+--------+-----------------+------------+
```

```sql
SELECT id, name, email, contact, created_at
FROM razorpay.customers
LIMIT 10;
```

```text
+----+------+-------+---------+------------+
| id | name | email | contact | created_at |
+----+------+-------+---------+------------+
+----+------+-------+---------+------------+
```

```sql
SELECT id, type, invoice_number, status, customer_id, amount, currency, created_at
FROM razorpay.invoices
LIMIT 10;
```

```text
+----+------+----------------+--------+-------------+--------+----------+------------+
| id | type | invoice_number | status | customer_id | amount | currency | created_at |
+----+------+----------------+--------+-------------+--------+----------+------------+
+----+------+----------------+--------+-------------+--------+----------+------------+
```

```sql
SELECT id, amount, currency, status, reference_id, short_url, created_at
FROM razorpay.payment_links
LIMIT 10;
```

```text
+----+--------+----------+--------+--------------+-----------+------------+
| id | amount | currency | status | reference_id | short_url | created_at |
+----+--------+----------+--------+--------------+-----------+------------+
+----+--------+----------+--------+--------------+-----------+------------+
```

```sql
SELECT id, reference_id, payment_id, status
FROM razorpay.payment_links
WHERE reference_id = 'sample-reference'
  AND payment_id = 'pay_sample'
LIMIT 10;
```

```text
+----+--------------+------------+--------+
| id | reference_id | payment_id | status |
+----+--------------+------------+--------+
+----+--------------+------------+--------+
```

```sql
SELECT id, amount, status, fees, tax, utr, created_at
FROM razorpay.settlements
LIMIT 10;
```

```text
+----+--------+--------+------+-----+-----+------------+
| id | amount | status | fees | tax | utr | created_at |
+----+--------+--------+------+-----+-----+------------+
+----+--------+--------+------+-----+-----+------------+
```
