# PayPal Source

Query PayPal balances, transactions, invoices, and webhooks through Coral SQL.

## Summary

This source lets Coral query PayPal reporting balance rows, transaction-search rows, transaction-search response metadata, invoice rows, and configured webhooks. It exposes six tables: `balance_summary`, `balances`, `transaction_search_summary`, `transaction_search`, `invoices`, and `webhooks`. It targets PayPal REST APIs with OAuth 2.0 bearer-token authentication and keeps mutating checkout, payout, refund, invoice-write, and webhook-write operations out of scope.

## Provider docs

- Get API credentials: https://docs.paypal.ai/get-started/how-to/use-rest-api/get-api-credentials
- REST API requests: https://docs.paypal.ai/developer/how-to/api/make-api-requests
- Balances: https://docs.paypal.ai/reference/api/rest/balances/list-all-balances
- Transactions: https://docs.paypal.ai/reference/api/rest/transactions/list-transactions
- Invoices: https://docs.paypal.ai/reference/api/rest/invoices/list-invoices
- Webhooks: https://docs.paypal.ai/reference/api/rest/webhooks/list-webhooks
- Apps, scopes, and credentials: https://docs.paypal.ai/developer/how-to/apps-scopes-credentials

## Authentication

PayPal REST APIs use OAuth 2.0 access tokens. Exchange your PayPal REST app client ID and secret for an access token, then add the source with that token.

Sandbox token example:

```bash
curl -s -u "$PAYPAL_CLIENT_ID:$PAYPAL_CLIENT_SECRET" \
  -H "Accept: application/json" \
  -H "Accept-Language: en_US" \
  -d "grant_type=client_credentials" \
  https://api-m.sandbox.paypal.com/v1/oauth2/token
```

Add the community source:

```bash
PAYPAL_BASE_URL=https://api-m.sandbox.paypal.com \
PAYPAL_ACCESS_TOKEN=... \
coral source add --file sources/community/paypal/manifest.yaml
```

For live PayPal, use `https://api-m.paypal.com` as `PAYPAL_BASE_URL` and a live access token created from live REST app credentials.

The access token must include permissions for the tables you want to query. PayPal uses separate OAuth scopes for these APIs:

- `paypal.balance_summary` and `paypal.balances`: `https://uri.paypal.com/services/reporting/balances/read`
- `paypal.transaction_search_summary` and `paypal.transaction_search`: `https://uri.paypal.com/services/reporting/search/read`
- `paypal.invoices`: `https://uri.paypal.com/services/invoicing/invoices/read`
- `paypal.webhooks`: `https://uri.paypal.com/services/applications/webhooks`

PayPal returns the scopes granted to the token in the token response. The OpenAPI specs for Transaction Search, Invoices, and Webhooks list the endpoint-level OAuth scopes used by these tables.

## Request limits

This source performs live read-only PayPal API requests. It does not create, capture, refund, send, or mutate PayPal resources. PayPal access tokens expire, sandbox and live credentials are separate, and individual APIs can enforce provider-specific limits such as transaction-search date-window limits.

## Source shape

The source exposes six tables:

- `paypal.balance_summary` returns top-level metadata and the raw `balances` array from `GET /v1/reporting/balances`.
- `paypal.balances` returns one SQL row per PayPal balance entry from the documented `balances[]` array.
- `paypal.transaction_search_summary` returns top-level metadata and the raw `transaction_details` array from `GET /v1/reporting/transactions`.
- `paypal.transaction_search` returns one SQL row per PayPal transaction detail from the documented `transaction_details[]` array.
- `paypal.invoices` lists invoice rows from `GET /v2/invoicing/invoices`.
- `paypal.webhooks` lists app webhooks from `GET /v1/notifications/webhooks`.

## Source scope

- Targets PayPal REST APIs through `PAYPAL_BASE_URL`; sandbox is the default.
- Requires `PAYPAL_ACCESS_TOKEN` bearer authentication.
- `paypal.transaction_search` and `paypal.transaction_search_summary` require `start_date` and `end_date`.
- PayPal defaults `balance_affecting_records_only` to `Y`; pass `balance_affecting_records_only = 'N'` to request non-balance-affecting transactions too.
- `paypal.transaction_search` uses PayPal page pagination with a conservative default page size.
- `paypal.invoices` uses page pagination with `page` and `page_size`.
- `paypal.webhooks` is read-only and supports the optional PayPal `anchor_type` filter.

## Limitations

- The source does not perform the client-credentials token exchange itself. Generate a PayPal access token outside Coral and provide it as `PAYPAL_ACCESS_TOKEN`.
- PayPal access tokens expire. Refresh the token and re-add/update the source when needed.
- Checkout order creation/capture, payments capture/refund, payouts, subscriptions, invoice creation/update/send, webhook creation/update/delete, disputes, vault, and tracking endpoints are intentionally omitted.
- PayPal documents `as_of_time`, `last_refresh_time`, and transaction search date metadata, but sandbox responses can omit those fields; the corresponding summary columns are nullable.
- PayPal enforces provider-specific transaction-search date-window limits. Keep validation ranges short.
- Some tables may return zero rows in a fresh sandbox account, depending on app scopes and sandbox data.

## Tables

### `paypal.balance_summary`

Returns the top-level PayPal balance response.

```sql
SELECT account_id, substr(CAST(balances AS VARCHAR), 1, 80) AS balances_preview
FROM paypal.balance_summary
LIMIT 1;
```

### `paypal.balances`

Returns one row per PayPal balance entry.

```sql
SELECT currency, primary, total_balance_currency_code,
       total_balance_value, available_balance_value
FROM paypal.balances
LIMIT 5;
```

### `paypal.transaction_search_summary`

Returns top-level PayPal transaction search metadata for a required date window.

```sql
SELECT account_number, page, total_items, total_pages
FROM paypal.transaction_search_summary
WHERE start_date = '2026-05-20T00:00:00Z'
  AND end_date = '2026-06-04T00:00:00Z'
  AND fields = 'all'
  AND transaction_type = 'T1900'
  AND transaction_currency = 'USD'
  AND balance_affecting_records_only = 'Y'
LIMIT 1;
```

### `paypal.transaction_search`

Returns one row per PayPal transaction detail for a required date window.

```sql
SELECT transaction_id, transaction_event_code, transaction_status,
       transaction_amount_currency_code, transaction_amount_value
FROM paypal.transaction_search
WHERE start_date = '2026-05-20T00:00:00Z'
  AND end_date = '2026-06-04T00:00:00Z'
  AND fields = 'all'
  AND transaction_type = 'T1900'
  AND transaction_currency = 'USD'
  AND balance_affecting_records_only = 'Y'
LIMIT 5;
```

### `paypal.invoices`

Lists PayPal invoices visible to the access token.

```sql
SELECT id, status, invoice_number, currency_code, invoice_date, create_time
FROM paypal.invoices
LIMIT 10;
```

### `paypal.webhooks`

Lists webhooks configured for the PayPal app.

```sql
SELECT id, url, event_types
FROM paypal.webhooks
LIMIT 10;
```

## Live validation output

Run these checks after setting `PAYPAL_BASE_URL` and `PAYPAL_ACCESS_TOKEN`.

```bash
$ coral source lint sources/community/paypal/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/paypal/manifest.yaml
Added source paypal

  PASS paypal connected successfully

    paypal (6 tables)
    - balance_summary
    - balances
    - invoices
    - transaction_search
    - transaction_search_summary
    - webhooks
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT currency, primary, total_balance_value FROM paypal.balances LIMIT 1
      1 row
```

```bash
$ coral source test paypal
  PASS paypal connected successfully

    paypal (6 tables)
    - balance_summary
    - balances
    - invoices
    - transaction_search
    - transaction_search_summary
    - webhooks
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT currency, primary, total_balance_value FROM paypal.balances LIMIT 1
      1 row
```

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'paypal'
ORDER BY table_name;
```

```text
+----------------------------+
| table_name                 |
+----------------------------+
| balance_summary            |
| balances                   |
| invoices                   |
| transaction_search         |
| transaction_search_summary |
| webhooks                   |
+----------------------------+
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'paypal'
ORDER BY key;
```

```text
+---------------------+----------+----------+
| key                 | kind     | required |
+---------------------+----------+----------+
| PAYPAL_ACCESS_TOKEN | secret   | true     |
| PAYPAL_BASE_URL     | variable | false    |
+---------------------+----------+----------+
```

```sql
SELECT table_name, column_name, data_type
FROM coral.columns
WHERE schema_name = 'paypal'
ORDER BY table_name, ordinal_position;
```

```text
+----------------------------+----------------------------------+-----------+
| table_name                 | column_name                      | data_type |
+----------------------------+----------------------------------+-----------+
| balance_summary            | as_of_time_filter                | Utf8      |
| balance_summary            | currency_code_filter             | Utf8      |
| balance_summary            | account_id                       | Utf8      |
| balance_summary            | as_of_time                       | Timestamp |
| balance_summary            | last_refresh_time                | Timestamp |
| balance_summary            | balances                         | Json      |
| balances                   | as_of_time_filter                | Utf8      |
| balances                   | currency_code_filter             | Utf8      |
| balances                   | currency                         | Utf8      |
| balances                   | primary                          | Boolean   |
| balances                   | total_balance_currency_code      | Utf8      |
| balances                   | total_balance_value              | Utf8      |
| balances                   | available_balance_currency_code  | Utf8      |
| balances                   | available_balance_value          | Utf8      |
| balances                   | withheld_balance_currency_code   | Utf8      |
| balances                   | withheld_balance_value           | Utf8      |
| invoices                   | id                               | Utf8      |
| invoices                   | parent_id                        | Utf8      |
| invoices                   | status                           | Utf8      |
| invoices                   | invoice_number                   | Utf8      |
| invoices                   | currency_code                    | Utf8      |
| invoices                   | invoice_date                     | Utf8      |
| invoices                   | due_date                         | Utf8      |
| invoices                   | create_time                      | Timestamp |
| invoices                   | last_update_time                 | Timestamp |
| invoices                   | detail                           | Json      |
| invoices                   | invoicer                         | Json      |
| invoices                   | primary_recipients               | Json      |
| invoices                   | amount                           | Json      |
| invoices                   | due_amount                       | Json      |
| invoices                   | links                            | Json      |
| transaction_search         | start_date                       | Utf8      |
| transaction_search         | end_date                         | Utf8      |
| transaction_search         | fields                           | Utf8      |
| transaction_search         | transaction_id_filter            | Utf8      |
| transaction_search         | transaction_status_filter        | Utf8      |
| transaction_search         | transaction_type                 | Utf8      |
| transaction_search         | transaction_currency             | Utf8      |
| transaction_search         | balance_affecting_records_only   | Utf8      |
| transaction_search         | transaction_id                   | Utf8      |
| transaction_search         | paypal_reference_id              | Utf8      |
| transaction_search         | transaction_event_code           | Utf8      |
| transaction_search         | transaction_initiation_date      | Timestamp |
| transaction_search         | transaction_updated_date         | Timestamp |
| transaction_search         | transaction_status               | Utf8      |
| transaction_search         | transaction_amount_currency_code | Utf8      |
| transaction_search         | transaction_amount_value         | Utf8      |
| transaction_search         | fee_amount_currency_code         | Utf8      |
| transaction_search         | fee_amount_value                 | Utf8      |
| transaction_search         | ending_balance_currency_code     | Utf8      |
| transaction_search         | ending_balance_value             | Utf8      |
| transaction_search         | available_balance_currency_code  | Utf8      |
| transaction_search         | available_balance_value          | Utf8      |
| transaction_search         | invoice_id                       | Utf8      |
| transaction_search         | transaction_info                 | Json      |
| transaction_search         | payer_account_id                 | Utf8      |
| transaction_search         | payer_email_address              | Utf8      |
| transaction_search         | payer_info                       | Json      |
| transaction_search         | shipping_info                    | Json      |
| transaction_search         | cart_info                        | Json      |
| transaction_search_summary | start_date                       | Utf8      |
| transaction_search_summary | end_date                         | Utf8      |
| transaction_search_summary | fields                           | Utf8      |
| transaction_search_summary | page_size                        | Int64     |
| transaction_search_summary | page_filter                      | Int64     |
| transaction_search_summary | transaction_id                   | Utf8      |
| transaction_search_summary | transaction_status               | Utf8      |
| transaction_search_summary | transaction_type                 | Utf8      |
| transaction_search_summary | transaction_currency             | Utf8      |
| transaction_search_summary | balance_affecting_records_only   | Utf8      |
| transaction_search_summary | account_number                   | Utf8      |
| transaction_search_summary | returned_start_date              | Timestamp |
| transaction_search_summary | returned_end_date                | Timestamp |
| transaction_search_summary | last_refreshed_datetime          | Timestamp |
| transaction_search_summary | page                             | Int64     |
| transaction_search_summary | total_items                      | Int64     |
| transaction_search_summary | total_pages                      | Int64     |
| transaction_search_summary | transaction_details              | Json      |
| transaction_search_summary | links                            | Json      |
| webhooks                   | anchor_type                      | Utf8      |
| webhooks                   | id                               | Utf8      |
| webhooks                   | url                              | Utf8      |
| webhooks                   | event_types                      | Json      |
| webhooks                   | links                            | Json      |
+----------------------------+----------------------------------+-----------+
```

```sql
SELECT account_id, substr(CAST(balances AS VARCHAR), 1, 80) AS balances_preview
FROM paypal.balance_summary
LIMIT 1;
```

```text
+---------------+----------------------------------------------------------------------------------+
| account_id    | balances_preview                                                                 |
+---------------+----------------------------------------------------------------------------------+
| <account_id>  | [{"currency":"USD","total_balance":{"currency_code":"USD","value":"5000.00"},"av |
+---------------+----------------------------------------------------------------------------------+
```

```sql
SELECT currency, primary, total_balance_currency_code,
       total_balance_value, available_balance_value
FROM paypal.balances
LIMIT 5;
```

```text
+----------+---------+-----------------------------+---------------------+-------------------------+
| currency | primary | total_balance_currency_code | total_balance_value | available_balance_value |
+----------+---------+-----------------------------+---------------------+-------------------------+
| USD      |         | USD                         | 5000.00             | 5000.00                 |
+----------+---------+-----------------------------+---------------------+-------------------------+
```

```sql
SELECT account_number, page, total_items, total_pages
FROM paypal.transaction_search_summary
WHERE start_date = '2026-05-20T00:00:00Z'
  AND end_date = '2026-06-04T00:00:00Z'
  AND fields = 'all'
  AND transaction_type = 'T1900'
  AND transaction_currency = 'USD'
  AND balance_affecting_records_only = 'Y'
LIMIT 1;
```

```text
+----------------+------+-------------+-------------+
| account_number | page | total_items | total_pages |
+----------------+------+-------------+-------------+
| <account_id>    | 1    | 1           | 1           |
+----------------+------+-------------+-------------+
```

```sql
SELECT transaction_id, transaction_event_code, transaction_status,
       transaction_amount_currency_code, transaction_amount_value
FROM paypal.transaction_search
WHERE start_date = '2026-05-20T00:00:00Z'
  AND end_date = '2026-06-04T00:00:00Z'
  AND fields = 'all'
  AND transaction_type = 'T1900'
  AND transaction_currency = 'USD'
  AND balance_affecting_records_only = 'Y'
LIMIT 5;
```

```text
+-------------------+------------------------+--------------------+----------------------------------+--------------------------+
| transaction_id    | transaction_event_code | transaction_status | transaction_amount_currency_code | transaction_amount_value |
+-------------------+------------------------+--------------------+----------------------------------+--------------------------+
| <transaction_id> | T1900                  | S                  | USD                              | 5000.00                  |
+-------------------+------------------------+--------------------+----------------------------------+--------------------------+
```

```sql
SELECT id, status, invoice_number, currency_code, invoice_date, create_time
FROM paypal.invoices
LIMIT 10;
```

```text
+----+--------+----------------+---------------+--------------+-------------+
| id | status | invoice_number | currency_code | invoice_date | create_time |
+----+--------+----------------+---------------+--------------+-------------+
+----+--------+----------------+---------------+--------------+-------------+
```

```sql
SELECT id, url, event_types
FROM paypal.webhooks
LIMIT 10;
```

```text
+----+-----+-------------+
| id | url | event_types |
+----+-----+-------------+
+----+-----+-------------+
```
