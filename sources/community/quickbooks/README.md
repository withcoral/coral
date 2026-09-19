# QuickBooks Online Community Source

Query QuickBooks Online accounting data through Coral SQL using the
[QuickBooks Online Accounting API](https://developer.intuit.com/app/developer/qbo/docs/get-started).
The source is read-only: it only sends `GET` requests.

## Setup

### 1. Find your company ID

The company ID (realm ID) is in QuickBooks under **Settings > Additional
info**. Intuit also returns it as the `realmId` parameter on the OAuth
redirect.

### 2. Add the source

With OAuth, which lets Coral refresh the one-hour access token for you:

```bash
coral source add --interactive --file sources/community/quickbooks/manifest.yaml
```

Choose **Connect with QuickBooks** and enter the Client ID and Client Secret
of an app from [developer.intuit.com](https://developer.intuit.com) that has
the Accounting scope and `http://localhost:53682/oauth/callback` as a redirect
URI.

Intuit only accepts `localhost` redirect URIs on Development keys, so OAuth
setup works for sandbox companies only. For a production company, pass an
access token from your own OAuth integration or the
[OAuth 2.0 Playground](https://developer.intuit.com/app/developer/playground):

```bash
export QUICKBOOKS_REALM_ID="<company-id>"
export QUICKBOOKS_ACCESS_TOKEN="<access-token>"
coral source add --file sources/community/quickbooks/manifest.yaml
```

A pasted token expires after one hour and Coral cannot refresh it. Run the
same command with a new token when queries start failing with a 401.

For a sandbox company, also set
`QUICKBOOKS_BASE_URL=https://sandbox-quickbooks.api.intuit.com`.

### 3. Verify

```bash
coral source test quickbooks
```

## Tables

### Lists

| Table | QuickBooks entity | Notes |
|---|---|---|
| `company_info` | CompanyInfo | One row |
| `accounts` | Account | Chart of accounts with current balances |
| `customers` | Customer | Includes sub-customers and projects |
| `vendors` | Vendor | |
| `employees` | Employee | No payroll data |
| `items` | Item | Products, services, and categories |
| `classes` | Class | Empty unless class tracking is on |
| `departments` | Department | Locations in the QuickBooks UI |
| `terms` | Term | |
| `payment_methods` | PaymentMethod | |
| `tax_codes` | TaxCode | |
| `tax_rates` | TaxRate | |

QuickBooks marks deleted list records inactive instead of removing them, and
old transactions still point at them. List tables return inactive records too,
so those joins keep working. Filter on `active` to leave them out.

### Transactions

| Table | QuickBooks entity |
|---|---|
| `invoices` | Invoice |
| `estimates` | Estimate |
| `sales_receipts` | SalesReceipt |
| `credit_memos` | CreditMemo |
| `refund_receipts` | RefundReceipt |
| `payments` | Payment |
| `deposits` | Deposit |
| `bills` | Bill |
| `bill_payments` | BillPayment |
| `purchases` | Purchase (expenses, checks, credit card charges) |
| `purchase_orders` | PurchaseOrder |
| `vendor_credits` | VendorCredit |
| `journal_entries` | JournalEntry |
| `transfers` | Transfer |
| `time_activities` | TimeActivity |

References to other records become `<name>_id` and `<name>_name` columns, so
`invoices.customer_id` joins to `customers.id`. Line items, linked
transactions, addresses, and custom fields are JSON columns. `txn_date` and
`due_date` are `YYYY-MM-DD` strings; `created_at` and `updated_at` are UTC
timestamps.

**Optional filters:** `updated_since` on every table above except
`company_info` and `tax_codes`, plus `txn_date_from` and `txn_date_to` on
transaction tables. Coral sends these to QuickBooks, so they cut both API
calls and rows transferred. Use them with `=`. Other predicates run in Coral
after the rows are fetched.

### Reports

| Table | Optional filters |
|---|---|
| `profit_and_loss` | `start_date`, `end_date`, `date_macro`, `accounting_method`, `summarize_column_by` |
| `balance_sheet` | same as `profit_and_loss` |
| `trial_balance` | same as `profit_and_loss` |
| `cash_flow` | same, without `accounting_method` |
| `aged_receivables` | `report_date`, `aging_period`, `num_periods` |
| `aged_payables` | `report_date`, `aging_period`, `num_periods` |

Each row is one top-level row of the report. Section rows fill `group`,
`section`, `summary_label`, `summary_value`, and `summary`, with nested rows
as JSON in `rows`. Data rows, which the aging reports and trial balance use
for each customer, vendor, or account, fill `name`, `entity_id`, and
`col_data`. Values are strings as QuickBooks returns them, so cast them before
doing math. Without date filters QuickBooks reports on the fiscal year to
date.

## Example queries

```sql
-- Customers with the largest open balance
SELECT c.display_name, count(*) AS open_invoices, sum(i.balance) AS open_balance
FROM quickbooks.invoices i
JOIN quickbooks.customers c ON c.id = i.customer_id
WHERE i.balance > 0
GROUP BY 1
ORDER BY 3 DESC
LIMIT 10;

-- Invoices dated in Q3 2026
SELECT doc_number, customer_name, txn_date, total_amt, balance
FROM quickbooks.invoices
WHERE txn_date_from = '2026-07-01' AND txn_date_to = '2026-09-30';

-- Revenue by product or service
SELECT json_get_str(line, 'SalesItemLineDetail', 'ItemRef', 'name') AS item,
       sum(json_get_float(line, 'Amount')) AS revenue
FROM (SELECT unnest(json_get_array(lines)) AS line FROM quickbooks.invoices)
WHERE json_get_str(line, 'DetailType') = 'SalesItemLineDetail'
GROUP BY 1
ORDER BY 2 DESC;

-- Profit and loss for the fiscal year
SELECT "group", summary_label, summary_value
FROM quickbooks.profit_and_loss
WHERE date_macro = 'This Fiscal Year';

-- Receivables aging, largest first
SELECT name AS customer, json_get_str(col_data, 6, 'value') AS total
FROM quickbooks.aged_receivables
WHERE report_date = '2026-09-30'
ORDER BY CAST(json_get_str(col_data, 6, 'value') AS DOUBLE) DESC NULLS LAST;
```

## Limitations

- OAuth setup works with sandbox companies only, as described in Setup.
- `tax_codes` takes no filters and returns active codes only. QuickBooks drops
  its built-in `TAX` and `NON` codes from any query that has a `WHERE` clause.
- Reports expose top-level rows; deeper levels stay in `rows`. The number of
  cells in `col_data` and `summary` changes with `summarize_column_by` and the
  aging settings. QuickBooks truncates very large reports, so keep date ranges
  short when you add a column breakdown.
- Deleted transactions are not returned, and Change Data Capture is not
  exposed. Attachments, budgets, recurring transactions, and preferences are
  also out of scope.
- A full table scan costs one request per 1000 rows plus one, because paging
  ends on the first empty page. Intuit
  [meters read calls](https://developer.intuit.com/app/developer/qbo/docs/get-started/partner-faq)
  against the app that issued the token and limits each company to 500
  requests a minute.
- QuickBooks can return a fault inside an HTTP 200 response. Coral reports 4xx
  and 5xx faults with Intuit's message, but a fault in a 200 shows up as an
  empty result.
