# Zoho Books Custom Source Spec (Zoho Books API v3)

## Overview

This source specification adds support for querying data from **Zoho Books** through Coral using the **Zoho Books API v3**. It enables analytics and reporting on financial, customer, vendor, invoice, payment, and operational data using Coral SQL.

## Why Zoho Books?

Zoho Books is widely used by small and medium-sized businesses for accounting and financial management. This source allows Coral users to analyze business data directly from Zoho Books without exporting data manually.

### Use Cases

* Invoice and payment analytics
* Accounts receivable and payable visibility
* Customer financial insights
* Vendor performance analysis
* Expense reporting
* Purchase and sales document reporting
* Operational dashboards and business intelligence

---

## Features

* HTTP-based source using Zoho Books API v3
* OAuth token authentication
* Organization-scoped data access
* Regional API endpoint support
* Pagination support for list endpoints
* Typed schemas for major business entities
* Configurable API domain support

---
## Source Inputs

| Input                | Type     | Required | Default                   |
| -------------------- | -------- | -------- | ------------------------- |
| ZOHO_API_DOMAIN      | Variable | No       | https://www.zohoapis.com  |
| ZOHO_ORGANIZATION_ID | Variable | Yes      | -                         |
| ZOHO_OAUTH_TOKEN     | Secret   | Yes      | -                         |

---

## Authentication

The source uses Zoho OAuth access tokens.

Required HTTP header:

```http
Authorization: Zoho-oauthtoken <token>
```

Example:

```http
Authorization: Zoho-oauthtoken 1000.xxxxxxxxxxxxxxxxx
```

> Access token generation and refresh management are handled outside the source configuration. The access token should be supplied through the `ZOHO_OAUTH_TOKEN` secret input.

---

## Required OAuth Scopes

Zoho Books scopes are module-specific. This source is read-only and requires `READ` scopes that match the exposed tables.

### Minimum scopes for first success (based on current test queries)

- `ZohoBooks.contacts.READ`
- `ZohoBooks.invoices.READ`
- `ZohoBooks.settings.READ`

### Full scopes for all currently exposed tables

| Table | Scope |
| --- | --- |
| organizations | `ZohoBooks.settings.READ` |
| contacts | `ZohoBooks.contacts.READ` |
| invoices | `ZohoBooks.invoices.READ` |
| estimates | `ZohoBooks.estimates.READ` |
| sales_orders | `ZohoBooks.salesorders.READ` |
| bills | `ZohoBooks.bills.READ` |
| purchase_orders | `ZohoBooks.purchaseorders.READ` |
| expenses | `ZohoBooks.expenses.READ` |
| credit_notes | `ZohoBooks.creditnotes.READ` |
| customer_payments | `ZohoBooks.customerpayments.READ` |
| vendor_payments | `ZohoBooks.vendorpayments.READ` |
| items | `ZohoBooks.settings.READ` |
| chart_of_accounts | `ZohoBooks.accountants.READ` |
| journals | `ZohoBooks.accountants.READ` |
| projects | `ZohoBooks.projects.READ` |

---

## OAuth Token Generation and Refresh (Domain-Specific)

Use the Zoho Accounts domain for your data center while generating and refreshing tokens.

| Region | Accounts Domain |
| --- | --- |
| Global (US) | `https://accounts.zoho.com` |
| Europe | `https://accounts.zoho.eu` |
| India | `https://accounts.zoho.in` |
| Australia | `https://accounts.zoho.com.au` |
| Japan | `https://accounts.zoho.jp` |
| Canada | `https://accounts.zoho.ca` |
| China | `https://accounts.zoho.com.cn` |
| Saudi Arabia | `https://accounts.zoho.sa` |

1. Generate grant code  
`GET {ACCOUNTS_DOMAIN}/oauth/v2/auth` with:
- `scope=<comma-separated scopes>`
- `response_type=code`
- `client_id=<client_id>`
- `redirect_uri=<redirect_uri>`
- `access_type=offline` (required for refresh token)

2. Exchange grant code for tokens  
`POST {ACCOUNTS_DOMAIN}/oauth/v2/token` with:
- `grant_type=authorization_code`
- `client_id`
- `client_secret`
- `redirect_uri`
- `code`

3. Refresh access token  
`POST {ACCOUNTS_DOMAIN}/oauth/v2/token` with:
- `grant_type=refresh_token`
- `refresh_token`
- `client_id`
- `client_secret`

4. Call Zoho Books APIs using:
- Header: `Authorization: Zoho-oauthtoken <access_token>`
- API domain from your region (for example `https://www.zohoapis.com`)

Reference: https://www.zoho.com/books/api/v3/oauth/

---

## Supported Tables

The source currently exposes the following tables:

| Table |
| --- |
| organizations |
| contacts |
| invoices |
| estimates |
| sales_orders |
| bills |
| purchase_orders |
| expenses |
| credit_notes |
| customer_payments |
| vendor_payments |
| items |
| chart_of_accounts |
| journals |
| projects |

### Quick schema orientation

- Master data: `contacts`, `items`, `chart_of_accounts`, `projects`, `organizations`
- Sales cycle: `estimates`, `sales_orders`, `invoices`, `credit_notes`, `customer_payments`
- Purchase cycle: `purchase_orders`, `bills`, `vendor_payments`, `expenses`
- Accounting/ledger context: `journals`

---

## Regional Support

Zoho Books supports multiple regional API domains.

Examples:

| Region    | API Domain                  |
| --------- | --------------------------- |
| Global    | https://www.zohoapis.com    |
| Europe    | https://www.zohoapis.eu     |
| India     | https://www.zohoapis.in     |
| Australia | https://www.zohoapis.com.au |

Configure the appropriate domain through:

```text
ZOHO_API_DOMAIN
```

---

## Pagination

List endpoints automatically handle paginated responses from the Zoho Books API, allowing Coral to retrieve complete datasets across multiple pages.

---

## Credentials and Organization ID

You need:

1. `ZOHO_OAUTH_TOKEN` (access token)
2. `ZOHO_ORGANIZATION_ID`
3. `ZOHO_API_DOMAIN` (region-specific API domain)

If you do not know the organization ID yet, use the `organizations` table or the Zoho Books UI Profile tab to discover it, then supply it for the transactional tables that require it.

How to get `organization_id`:

- Call `GET {ZOHO_API_DOMAIN}/books/v3/organizations` with a valid token, or
- Copy it from the Zoho Books app URL after signing in, or
- Open Zoho Books UI -> Profile tab and copy the organization ID shown there.
- The `organizations` table is the discovery path for this ID and does not require `organization_id` itself.

How to get tokens:

- Create a Zoho API client in Zoho API Console (Self Client is commonly used for server-to-server testing).
- Use your region-specific Zoho Accounts domain for OAuth (`/oauth/v2/auth` and `/oauth/v2/token`).
- Request the required module scopes listed above.
- Use `access_type=offline` to receive a refresh token.

Authorization code exchange example (region-specific endpoint):

```http
POST https://accounts.zoho.in/oauth/v2/token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
client_id=YOUR_CLIENT_ID&
client_secret=YOUR_CLIENT_SECRET&
code=YOUR_AUTHORIZATION_CODE
```

This returns an access token (and, when applicable, refresh token data) that you can use as `ZOHO_OAUTH_TOKEN`.

---

## Example Queries (Filtered, First-Success Friendly)

```sql
-- Verify org access
SELECT organization_id, name, currency_code
FROM zoho_books.organizations
LIMIT 1;
```

```sql
-- Active customer contacts only
SELECT contact_id, contact_name, contact_type, status
FROM zoho_books.contacts
WHERE contact_type = 'customer' AND filter_by = 'Status.Active'
LIMIT 25;
```

```sql
-- Invoices for a specific customer and date window
SELECT invoice_id, invoice_number, customer_name, due_date, total, balance
FROM zoho_books.invoices
WHERE customer_id = 'YOUR_CUSTOMER_ID'
  AND date_start = '2026-01-01'
  AND date_end = '2026-03-31'
LIMIT 50;
```

```sql
-- Expenses for a vendor in a date range
SELECT expense_id, vendor_name, date, total, currency_code
FROM zoho_books.expenses
WHERE vendor_id = 'YOUR_VENDOR_ID'
  AND date_start = '2026-01-01'
  AND date_end = '2026-01-31'
LIMIT 50;
```

```sql
-- Item search
SELECT item_id, name, sku, status, rate
FROM zoho_books.items
WHERE search_text = 'service'
LIMIT 25;
```

---

## Rate Limits and Usage Guidance

- Zoho Books API limit is documented as **100 requests/minute per organization**.
- This source uses pagination (`page`, `per_page`) and now supports filter pushdown to reduce request volume.
- Prefer filtered queries over full-table scans for large modules like contacts, invoices, and expenses.

---

## Zoho Documentation Links

- Zoho Books OAuth (v3): https://www.zoho.com/books/api/v3/oauth/
- Zoho Books API Introduction and limits: https://www.zoho.com/books/api/v3/introduction/
- Zoho Books Pagination: https://www.zoho.com/books/api/v3/pagination/
- Invoices API: https://www.zoho.com/books/api/v3/invoices/
- Contacts API: https://www.zoho.com/books/api/v3/contacts/
- Expenses API: https://www.zoho.com/books/api/v3/expenses/
- Items API: https://www.zoho.com/books/api/v3/items/

---

## Validation

The source specification has been validated using:

```bash
coral source lint manifest.yaml
```

Validation completed successfully.

Runtime behavior was also validated against a real Zoho Books organization (sanitized):

```bash
coral source add --interactive --file manifest.yaml
```

Result summary:

- Added source `zoho_books` successfully
- Detected 15 tables
- Query tests: `4 declared`, `4 passed`, `0 failed`

```bash
coral source test zoho_books
```

Result summary:

- `SELECT * FROM zoho_books.organizations LIMIT 1` -> 1 row
- `SELECT * FROM zoho_books.contacts LIMIT 1` -> 1 row
- `SELECT * FROM zoho_books.invoices LIMIT 1` -> 1 row
- `SELECT * FROM zoho_books.items LIMIT 1` -> 1 row

Representative query checks:

```bash
coral sql "SELECT * FROM zoho_books.organizations LIMIT 5"
coral sql "SELECT * FROM zoho_books.contacts LIMIT 5"
coral sql "SELECT * FROM zoho_books.invoices LIMIT 5"
```

All queries returned rows successfully. Sensitive values such as organization IDs, names, and any confidential fields should be redacted in shared logs.

---

## Notes

* Built for Zoho Books API v3.
* Requires a valid Zoho Books organization ID.
* Requires an OAuth access token with appropriate scopes.
* Token refresh is not managed by the source and must be handled externally.
* Supports region-specific Zoho deployments through configurable domains.

---

## Source File

```text
manifest.yaml
```

This file defines the complete Coral source specification for integrating Zoho Books data into Coral SQL workflows.
