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
* Configurable API and Accounts domains

---

## Source Inputs

| Input                | Type     | Required | Default                   |
| -------------------- | -------- | -------- | ------------------------- |
| ZOHO_API_DOMAIN      | Variable | No       | https://www.zohoapis.com  |
| ZOHO_ACCOUNTS_DOMAIN | Variable | No       | https://accounts.zoho.com |
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

## Supported Tables

The source exposes the following primary tables:

| Table             |
| ----------------- |
| organizations     |
| contacts          |
| invoices          |
| expenses          |
| customer_payments |
| bills             |
| items             |

Additional related entities may be available as defined within the source specification.

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

and

```text
ZOHO_ACCOUNTS_DOMAIN
```

---

## Pagination

List endpoints automatically handle paginated responses from the Zoho Books API, allowing Coral to retrieve complete datasets across multiple pages.

---

## Validation

The source specification has been validated using:

```bash
coral source lint zoho_books.yaml
```

Validation completed successfully.

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
zoho_books.yaml
```

This file defines the complete Coral source specification for integrating Zoho Books data into Coral SQL workflows.
