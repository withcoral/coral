# Chargebee

**Version:** 1.0.0
**Backend:** HTTP
**Base URL:** `https://{your-site}.chargebee.com`

Query Chargebee subscription billing data as SQL tables. Inspect subscriptions, customers, invoices, and plans. Join with Freshdesk tickets or Linear issues for customer health and churn intelligence.

## Scope: Product Catalog v1

This source targets Chargebee **Product Catalog v1**, where subscriptions are organized around **plans** (`chargebee.plans`, referenced by the `plan_id` column on subscriptions). If your Chargebee site runs **Product Catalog v2**, billing is organized around `items` and `item_prices`, which this version does not cover yet. Check **Settings → Configure Chargebee → Product Catalog** in your dashboard to confirm which catalog your site uses before installing.

## Tables

| Table | Description | Pushed-down filters |
|-------|-------------|-----------------|
| `chargebee.subscriptions` | Subscriptions with status, MRR, billing cycle, and term dates | `status`, `customer_id`, `updated_after` |
| `chargebee.customers` | Customers with contact details and MRR | `email`, `updated_after` |
| `chargebee.invoices` | Invoices with amounts, status, and payment timestamps | `status`, `customer_id`, `updated_after` |
| `chargebee.plans` | Plan definitions with pricing and billing configuration (PCv1) | `status` |

Filters listed above are pushed to the Chargebee API (e.g. `status[is]=active`, `updated_at[after]=<epoch>`), so the `fetch_limit_default` of 100 rows applies to the filtered set.

`updated_after` takes a Unix epoch second value and maps to Chargebee's [`updated_at[after]`](https://apidocs.chargebee.com/docs/api/list-ops) list operator. Use it for incremental "changed since" queries, for example `WHERE updated_after = 1714521600`.

> **Important: client-side `WHERE` only filters fetched rows.** A SQL `WHERE` on any column *not* in the pushed-down list above (including a bare `updated_at` or `created_at` comparison, or `plan_id`) is applied by Coral *after* fetching, so it only sees the first `fetch_limit_default` rows. For correct "changed since" results on large accounts, always pass `updated_after` so the time bound reaches the Chargebee API. `plan_id` is intentionally not pushed down: Chargebee's documented PCv1 subscription list does not expose a `plan_id` list operator, so filter it client-side or via `customer_id`.

## Authentication

Requires `CHARGEBEE_SITE` and `CHARGEBEE_API_KEY`.

**To get your API key:**

1. Log in to your Chargebee dashboard
2. Go to **Settings** -> **Configure Chargebee** -> **API Keys & Webhooks**
3. Create or copy a read-only API key

**To find your site name:**

Your Chargebee URL is `https://{site}.chargebee.com`. Enter just the subdomain (e.g. `acme`, not the full URL). For a test site use `acme-test`.

**Note on the password field:**

Chargebee documents HTTP Basic Auth with the API key as the username and an [empty password](https://apidocs.chargebee.com/docs/api/auth). The Coral source spec requires `minLength: 1` for BasicAuth passwords, so a zero-length password fails schema validation. The manifest therefore sends a single placeholder character `x` as the password, producing `Authorization: Basic base64(<api_key>:x)`. Per Chargebee's documentation, authentication is keyed on the API key in the username field and the password is left blank, so a placeholder in the password position should not affect the outcome. This note documents the workaround so it is transparent. It has not been independently verified against arbitrary password values, so if you hit an auth error, confirm your API key is a valid read-only key for the site.

## Install

```bash
CHARGEBEE_SITE=acme-test \
CHARGEBEE_API_KEY=your-key \
coral source add --file sources/community/chargebee/manifest.yaml
```

## Verify your install

After adding the source, confirm it loaded and that your credentials work:

```bash
coral source add --file sources/community/chargebee/manifest.yaml
coral source test chargebee
```

A successful run lists the four tables and passes the declared test query. The block below shows the **expected shape** of that output (illustrative, with placeholder IDs), not a captured run from a specific account:

```text
  ✓ chargebee connected successfully
  Secrets: keyring

    chargebee (4 tables)
    ├─ customers
    ├─ invoices
    ├─ plans
    └─ subscriptions

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, email FROM chargebee.customers LIMIT 1
      1 row
```

A representative subscriptions query and the shape of its result:

```bash
coral sql "SELECT id, status, mrr, currency_code FROM chargebee.subscriptions WHERE status = 'active' LIMIT 3"
```

```text
+--------------+--------+-------+---------------+
| id           | status | mrr   | currency_code |
+--------------+--------+-------+---------------+
| sub_Abc12345 | active | 4900  | USD           |
| sub_Def67890 | active | 9900  | USD           |
| sub_Ghi11223 | active | 14900 | USD           |
+--------------+--------+-------+---------------+
3 rows
```

## Example Queries

Active subscriptions by MRR (pushes `status = 'active'` to Chargebee API):

```sql
SELECT id, customer_id, plan_id, status, mrr, currency_code, current_term_end
FROM chargebee.subscriptions
WHERE status = 'active'
ORDER BY mrr DESC
LIMIT 50;
```

Subscriptions for a specific customer:

```sql
SELECT id, plan_id, status, mrr, current_term_end
FROM chargebee.subscriptions
WHERE customer_id = 'cust_Abc12345'
ORDER BY current_term_end ASC;
```

Customers by MRR:

```sql
SELECT id, email, company, mrr, currency_code
FROM chargebee.customers
WHERE mrr > 0
ORDER BY mrr DESC
LIMIT 50;
```

Overdue invoices (pushes `status = 'payment_due'` to Chargebee API):

```sql
SELECT id, customer_id, subscription_id, total, amount_due, due_date, currency_code
FROM chargebee.invoices
WHERE status = 'payment_due'
ORDER BY due_date ASC;
```

All active plans with pricing:

```sql
SELECT id, name, price, currency_code, period, period_unit, pricing_model, trial_period
FROM chargebee.plans
WHERE status = 'active'
ORDER BY price DESC;
```

Revenue at risk — cancelled subscriptions with MRR:

```sql
SELECT id, customer_id, plan_id, mrr, cancelled_at
FROM chargebee.subscriptions
WHERE status = 'cancelled'
  AND mrr > 0
ORDER BY cancelled_at DESC;
```

Incremental sync — subscriptions changed since a timestamp (pushes `updated_at[after]` to the Chargebee API so the fetch limit applies to the changed set, not the whole site):

```sql
SELECT id, customer_id, status, mrr, updated_at
FROM chargebee.subscriptions
WHERE updated_after = 1714521600
ORDER BY updated_at DESC;
```

## Cross-Source JOIN Example

At-risk accounts — customers with active subscriptions and open Freshdesk tickets:

```sql
WITH active_subs AS (
    SELECT customer_id, SUM(mrr) AS total_mrr, currency_code
    FROM chargebee.subscriptions
    WHERE status = 'active'
    GROUP BY customer_id, currency_code
),
open_tickets AS (
    SELECT requester_id, COUNT(*) AS open_ticket_count
    FROM freshdesk.tickets
    WHERE status IN (2, 3)
    GROUP BY requester_id
)
SELECT
    c.id          AS customer_id,
    c.email,
    c.company,
    s.total_mrr,
    s.currency_code,
    t.open_ticket_count
FROM chargebee.customers c
JOIN active_subs s ON s.customer_id = c.id
LEFT JOIN open_tickets t ON t.requester_id = c.id
WHERE t.open_ticket_count > 0
ORDER BY s.total_mrr DESC;
```

## Status and Enum Reference

### Subscription status

| Value | Meaning |
|-------|---------|
| `active` | Currently active and billing |
| `in_trial` | In a free trial period |
| `non_renewing` | Active but will not renew at term end |
| `paused` | Billing paused |
| `cancelled` | Subscription cancelled |
| `future` | Scheduled to start in the future |

### Invoice status

| Value | Meaning |
|-------|---------|
| `paid` | Fully paid |
| `payment_due` | Payment overdue |
| `not_paid` | Could not be collected |
| `voided` | Voided before payment |
| `pending` | Not yet finalized |
| `posted` | Finalized, awaiting payment |

### Plan pricing_model

| Value | Meaning |
|-------|---------|
| `flat_fee` | Fixed price per period |
| `per_unit` | Price per unit of quantity |
| `tiered` | Price depends on quantity tiers |
| `volume` | Bulk pricing based on total volume |
| `stairstep` | Step pricing at quantity thresholds |

## Notes

- All tables are strictly read-only.
- Chargebee paginates list endpoints with cursor-based pagination via `next_offset`. Coral handles pagination automatically up to `fetch_limit_default` (100 rows per table by default).
- All amount fields (`mrr`, `price`, `total`, `amount_due`, etc.) are in the **smallest currency unit** (e.g. cents for USD). Divide by 100 for display values.
- All timestamp fields (`created_at`, `current_term_end`, `paid_at`, etc.) are **Unix epoch seconds**.
- `chargebee.plans` covers Product Catalog v1. If your site uses Product Catalog v2, use `items` and `item_prices` endpoints instead (not covered by this spec).
- Rate limits vary by Chargebee plan. The connector handles `429` responses automatically via `Retry-After`.
