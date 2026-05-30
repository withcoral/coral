# Airtable

Query Airtable operational bases through Coral SQL. This community source is
designed for a conventional enterprise Airtable base with tables for employees,
departments, assets, IT tickets, projects, tasks, clients, invoices, vendors,
purchase orders, time logs, and expenses.

## Setup

Create an Airtable personal access token at
<https://airtable.com/create/tokens>. Grant it access to the base you want to
query and include the `data.records:read` scope.

Find your Airtable base ID in the base URL or in Airtable's API documentation
for the base. Base IDs start with `app`.

```bash
export AIRTABLE_ACCESS_TOKEN="<your-airtable-token>"
export AIRTABLE_BASE_ID="<your-base-id>"
coral source add --file sources/community/airtable/manifest.yaml
```

`AIRTABLE_ACCESS_TOKEN` should be a personal access token or OAuth access token
sent as `Authorization: Bearer <token>`. For read-only Coral queries, the token
needs `data.records:read` on the configured base. If you want to create a test
base or seed validation records through the Airtable API, use a temporary token
with `schema.bases:write` and `data.records:write`, then rotate it after setup.

By default, the source queries tables named `employees`, `departments`,
`assets`, `it_tickets`, `projects`, `tasks`, `clients`, `invoices`, `vendors`,
`purchase_orders`, `time_logs`, and `expenses`. If your base uses different
table names or table IDs, override the corresponding variable before adding the
source. Prefer Airtable table IDs. If you use names with spaces, URL-encode the
spaces as `%20`:

```bash
export AIRTABLE_EMPLOYEES_TABLE="tblXXXXXXXXXXXXXX"
export AIRTABLE_IT_TICKETS_TABLE="IT%20Tickets"
export AIRTABLE_PURCHASE_ORDERS_TABLE="Purchase%20Orders"
```

## Tables

| Table | Description |
| --- | --- |
| `employees` | Employee directory records. |
| `departments` | Department records. |
| `assets` | Hardware, software, and other asset inventory records. |
| `it_tickets` | IT support and operations ticket records. |
| `projects` | Project records. |
| `tasks` | Task records. |
| `clients` | Client or customer account records. |
| `invoices` | Invoice records. |
| `vendors` | Vendor records. |
| `purchase_orders` | Purchase order records. |
| `time_logs` | Time tracking records. |
| `expenses` | Expense records. |

Every table returns Airtable record metadata plus common convenience fields:
`id`, `created_at`, `name`, `status`, `email`, `department`, `owner`,
`priority`, `amount`, `start_date`, `due_date`, `end_date`, `description`, and
`notes`. The full Airtable `fields` object is exposed as JSON so you can query
base-specific fields with Coral's JSON functions.

## Filters

Every table supports these optional provider-side filters:

| Filter | Description |
| --- | --- |
| `view` | Airtable saved view name or view ID. |
| `filter_by_formula` | Airtable formula passed as `filterByFormula`. |

For example:

```sql
SELECT id, name, status, email
FROM airtable.employees
WHERE filter_by_formula = '{Status} = ''Active'''
ORDER BY name
LIMIT 20;
```

## Search functions

The source also includes five search functions that use Airtable
`filterByFormula`:

| Function | Target table |
| --- | --- |
| `search_employees(formula, view)` | `employees` |
| `search_assets(formula, view)` | `assets` |
| `search_it_tickets(formula, view)` | `it_tickets` |
| `search_projects(formula, view)` | `projects` |
| `search_clients(formula, view)` | `clients` |

Example:

```sql
SELECT id, name, status, department
FROM airtable.search_employees(
  formula => 'AND({Status} = ''Active'', {Department} = ''IT'')'
)
LIMIT 10;
```

Find high-priority open tickets:

```sql
SELECT id, name, status, priority, owner
FROM airtable.search_it_tickets(
  formula => 'AND({Status} != ''Closed'', {Priority} = ''High'')'
)
LIMIT 10;
```

## Base-specific fields

Airtable bases often have custom field names. Use the `fields` JSON column for
fields that are not exposed as convenience columns:

```sql
SELECT
  id,
  name,
  json_get_str(fields, 'Manager') AS manager,
  json_get_str(fields, 'Laptop Serial') AS laptop_serial
FROM airtable.employees
LIMIT 20;
```

## Notes

- Airtable returns list records in pages of up to 100 records. Coral follows
  the returned `offset` cursor until the SQL limit is satisfied or Airtable has
  no more records.
- Airtable omits empty field values from record payloads. Missing convenience
  columns therefore appear as `NULL`.
- Airtable applies a rate limit of 5 requests per second per base.
- `filterByFormula` must be a valid Airtable formula for the target table. If a
  `view` is also provided, Airtable returns only records in that view that also
  satisfy the formula.

## Validation

```bash
coral source lint sources/community/airtable/manifest.yaml
coral source add --file sources/community/airtable/manifest.yaml
coral source test airtable
```

The default declared tests read one row from `airtable.employees` and one row
from `airtable.projects`, so your configured base should contain those tables
or matching table-variable overrides.

### Captured live validation

The following output was captured against a live Airtable base with the 12
default tables and 125 seeded employee records.

#### Add source

```bash
coral source add --file sources/community/airtable/manifest.yaml
```

```text
Added source airtable (secrets: keychain)

  ✓ airtable connected successfully
  Secrets: keychain

    airtable (12 tables)
    ├─ assets
    ├─ clients
    ├─ departments
    ├─ employees
    ├─ expenses
    ├─ invoices
    ├─ it_tickets
    ├─ projects
    ├─ purchase_orders
    └─ ... and 3 more
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT id, name, status FROM airtable.employees LIMIT 1
      1 row

    ✓ SELECT id, name, status FROM airtable.projects LIMIT 1
      1 row
```

#### Source test

```bash
coral source test airtable
```

```text
  ✓ airtable connected successfully
  Secrets: keychain

    airtable (12 tables)
    ├─ assets
    ├─ clients
    ├─ departments
    ├─ employees
    ├─ expenses
    ├─ invoices
    ├─ it_tickets
    ├─ projects
    ├─ purchase_orders
    ├─ tasks
    ├─ time_logs
    └─ vendors
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT id, name, status FROM airtable.employees LIMIT 1
      1 row

    ✓ SELECT id, name, status FROM airtable.projects LIMIT 1
      1 row
```

#### Representative queries

```sql
SELECT name, status, email, department
FROM airtable.employees
ORDER BY name
LIMIT 5;
```

```text
+---------------+-------------+-----------------------+------------+
| name          | status      | email                 | department |
+---------------+-------------+-----------------------+------------+
| employees 001 | Active      | employee1@example.com | Finance    |
| employees 003 | In Progress | employee3@example.com | Operations |
| employees 004 | Active      | employee4@example.com | IT         |
| employees 005 | Active      | employee5@example.com | Finance    |
| employees 006 | In Progress | employee6@example.com | HR         |
+---------------+-------------+-----------------------+------------+
```

```sql
SELECT name, status, priority, owner
FROM airtable.search_it_tickets(
  formula => 'AND({Status} != ''Closed'', {Priority} = ''High'')'
)
LIMIT 5;
```

```text
+----------------+--------+----------+---------+
| name           | status | priority | owner   |
+----------------+--------+----------+---------+
| it tickets 002 | Open   | High     | Owner 2 |
+----------------+--------+----------+---------+
```

```sql
SELECT COUNT(*) AS employee_rows
FROM (SELECT id FROM airtable.employees LIMIT 125);
```

```text
+---------------+
| employee_rows |
+---------------+
| 125           |
+---------------+
```

The 125-row query verified that Coral followed Airtable's first-page `offset`
cursor after the initial 100-record page.
