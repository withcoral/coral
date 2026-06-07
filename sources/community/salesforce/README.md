# Salesforce

**Version:** 0.1.0
**Backend:** HTTP (Salesforce REST API v59.0)
**Base URL:** `https://{your-instance}.my.salesforce.com`

Query Salesforce CRM accounts, contacts, leads, opportunities, and cases as
SQL tables. Join contacts with Linear issues or other sources using email,
account IDs, and conversion IDs.

## Tables

| Table | SOQL pushdown filters | Cap | Description |
| --- | --- | --- | --- |
| `salesforce.accounts` | `industry` | 2000 | Companies with revenue, billing location, and owner |
| `salesforce.contacts` | `account_id` | 2000 | Individuals with email, phone, and title |
| `salesforce.leads` | `is_converted`, `lead_source` | 2000 | Prospects with status and conversion state |
| `salesforce.opportunities` | `is_closed`, `account_id` | 2000 | Pipeline deals with stage, amount, and close date |
| `salesforce.cases` | `is_closed`, `priority`, `account_id` | 2000 | Support tickets with status and origin |

Each table fetches up to 2000 records ordered by `LastModifiedDate DESC`. Filters listed above are pushed into SOQL so the 2000-row cap applies to the filtered result set. SQL `WHERE` clauses on columns not listed as pushdown filters run client-side after fetching, and may miss rows in orgs larger than 2000 records per object.

## Authentication

Requires `SALESFORCE_API_URL` and `SALESFORCE_ACCESS_TOKEN`.

**To find your instance URL:**

Go to **Setup -> Company Information -> Instance URL**. Use the full URL without a trailing slash, for example `https://mycompany.my.salesforce.com`.

**To get an access token:**

1. Create a Connected App under **Setup -> App Manager -> New Connected App**
2. Enable **OAuth Settings** with the `api` scope
3. Retrieve a token using the Salesforce CLI:

```bash
sf org display --target-org <alias>
```

Or use the [Username-Password OAuth flow](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/quickstart_oauth.htm).

### Required permissions

The token's Salesforce user must have:

- **API Access** enabled in their user profile (Setup -> Users -> Profiles -> System Permissions)
- **Read** object permission on Account, Contact, Lead, Opportunity, and Case
- **Field-level security** granting read access to all queried fields

A 401 means the token is expired. A 403 `INSUFFICIENT_ACCESS_OR_READONLY` means the user profile lacks the required object or field permissions. A 403 `REQUEST_LIMIT_EXCEEDED` means the org has reached its daily API call limit.

See [User profiles and object permissions](https://help.salesforce.com/s/articleView?id=permissions_about_users_access.htm) and [API errors reference](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/errorcodes.htm).

## Install

```bash
SALESFORCE_API_URL=https://mycompany.my.salesforce.com \
SALESFORCE_ACCESS_TOKEN=your-token \
coral source add --file sources/community/salesforce/manifest.yaml
```

## Validation

```bash
# Add and run built-in test query (output sanitized)
coral source add --file sources/community/salesforce/manifest.yaml
# coral source test salesforce produces the same output
```

```text
  ✓ salesforce connected successfully
  Secrets: keyring

    salesforce (5 tables)
    ├─ accounts
    ├─ cases
    ├─ contacts
    ├─ leads
    └─ opportunities

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, name FROM salesforce.accounts LIMIT 1
      1 row
```

```bash
# Representative query (output sanitized)
coral sql "SELECT id, name, industry, annual_revenue FROM salesforce.accounts WHERE industry = 'Technology' LIMIT 3"
```

```text
+--------------------+----------------------------+--------------+------------------+
| id                 | name                       | industry     | annual_revenue   |
+--------------------+----------------------------+--------------+------------------+
| 0015g00000AbCdEfAA | Acme Corp                  | Technology   | 5000000.0        |
| 0015g00000BcDeFgBB | Globex Solutions           | Technology   | 12500000.0       |
| 0015g00000CdEfGhCC | Initech Systems            | Technology   | 800000.0         |
+--------------------+----------------------------+--------------+------------------+
3 rows
```

## Example Queries

Open opportunities (pushed to SOQL — `is_closed = false` filters server-side):

```sql
SELECT name, stage_name, amount, close_date, owner_id
FROM salesforce.opportunities
WHERE is_closed = false
ORDER BY close_date ASC
LIMIT 25;
```

High-priority open cases (both `is_closed` and `priority` pushed to SOQL):

```sql
SELECT id, subject, status, priority, account_id, created_date
FROM salesforce.cases
WHERE is_closed = false
  AND priority = 'High'
ORDER BY created_date DESC
LIMIT 25;
```

Unconverted leads from web (both `is_converted` and `lead_source` pushed to SOQL):

```sql
SELECT id, first_name, last_name, email, company, status
FROM salesforce.leads
WHERE is_converted = false
  AND lead_source = 'Web'
ORDER BY last_modified_date DESC
LIMIT 50;
```

Contacts at a specific account (pushed to SOQL):

```sql
SELECT id, first_name, last_name, email, title
FROM salesforce.contacts
WHERE account_id = '0015g00000AbCdEfAA'
LIMIT 50;
```

Contacts at Technology accounts (client-side join; use industry pushdown on accounts first):

```sql
SELECT c.email, c.first_name, c.last_name, a.name AS account_name
FROM salesforce.contacts c
JOIN salesforce.accounts a ON c.account_id = a.id
WHERE a.industry = 'Technology'
ORDER BY a.name ASC
LIMIT 50;
```

## Cross-Source JOIN Examples

Contacts with open Linear issues assigned to the same person (requires
`linear` source):

```sql
SELECT c.first_name, c.last_name, c.email, COUNT(li.id) AS open_issues
FROM salesforce.contacts c
JOIN linear.issues li ON LOWER(li.assignee_email) = LOWER(c.email)
WHERE li.state_type != 'completed'
GROUP BY c.first_name, c.last_name, c.email
ORDER BY open_issues DESC
LIMIT 20;
```

Open opportunities linked to contacts who have open Linear issues (requires
`linear` source):

```sql
SELECT o.name AS opportunity, o.stage_name, o.amount, li.title AS linear_issue
FROM salesforce.opportunities o
JOIN salesforce.contacts c ON o.account_id = c.account_id
JOIN linear.issues li ON LOWER(li.assignee_email) = LOWER(c.email)
WHERE o.is_closed = false
  AND li.state_type != 'completed'
LIMIT 20;
```

## Notes

- All tables are read-only.
- Filters listed in the pushdown column above are injected into SOQL so the 2000-row cap applies to the filtered set. All other SQL `WHERE` conditions run client-side after fetching.
- Salesforce returns additional query pages through a path-based
  `nextRecordsUrl`, which Coral's current HTTP source DSL cannot follow. Use
  the documented pushdown filters to keep matching result sets below 2000
  records.
- `close_date` and `converted_date` are date-only strings (`YYYY-MM-DD`), not full timestamps.
- `amount`, `annual_revenue`, and `probability` are `Float64` (Salesforce currency/percent fields).
- Access tokens expire; re-run `sf org display` or re-authorize to refresh.

## Rate limits

Salesforce enforces per-org daily API call limits. Each table query counts as
one API call. The response header `Sforce-Limit-Info: api-usage=NNN/MMMM`
shows current usage. If the daily limit is reached, the API returns `403
REQUEST_LIMIT_EXCEEDED`. Coral does not automatically retry generic `403`
responses because the same status also represents permission failures. Check
current usage and limits at `GET /services/data/v59.0/limits`.

See [Salesforce API limits](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_limits.htm) and [monitoring API usage](https://developer.salesforce.com/blogs/2024/11/api-limits-and-monitoring-your-api-usage).
