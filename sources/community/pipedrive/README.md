# Pipedrive (Community)

**Version:** 0.1.0
**Backend:** HTTP (Pipedrive REST API)
**Tables:** 10
**Functions:** 3
**Base URL:** `https://<company>.pipedrive.com`

Query your Pipedrive CRM data through SQL. Explore deals, contacts,
organizations, activities, notes, leads, products, pipelines, stages, and
users directly from Coral.

Designed for CRM analytics, sales reporting, pipeline forecasting, activity
tracking, and customer relationship insights.

## Setup

### 1. Generate a Pipedrive API token

In Pipedrive, navigate to:

**Settings → Personal preferences → API**

Copy your personal API token and note your company domain from the URL.

> **Note:** this source uses personal API token authentication via the
> `x-api-token` header. OAuth access tokens are not supported in this version.
> See [Pipedrive authentication docs](https://pipedrive.readme.io/docs/core-api-concepts-authentication) for details.

### 2. Set your credentials

```sh
export PIPEDRIVE_API_TOKEN="<your-pipedrive-api-token>"
export PIPEDRIVE_COMPANY_DOMAIN="<your-company-domain>"
```

If your Pipedrive URL is `https://coral-testing.pipedrive.com`, set:

```sh
export PIPEDRIVE_COMPANY_DOMAIN="coral-testing"
```

### 3. Add the source

```sh
coral source add --file sources/community/pipedrive/manifest.yaml
```

Or interactively:

```sh
coral source add --interactive --file sources/community/pipedrive/manifest.yaml
```

### 4. Verify

```sh
coral sql "SELECT id, name FROM pipedrive.pipelines LIMIT 5"
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `pipedrive.pipelines` | Sales pipelines | — | — |
| `pipedrive.stages` | Pipeline stages | — | `pipeline_id` |
| `pipedrive.deals` | CRM deals/opportunities | — | `status`, `pipeline_id`, `stage_id`, `owner_id`, `person_id`, `org_id` |
| `pipedrive.persons` | Contacts/persons | — | `owner_id`, `org_id` |
| `pipedrive.organizations` | Organizations/companies | — | `owner_id` |
| `pipedrive.activities` | Calls, meetings, tasks, emails | — | `done`, `owner_id`, `deal_id`, `lead_id`, `person_id`, `org_id`, `updated_since`, `updated_until` |
| `pipedrive.notes` | Notes attached to entities | — | `deal_id`, `lead_id`, `person_id`, `org_id`, `owner_id` |
| `pipedrive.leads` | Leads Inbox leads | — | — |
| `pipedrive.products` | Product catalog | — | — |
| `pipedrive.users` | Company users/team members | — | — |

All tables are read-only. This source does not create, update, or delete
Pipedrive data.

## API versions

This source uses a mix of Pipedrive API v1 and v2 endpoints, all accessed
through your company domain (`https://<company>.pipedrive.com`). See
[Pipedrive API requests docs](https://pipedrive.readme.io/docs/core-api-concepts-requests)
and [Pipedrive API v2 overview](https://pipedrive.readme.io/docs/pipedrive-api-v2) for
full endpoint references.

| Table / Function | Endpoint version | Pagination |
|---|---|---|
| `pipelines` | v2 | cursor |
| `stages` | v2 | cursor |
| `deals` | v2 | cursor |
| `persons` | v2 | cursor |
| `organizations` | v2 | cursor |
| `activities` | v2 | cursor |
| `products` | v2 | cursor |
| `notes` | v1 | offset (`start` / `limit`) |
| `leads` | v1 | offset (`start` / `limit`) |
| `users` | v1 | none (endpoint returns all users in one response) |
| `search_deals` | v2 | cursor |
| `search_persons` | v2 | cursor |
| `search_organizations` | v2 | cursor |

`notes`, `leads`, and `users` use v1 endpoints because Pipedrive has not
published v2 equivalents for these resources.

Key v2 behavioral notes:

- Timestamps are RFC 3339 format (e.g. `2024-01-01T00:00:00Z`)
- Related object fields (`person_id`, `org_id`, `owner_id`) return plain integer IDs, not embedded objects
- `user_id` on deals and activities is renamed to `owner_id` in v2
- `active_flag` is replaced by `is_deleted` (negated semantics) on v2 resources
- `busy_flag` on activities is renamed to `busy` in v2
- `address` on organizations and `location` on activities are nested objects, flattened with double-underscore notation (e.g. `address__country`, `location__value`)
- The `users` table exposes an `access` column containing the raw `access[]` JSON array per the documented v1 response shape; use this to determine admin status per application instead of a scalar `is_admin` field
- The `GET /api/v1/users` endpoint returns all users in a single response and does not support pagination parameters

## Search functions

All search functions require a `term` of at least 2 characters. When
`exact_match` is set to `true`, a single character is accepted. Shorter terms
will be rejected by Pipedrive's API before any results are returned.

| Function | Description |
|---|---|
| `search_deals(term)` | Search deals by title, notes, and custom fields |
| `search_persons(term)` | Search persons by name, email, phone, and notes |
| `search_organizations(term)` | Search organizations by name, address, and notes |

Search functions use Pipedrive v2 search endpoints with cursor pagination and
return provider-ranked results. They are ideal for discovery before querying
the main tables.

## Example queries

Open deals by pipeline:

```sql
SELECT
  p.name AS pipeline_name,
  COUNT(*) AS open_deals,
  SUM(d.value) AS total_value
FROM pipedrive.deals d
JOIN pipedrive.pipelines p ON d.pipeline_id = p.id
WHERE d.status = 'open'
GROUP BY p.name
ORDER BY total_value DESC;
```

Won deals with owner name:

```sql
SELECT
  d.id,
  d.title,
  d.value,
  d.currency,
  d.won_time,
  u.name AS owner_name
FROM pipedrive.deals d
JOIN pipedrive.users u ON d.owner_id = u.id
WHERE d.status = 'won'
ORDER BY d.won_time DESC
LIMIT 20;
```

Lost deals with reasons:

```sql
SELECT
  title,
  value,
  lost_reason,
  lost_time
FROM pipedrive.deals
WHERE status = 'lost'
ORDER BY lost_time DESC
LIMIT 20;
```

Upcoming activities for open deals:

```sql
SELECT
  a.subject,
  a.due_date,
  a.type,
  d.title AS deal_title,
  d.value
FROM pipedrive.activities a
JOIN pipedrive.deals d ON a.deal_id = d.id
WHERE a.done = false
  AND d.status = 'open'
ORDER BY a.due_date ASC
LIMIT 20;
```

Activities per sales rep:

```sql
SELECT
  u.name,
  COUNT(a.id) AS total_activities,
  SUM(CASE WHEN a.done THEN 1 ELSE 0 END) AS completed
FROM pipedrive.activities a
JOIN pipedrive.users u ON a.owner_id = u.id
GROUP BY u.name
ORDER BY total_activities DESC;
```

Organizations with most open deals:

```sql
SELECT
  o.name,
  COUNT(d.id) AS open_deals,
  SUM(d.value) AS pipeline_value
FROM pipedrive.organizations o
JOIN pipedrive.deals d ON d.org_id = o.id
WHERE d.status = 'open'
GROUP BY o.name
ORDER BY pipeline_value DESC
LIMIT 20;
```

Search for deals by keyword:

```sql
SELECT *
FROM pipedrive.search_deals(term => 'enterprise renewal')
LIMIT 10;
```

Search persons within an organization:

```sql
SELECT *
FROM pipedrive.search_persons(term => 'sarah', organization_id => 42)
LIMIT 10;
```

## Validation

Lint the manifest:

```sh
coral source lint sources/community/pipedrive/manifest.yaml
```

Add the source:

```sh
export PIPEDRIVE_API_TOKEN="<your-pipedrive-api-token>"
export PIPEDRIVE_COMPANY_DOMAIN="<your-company-domain>"
coral source add --file sources/community/pipedrive/manifest.yaml
```

Validate core tables:

```sh
coral sql "SELECT id, name FROM pipedrive.pipelines LIMIT 5"
coral sql "SELECT id, name, pipeline_id FROM pipedrive.stages LIMIT 5"
coral sql "SELECT id, title, status, value FROM pipedrive.deals LIMIT 5"
coral sql "SELECT id, name, org_id FROM pipedrive.persons LIMIT 5"
coral sql "SELECT id, name, owner_id FROM pipedrive.organizations LIMIT 5"
coral sql "SELECT id, subject, type, done FROM pipedrive.activities LIMIT 5"
coral sql "SELECT id, deal_id, user_id FROM pipedrive.notes LIMIT 5"
coral sql "SELECT id, title, source_name FROM pipedrive.leads LIMIT 5"
coral sql "SELECT id, name, code FROM pipedrive.products LIMIT 5"
coral sql "SELECT id, name, email FROM pipedrive.users LIMIT 5"
```

Validate search functions:

```sh
coral sql "SELECT * FROM pipedrive.search_deals(term => 'enterprise') LIMIT 5"
coral sql "SELECT * FROM pipedrive.search_persons(term => 'john') LIMIT 5"
coral sql "SELECT * FROM pipedrive.search_organizations(term => 'acme') LIMIT 5"
```

Inspect registered tables and columns:

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'pipedrive'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'pipedrive' ORDER BY table_name, ordinal_position"
```

## Notes

- Authenticates with `x-api-token` header (personal API token only)
- v2 tables use cursor pagination; v1 tables (`notes`, `leads`) use offset pagination (`start` / `limit`)
- `GET /api/v1/users` returns all users in a single response with no pagination parameters
- `is_deleted = true` means soft-deleted; entities are fully deleted 30 days after last activity
- Nested fields like `address` and `location` are flattened with double-underscore notation (e.g. `address__country`, `location__value`)
- The `notes` table `owner_id` filter maps to the `user_id` query parameter on the v1 API
- The `access` column on `users` is a raw JSON array — parse it to determine per-application admin status
- API permissions and data visibility depend on the authenticated user's role
- Large CRM accounts should always use `LIMIT` when querying

## Out of scope for v1

- Write operations (create/update/delete)
- Custom field schema introspection
- Webhooks and real-time sync
- Files and attachments APIs
- Email sync APIs
- OAuth authentication flow
