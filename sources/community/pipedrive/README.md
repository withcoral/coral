# Pipedrive (Community)

**Version:** 0.1.0
**Backend:** HTTP (Pipedrive REST API v1)
**Tables:** 10
**Functions:** 3
**Base URL:** `https://api.pipedrive.com/v1`

Query your Pipedrive CRM data through SQL. Explore deals, contacts,
organizations, activities, notes, leads, products, pipelines, stages, and
users directly from Coral.

Designed for CRM analytics, sales reporting, pipeline forecasting, activity
tracking, and customer relationship insights.

## Setup

### 1. Generate a Pipedrive API token

In Pipedrive, navigate to:

**Settings → Personal preferences → API**

Copy your personal API token.

> **Note:** this source currently supports API token authentication only.

### 2. Set your credentials

```sh
export PIPEDRIVE_API_TOKEN="<your-pipedrive-api-token>"
```

### 3. Add the source

```sh
cargo run -p coral-cli -- source add --file sources/community/pipedrive/manifest.yaml
```

### 4. Verify

```sh
cargo run -p coral-cli -- sql "SELECT id, name FROM pipedrive.pipelines LIMIT 5"
```

You should see your available sales pipelines.

---

# Tables

| Table                     | Description                    | Required filters | Optional filters                               |
| ------------------------- | ------------------------------ | ---------------- | ---------------------------------------------- |
| `pipedrive.pipelines`     | Sales pipelines                | —                | —                                              |
| `pipedrive.stages`        | Pipeline stages                | —                | `pipeline_id`                                  |
| `pipedrive.deals`         | CRM deals/opportunities        | —                | `status`, `pipeline_id`, `stage_id`, `user_id` |
| `pipedrive.persons`       | Contacts/persons               | —                | `user_id`                                      |
| `pipedrive.organizations` | Organizations/companies        | —                | `user_id`                                      |
| `pipedrive.activities`    | Calls, meetings, tasks, emails | —                | `done`, `user_id`, `type`                      |
| `pipedrive.notes`         | Notes attached to entities     | —                | `deal_id`, `person_id`, `org_id`, `user_id`    |
| `pipedrive.leads`         | Leads Inbox leads              | —                | —                                              |
| `pipedrive.products`      | Product catalog                | —                | —                                              |
| `pipedrive.users`         | Company users/team members     | —                | —                                              |

All tables are read-only. This source does not create, update, or delete
Pipedrive data.

---

# Search functions

| Function                     | Description                                      |
| ---------------------------- | ------------------------------------------------ |
| `search_deals(term)`         | Search deals by title, notes, and custom fields  |
| `search_persons(term)`       | Search persons by name, email, phone, and notes  |
| `search_organizations(term)` | Search organizations by name, address, and notes |

Search functions return provider-ranked results directly from Pipedrive's
search APIs and are ideal for discovery workflows before querying tables.

---

# Authentication

This source authenticates using:

```http
Authorization: Bearer <PIPEDRIVE_API_TOKEN>
```

The API token inherits the permissions and visibility scope of the authenticated
Pipedrive user.

---

# Tables reference

## `pipelines`

Lists all sales pipelines configured in your Pipedrive account.

Useful for:

* Revenue forecasting
* Pipeline health reporting
* Stage conversion analysis
* Multi-pipeline CRM setups

Example:

```sql
SELECT id, name, active, deal_probability
FROM pipedrive.pipelines
ORDER BY order_nr;
```

---

## `stages`

Lists stages across pipelines.

Optionally filter by `pipeline_id`.

Example:

```sql
SELECT id, name, pipeline_name, deal_probability
FROM pipedrive.stages
WHERE pipeline_id = 1
ORDER BY order_nr;
```

---

## `deals`

Core sales opportunities table.

Supports filtering by:

* `status`
* `pipeline_id`
* `stage_id`
* `user_id`

Common joins:

* `persons`
* `organizations`
* `pipelines`
* `stages`
* `activities`
* `notes`

Example:

```sql
SELECT
  id,
  title,
  value,
  currency,
  status,
  probability,
  expected_close_date
FROM pipedrive.deals
WHERE status = 'open'
ORDER BY update_time DESC
LIMIT 20;
```

Won deals:

```sql
SELECT
  id,
  title,
  value,
  won_time,
  user_id__name
FROM pipedrive.deals
WHERE status = 'won'
ORDER BY won_time DESC
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

Pipeline revenue forecast:

```sql
SELECT
  pipeline_id,
  SUM(weighted_value) AS weighted_pipeline_value
FROM pipedrive.deals
WHERE status = 'open'
GROUP BY pipeline_id
ORDER BY weighted_pipeline_value DESC;
```

---

## `persons`

CRM contacts/persons.

Each person may belong to an organization and may be associated with multiple
deals and activities.

Example:

```sql
SELECT
  id,
  name,
  org_name,
  open_deals_count,
  activities_count
FROM pipedrive.persons
ORDER BY update_time DESC
LIMIT 20;
```

---

## `organizations`

Companies and organizations stored in Pipedrive.

Useful for account-based sales analytics and CRM enrichment workflows.

Example:

```sql
SELECT
  id,
  name,
  people_count,
  open_deals_count,
  won_deals_count
FROM pipedrive.organizations
ORDER BY open_deals_count DESC
LIMIT 20;
```

Organizations by country:

```sql
SELECT
  name,
  address_country,
  address_locality
FROM pipedrive.organizations
WHERE address_country IS NOT NULL
LIMIT 50;
```

---

## `activities`

Meetings, calls, tasks, emails, lunches, and other scheduled CRM activities.

Supports filtering by:

* `done`
* `user_id`
* `type`

Example:

```sql
SELECT
  subject,
  type,
  due_date,
  due_time,
  person_name,
  deal_title
FROM pipedrive.activities
WHERE done = 0
ORDER BY due_date ASC
LIMIT 20;
```

Completed calls:

```sql
SELECT
  subject,
  marked_as_done_time,
  user_id__name
FROM pipedrive.activities
WHERE done = 1
  AND type = 'call'
ORDER BY marked_as_done_time DESC
LIMIT 20;
```

---

## `notes`

Notes attached to deals, persons, and organizations.

Supports filtering by:

* `deal_id`
* `person_id`
* `org_id`
* `user_id`

Example:

```sql
SELECT
  id,
  deal_id,
  user_id__name,
  add_time
FROM pipedrive.notes
WHERE deal_id = 123
ORDER BY add_time DESC;
```

---

## `leads`

Pre-qualified leads stored in Pipedrive's Leads Inbox.

Example:

```sql
SELECT
  title,
  source_name,
  expected_close_date,
  was_seen
FROM pipedrive.leads
WHERE is_archived = false
ORDER BY add_time DESC
LIMIT 20;
```

---

## `products`

Product catalog entries.

Products may be attached to deals with quantities and prices.

Example:

```sql
SELECT
  id,
  name,
  code,
  category,
  active_flag
FROM pipedrive.products
ORDER BY name;
```

---

## `users`

Company users/team members.

Useful for resolving ownership relationships across deals, activities,
organizations, and persons.

Example:

```sql
SELECT
  id,
  name,
  email,
  timezone_name,
  default_currency
FROM pipedrive.users
ORDER BY name;
```

---

# Search functions reference

## `search_deals`

Search deals by title, notes, and custom fields.

Example:

```sql
SELECT *
FROM search_deals(
  term => 'enterprise renewal'
);
```

Exact match search:

```sql
SELECT *
FROM search_deals(
  term => 'Acme Corp Renewal',
  exact_match => true
);
```

---

## `search_persons`

Search contacts by name, email, phone, and notes.

Example:

```sql
SELECT *
FROM search_persons(
  term => 'john'
);
```

Search persons within an organization:

```sql
SELECT *
FROM search_persons(
  term => 'sarah',
  org_id => 42
);
```

---

## `search_organizations`

Search organizations by name, address, and notes.

Example:

```sql
SELECT *
FROM search_organizations(
  term => 'acme'
);
```

---

# Example analytics queries

## Open deals by pipeline

```sql
SELECT
  p.name AS pipeline_name,
  COUNT(*) AS open_deals,
  SUM(d.value) AS total_value
FROM pipedrive.deals d
JOIN pipedrive.pipelines p
  ON d.pipeline_id = p.id
WHERE d.status = 'open'
GROUP BY p.name
ORDER BY total_value DESC;
```

---

## Deal conversion by stage

```sql
SELECT
  s.name AS stage_name,
  COUNT(*) AS deals
FROM pipedrive.deals d
JOIN pipedrive.stages s
  ON d.stage_id = s.id
GROUP BY s.name
ORDER BY deals DESC;
```

---

## Activities per sales rep

```sql
SELECT
  u.name,
  COUNT(a.id) AS total_activities,
  SUM(CASE WHEN a.done THEN 1 ELSE 0 END) AS completed_activities
FROM pipedrive.activities a
JOIN pipedrive.users u
  ON a.user_id__value = u.id
GROUP BY u.name
ORDER BY total_activities DESC;
```

---

## Organizations with highest won deal count

```sql
SELECT
  name,
  won_deals_count,
  open_deals_count
FROM pipedrive.organizations
ORDER BY won_deals_count DESC
LIMIT 20;
```

---

## Upcoming activities for open deals

```sql
SELECT
  a.subject,
  a.due_date,
  a.person_name,
  d.title,
  d.value
FROM pipedrive.activities a
JOIN pipedrive.deals d
  ON a.deal_id = d.id
WHERE a.done = 0
  AND d.status = 'open'
ORDER BY a.due_date ASC
LIMIT 20;
```

---

# Pagination and filtering

All tables use offset pagination:

* `start`
* `limit`

Default page size is `100`.
Maximum page size is `500`.

Use `LIMIT` in SQL queries when exploring large CRM datasets.

Many filters are pushed down directly to the Pipedrive API for better
performance and reduced network usage.

---

# Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/pipedrive/manifest.yaml
```

Add the source:

```sh
export PIPEDRIVE_API_TOKEN="<your-pipedrive-api-token>"

cargo run -p coral-cli -- source add --file sources/community/pipedrive/manifest.yaml
```

Validate core tables:

```sh
# pipelines
cargo run -p coral-cli -- sql "SELECT id, name FROM pipedrive.pipelines LIMIT 5"

# stages
cargo run -p coral-cli -- sql "SELECT id, name, pipeline_name FROM pipedrive.stages LIMIT 5"

# deals
cargo run -p coral-cli -- sql "SELECT id, title, status, value FROM pipedrive.deals LIMIT 5"

# persons
cargo run -p coral-cli -- sql "SELECT id, name, org_name FROM pipedrive.persons LIMIT 5"

# organizations
cargo run -p coral-cli -- sql "SELECT id, name, people_count FROM pipedrive.organizations LIMIT 5"

# activities
cargo run -p coral-cli -- sql "SELECT id, subject, type, done FROM pipedrive.activities LIMIT 5"

# notes
cargo run -p coral-cli -- sql "SELECT id, deal_id, user_id__name FROM pipedrive.notes LIMIT 5"

# leads
cargo run -p coral-cli -- sql "SELECT id, title, source_name FROM pipedrive.leads LIMIT 5"

# products
cargo run -p coral-cli -- sql "SELECT id, name, code FROM pipedrive.products LIMIT 5"

# users
cargo run -p coral-cli -- sql "SELECT id, name, email FROM pipedrive.users LIMIT 5"
```

Validate search functions:

```sh
cargo run -p coral-cli -- sql "SELECT * FROM search_deals(term => 'enterprise') LIMIT 5"

cargo run -p coral-cli -- sql "SELECT * FROM search_persons(term => 'john') LIMIT 5"

cargo run -p coral-cli -- sql "SELECT * FROM search_organizations(term => 'acme') LIMIT 5"
```

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'pipedrive'"

cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'pipedrive' ORDER BY table_name, ordinal_position"
```

---

# Notes

* Uses the Pipedrive REST API v1
* Authentication currently supports personal API tokens only
* API permissions and visibility depend on the authenticated user
* All tables are read-only
* Offset pagination is used for all resources
* Search functions use provider-native ranking from Pipedrive
* Many nested API fields are flattened into SQL-friendly column names
* Timestamp fields are returned as ISO 8601 strings
* Some entities may be hidden depending on company visibility rules
* Large CRM accounts should always query with `LIMIT`

---

# Out of scope for v1

* Write operations (create/update/delete)
* Webhooks
* Real-time sync
* Files and attachments APIs
* Email sync APIs
* Recurring products/subscriptions
* Custom field schema introspection
* Incremental sync cursors
* OAuth authentication flow
