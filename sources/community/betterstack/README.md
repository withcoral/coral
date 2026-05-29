# Better Stack

Query monitors, incidents, heartbeats, status pages, on-call
schedules, monitor groups, heartbeat groups, email integrations,
and incoming webhooks from Better Stack Uptime.

## Setup

### Get Your API Token

1. Log in to [Better Stack](https://betterstack.com)
2. Navigate to **Settings > API tokens** or see the
   [Getting Started guide](https://betterstack.com/docs/uptime/api/getting-started-with-uptime-api/)
3. Create a team-scoped **Uptime API token** or use a **Global API token**
4. Copy the token

### Add the Source

```bash
export BETTERSTACK_API_TOKEN="your_api_token"
coral source add --file sources/community/betterstack/manifest.yaml
```

## Tables

### `monitors`

Lists all uptime monitors with their URL, type, status, check
frequency, HTTP method, regions, SSL/domain expiration, and
notification settings (31 columns).

**Useful for:**

- Auditing monitor configurations, check frequencies, and timeouts
- Identifying paused or down monitors
- Reviewing notification settings (call, SMS, email, push)
- Checking SSL and domain expiration alerts

**Example:**

```sql
SELECT id, pronounceable_name, url, monitor_type, status,
       check_frequency, last_checked_at, request_timeout
FROM betterstack.monitors
LIMIT 20;
```

### `monitor_groups`

Lists all monitor groups for organizing monitors (7 columns).

**Example:**

```sql
SELECT id, name, sort_index, paused, team_name
FROM betterstack.monitor_groups;
```

### `incidents`

Lists all incidents with cause, status, timeline, notification
details, escalation policy, and response data (23 columns).

**Useful for:**

- Reviewing incident history and resolution times
- Auditing notification delivery (call, SMS, email, push)
- Tracking acknowledgment and resolution patterns
- Inspecting response content that triggered incidents

**Example:**

```sql
SELECT id, name, cause, status, started_at,
       acknowledged_at, resolved_at
FROM betterstack.incidents
LIMIT 20;
```

### `heartbeats`

Lists all heartbeat monitors for tracking cron jobs, background
workers, and scheduled tasks (16 columns).

**Useful for:**

- Identifying heartbeats that are down or pending
- Auditing ping intervals and grace periods
- Reviewing heartbeat notification settings

**Example:**

```sql
SELECT id, name, status, period, grace, created_at
FROM betterstack.heartbeats
LIMIT 20;
```

### `heartbeat_groups`

Lists all heartbeat groups for organizing heartbeats (7 columns).

**Example:**

```sql
SELECT id, name, sort_index, paused, team_name
FROM betterstack.heartbeat_groups;
```

### `status_pages`

Lists all status pages with their company name, subdomain,
custom domain, and timezone (9 columns).

**Example:**

```sql
SELECT id, company_name, subdomain, custom_domain, timezone
FROM betterstack.status_pages;
```

### `on_call_schedules`

Lists all on-call schedules with default calendar flag, team,
and currently on-call users (4 columns).

**Example:**

```sql
SELECT id, name, default_calendar, on_call_users
FROM betterstack.on_call_schedules;
```

### `email_integrations`

Lists all email integrations configured for incident reporting,
including email address, notification settings, and pause state
(12 columns).

**Example:**

```sql
SELECT id, name, email_address, policy_id, paused, team_name
FROM betterstack.email_integrations;
```

### `incoming_webhooks`

Lists all incoming webhook integrations for triggering incidents
from external tools, including webhook URL, notification settings,
and pause state (12 columns).

**Example:**

```sql
SELECT id, name, url, policy_id, paused, team_name
FROM betterstack.incoming_webhooks;
```

### `status_page_resources`

Lists all resources (monitors/heartbeats) attached to a status page,
including availability, current status, and history (13 columns).

**Requires:** `status_page_id` filter

**Example:**

```sql
SELECT id, resource_id, resource_type, public_name,
       status, availability, history
FROM betterstack.status_page_resources
WHERE status_page_id = '12345';
```

### `status_page_sections`

Lists all sections on a status page. Sections group resources
into named categories (4 columns).

**Requires:** `status_page_id` filter

**Example:**

```sql
SELECT id, name, position
FROM betterstack.status_page_sections
WHERE status_page_id = '12345';
```

### `status_page_reports`

Lists all status reports (incident updates) on a status page,
including aggregate state and affected resources (8 columns).

**Requires:** `status_page_id` filter

**Example:**

```sql
SELECT id, title, report_type, starts_at, ends_at,
       aggregate_state, affected_resources
FROM betterstack.status_page_reports
WHERE status_page_id = '12345';
```

## Authentication

The source uses Bearer token authentication. Generate a token at
https://betterstack.com (Settings > API tokens).

- **Team-scoped Uptime API token** — access resources for one team
- **Global API token** — access resources across all teams

## Inputs

| Input | Kind | Description |
|---|---|---|
| `BETTERSTACK_API_TOKEN` | secret | API access token |

## Pagination

All tables use page-number pagination (`page` + `per_page`
query parameters). Coral automatically paginates through all
pages to return complete results.

- Default page size: 50
- Maximum page size: 250 (v2 endpoints), 50 (v3 incidents)
- Pages start at 1

## JSON:API Format

Better Stack uses the [JSON:API](https://jsonapi.org/) response
format. Each resource has:

- `id` — at the top level of each resource object
- `attributes` — nested object containing all fields

Coral flattens this automatically — you query columns by their
attribute names directly (e.g. `pronounceable_name`, `status`).

## API Versioning

Most endpoints use API v2. The incidents endpoint has been
migrated to API v3 by Better Stack. The source handles this
by using `/api` as the base URL and specifying the version
in each table's path (`/v2/monitors`, `/v3/incidents`, etc.).

## Example Queries

### Find all monitors that are currently down

```sql
SELECT id, pronounceable_name, url, status, last_checked_at
FROM betterstack.monitors
WHERE status = 'down';
```

### Review incident timeline

```sql
SELECT id, name, cause, status, started_at,
       acknowledged_at, resolved_at
FROM betterstack.incidents
WHERE resolved_at IS NOT NULL
LIMIT 20;
```

### List heartbeats that haven't reported

```sql
SELECT id, name, status, period, grace
FROM betterstack.heartbeats
WHERE status = 'down';
```

### Audit monitor notification settings

```sql
SELECT pronounceable_name, call, sms, email, push, policy_id
FROM betterstack.monitors;
```

### Check SSL expiration alerts

```sql
SELECT pronounceable_name, url, ssl_expiration, domain_expiration
FROM betterstack.monitors
WHERE ssl_expiration IS NOT NULL;
```

### Review status page resource availability

```sql
SELECT resource_type, public_name, status, availability
FROM betterstack.status_page_resources
WHERE status_page_id = '12345';
```

### Check on-call schedules and their teams

```sql
SELECT name, default_calendar, on_call_users
FROM betterstack.on_call_schedules;
```

### Audit webhook and email integrations

```sql
SELECT name, url, paused, policy_id
FROM betterstack.incoming_webhooks;

SELECT name, email_address, paused, policy_id
FROM betterstack.email_integrations;
```

## Notes

- The source is read-only — no create, update, or delete operations
- All resources use the JSON:API format with `data[].attributes`
- The `id` field is a string (not an integer) per JSON:API spec
- Incident `email_notify` column maps to the API's `email`
  attribute (renamed to avoid SQL reserved word conflicts)
- The same `email_notify` renaming applies to heartbeats,
  email_integrations, and incoming_webhooks
- Timestamps are ISO 8601 strings
- A Global API token can access resources across all teams;
  a team-scoped token is limited to its team's resources
- The `team_name` column appears on most tables when using a
  Global API token
- `on_call_users` and `affected_resources` columns contain
  JSON arrays from the API's relationship/attribute data
