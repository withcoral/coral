# Rootly (Community)

**Version:** 0.1.0
**Backend:** HTTP (Rootly REST API v1)
**Tables:** 6
**Base URL:** `https://api.rootly.com/v1`

Query incidents, services, users, teams, alerts, and on-call schedules from
Rootly via SQL. Designed for incident analytics: MTTR reporting, response
time tracking, on-call coverage visibility, and cross-source joins with the
bundled **Jira**, **Linear**, **GitHub**, and **Shortcut** sources.

## Setup

### 1. Generate a Rootly API key

1. In your Rootly workspace, go to
   **Organization Settings → API Keys**
2. Click **Generate API Key**, give it a name, and copy the key.

> This source also works with Rootly OAuth 2.0 access tokens — paste
> the OAuth token the same way as an API key.

### 2. Set your token

```sh
export ROOTLY_TOKEN="<your-api-key>"
```

### 3. Add the source

```sh
cargo run -p coral-cli -- source add --file sources/community/rootly/manifest.yaml
```

### 4. Verify

```sh
cargo run -p coral-cli -- sql "SELECT id, full_name, email FROM rootly.users LIMIT 5"
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `rootly.users` | Users in the organization | — |
| `rootly.services` | Services defined in the organization | — |
| `rootly.teams` | Teams in the organization | — |
| `rootly.incidents` | Incidents in the organization | — |
| `rootly.alerts` | Alerts in the organization | — |
| `rootly.schedules` | On-call schedules | — |

All tables are read-only. This source does not create, modify, or delete any
Rootly data.

> **Note:** Rootly uses JSON:API format. All resource fields are sourced
> from the nested `attributes` object in each response item. `id` is at
> the top level of each item.

### `users`

Lists all users in the organization. Join `email` to `linear.users` or
`github.members` for cross-source team analytics.

### `services`

Lists all services. Use `pagerduty_id` or `opsgenie_id` to join with
PagerDuty or OpsGenie sources. Use `github_repository_name` to join with
`github.repos`.

### `teams`

Lists all teams. Use `pagerduty_id` to join with PagerDuty teams.

### `incidents`

Lists all incidents. Use `status` to filter locally:

| Value | Meaning |
|---|---|
| `started` | Incident is active |
| `mitigated` | Incident has been mitigated |
| `resolved` | Incident is resolved |
| `cancelled` | Incident was cancelled |

Use `jira_issue_key`, `linear_issue_id`, `github_issue_id`, or
`shortcut_story_id` for cross-source joins. Use `started_at`,
`mitigated_at`, and `resolved_at` for MTTR and response-time analytics.

### `alerts`

Lists all alerts. Use `source` to filter by the originating alert system.

### `schedules`

Lists all on-call schedules. Use `owner_user_id` to join with
`rootly.users.id`. Use `all_time_coverage` to identify 24/7 schedules.

## Example queries

List all users:

```sql
SELECT id, full_name, email, role_id, time_zone
FROM rootly.users
ORDER BY full_name
LIMIT 20;
```

List services with GitHub repository connections:

```sql
SELECT id, name, slug, github_repository_name, github_repository_branch
FROM rootly.services
WHERE github_repository_name IS NOT NULL
ORDER BY name
LIMIT 20;
```

Active incidents by severity:

```sql
SELECT id, title, severity_name, status, started_at, slack_channel_name
FROM rootly.incidents
WHERE status = 'started'
ORDER BY started_at DESC
LIMIT 20;
```

Resolved incidents for MTTR analysis:

```sql
SELECT
  id,
  title,
  severity_name,
  started_at,
  mitigated_at,
  resolved_at
FROM rootly.incidents
WHERE status = 'resolved'
ORDER BY resolved_at DESC
LIMIT 50;
```

Incidents linked to Jira issues (cross-source):

```sql
SELECT
  i.id,
  i.title,
  i.severity_name,
  i.status,
  i.jira_issue_key,
  i.started_at
FROM rootly.incidents i
WHERE i.jira_issue_key IS NOT NULL
ORDER BY i.started_at DESC
LIMIT 20;
```

On-call schedules with owner details:

```sql
SELECT
  s.id,
  s.name,
  s.all_time_coverage,
  s.shift_report_time_zone,
  u.full_name AS owner_name,
  u.email AS owner_email
FROM rootly.schedules s
LEFT JOIN rootly.users u ON s.owner_user_id = u.id
ORDER BY s.name
LIMIT 20;
```

Alerts by source system:

```sql
SELECT source, COUNT(*) AS alert_count
FROM rootly.alerts
GROUP BY source
ORDER BY alert_count DESC
LIMIT 10;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/rootly/manifest.yaml
```

Add the source and validate each table:

```sh
export ROOTLY_TOKEN="<your-api-key>"
cargo run -p coral-cli -- source add --file sources/community/rootly/manifest.yaml

# users — no required filters
cargo run -p coral-cli -- sql "SELECT id, full_name, email FROM rootly.users LIMIT 5"

# services — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, slug, pagerduty_id FROM rootly.services LIMIT 5"

# teams — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, slug FROM rootly.teams LIMIT 5"

# incidents — no required filters
cargo run -p coral-cli -- sql "SELECT id, title, status, severity_name, started_at, resolved_at FROM rootly.incidents LIMIT 5"

# alerts — no required filters
cargo run -p coral-cli -- sql "SELECT id, alert_id, source, created_at FROM rootly.alerts LIMIT 5"

# schedules — no required filters
cargo run -p coral-cli -- sql "SELECT id, name, owner_user_id, all_time_coverage FROM rootly.schedules LIMIT 5"
```

Inspect registered tables and columns:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'rootly'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'rootly' ORDER BY table_name, ordinal_position"
```

## Notes

- **JSON:API format:** Rootly uses JSON:API. All resource fields are nested
  under `attributes` in the API response. `id` is at the top level of each
  item. This is handled transparently by the column path expressions.
- **Auth:** both Rootly API keys and OAuth 2.0 access tokens work with
  `Authorization: Bearer`. Rootly detects the token type automatically.
- **`Content-Type`:** Rootly requires `application/vnd.api+json` not
  `application/json`.
- **Pagination:** all tables use `page[number]` and `page[size]` query
  parameters. Default page size is 25; maximum is 100.
- **Cross-source joins:** `incidents` exposes `jira_issue_key`,
  `linear_issue_id`, `github_issue_id`, and `shortcut_story_id` for
  joining with other Coral sources.

## Out of scope for v1

- Incident action items and timeline events
- Retrospectives
- Workflows and playbooks
- OAuth authorization-code flow (deferred — use API key for now)
- Write operations of any kind
