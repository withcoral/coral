# Rootly (Community)

**Version:** 0.1.0
**Backend:** HTTP (Rootly REST API v1)
**Tables:** 6
**Base URL:** `https://api.rootly.com/v1`

Query incidents, services, users, teams, alerts, and on-call schedules from Rootly via SQL.

Designed for **incident lifecycle analytics and operational reliability workflows**, including:

* Incident detection → acknowledgment → mitigation → resolution tracking
* MTTR / MTTD / MTTA analysis
* On-call coverage and ownership visibility
* Alert correlation and incident causality analysis
* Cross-source joins with Jira, Linear, GitHub, and Shortcut

---

## Setup

### 1. Generate a Rootly API key

1. In your Rootly workspace, go to:
   **Organization Settings → API Keys**
2. Generate a new API key and copy it.

> The API key must have **read access to incidents, services, teams, users, alerts, and schedules**.
> Keys are **workspace-scoped** and inherit permissions based on the role that created them.

Rootly also supports OAuth 2.0 tokens — both API keys and OAuth tokens can be used interchangeably via the same `Authorization: Bearer` header.

### 2. Set your token

```sh
export ROOTLY_TOKEN="<your-api-key-or-oauth-token>"
```

### 3. Add the source

```sh
coral source add --file sources/community/rootly/manifest.yaml
```

### 4. First successful query (recommended)

```sql
SELECT id, title, status, started_at
FROM rootly.incidents
ORDER BY started_at DESC
LIMIT 5;
```

Or validate service ingestion:

```sql
SELECT id, name, slug, github_repository_name
FROM rootly.services
LIMIT 5;
```

---

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `rootly.users` | Users in the organization | — | — |
| `rootly.services` | Services defined in the organization | — | — |
| `rootly.teams` | Teams in the organization | — | — |
| `rootly.incidents` | Incident lifecycle records (MTTR/MTTD/MTTA) | — | `status`, `severity`, `started_at_gte`, `started_at_lte`, `created_at_gte`, `created_at_lte` |
| `rootly.alerts` | Alerts from external monitoring systems | — | `status`, `source`, `created_at_gte`, `created_at_lte` |
| `rootly.schedules` | On-call schedules | — | — |

All tables are **read-only**. This source does not create, modify, or delete Rootly data.

---

## API behavior

### JSON:API format

Rootly uses JSON:API. All resource fields are nested under `attributes`, while `id` is at the top level. The `role_id` column on `users` is sourced from `relationships.role.data.id`.

Severity on incidents is a nested JSON:API relationship. The manifest maps it via double-underscore notation:

* `severity__name` → `attributes.severity.data.attributes.name`
* `severity__slug` → `attributes.severity.data.attributes.slug`

### Pagination

All endpoints use page-based pagination:

* `page[number]` / `page[size]`
* Default page size: 25, maximum: 100

### Pushed filters

`incidents` and `alerts` support server-side filter pushdown — these reduce API traffic and pagination depth for large workspaces:

**incidents:** `status`, `severity`, `started_at_gte`, `started_at_lte`, `created_at_gte`, `created_at_lte`

**alerts:** `status`, `source`, `created_at_gte`, `created_at_lte`

For large organizations always supply a time filter to avoid unbounded pagination scans.

### Rate limits

Rootly's default rate limit is **3000 GET, HEAD, and OPTIONS calls per API key per minute**, calculated over a 1-minute sliding window. Contact your Rootly Customer Success Manager to increase this threshold.

* `429 Too Many Requests` responses should be retried with exponential backoff
* `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers are included in every response

---

## Example queries

### Active incidents

```sql
SELECT id, title, severity__name, status, started_at
FROM rootly.incidents
WHERE status = 'started'
ORDER BY started_at DESC
LIMIT 20;
```

### Resolved incidents (MTTR analysis)

```sql
SELECT id, title, severity__slug, started_at, mitigated_at, resolved_at
FROM rootly.incidents
WHERE status = 'resolved'
ORDER BY resolved_at DESC
LIMIT 50;
```

### Incidents in a time window (pushed filter)

```sql
SELECT id, title, severity__slug, status, started_at
FROM rootly.incidents
WHERE started_at_gte = '2025-01-01T00:00:00Z'
  AND started_at_lte = '2025-03-31T23:59:59Z'
ORDER BY started_at DESC;
```

### Filter incidents by severity (pushed filter)

```sql
SELECT id, title, severity__name, status, started_at
FROM rootly.incidents
WHERE severity = 'sev0'
ORDER BY started_at DESC
LIMIT 20;
```

### Services with GitHub mapping

```sql
SELECT id, name, slug, github_repository_name, github_repository_branch
FROM rootly.services
WHERE github_repository_name IS NOT NULL
ORDER BY name
LIMIT 20;
```

### On-call ownership view

```sql
SELECT
  s.id,
  s.name,
  s.all_time_coverage,
  u.full_name AS owner_name,
  u.email AS owner_email
FROM rootly.schedules s
LEFT JOIN rootly.users u ON s.owner_user_id = u.id
ORDER BY s.name
LIMIT 20;
```

### Alerts by source system

```sql
SELECT source, COUNT(*) AS alert_count
FROM rootly.alerts
GROUP BY source
ORDER BY alert_count DESC
LIMIT 10;
```

### Alerts with external correlation

```sql
SELECT id, short_id, external_id, external_url, source, status, created_at
FROM rootly.alerts
WHERE status = 'triggered'
ORDER BY created_at DESC
LIMIT 20;
```

---

## Validation

### Lint manifest

```sh
coral source lint sources/community/rootly/manifest.yaml
```

### Add source

```sh
export ROOTLY_TOKEN="<your-api-key>"
coral source add --file sources/community/rootly/manifest.yaml
```

### Validate tables

```sh
coral sql "SELECT id, full_name, email FROM rootly.users LIMIT 5"
coral sql "SELECT id, name, slug FROM rootly.services LIMIT 5"
coral sql "SELECT id, name, slug FROM rootly.teams LIMIT 5"
coral sql "SELECT id, title, status, severity__slug FROM rootly.incidents LIMIT 5"
coral sql "SELECT id, short_id, summary, source, status FROM rootly.alerts LIMIT 5"
coral sql "SELECT id, name, owner_user_id FROM rootly.schedules LIMIT 5"
```

### Inspect registered tables and columns

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'rootly'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'rootly' ORDER BY table_name, ordinal_position"
```

---

> **Building from source?** Replace `coral` with `cargo run -p coral-cli --` in all commands above.

---

## Notes

* **JSON:API format:** all fields are under `attributes`, `id` is top-level; `role_id` on users comes from `relationships.role.data.id`
* **Auth:** API keys and OAuth tokens both use `Authorization: Bearer`
* **Pagination:** consistent page-based pagination (`page[number]` / `page[size]`) across all endpoints; default 25, max 100
* **Pushed filters:** `incidents` and `alerts` support API-side filtering by status, severity/source, and date range — always use these for large workspaces
* **Rate limits:** default 3000 GET requests per API key per minute; retry on 429 with backoff
* **Timestamps:** ISO 8601 format throughout
* **Severity:** nested JSON:API relationship on incidents — access via `severity__name` and `severity__slug`; use the `severity` filter with the slug value
* **Alert fields:** mapped from the documented Rootly alert schema (`short_id`, `summary`, `description`, `external_id`, `external_url`, `deduplication_key`, `notification_target_id`)
* **Cross-source joins:** incidents expose `jira_issue_key`, `linear_issue_id`, `github_issue_id`, and `shortcut_story_id` for correlation

---

## Out of scope for v1

* Incident timeline events / retrospectives
* Workflow automation / playbooks
* Write operations (create/update/delete)
* OAuth authorization code flow
* Advanced incident mutation APIs
