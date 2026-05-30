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

---

### 2. Set your token

```sh
export ROOTLY_TOKEN="<your-api-key-or-oauth-token>"
```

---

### 3. Add the source

```sh
coral source add --file sources/community/rootly/manifest.yaml
```

---

### 4. First successful query (recommended)

Use this to verify your setup and immediately see operational data:

```sql
SELECT id, title, status, severity_name, started_at
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

| Table              | Description                                 | Required filters |
| ------------------ | ------------------------------------------- | ---------------- |
| `rootly.users`     | Users in the organization                   | —                |
| `rootly.services`  | Services defined in the organization        | —                |
| `rootly.teams`     | Teams in the organization                   | —                |
| `rootly.incidents` | Incident lifecycle records (MTTR/MTTD/MTTA) | —                |
| `rootly.alerts`    | Alerts from external monitoring systems     | —                |
| `rootly.schedules` | On-call schedules                           | —                |

All tables are **read-only**. This source does not create, modify, or delete Rootly data.

---

## API Behavior

### JSON:API format

Rootly uses JSON:API. All fields are nested under `attributes`, while `id` is at the top level.

---

### Pagination

Rootly uses **page-based pagination across all endpoints**:

* `page[number]`
* `page[size]`
* Default page size: 25
* Maximum page size: 100

Pagination is consistent across all resources (users, incidents, services, alerts, etc.). Large datasets (especially incidents and alerts) should always expect multiple pages.

---

### Date filtering behavior

For performance and operational use cases, always filter large datasets by time.
These filters push down to the Rootly API — they are not applied locally.

Supported pushed filters for `incidents`:

* `status`, `severity`
* `started_at_gte` / `started_at_lte`
* `created_at_gte` / `created_at_lte`

Supported pushed filters for `alerts`:

* `status`, `source`
* `created_at_gte` / `created_at_lte`

For incident-heavy workloads, use date ranges (e.g. last 7/30/90 days) to avoid large pagination scans.

---

### Rate limits

Rootly enforces **workspace-level API rate limits**.

* Requests may be throttled during high incident activity
* `429 Too Many Requests` responses should be retried with exponential backoff
* Long-running incident queries may hit limits in large organizations

---

## Example queries

### Active incidents

```sql
SELECT id, title, severity_name, status, started_at
FROM rootly.incidents
WHERE status = 'started'
ORDER BY started_at DESC
LIMIT 20;
```

---

### Resolved incidents (MTTR analysis)

```sql
SELECT id, title, severity_name, started_at, mitigated_at, resolved_at
FROM rootly.incidents
WHERE status = 'resolved'
ORDER BY resolved_at DESC
LIMIT 50;
```

---

### Incidents in a time window

```sql
SELECT id, title, severity_name, status, started_at
FROM rootly.incidents
WHERE started_at_gte = '2025-01-01T00:00:00Z'
  AND started_at_lte = '2025-03-31T23:59:59Z'
ORDER BY started_at DESC;
```

---

### Services with GitHub mapping

```sql
SELECT id, name, slug, github_repository_name, github_repository_branch
FROM rootly.services
WHERE github_repository_name IS NOT NULL
ORDER BY name
LIMIT 20;
```

---

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

---

### Alerts by source system

```sql
SELECT source, COUNT(*) AS alert_count
FROM rootly.alerts
GROUP BY source
ORDER BY alert_count DESC
LIMIT 10;
```

---

### Alerts with external correlation

```sql
SELECT id, external_id, external_url, source, status, created_at
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

---

### Add source

```sh
export ROOTLY_TOKEN="<your-api-key>"
coral source add --file sources/community/rootly/manifest.yaml
```

---

### Validate tables

```sh
coral sql "SELECT id, full_name, email FROM rootly.users LIMIT 5"
coral sql "SELECT id, name, slug FROM rootly.services LIMIT 5"
coral sql "SELECT id, name, slug FROM rootly.teams LIMIT 5"
coral sql "SELECT id, title, status, severity_name FROM rootly.incidents LIMIT 5"
coral sql "SELECT id, external_id, source, status FROM rootly.alerts LIMIT 5"
coral sql "SELECT id, name, owner_user_id FROM rootly.schedules LIMIT 5"
```

---

## Notes

* **JSON:API format:** all fields are under `attributes`, `id` is top-level
* **Auth:** API keys and OAuth tokens both use `Authorization: Bearer`
* **Pagination:** consistent page-based pagination across all endpoints
* **Rate limits:** workspace-level limits; retry on 429 with backoff
* **Date fields:** timestamps are ISO 8601 format
* **Pushed filters:** `incidents` and `alerts` support server-side filtering by status, source, and date range — use these for large workspaces
* **Cross-source joins:** incidents expose Jira, Linear, GitHub, Shortcut IDs for correlation
* **Operational use case:** optimized for incident lifecycle and reliability analytics

---

## Out of scope for v1

* Incident timeline events / retrospectives
* Workflow automation / playbooks
* Write operations (create/update/delete)
* OAuth authorization code flow
* Advanced incident mutation APIs
