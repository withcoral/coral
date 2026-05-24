# Statuspage.io

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `https://api.statuspage.io/v1`

Query active incidents, incident history, scheduled maintenance windows, and component health from your [Statuspage.io](https://www.statuspage.io/) status page.

## Authentication

Requires a `STATUSPAGE_API_KEY` and a `STATUSPAGE_PAGE_ID`.

- Find both at: **manage.statuspage.io → avatar → API info**
- The API key is a long hex string (e.g. `89a229ce1a8dbcf9ff30430fbe...`)
- The page ID is a short alphanumeric ID (e.g. `gytm4qzbx9t6`) — not the subdomain

```bash
STATUSPAGE_API_KEY=<token> STATUSPAGE_PAGE_ID=<page_id> \
  coral source add --file sources/community/statuspage/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
coral source add --file sources/community/statuspage/manifest.yaml --interactive
```

## Tables

| Table | Description | Optional filters |
|---|---|---|
| `incidents` | Unresolved (active) incidents — investigating, identified, monitoring | — |
| `all_incidents` | All incidents regardless of status, including resolved and historical | — |
| `scheduled_maintenances` | All scheduled maintenance windows regardless of state | — |
| `components` | All components and their current operational status | — |

### Which incidents table to use

| Goal | Table |
|---|---|
| Detect a live outage right now | `incidents` |
| Query resolved or historical incidents | `all_incidents` |
| Calculate MTTR / incident trends | `all_incidents` |
| See upcoming maintenance windows | `scheduled_maintenances` |
| See active maintenance windows | `scheduled_maintenances` |

### Incident status values

| Table | Status values |
|---|---|
| `incidents` | `investigating`, `identified`, `monitoring` only |
| `all_incidents` | `investigating`, `identified`, `monitoring`, `resolved`, `postmortem` |
| `scheduled_maintenances` | `scheduled`, `in_progress`, `verifying`, `completed` |

### Component status values

| Value | Meaning |
|---|---|
| `operational` | Component is healthy |
| `degraded_performance` | Component is slower than normal |
| `partial_outage` | Component is partially unavailable |
| `major_outage` | Component is fully unavailable |
| `under_maintenance` | Component is under scheduled maintenance |

## Quick start

```bash
# Active incidents right now
coral sql "
  SELECT id, name, status, impact, shortlink, created_at
  FROM statuspage.incidents
  ORDER BY created_at DESC
"

# All incidents in the last 30 days (resolved + active)
coral sql "
  SELECT id, name, status, impact, created_at, resolved_at
  FROM statuspage.all_incidents
  WHERE created_at >= NOW() - INTERVAL '30 days'
  ORDER BY created_at DESC
"

# Incidents that are still open (from all_incidents)
coral sql "
  SELECT id, name, status, impact, created_at
  FROM statuspage.all_incidents
  WHERE resolved_at IS NULL
  ORDER BY created_at DESC
"

# Resolved incidents — calculate duration
coral sql "
  SELECT
    name,
    impact,
    created_at,
    resolved_at
  FROM statuspage.all_incidents
  WHERE status = 'resolved'
  ORDER BY created_at DESC
  LIMIT 20
"

# Upcoming scheduled maintenance windows
coral sql "
  SELECT name, status, impact, scheduled_for, scheduled_until
  FROM statuspage.scheduled_maintenances
  WHERE status = 'scheduled'
  ORDER BY scheduled_for ASC
"

# Maintenance windows currently in progress
coral sql "
  SELECT name, scheduled_for, scheduled_until, incident_updates__body
  FROM statuspage.scheduled_maintenances
  WHERE status = 'in_progress'
"

# Critical or major active incidents
coral sql "
  SELECT id, name, status, impact, shortlink, created_at
  FROM statuspage.incidents
  WHERE impact IN ('major', 'critical')
  ORDER BY created_at DESC
"

# Check for degraded components
coral sql "
  SELECT name, status, updated_at
  FROM statuspage.components
  WHERE status != 'operational'
    AND group != true
  ORDER BY updated_at DESC
"

# Full component inventory sorted by display order
coral sql "
  SELECT id, name, status, description, group_id, position
  FROM statuspage.components
  ORDER BY position
"

# Incident frequency by impact level (last 90 days)
coral sql "
  SELECT impact, COUNT(*) AS count
  FROM statuspage.all_incidents
  WHERE created_at >= NOW() - INTERVAL '90 days'
  GROUP BY impact
  ORDER BY count DESC
"
```

## JOIN examples

```bash
# Cross-reference active incidents with Sentry error spikes
coral sql "
  SELECT
    s.title            AS sentry_issue,
    s.culprit,
    i.name             AS active_incident,
    i.impact,
    i.status           AS incident_status,
    i.shortlink
  FROM sentry.issues s
  JOIN statuspage.incidents i
    ON i.status IN ('investigating', 'identified', 'monitoring')
  WHERE s.times_seen > 100
    AND s.first_seen >= i.created_at
  ORDER BY s.times_seen DESC
  LIMIT 20
"

# Check for degraded components alongside triggered PagerDuty alerts
coral sql "
  SELECT
    p.title            AS alert,
    p.created_at       AS alerted_at,
    c.name             AS component,
    c.status,
    c.updated_at       AS component_updated_at
  FROM pagerduty.incidents p
  JOIN statuspage.components c
    ON c.status != 'operational'
  WHERE p.status = 'triggered'
  ORDER BY p.created_at DESC
  LIMIT 20
"

# Correlate PagerDuty alerts with scheduled maintenance windows
# (to distinguish 'expected degradation' from unexpected incidents)
coral sql "
  SELECT
    p.title            AS alert,
    p.created_at       AS alerted_at,
    m.name             AS maintenance_window,
    m.status           AS maintenance_status,
    m.scheduled_for,
    m.scheduled_until
  FROM pagerduty.incidents p
  JOIN statuspage.scheduled_maintenances m
    ON m.status IN ('scheduled', 'in_progress')
    AND p.created_at BETWEEN m.scheduled_for AND m.scheduled_until
  ORDER BY p.created_at DESC
  LIMIT 20
"
```

## Schema reference

### `statuspage.incidents`

Active (unresolved) incidents only. Queries `/incidents/unresolved`.

| Column | Type | Description |
|---|---|---|
| `id` | `Utf8` | Unique incident identifier |
| `name` | `Utf8` | Incident title |
| `status` | `Utf8` | `investigating`, `identified`, or `monitoring` |
| `impact` | `Utf8` | `none`, `minor`, `major`, or `critical` |
| `shortlink` | `Utf8` | Public short URL to the incident |
| `created_at` | `Timestamp` | When the incident was opened |
| `updated_at` | `Timestamp` | Most recent update time |
| `monitoring_at` | `Timestamp` | When the incident entered monitoring (nullable) |
| `resolved_at` | `Timestamp` | Always `NULL` on this endpoint |
| `incident_updates__body` | `Utf8` | All update message bodies joined by newline, newest-first |

### `statuspage.all_incidents`

All incidents including resolved and historical. Queries `/incidents`.

| Column | Type | Description |
|---|---|---|
| `id` | `Utf8` | Unique incident identifier |
| `name` | `Utf8` | Incident title |
| `status` | `Utf8` | `investigating`, `identified`, `monitoring`, `resolved`, or `postmortem` |
| `impact` | `Utf8` | `none`, `minor`, `major`, or `critical` |
| `shortlink` | `Utf8` | Public short URL to the incident |
| `created_at` | `Timestamp` | When the incident was opened |
| `updated_at` | `Timestamp` | Most recent update time |
| `monitoring_at` | `Timestamp` | When the incident entered monitoring (nullable) |
| `resolved_at` | `Timestamp` | When resolved — `NULL` if still active |
| `incident_updates__body` | `Utf8` | All update message bodies joined by newline, newest-first |

### `statuspage.scheduled_maintenances`

All scheduled maintenance windows. Queries `/incidents/scheduled`.

| Column | Type | Description |
|---|---|---|
| `id` | `Utf8` | Unique maintenance window identifier |
| `name` | `Utf8` | Maintenance window title |
| `status` | `Utf8` | `scheduled`, `in_progress`, `verifying`, or `completed` |
| `impact` | `Utf8` | `none`, `minor`, `major`, or `critical` |
| `shortlink` | `Utf8` | Public short URL to the maintenance window |
| `scheduled_for` | `Timestamp` | Planned start time |
| `scheduled_until` | `Timestamp` | Planned end time |
| `scheduled_auto_in_progress` | `Boolean` | Automatically transitions to `in_progress` at `scheduled_for` |
| `scheduled_auto_completed` | `Boolean` | Automatically transitions to `completed` at `scheduled_until` |
| `scheduled_remind_prior` | `Boolean` | Subscribers reminded 60 minutes before start |
| `created_at` | `Timestamp` | When the maintenance window was created |
| `updated_at` | `Timestamp` | Most recent update time |
| `incident_updates__body` | `Utf8` | All update message bodies joined by newline, newest-first |

### `statuspage.components`

| Column | Type | Description |
|---|---|---|
| `id` | `Utf8` | Unique component identifier |
| `name` | `Utf8` | Component display name |
| `status` | `Utf8` | `operational`, `degraded_performance`, `partial_outage`, `major_outage`, or `under_maintenance` |
| `description` | `Utf8` | Optional description of the component |
| `group` | `Boolean` | If `true`, this row is a component group header — `status` will be `NULL` |
| `created_at` | `Timestamp` | When the component was created |
| `updated_at` | `Timestamp` | Most recent status change time |
| `position` | `Int64` | Display order on the status page |
| `showcase` | `Boolean` | Whether this component appears on the public page |
| `only_show_if_degraded` | `Boolean` | If `true`, hidden from the public page when operational |
| `group_id` | `Utf8` | Component group ID (nullable) |
| `page_id` | `Utf8` | Statuspage page this component belongs to |

## Authentication

The `Authorization` header is sent as:

```
Authorization: OAuth <STATUSPAGE_API_KEY>
```

The API key is stored in Coral's secret store and never exposed through `coral.inputs`.

## Pagination

All tables use page-based pagination with a page size of 100 (`per_page=100`). For most status pages this fits all active data in a single request. Coral will automatically paginate through all pages for larger datasets.

## Notes

- The `incidents` table queries `/incidents/unresolved` — it never returns resolved incidents. Use `all_incidents` for resolved and historical data.
- The `scheduled_maintenances` table queries `/incidents/scheduled`. Status values (`scheduled`, `in_progress`, `verifying`, `completed`) are distinct from realtime incident statuses.
- The `components` table includes group header rows (`group = true`) with `NULL` status. Add `WHERE group != true` or `WHERE status IS NOT NULL` to exclude them.
- `incident_updates__body` joins all update message bodies in the order the API returns them (newest-first). Each update is separated by a newline character.
- The Statuspage Management API is rate-limited to **1 request/second** per token.
- The `all_incidents` table caps automatic pagination at **10 pages (1,000 incidents)** to avoid unbounded fetches against long-running pages. Use an explicit SQL `LIMIT` to fetch fewer, or raise the cap by adjusting `max_pages` in the source spec for your use case.
- HTTP 404 from any table is treated as a real error, not an empty result. A 404 means your `STATUSPAGE_PAGE_ID` is invalid or the API key does not have access to the page — check your inputs with `coral source test statuspage`.