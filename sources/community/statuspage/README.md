# Statuspage.io

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `https://api.statuspage.io/v1`

Query active incidents and component health from your [Statuspage.io](https://www.statuspage.io/) status page.

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
| `incidents` | Unresolved (active) incidents on your status page | — |
| `components` | Infrastructure components and their current health status | — |

### Incidents endpoint note

The `incidents` table queries the `/incidents/unresolved` endpoint exclusively.
It only ever returns incidents with a `status` of `investigating`, `identified`, or `monitoring`.
Resolved incidents are not included — query your Statuspage dashboard for historical data.

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
# Confirm connectivity — see all active incidents
coral sql "SELECT id, name, status, impact, shortlink, created_at FROM statuspage.incidents"

# Check for any non-operational components
coral sql "
  SELECT name, status, updated_at
  FROM statuspage.components
  WHERE status != 'operational'
  ORDER BY updated_at DESC
"

# Critical or major incidents only
coral sql "
  SELECT id, name, status, impact, shortlink, created_at
  FROM statuspage.incidents
  WHERE impact IN ('major', 'critical')
  ORDER BY created_at DESC
"

# Incidents currently under investigation
coral sql "
  SELECT id, name, impact, shortlink, created_at, incident_updates__body
  FROM statuspage.incidents
  WHERE status = 'investigating'
  ORDER BY created_at DESC
"

# All components grouped by status
coral sql "
  SELECT status, COUNT(*) AS count
  FROM statuspage.components
  GROUP BY status
  ORDER BY count DESC
"

# Components that are hidden unless degraded
coral sql "
  SELECT name, status, updated_at
  FROM statuspage.components
  WHERE only_show_if_degraded = true
  ORDER BY name
"

# Full component inventory
coral sql "
  SELECT id, name, status, description, group_id, position
  FROM statuspage.components
  ORDER BY position
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
```

## Discovery order

```text
incidents
  → id       (incident identifier, for correlation with external sources)
  → shortlink (public URL to share in alerts)

components
  → group_id → components.id  (parent component group)
  → page_id  (the Statuspage page this component belongs to)
```