# Rootly source

Query Rootly incident response data through Coral SQL.

This community source exposes read-only Rootly REST API data for SRE,
DevOps, platform engineering, incident response, and operational review
workflows.

## Configuration

Create a Rootly API token from:

`Organization Settings -> API Keys`

Then configure the source:

```bash
export ROOTLY_API_TOKEN="rootly_api_token"

coral source add --file sources/community/rootly/manifest.yaml
coral source test rootly
```

## Example Queries

Recent incidents:

```sql
SELECT
  sequential_id,
  title,
  status,
  severity_name,
  url,
  created_at
FROM rootly.incidents
ORDER BY created_at DESC
LIMIT 25;
```

Open incident action items:

```sql
SELECT
  summary,
  kind,
  priority,
  status,
  due_date,
  url
FROM rootly.action_items
WHERE status = 'open'
LIMIT 50;
```

Timeline events for an incident:

```sql
SELECT event, visibility, occurred_at
FROM rootly.incident_events
WHERE incident_id = 'incident-id'
ORDER BY occurred_at DESC;
```

Service ownership:

```sql
SELECT
  name,
  slug,
  owner_group_ids,
  owner_user_ids,
  pagerduty_id,
  opsgenie_id
FROM rootly.services
LIMIT 50;
```

API key hygiene:

```sql
SELECT
  name,
  kind,
  expires_at,
  last_used_at
FROM rootly.api_keys
WHERE active = true
LIMIT 50;
```

## Tables

- `rootly.incidents`
- `rootly.incident_events`
- `rootly.action_items`
- `rootly.services`
- `rootly.severities`
- `rootly.environments`
- `rootly.teams`
- `rootly.users`
- `rootly.api_keys`

## Notes

- This source only performs read-only `GET` requests.
- Rootly uses JSON:API response envelopes; most fields are mapped from
  `attributes`.
- `rootly.incident_events` requires an `incident_id` filter.
- API key token values are not returned by Rootly; `rootly.api_keys` exposes
  key metadata only.
