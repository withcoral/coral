# incident.io source

Query incident.io incident response data through Coral SQL.

This community source exposes read-only incident.io API data for SRE,
DevOps, platform engineering, incident response, and operational review
workflows.

## Configuration

Create an incident.io API key from:

`Settings -> API keys`

Then configure the source:

```bash
export INCIDENT_IO_API_TOKEN="incident_io_api_key"

coral source add --file sources/community/incident_io/manifest.yaml
coral source test incident_io
```

## Example Queries

Recent incidents:

```sql
SELECT
  reference,
  name,
  status__category,
  severity__name,
  permalink,
  created_at
FROM incident_io.incidents
ORDER BY created_at DESC
LIMIT 25;
```

Open follow-up actions:

```sql
SELECT
  description,
  status,
  priority,
  incident__reference,
  due_at
FROM incident_io.actions
WHERE status = 'outstanding'
LIMIT 50;
```

Current on-call schedules:

```sql
SELECT
  id,
  name,
  timezone,
  current_shifts
FROM incident_io.schedules
LIMIT 50;
```

Severity configuration:

```sql
SELECT id, name, rank, description
FROM incident_io.severities
ORDER BY rank;
```

## Tables

- `incident_io.incidents`
- `incident_io.actions`
- `incident_io.users`
- `incident_io.schedules`
- `incident_io.incident_roles`
- `incident_io.severities`

## Notes

- This source only performs read-only `GET` requests.
- v2 endpoints use cursor pagination with `after`.
- `incident_io.severities` uses the v1 severities endpoint because incident.io
  documents severity configuration there.
