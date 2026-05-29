# Opsgenie Community Source

Query Opsgenie alerts, teams, users, schedules, escalations, and on-call
responders through Coral SQL.

## Setup

### 1. Create an Opsgenie API integration key

Create an API integration in Opsgenie and copy the integration API key. Grant
read access to the resources you want Coral to query.

### 2. Add the source

```bash
export OPSGENIE_API_BASE_URL="https://api.opsgenie.com/v2"
export OPSGENIE_API_KEY="<your-api-key>"
coral source add --file sources/community/opsgenie/manifest.yaml
```

Use `https://api.eu.opsgenie.com/v2` for EU-region Opsgenie accounts.

### 3. Verify

```bash
coral source test opsgenie
```

The built-in test query reads `opsgenie.teams`, which verifies that the API
base URL and GenieKey credential are usable.

## Tables

### `opsgenie.alerts`

Lists alerts visible to the API key.

**Optional filters:** `query`, `search_identifier`, `sort`, `order`

### `opsgenie.teams`

Lists teams visible to the API key.

### `opsgenie.users`

Lists Opsgenie users visible to the API key.

**Optional filter:** `query`

### `opsgenie.schedules`

Lists on-call schedules.

**Optional filters:** `query`, `expand`

### `opsgenie.escalations`

Lists escalation policies.

**Optional filter:** `query`

### `opsgenie.on_calls`

Returns current on-call participants for a schedule.

**Required filter:** `schedule_identifier`
**Optional filters:** `flat`, `date`

## Example Queries

```sql
-- List open P1 and P2 alerts
SELECT tiny_id, message, priority, status, created_at
FROM opsgenie.alerts
WHERE query = 'status: open AND priority: (P1 OR P2)'
ORDER BY created_at DESC
LIMIT 20;

-- Inventory teams
SELECT id, name, description
FROM opsgenie.teams
ORDER BY name;

-- Find users by name or email
SELECT username, full_name, verified, blocked
FROM opsgenie.users
WHERE query = 'alice'
LIMIT 10;

-- List schedules with owner teams
SELECT id, name, timezone, owner_team__name, enabled
FROM opsgenie.schedules
ORDER BY name;

-- Inspect on-call responders for a schedule
SELECT schedule_name, participants
FROM opsgenie.on_calls
WHERE schedule_identifier = 'primary-on-call';

-- Review escalation policies
SELECT name, owner_team__name, enabled, rules
FROM opsgenie.escalations
ORDER BY name;
```

## Validation

```bash
coral source lint sources/community/opsgenie/manifest.yaml
export OPSGENIE_API_BASE_URL="https://api.opsgenie.com/v2"
export OPSGENIE_API_KEY="<your-api-key>"
coral source add --file sources/community/opsgenie/manifest.yaml
coral source test opsgenie
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'opsgenie'"
coral sql "SELECT id, name, description FROM opsgenie.teams LIMIT 5"
```

## Limitations

- **Read-only.** This source does not acknowledge, close, create, or update
  alerts and does not change on-call configuration.
- **Permissions apply.** Query results depend on the API integration key's
  permissions.
- **On-call lookups are schedule-scoped.** Query `opsgenie.schedules` first,
  then use a schedule ID or name as `schedule_identifier`.
- **No incident, heartbeat, service, maintenance, or integration tables in
  v1.** The first version focuses on alert and on-call operations.
