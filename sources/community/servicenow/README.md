# ServiceNow Coral Source

## What This Source Exposes

This source exposes read-only ServiceNow ITSM and CMDB metadata through the
ServiceNow Table API. It covers incidents, change requests, problems,
configuration items, and users.

It does not create, update, or delete ServiceNow records.

## Authentication

Use a least-privilege ServiceNow integration user with read access to the
tables you want Coral to inspect.

| Input | Kind | Description |
|---|---|---|
| `SERVICENOW_INSTANCE_URL` | variable | ServiceNow instance URL, without a trailing slash. |
| `SERVICENOW_USERNAME` | variable | ServiceNow username for Basic auth. |
| `SERVICENOW_PASSWORD` | secret | ServiceNow password or API credential. |

## Setup

```bash
SERVICENOW_INSTANCE_URL=https://example.service-now.com \
SERVICENOW_USERNAME=integration.user \
SERVICENOW_PASSWORD=your_password \
coral source add --file sources/community/servicenow/manifest.yaml
```

## Tables

| Table | Purpose |
|---|---|
| `servicenow.incidents` | Incident ticket inventory and triage metadata. |
| `servicenow.change_requests` | Change request inventory and planning metadata. |
| `servicenow.problems` | Problem records and known-error metadata. |
| `servicenow.cmdb_ci` | Configuration item inventory from the CMDB. |
| `servicenow.users` | ServiceNow users visible to the integration user. |

## Example Queries

High-priority active incidents:

```sql
SELECT number, short_description, priority, state, assignment_group
FROM servicenow.incidents
WHERE query = 'active=true^priority=1'
LIMIT 25;
```

Upcoming active changes:

```sql
SELECT number, short_description, state, risk, start_date, end_date
FROM servicenow.change_requests
WHERE query = 'active=true'
LIMIT 25;
```

Configuration items by class:

```sql
SELECT sys_class_name, COUNT(*) AS ci_count
FROM servicenow.cmdb_ci
GROUP BY sys_class_name
ORDER BY ci_count DESC;
```

Active users:

```sql
SELECT user_name, name, email, department
FROM servicenow.users
WHERE query = 'active=true'
LIMIT 25;
```

## Limitations

- ServiceNow table access is governed by the integration user's ACLs.
- The `query` filter maps to ServiceNow encoded query syntax through
  `sysparm_query`.
- This source requests explicit `sysparm_fields` field lists instead of raw
  table payloads.
- Timestamps are exposed as strings because ServiceNow Table API date formats
  can vary by instance settings.

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/servicenow/manifest.yaml
Coral manifest schema validation: passed for sources/community/servicenow/manifest.yaml
git diff --check: passed
make lint-sources: passed
Live API tests: passed against a ServiceNow PDI
```

Live Coral evidence:

```text
✓ servicenow connected successfully
Secrets: keychain

servicenow (5 tables)
├─ change_requests
├─ cmdb_ci
├─ incidents
├─ problems
└─ users
Query tests
2 declared · 2 passed · 0 failed

✓ SELECT sys_id, number, short_description FROM servicenow.incidents LIMIT 1
  1 row

✓ SELECT sys_id, name, user_name FROM servicenow.users LIMIT 1
  1 row
```

Representative query:

```sql
SELECT sys_id, user_name, name, active
FROM servicenow.users
LIMIT 3;
```

The manifest includes `test_queries`, and no credentials or customer data are
committed.
