# Fivetran

Query Fivetran account metadata from Coral. The source covers group,
destination, connection, user, and dbt transformation project inventory without
returning connector or destination configuration payloads that may contain
connection details.

## Authentication

Create a scoped Fivetran REST API key and provide:

| Input | Description |
| --- | --- |
| `FIVETRAN_API_KEY` | Fivetran API key. |
| `FIVETRAN_API_SECRET` | Fivetran API secret. |

Both values are modeled as secrets. Use the narrowest RBAC scope that can read
the metadata you want Coral agents to inspect.

## Tables

| Table | Description |
| --- | --- |
| `fivetran.groups` | Groups accessible to the API key. |
| `fivetran.destinations` | Destination inventory. Supports `group_id`. |
| `fivetran.connections` | Connection status and sync metadata. Supports `group_id` and `schema`. |
| `fivetran.users` | Account users visible to the API key. |
| `fivetran.transformation_projects` | dbt Core transformation project metadata. Supports `group_id`. |

## Examples

List paused or failing connections:

```sql
SELECT id, service, schema, paused, setup_state, sync_state, update_state
FROM fivetran.connections
WHERE paused = true OR sync_state <> 'succeeded';
```

Inspect connections in one group:

```sql
SELECT id, service, schema, succeeded_at, failed_at
FROM fivetran.connections
WHERE group_id = 'group_id';
```

Review destination coverage:

```sql
SELECT group_id, service, region, setup_status
FROM fivetran.destinations;
```

## Notes

- Fivetran list endpoints are cursor paginated with `cursor` and `limit`.
- The source omits connector and destination `config` payloads to avoid
  exposing source credentials, host names, or other connection parameters.
- Fivetran API responses depend on the API key owner's RBAC permissions.
- Live API tests passed against a Fivetran account. The account had no
  configured groups or connections, so the declared queries returned zero rows
  while still proving authentication, pagination, and table wiring.

## Validation

- YAML parsing: passed
- Coral manifest schema validation: passed
- `git diff --check`: passed
- `make lint-sources`: passed
- Live API tests: passed against a Fivetran account

Live Coral evidence:

```text
✓ fivetran connected successfully
Secrets: keychain

fivetran (5 tables)
├─ connections
├─ destinations
├─ groups
├─ transformation_projects
└─ users
Query tests
2 declared · 2 passed · 0 failed

✓ SELECT id, name FROM fivetran.groups LIMIT 1
  0 rows

✓ SELECT id, service, group_id FROM fivetran.connections LIMIT 1
  0 rows
```
