# Fivetran

Query Fivetran account metadata from Coral. The source covers group,
destination, connection, user, and transformation project inventory without
returning connector or destination configuration payloads that may contain
connection details.

## Authentication

Create a Fivetran REST API key from the Fivetran dashboard, usually under
account settings and system/API keys. Follow Fivetran's official REST API
getting started guide for the exact credential flow:
<https://fivetran.com/docs/rest-api/getting-started>.

| Input | Description |
| --- | --- |
| `FIVETRAN_API_KEY` | Fivetran API key. |
| `FIVETRAN_API_SECRET` | Fivetran API secret. |

Both values are modeled as secrets. Use a scoped system key or least-privilege
API key that can read only the metadata Coral agents need to inspect.

## Tables

| Table | Description |
| --- | --- |
| `fivetran.groups` | Groups accessible to the API key. |
| `fivetran.destinations` | Destination inventory. Filter returned `group_id` locally in SQL. |
| `fivetran.connections` | Connection status and sync metadata. Supports `group_id` and `schema`. |
| `fivetran.users` | Account users visible to the API key. Requires `USER:READ`. |
| `fivetran.transformation_projects` | Transformation project metadata. Filter returned `group_id` locally in SQL. |

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
- Fivetran can return HTTP 429 when account-level API quotas or rate limits are
  reached. Coral honors provider responses, and callers should retry after the
  server's `Retry-After` guidance when present:
  <https://fivetran.com/docs/rest-api/getting-started/rate-limiting>.
- The source omits connector and destination `config` payloads to avoid
  exposing source credentials, host names, or other connection parameters.
- Fivetran API responses depend on the API key owner's RBAC permissions.
- The users table requires Fivetran `USER:READ` permission. A scoped key without
  that permission can still use the other inventory tables.
- Live API tests passed against a Fivetran account. The account had no
  configured inventory for several tables, so some declared queries returned
  zero rows while still proving authentication, pagination, and table wiring.

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
4 declared · 4 passed · 0 failed

✓ SELECT id, name FROM fivetran.groups LIMIT 1
  0 rows

✓ SELECT id, service, group_id FROM fivetran.connections LIMIT 1
  0 rows

✓ SELECT id, group_id, service FROM fivetran.destinations LIMIT 1
  0 rows

✓ SELECT id, type, group_id FROM fivetran.transformation_projects LIMIT 1
  0 rows

Additional users-table check with the scoped test key:

SELECT id, email FROM fivetran.users LIMIT 1
403 Forbidden: required permission USER:READ
```
