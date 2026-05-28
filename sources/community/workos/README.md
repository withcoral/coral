# WorkOS

Query WorkOS identity and organization metadata from Coral. The source covers
organizations, User Management users, organization memberships, Directory Sync
directories, and WorkOS Events.

## Authentication

Create a WorkOS secret API key in the WorkOS dashboard and provide:

| Input | Description |
| --- | --- |
| `WORKOS_API_KEY` | WorkOS secret API key sent as a bearer token. |

The key is modeled as a secret. Use the narrowest dashboard role and
environment access that can read the metadata Coral agents need.

Official docs:

- <https://workos.com/docs/reference>
- <https://workos.com/docs/reference/organization/list>
- <https://workos.com/docs/reference/user-management/user/list>
- <https://workos.com/docs/reference/events/list-events>

## Tables

| Table | Description |
| --- | --- |
| `workos.organizations` | WorkOS organizations. |
| `workos.users` | User Management users. Supports `organization_id` and `email`. |
| `workos.organization_memberships` | User-to-organization memberships. Supports `organization_id` and `user_id`. |
| `workos.directories` | Directory Sync directories. |
| `workos.events` | WorkOS environment events. Supports organization, event, time range, and order filters. |

## Examples

List organizations:

```sql
SELECT id, name, created_at
FROM workos.organizations
ORDER BY created_at DESC
LIMIT 25;
```

Find users in one organization:

```sql
SELECT id, email, first_name, last_name, email_verified
FROM workos.users
WHERE organization_id = 'org_...'
LIMIT 25;
```

Review recent events:

```sql
SELECT id, event, created_at
FROM workos.events
WHERE range_start = '2026-05-01T00:00:00Z'
ORDER BY created_at DESC
LIMIT 25;
```

## Notes

- WorkOS list endpoints are cursor paginated with `after` and `limit`.
- The source is read-only and does not create, update, or delete WorkOS
  resources.
- Event `data` and `context` are JSON metadata columns; avoid selecting them in
  broad queries unless you need the raw event payload.
- Results depend on the API key's environment and permissions.

## Validation

- YAML parsing: passed
- Coral manifest schema validation: passed
- `git diff --check`: passed
- `make lint-sources`: passed
- Live API tests: passed against a WorkOS staging environment

Live Coral evidence:

```text
✓ workos connected successfully

workos (5 tables)
├─ directories
├─ events
├─ organization_memberships
├─ organizations
└─ users
Query tests
2 declared · 2 passed · 0 failed

✓ SELECT id, name FROM workos.organizations LIMIT 1
  1 row

✓ SELECT id, email FROM workos.users LIMIT 1
  0 rows
```
