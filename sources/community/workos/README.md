# WorkOS

Query WorkOS identity and organization metadata from Coral. The source covers
organizations, User Management users, organization memberships, Directory Sync
directories, and WorkOS Events.

## Authentication

Create a WorkOS secret API key in the WorkOS dashboard and provide:

| Input | Description |
| --- | --- |
| `WORKOS_API_KEY` | WorkOS secret API key sent as a bearer token. |

The key is modeled as a secret. WorkOS secret keys are environment-scoped
credentials that can perform API requests for that environment; this source is
read-only, but the key itself is not inherently read-only. Use a non-production
or least-privileged environment key whenever possible, rotate it if exposed, and
only share it with Coral agents that need identity and audit metadata access.

Official docs:

- <https://workos.com/docs/reference>
- <https://workos.com/docs/reference/api-authentication>
- <https://workos.com/docs/reference/organization/list>
- <https://workos.com/docs/reference/user-management/user/list>
- <https://workos.com/docs/reference/authkit/organization-membership>
- <https://workos.com/docs/reference/directory-sync/directory>
- <https://workos.com/docs/reference/events/list-events>

## Tables

| Table | Description |
| --- | --- |
| `workos.organizations` | WorkOS organizations. |
| `workos.users` | User Management users. Supports `organization_id` and `email`. |
| `workos.organization_memberships_by_organization` | User-to-organization memberships. Requires `organization_id`; supports `statuses`. |
| `workos.organization_memberships_by_user` | User-to-organization memberships. Requires `user_id`; supports `statuses`. |
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

Review memberships for one organization:

```sql
SELECT id, user_id, organization_id, status, role_slug
FROM workos.organization_memberships_by_organization
WHERE organization_id = 'org_...'
LIMIT 25;
```

Review pending or inactive memberships:

```sql
SELECT id, user_id, organization_id, status, role_slug
FROM workos.organization_memberships_by_organization
WHERE organization_id = 'org_...'
  AND statuses = 'pending,inactive'
LIMIT 25;
```

Review memberships for one user:

```sql
SELECT id, user_id, organization_id, status, role_slug
FROM workos.organization_memberships_by_user
WHERE user_id = 'user_...'
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
- WorkOS requires `organization_id` or `user_id` for membership queries, so the
  source exposes separate membership tables with required scope filters instead
  of an unscoped membership table.
- WorkOS membership lists return active memberships by default. Use the
  `statuses` filter, such as `pending,inactive`, when auditing invitations or
  inactive memberships.
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

workos (6 tables)
├─ directories
├─ events
├─ organization_memberships_by_organization
├─ organization_memberships_by_user
├─ organizations
└─ users
Query tests
3 declared · 3 passed · 0 failed

✓ SELECT id, name FROM workos.organizations LIMIT 1
  1 row

✓ SELECT id, email FROM workos.users LIMIT 1
  0 rows

✓ SELECT id, name FROM workos.directories LIMIT 1
  0 rows
```
