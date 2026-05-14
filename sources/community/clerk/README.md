# Clerk source

This community source queries the [Clerk Backend API](https://clerk.com/docs/reference/backend-api) so you can inspect identity, organization, membership, invitation, session, domain, role, and waitlist metadata with SQL.

The v1 source is read-only and uses Clerk secret-key authentication.

## Configure

Create or copy a Clerk Secret Key from the Clerk Dashboard API keys page, then add the source:

```sh
export CLERK_SECRET_KEY="sk_test_..."
coral source add --file sources/community/clerk/manifest.yaml
```

Secret keys authenticate Backend API requests. Do not expose them in frontend code or commit them to the repository.

## Start querying

List recent users:

```sql
SELECT id, username, first_name, last_name, primary_email_address_id, created_at
FROM clerk.users
ORDER BY created_at DESC
LIMIT 20;
```

Search users by partial identity data:

```sql
SELECT id, username, first_name, last_name, primary_email_address_id
FROM clerk.users
WHERE query = 'alice'
LIMIT 20;
```

List organizations:

```sql
SELECT id, name, slug, members_count, created_at
FROM clerk.organizations
WHERE include_members_count IS TRUE
LIMIT 20;
```

Audit organization memberships across the instance:

```sql
SELECT organization_id, organization__name, user_id, public_user_data__identifier, role
FROM clerk.organization_memberships
LIMIT 50;
```

Inspect memberships for one organization:

```sql
SELECT user_id, public_user_data__identifier, role, permissions, created_at
FROM clerk.organization_memberships_by_organization
WHERE organization_id = 'org_...'
LIMIT 50;
```

Find pending organization invitations:

```sql
SELECT organization_id, email_address, role, status, expires_at
FROM clerk.organization_invitations
WHERE status = 'pending'
LIMIT 50;
```

Inspect sessions for a user:

```sql
SELECT id, user_id, client_id, status, last_active_at, expire_at
FROM clerk.sessions
WHERE user_id = 'user_...'
LIMIT 20;
```

Review organization domains:

```sql
SELECT organization_id, name, enrollment_mode, verification__status
FROM clerk.organization_domains
LIMIT 50;
```

Review organization permissions and role sets:

```sql
SELECT key, name, description
FROM clerk.organization_permissions
ORDER BY key
LIMIT 50;
```

```sql
SELECT key, name, type, roles, default_role
FROM clerk.role_sets
LIMIT 20;
```

## Tables

| Table | Purpose |
|---|---|
| `users` | Users in the Clerk instance. |
| `organizations` | Organizations in the Clerk instance. |
| `organization_memberships` | Organization memberships across the instance. |
| `organization_memberships_by_organization` | Organization memberships scoped to a required `organization_id`. |
| `organization_invitations` | Organization invitations across the instance. |
| `organization_invitations_by_organization` | Organization invitations scoped to a required `organization_id`. |
| `sessions` | Sessions for a required `user_id`. |
| `invitations` | Application-level invitations. |
| `organization_domains` | Organization domains across the instance. |
| `organization_permissions` | Organization permission definitions. |
| `role_sets` | Organization role sets. |
| `domains` | Clerk instance domains. |
| `oauth_applications` | OAuth applications configured in Clerk. |
| `waitlist_entries` | Waitlist entries in the Clerk instance. |

## Notes

- The source is read-only and does not call Clerk mutation endpoints.
- Organization tables require the Organizations feature to be enabled in the Clerk instance. Instances without Organizations enabled can still use non-organization tables such as `users`, `domains`, `invitations`, `oauth_applications`, and `waitlist_entries`.
- `sessions` requires `user_id` because Clerk recommends scoping session list requests by user or client.
- Some nested Clerk objects are exposed as `Json` columns, including user metadata, contact identifiers, external accounts, membership permissions, roles, and domain verification details.
- Backend API access can expose sensitive identity and private metadata. Use a least-privilege secret key and restrict who can query the source.
