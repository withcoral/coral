# Clerk

Query users, organizations, memberships, invitations, and sessions from Clerk.

## Setup

### Get Your Secret Key

1. Log in to the [Clerk Dashboard](https://dashboard.clerk.com)
2. Navigate to **API Keys**
3. Copy your **Secret Key** (starts with `sk_live_` or `sk_test_`)

### Add the Source

```bash
export CLERK_SECRET_KEY="sk_test_..."
coral source add --file sources/community/clerk/manifest.yaml
```

## Tables

### `users`

Lists all users for the Clerk application. Returns profile data,
authentication settings, metadata, and account status. This is the
primary discovery table — use `id` to query sessions or join with
organization memberships.

**Useful for:**

- User inventory across your application
- Auditing authentication settings (2FA, password, banned/locked)
- Reviewing OAuth provider connections via `external_accounts`
- Tracking sign-in activity and account creation dates

### `organizations`

Lists all organizations for the Clerk application. Returns
organization profile data, membership counts, and metadata.

**Useful for:**

- Organization inventory and membership tracking
- Auditing membership limits and admin permissions
- Getting organization IDs for membership queries

### `organization_memberships`

Lists all memberships for a specific organization. Returns member
roles, permissions, and public user data.

**Requires:** `organization_id` filter (from `organizations`)

**Useful for:**

- Auditing member roles and permissions per organization
- Identifying admins and reviewing access levels
- Cross-referencing membership data with user profiles

**Example:**

```sql
SELECT organization_id, id, role, public_user_data
FROM clerk.organization_memberships
WHERE organization_id = 'org_abc123';
```

### `invitations`

Lists all invitations for the Clerk application. Returns invitation
status, email address, and metadata. Invitations are returned sorted
by descending creation date. By default, Clerk's API returns non-revoked
invitations. To find revoked invitations, you must explicitly query
`WHERE status = 'revoked'`.

**Useful for:**

- Tracking pending, accepted, and revoked invitations
- Auditing invitation activity
- Monitoring onboarding funnel

### `sessions`

Lists all sessions for the Clerk application. Returns session status,
associated user and client IDs, and activity timestamps.

**Requires:** `user_id` filter. Use `client_id` or `status` as optional
filters to narrow the returned sessions.

**Useful for:**

- Monitoring active sessions
- Auditing session lifetimes and expiration
- Identifying impersonated sessions via the `actor` field
- Tracking last active organization per session

## Authentication

The source uses Bearer token authentication with your Clerk Secret Key.
The key is sent as a `secret` input and never exposed in query results.

## Limits

- All list endpoints use offset pagination with a maximum page size
  of 500. The source defaults to 100 per page.
- `organization_memberships` requires an `organization_id` filter —
  it queries one organization at a time.
- `sessions` requires a `user_id` filter — it queries one user's
  sessions at a time. Add `client_id` or `status` when you want a
  narrower result set.
- Nested arrays (`email_addresses`, `phone_numbers`, `web3_wallets`,
  `external_accounts`) are returned as Json columns. Use
  `json_get_*` SQL functions to extract individual values.
- Metadata fields (`public_metadata`, `private_metadata`,
  `unsafe_metadata`) are returned as Json columns.

## Example Queries

### List all users with their auth settings

```sql
SELECT id, first_name, last_name, username,
       password_enabled, two_factor_enabled,
       banned, locked
FROM clerk.users;
```

### Find users who signed in recently

```sql
SELECT id, first_name, last_name,
       last_sign_in_at, last_active_at
FROM clerk.users
WHERE last_sign_in_at IS NOT NULL
ORDER BY last_sign_in_at DESC
LIMIT 20;
```

### List all organizations with member counts

```sql
SELECT id, name, slug, members_count,
       max_allowed_memberships, created_at
FROM clerk.organizations;
```

### Audit organization memberships

```sql
SELECT organization_id, id, role, public_user_data
FROM clerk.organization_memberships
WHERE organization_id = 'org_abc123';
```

### Find pending invitations before they expire

```sql
SELECT id, email_address, status, expires_at, created_at
FROM clerk.invitations
WHERE status = 'pending';
```

### Check active sessions

```sql
SELECT id, user_id, status,
       last_active_at, latest_activity, expire_at
FROM clerk.sessions
WHERE user_id = 'user_abc123' AND status = 'active'
LIMIT 20;
```

## Notes

- All endpoints are under `https://api.clerk.com/v1/` and use offset
  pagination with `limit` and `offset` query parameters
- Timestamps use `format_timestamp` with `milliseconds` input — Clerk
  returns Unix timestamps in milliseconds
- The `organizations` table automatically includes `members_count` by
  passing `include_members_count=true` to the API
- Complex user data like email addresses, phone numbers, and OAuth
  accounts are preserved as Json columns for flexibility
- The `organization_memberships` table uses the `organization_id`
  filter in the URL path — get org IDs from `clerk.organizations`
  first
- The `sessions` table requires `user_id` because Clerk requires a
  user or client identifier for session-list requests
