# Auth0 Community Source

Query Auth0 users, roles, clients, connections, organizations, log events, and
actions through Coral SQL using the Auth0 Management API v2.

## Setup

### 1. Obtain a Management API token

1. Open the [Auth0 Dashboard](https://manage.auth0.com)
2. Go to **Applications › APIs › Auth0 Management API › Test**
3. Expand **Advanced Settings** and ensure the following scopes are enabled:
   `read:users`, `read:roles`, `read:clients`, `read:connections`,
   `read:organizations`, `read:logs`, `read:actions`
4. Click **Copy Token** and save it securely

For production use, create a dedicated Machine-to-Machine application,
authorize it against the Management API with the required scopes, and use its
access token instead of the Test token (which expires in 24 hours).

### 2. Add the source

```bash
export AUTH0_DOMAIN="dev-abc123.us.auth0.com"
export AUTH0_MANAGEMENT_API_TOKEN="<your-token>"
coral source add --file sources/community/auth0/manifest.yaml
```

Do not include `https://` or a trailing slash in `AUTH0_DOMAIN`. If you use a
custom domain, use that value instead.

### 3. Verify

```bash
coral source test auth0
```

The built-in test query reads `auth0.users`, which confirms that the domain
and token are valid.

## Tables

### `auth0.users`

Lists Auth0 users in the tenant (up to 100 per query).

**Optional filters:** `q` (Lucene syntax), `search_engine`

Use `search_engine = 'v3'` (the default) for full Lucene support.

### `auth0.roles`

Lists roles defined in the tenant.

**Optional filter:** `name_filter` (prefix match on role name)

### `auth0.clients`

Lists Auth0 applications registered in the tenant.

**Optional filter:** `app_type` (`spa`, `native`, `regular_web`, `non_interactive`)

### `auth0.connections`

Lists identity provider connections configured in the tenant.

**Optional filters:** `strategy`, `name`

### `auth0.organizations`

Lists organizations defined in the tenant.

### `auth0.logs`

Lists the most recent 100 tenant log events, sorted newest-first.

**Optional filters:** `type` (event type code), `q` (Lucene syntax)

Common `type` values:

| Code | Meaning |
|------|---------|
| `s`  | Success login |
| `f`  | Failed login |
| `slo` | Successful logout |
| `sce` | Success change email |
| `scpn` | Success change phone number |
| `limit_wc` | Blocked account (too many failed logins) |

### `auth0.actions`

Lists Auth0 Actions defined in the tenant.

**Optional filter:** `trigger_id` (e.g. `post-login`, `pre-user-registration`)

## Example Queries

```sql
-- List recently created users
SELECT user_id, email, name, connection, created_at
FROM auth0.users
ORDER BY created_at DESC
LIMIT 20;

-- Find a user by email (Lucene search)
SELECT user_id, email, name, logins_count, last_login
FROM auth0.users
WHERE q = 'email:"alice@example.com"';

-- Find blocked users
SELECT user_id, email, logins_count
FROM auth0.users
WHERE q = 'blocked:true';

-- List users with more than 100 logins
SELECT user_id, email, logins_count, last_login
FROM auth0.users
WHERE q = 'logins_count:[100 TO *]'
ORDER BY logins_count DESC;

-- List all roles
SELECT id, name, description
FROM auth0.roles
ORDER BY name;

-- Inventory applications by type
SELECT client_id, name, app_type, tenant
FROM auth0.clients
ORDER BY app_type, name;

-- List social and enterprise connections
SELECT id, name, strategy
FROM auth0.connections
WHERE strategy != 'auth0'
ORDER BY strategy;

-- Review recent failed login events
SELECT log_id, date, user_name, ip, description
FROM auth0.logs
WHERE type = 'f'
ORDER BY date DESC
LIMIT 50;

-- Audit recent admin activity
SELECT log_id, date, type, client_name, user_name, ip
FROM auth0.logs
ORDER BY date DESC
LIMIT 100;

-- List deployed actions for the post-login trigger
SELECT id, name, status, runtime, updated_at
FROM auth0.actions
WHERE trigger_id = 'post-login'
ORDER BY name;
```

## Validation

```bash
coral source lint sources/community/auth0/manifest.yaml

export AUTH0_DOMAIN="dev-abc123.us.auth0.com"
export AUTH0_MANAGEMENT_API_TOKEN="<your-token>"
coral source add --file sources/community/auth0/manifest.yaml
coral source test auth0

coral sql "SELECT * FROM coral.tables WHERE schema_name = 'auth0'"
coral sql "SELECT user_id, email, name FROM auth0.users LIMIT 5"
coral sql "SELECT id, name, description FROM auth0.roles"
coral sql "SELECT client_id, name, app_type FROM auth0.clients"
coral sql "SELECT id, name, strategy FROM auth0.connections"
coral sql "SELECT log_id, date, type, user_name FROM auth0.logs LIMIT 10"
coral sql "SELECT id, name, status FROM auth0.actions"
```

## Limitations

- **Read-only.** This source does not create, update, or delete any Auth0
  resources.
- **Token scopes apply.** Tables only return objects visible to the scopes
  granted to the Management API token.
- **Pagination is limited to 100 results per query.** For large tenants use
  the `q` filter (Lucene syntax on `auth0.users` and `auth0.logs`) to narrow
  result sets. Full cursor-based pagination is planned for v0.2.
- **Test tokens expire in 24 hours.** For long-running or automated use,
  create a Machine-to-Machine application with the required scopes.
- **No write, user-management, or branding tables in v0.1.** The first
  version focuses on identity inventory, application configuration, and
  audit logs.
