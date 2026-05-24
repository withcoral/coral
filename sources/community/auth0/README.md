# Auth0 Community Source

Query Auth0 tenant data — users, clients, connections, roles, organizations,
resource servers, client grants, actions, logs, grants, custom domains, and
tenant settings — through Coral SQL using the Auth0 Management API v2.

## Setup

### 1. Create a Management API access token

Option A — **API Explorer token** (quick start):

1. Open the [Auth0 Dashboard](https://manage.auth0.com)
2. Go to **Applications > APIs > Auth0 Management API > API Explorer**
3. Copy the token

Option B — **Machine-to-machine application** (production):

1. Create a machine-to-machine application
2. Authorize it for the **Auth0 Management API**
3. Grant the required scopes (see below)
4. Use a client credentials grant to obtain a token

Required scopes depend on the tables you query:

| Scope | Tables |
| --- | --- |
| `read:users` | `users` |
| `read:clients` | `clients` |
| `read:connections` | `connections` |
| `read:roles` | `roles`, `role_permissions` |
| `read:roles` + `read:users` | `role_users` |
| `read:organizations` | `organizations` |
| `read:organization_members` | `organization_members` |
| `read:resource_servers` | `resource_servers` |
| `read:client_grants` | `client_grants` |
| `read:actions` | `actions` |
| `read:logs` | `logs` |
| `read:logs_users` | `logs` (user-related fields) |
| `read:grants` | `grants` |
| `read:custom_domains` | `custom_domains` |
| `read:tenant_settings` | `tenant_settings` |

### 2. Add the source

```bash
export AUTH0_DOMAIN="https://dev-abc123.us.auth0.com"
export AUTH0_API_TOKEN="<your-management-api-token>"
coral source add --file sources/community/auth0/manifest.yaml
```

Do not include a trailing slash in `AUTH0_DOMAIN`.

### 3. Verify

```bash
coral source test auth0
```

The built-in test query reads `auth0.users`, which verifies that the
domain and token are usable.

## Tables

### `auth0.users`

Lists all users in the tenant with profile, identity, and login data.

**Optional filters:** `q` (Lucene syntax), `sort`

### `auth0.clients`

Lists all applications (clients) registered in the tenant.

### `auth0.connections`

Lists all identity provider connections (database, social, enterprise,
passwordless).

**Optional filter:** `strategy`

### `auth0.roles`

Lists all RBAC roles defined in the tenant.

**Optional filter:** `name_filter`

### `auth0.role_users`

Lists users assigned to a specific role.

**Required filter:** `role_id`

### `auth0.role_permissions`

Lists permissions associated with a specific role.

**Required filter:** `role_id`

### `auth0.organizations`

Lists all organizations configured in the tenant.

### `auth0.organization_members`

Lists members of a specific organization.

**Required filter:** `organization_id`

### `auth0.resource_servers`

Lists all resource servers (APIs) registered in the tenant.

### `auth0.client_grants`

Lists all client grants — the scopes an application has been authorized
to request for a specific API.

**Optional filters:** `client_id`, `audience`

### `auth0.actions`

Lists all actions (serverless pipeline functions) in the tenant.

**Optional filters:** `trigger_id`, `deployed`

### `auth0.logs`

Lists tenant log events including authentication, signup, and API
operations.

**Optional filter:** `q` (Lucene syntax)

> **Note:** User-related fields (e.g. `user_name`, `user_id`) require the
> `read:logs_users` scope in addition to `read:logs`.

### `auth0.grants`

Lists all user grants — authorizations users have given to
applications.

**Optional filters:** `user_id`, `client_id`, `audience`

### `auth0.custom_domains`

Lists all custom domains configured for the tenant.

### `auth0.tenant_settings`

Returns the current tenant settings including branding, session
lifetimes, and feature flags. Always returns a single row.

## Example Queries

```sql
-- List users with their login counts
SELECT user_id, email, name, logins_count, last_login
FROM auth0.users
ORDER BY last_login DESC
LIMIT 20;

-- Search for users by email
SELECT user_id, email, name, identities
FROM auth0.users
WHERE q = 'email:"alice@example.com"'
LIMIT 5;

-- Inventory all applications
SELECT client_id, name, app_type, is_first_party
FROM auth0.clients
LIMIT 20;

-- List identity provider connections
SELECT id, name, strategy
FROM auth0.connections
LIMIT 20;

-- Roles and their assigned users
SELECT r.name AS role_name, ru.email, ru.name AS user_name
FROM auth0.roles r
JOIN auth0.role_users ru ON r.id = ru.role_id
LIMIT 20;

-- Organization membership
SELECT om.email, om.name, o.display_name AS org_name
FROM auth0.organizations o
JOIN auth0.organization_members om ON o.id = om.organization_id
LIMIT 20;

-- APIs with their token lifetimes
SELECT name, identifier, token_lifetime, signing_alg
FROM auth0.resource_servers
LIMIT 10;

-- Client grants: which apps can access which APIs
SELECT cg.client_id, c.name AS app_name, cg.audience, cg.scope
FROM auth0.client_grants cg
JOIN auth0.clients c ON cg.client_id = c.client_id
LIMIT 20;

-- Recent log events (failed logins)
SELECT log_id, date, type, user_name, ip, description
FROM auth0.logs
WHERE q = 'type:"f"'
LIMIT 20;

-- Tenant overview
SELECT friendly_name, support_email, default_directory,
       idle_session_lifetime, session_lifetime
FROM auth0.tenant_settings;
```

## Validation

```bash
coral source lint sources/community/auth0/manifest.yaml
export AUTH0_DOMAIN="https://dev-abc123.us.auth0.com"
export AUTH0_API_TOKEN="<your-management-api-token>"
coral source add --file sources/community/auth0/manifest.yaml
coral source test auth0
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'auth0'"
coral sql "SELECT user_id, email, name FROM auth0.users LIMIT 5"
```

## Provider Documentation

- [Auth0 Management API v2 overview](https://auth0.com/docs/api/management/v2)
- [Management API tokens](https://auth0.com/docs/secure/tokens/access-tokens/management-api-access-tokens)
- [API scopes](https://auth0.com/docs/get-started/apis/scopes/management-api-scopes)
- [Users – search](https://auth0.com/docs/api/management/v2/users/get-users)
- [Clients](https://auth0.com/docs/api/management/v2/clients/get-clients)
- [Connections](https://auth0.com/docs/api/management/v2/connections/get-connections)
- [Roles](https://auth0.com/docs/api/management/v2/roles/get-roles)
- [Role users](https://auth0.com/docs/api/management/v2/roles/get-role-user)
- [Role permissions](https://auth0.com/docs/api/management/v2/roles/get-role-permission)
- [Organizations](https://auth0.com/docs/api/management/v2/organizations/get-organizations)
- [Organization members](https://auth0.com/docs/api/management/v2/organizations/get-organization-members)
- [Resource servers](https://auth0.com/docs/api/management/v2/resource-servers/get-resource-servers)
- [Client grants](https://auth0.com/docs/api/management/v2/client-grants/get-client-grants)
- [Actions](https://auth0.com/docs/api/management/v2/actions/get-actions)
- [Logs](https://auth0.com/docs/api/management/v2/logs/get-logs)
- [Grants](https://auth0.com/docs/api/management/v2/grants/get-grants)
- [Custom domains](https://auth0.com/docs/api/management/v2/custom-domains/get-custom-domains)
- [Tenant settings](https://auth0.com/docs/api/management/v2/tenants/tenant-settings-route)

## Limitations

- **Read-only.** This source does not create, update, or delete Auth0
  resources.
- **Token expiry.** Management API access tokens expire (typically 24
  hours). You must refresh the token manually or via a client credentials
  grant before it expires.
- **Token scopes apply.** Tables only return data for which the access
  token has the required scopes.
- **User pagination limit.** Auth0 limits offset-based user pagination to
  1,000 results. For larger user sets, use the `q` filter to narrow
  results or consider checkpoint pagination (not supported in v1).
- **Log retention.** Auth0 retains logs for a limited period depending on
  your plan (2 days for free, 30 days for enterprise).
- **No Rules, Hooks, or Email Templates tables in v1.** Rules and Hooks
  are deprecated in favor of Actions. Future versions may add email
  templates and branding endpoints.
