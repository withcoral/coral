# Keycloak (Community)

**Version:** 0.1.0
**Backend:** HTTP (Keycloak Admin REST API)
**Tables:** 6
**Base URL:** `{{input.KEYCLOAK_BASE_URL}}`

Query Keycloak IAM inventory and admin audit data through Coral SQL using the
[Keycloak Admin REST API](https://www.keycloak.org/docs-api/latest/rest-api/index.html).
Use this source for realm discovery, user and group inventory, OAuth client
audits, realm role review, and admin event forensics across self-hosted
Keycloak or managed deployments that expose the standard `/admin` endpoints.

Coral exposes read-only `GET` tables. Write operations (create, update, delete)
are out of scope for v1.

## Install

Community sources are not bundled with the Coral binary. From the Coral repo
root (or with a copied manifest):

```bash
coral source add --file sources/community/keycloak/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Set credentials via environment variables (recommended) or
`coral source add --file ... --interactive`.

## Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `KEYCLOAK_BASE_URL` | variable | yes | Server root URL with **no** trailing slash (for example `http://localhost:8080` or `https://auth.example.com`). Coral calls `{base}/admin/realms/...`. |
| `KEYCLOAK_ACCESS_TOKEN` | secret | yes | Bearer access token for the Admin API. Short-lived; refresh or re-issue when queries return `401`. |

## Setup

### 1. Run Keycloak locally (optional)

Quick local instance for development:

```bash
docker run -p 8080:8080 \
  -e KEYCLOAK_ADMIN=admin \
  -e KEYCLOAK_ADMIN_PASSWORD=admin \
  quay.io/keycloak/keycloak:latest \
  start-dev
```

Default admin console: `http://localhost:8080` (user `admin` / password `admin`).

### 2. Obtain an access token

Coral sends `Authorization: Bearer <token>`. Tokens are obtained from your
realm’s OpenID Connect token endpoint (commonly `master` for admin tasks).

**Password grant (`admin-cli`, local dev):**

```bash
export KEYCLOAK_BASE_URL=http://localhost:8080

export KEYCLOAK_ACCESS_TOKEN=$(
  curl -s -X POST \
    "$KEYCLOAK_BASE_URL/realms/master/protocol/openid-connect/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=admin-cli" \
    -d "username=admin" \
    -d "password=admin" \
    -d "grant_type=password" \
  | jq -r .access_token
)

test -n "$KEYCLOAK_ACCESS_TOKEN" && echo "Token acquired"
```

**Client credentials (service account, production-friendly):**

Keycloak has two common setups. Roles are **not** assigned on the client
itself — they are assigned to the client's **service account user** under
**Clients → {your client} → Service account roles → Assign role**.

| Setup | Where to create the client | Where to assign roles | Token endpoint |
| --- | --- | --- | --- |
| **Single-realm** | Target realm (for example `my-app`) | That realm's built-in **`realm-management`** client | `/realms/my-app/protocol/openid-connect/token` |
| **Cross-realm** | **`master`** realm | **`master-realm`** for master data, plus each target realm's **`realm-management`** client for that realm's inventory | `/realms/master/protocol/openid-connect/token` |

**Single-realm (simplest first success):**

1. In the target realm, create a **confidential** client and enable **Service
   accounts**.
2. Open **Service account roles → Assign role**, filter by clients, choose
   **`realm-management`** (within a realm) or **`{realm}-realm`** (when assigning
   from a master-realm client to a target realm), and assign the read roles you
   need:

   Keycloak has no `view-groups` role. Use `query-groups` to list/search groups;
   add `view-users` if you need full group representations from the API.

   | Role | Tables |
   | --- | --- |
   | `query-realms` / `view-realm` | `realms` (lists realms the token can view) |
   | `query-users` / `view-users` | `users` |
   | `query-groups` (add `view-users` to read group details) | `groups` |
   | `query-clients` / `view-clients` | `clients` |
   | `view-realm` | `roles` |
   | `view-events` | `admin_events` |

3. Request a token from the **same realm** that hosts the client:

```bash
export KEYCLOAK_BASE_URL=https://auth.example.com
export KEYCLOAK_REALM=my-app              # realm that hosts the client
export KEYCLOAK_CLIENT_ID=my-admin-client
export KEYCLOAK_CLIENT_SECRET=<secret>

export KEYCLOAK_ACCESS_TOKEN=$(
  curl -s -X POST \
    "$KEYCLOAK_BASE_URL/realms/$KEYCLOAK_REALM/protocol/openid-connect/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials" \
    -d "client_id=$KEYCLOAK_CLIENT_ID" \
    -d "client_secret=$KEYCLOAK_CLIENT_SECRET" \
  | jq -r .access_token
)
```

**Cross-realm (master client):**

1. Create the confidential client in the **`master`** realm with **Service
   accounts** enabled.
2. Under **Service account roles**, assign:
   - From client **`master-realm`**: `view-realm` and `query-realms` so
     `GET /admin/realms` (the `realms` table) returns realms you can access.
   - From each target realm's **`realm-management`** client (for example
     `my-app-realm` in the role picker): the same `view-*` / `query-*` roles
     listed above for that realm's tables.
3. Request the token from **`master`** (`KEYCLOAK_REALM=master`).

`keycloak.realms` calls `GET /admin/realms`, which returns **only realms the
token can view**. A single-realm service account typically sees just that
realm; a cross-realm master client needs `query-realms` / `view-realm` on
`master-realm` plus per-realm `realm-management` roles for each target realm.

Grant only the roles required for read-only inventory and audit use cases.

### 3. Enable admin events (for `admin_events`)

If `keycloak.admin_events` returns no rows:

1. Open the target realm in the Admin Console.
2. **Realm settings → Events**.
3. Enable **Save events** and **Admin events** (and set retention as needed).

### 4. Add and verify the source

```bash
export KEYCLOAK_BASE_URL=http://localhost:8080   # or your deployment URL
export KEYCLOAK_ACCESS_TOKEN=<token>

coral source add --file sources/community/keycloak/manifest.yaml
coral source test keycloak
```

`coral source test keycloak` runs `SELECT realm, enabled FROM keycloak.realms
LIMIT 1`, which checks base URL, token, and Admin API reachability.

## Tables overview

| Table | API endpoint | Required filter | Pagination |
| --- | --- | --- | --- |
| `realms` | `GET /admin/realms` | — | Full list per request |
| `clients` | `GET /admin/realms/{realm}/clients` | `realm` | `first` / `max` |
| `groups` | `GET /admin/realms/{realm}/groups` | `realm` | `first` / `max` |
| `roles` | `GET /admin/realms/{realm}/roles` | `realm` | `first` / `max` |
| `users` | `GET /admin/realms/{realm}/users` | `realm` | `first` / `max` |
| `admin_events` | `GET /admin/realms/{realm}/admin-events` | `realm` | `first` / `max` |

Realm-scoped tables require a SQL filter, for example `WHERE realm = 'master'`.
Discover realm names from `keycloak.realms`.

## Filters and API mapping

Coral maps declared SQL filters to Keycloak query parameters. Only filters
listed below are pushed to the API; other `WHERE` clauses are applied after
fetch (or may not be supported).

| SQL filter | Keycloak query param | Tables |
| --- | --- | --- |
| `realm` | path `{realm}` | `clients`, `groups`, `roles`, `users`, `admin_events` |
| `search` | `search` | `roles`, `users` |
| `q` | `q` (custom-attribute `key:value` syntax) | `users` |
| `username` | `username` | `users` |
| `email` | `email` | `users` |
| `enabled` | `enabled` (boolean) | `users` |
| `date_from` | `dateFrom` | `admin_events` |
| `date_to` | `dateTo` | `admin_events` |
| `operation_types` | `operationTypes` | `admin_events` |
| `resource_path` | `resourcePath` | `admin_events` |
| `auth_client` | `authClient` | `admin_events` |
| `auth_user` | `authUser` | `admin_events` |
| `auth_ip_address` | `authIpAddress` | `admin_events` |

For `date_from` and `date_to` on `admin_events`, Coral forwards the filter
values directly to Keycloak's `dateFrom` / `dateTo` query parameters. Use
**`yyyy-MM-dd`** (for example `2026-05-01`) or **epoch milliseconds** (for
example `1746057600000`). Full ISO-8601 timestamps such as
`2026-05-01T00:00:00Z` are **not** accepted and can return `400 Bad Request`.

On `users`, use `email` or `search` for email/name lookups. The `q` filter maps
to Keycloak's custom-attribute query syntax (`key:value`, for example
`department:engineering` or `team:support status:active`).

`keycloak.groups` does not expose `search` or `q` filters in v1. Keycloak can
return nested matches under `subGroups` when those parameters are used, but this
source only emits the top-level response array as flat rows.

Always use `LIMIT` on large realms. Paginated tables use `max` (default 100,
cap 100 per request); Coral follows pages with `first` until the SQL `LIMIT`
is satisfied or the API returns fewer than `max` rows.

## Table reference

### `keycloak.realms`

All realms on the server. Entry point for discovering realm names.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Realm ID (often matches realm name) |
| `realm` | Utf8 | Realm name (URL segment) |
| `display_name` | Utf8 | Human-readable name |
| `enabled` | Boolean | Whether the realm is enabled |
| `ssl_required` | Utf8 | SSL policy (`external`, `all`, `none`, etc.) |

**Filters:** none

### `keycloak.users`

Users in a realm (minimal profile columns for inventory and joins).

| Column | Type | Description |
| --- | --- | --- |
| `realm` | Utf8 | Realm from the `realm` filter (echoed on each row) |
| `id` | Utf8 | User ID |
| `username` | Utf8 | Username |
| `email` | Utf8 | Email address |
| `first_name` | Utf8 | First name |
| `last_name` | Utf8 | Last name |
| `enabled` | Boolean | Account enabled |
| `email_verified` | Boolean | Email verified flag |
| `created_at` | Timestamp | Account creation time (Keycloak epoch milliseconds) |

**Required filter:** `realm`

**Optional filters:** `search`, `username`, `email`, `enabled`, `q` (custom
attributes as `key:value`)

### `keycloak.groups`

Top-level groups in a realm. Keycloak's `GET /admin/realms/{realm}/groups`
returns a hierarchy; Coral emits **only the top-level array** as flat rows with
**`id` and `name`**. Nested `subGroups` (including matches from Keycloak's
`search` / `q` parameters) are not flattened in v1.

| Column | Type | Description |
| --- | --- | --- |
| `realm` | Utf8 | Realm from the `realm` filter |
| `id` | Utf8 | Group ID |
| `name` | Utf8 | Group name |

**Required filter:** `realm`

**Optional filters:** none

### `keycloak.clients`

OAuth/OIDC (and SAML) clients in a realm.

| Column | Type | Description |
| --- | --- | --- |
| `realm` | Utf8 | Realm from the `realm` filter |
| `id` | Utf8 | Internal client UUID |
| `client_id` | Utf8 | Client identifier (`clientId`) |
| `name` | Utf8 | Display name |
| `description` | Utf8 | Client description |
| `enabled` | Boolean | Client enabled |
| `protocol` | Utf8 | Protocol (`openid-connect`, `saml`, etc.) |
| `public_client` | Boolean | Public vs confidential client |

**Required filter:** `realm`

### `keycloak.roles`

Realm-level roles (not per-client roles or user role mappings).

| Column | Type | Description |
| --- | --- | --- |
| `realm` | Utf8 | Realm from the `realm` filter |
| `id` | Utf8 | Role ID |
| `name` | Utf8 | Role name |
| `description` | Utf8 | Role description |
| `composite` | Boolean | Composite role flag |
| `client_role` | Boolean | Client role flag (`false` for realm roles) |

**Required filter:** `realm`

**Optional filter:** `search`

### `keycloak.admin_events`

Admin audit events for a realm.

| Column | Type | Description |
| --- | --- | --- |
| `realm` | Utf8 | Realm from the `realm` filter |
| `time` | Timestamp | Event time (epoch milliseconds) |
| `realm_id` | Utf8 | Realm ID on the event payload |
| `operation_type` | Utf8 | `CREATE`, `UPDATE`, `DELETE`, `ACTION`, etc. |
| `resource_type` | Utf8 | Affected resource type |
| `resource_path` | Utf8 | Affected resource path |
| `error` | Utf8 | Error message when the operation failed |
| `representation` | Utf8 | Serialized resource JSON when present (Keycloak returns a string; parse in SQL if needed) |

**Required filter:** `realm`

**Optional filters:** `date_from`, `date_to`, `operation_types`, `resource_path`,
`auth_client`, `auth_user`, `auth_ip_address`

## Example queries

### Realms and connectivity

```sql
SELECT realm, display_name, enabled, ssl_required
FROM keycloak.realms
ORDER BY realm;
```

```sql
SELECT realm, enabled
FROM keycloak.realms;
```

### Users

```sql
SELECT username, email, enabled, created_at
FROM keycloak.users
WHERE realm = 'master'
LIMIT 10;
```

```sql
SELECT id, username, email, first_name, last_name, enabled, email_verified
FROM keycloak.users
WHERE realm = 'my-app'
  AND enabled = true
ORDER BY username
LIMIT 100;
```

```sql
SELECT id, username, email
FROM keycloak.users
WHERE realm = 'my-app'
  AND email = 'alice@example.com'
LIMIT 10;
```

```sql
SELECT id, username, email
FROM keycloak.users
WHERE realm = 'my-app'
  AND q = 'department:engineering'
LIMIT 10;
```

### Groups, clients, and roles

```sql
SELECT id, name
FROM keycloak.groups
WHERE realm = 'my-app'
ORDER BY name
LIMIT 50;
```

```sql
SELECT client_id, id, name, enabled, protocol, public_client
FROM keycloak.clients
WHERE realm = 'my-app'
  AND enabled = true
LIMIT 50;
```

```sql
SELECT name, description, composite, client_role
FROM keycloak.roles
WHERE realm = 'my-app'
ORDER BY name;
```

### Admin events

```sql
SELECT time, operation_type, resource_type, resource_path, error
FROM keycloak.admin_events
WHERE realm = 'master'
ORDER BY time DESC
LIMIT 20;
```

```sql
SELECT operation_type, resource_path, representation
FROM keycloak.admin_events
WHERE realm = 'my-app'
  AND date_from = '2026-05-01'
  AND date_to = '2026-05-20'
ORDER BY time DESC
LIMIT 100;
```

Epoch-millisecond bounds are also valid:

```sql
SELECT operation_type, resource_path
FROM keycloak.admin_events
WHERE realm = 'my-app'
  AND date_from = '1746057600000'
  AND date_to = '1747785599000'
ORDER BY time DESC
LIMIT 100;
```

## Validation

Run before opening a PR or after changing the manifest:

```bash
# YAML style (repo root)
make lint-sources

# Manifest schema and table definitions
coral source lint sources/community/keycloak/manifest.yaml
```

Live smoke test against a running Keycloak (tested with
`quay.io/keycloak/keycloak:latest`, dev mode):

```bash
export KEYCLOAK_BASE_URL=http://localhost:8080
export KEYCLOAK_ACCESS_TOKEN=<token>

coral source add --file sources/community/keycloak/manifest.yaml
coral source test keycloak

coral sql "SELECT realm, enabled FROM keycloak.realms LIMIT 5"
coral sql "SELECT username, email FROM keycloak.users WHERE realm = 'master' LIMIT 5"
coral sql "SELECT operation_type, resource_path FROM keycloak.admin_events WHERE realm = 'master' ORDER BY time DESC LIMIT 5"

coral source info keycloak --verbose
coral sql "SELECT table_name, required_filters FROM coral.tables WHERE schema_name = 'keycloak'"
```

Sanitized output from a local run (password grant against `admin-cli` in
`master`; admin events empty until **Realm settings → Events** is enabled):

```text
$ coral source test keycloak

  ✓ keycloak connected successfully

    keycloak (6 tables)
    ├─ admin_events
    ├─ clients
    ├─ groups
    ├─ realms
    ├─ roles
    └─ users
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT realm, enabled FROM keycloak.realms LIMIT 1
      1 row

$ coral sql "SELECT realm, enabled FROM keycloak.realms LIMIT 5"
+--------+---------+
| realm  | enabled |
+--------+---------+
| master | true    |
+--------+---------+

$ coral sql "SELECT username, email FROM keycloak.users WHERE realm = 'master' LIMIT 5"
+----------+-------+
| username | email |
+----------+-------+
| admin    |       |
+----------+-------+

$ coral sql "SELECT operation_type, resource_path FROM keycloak.admin_events WHERE realm = 'master' ORDER BY time DESC LIMIT 5"
++
++

$ coral sql "SELECT table_name, required_filters FROM coral.tables WHERE schema_name = 'keycloak'"
+--------------+------------------+
| table_name   | required_filters |
+--------------+------------------+
| admin_events | realm            |
| clients      | realm            |
| groups       | realm            |
| realms       |                  |
| roles        | realm            |
| users        | realm            |
+--------------+------------------+
```

## Agent and SQL workflow tips

1. Start with `keycloak.realms` to list realm names.
2. Pass `WHERE realm = '<name>'` on every realm-scoped table.
3. Prefer API filters (`username`, `email`, `search`, custom-attribute `q`, date
   range on `admin_events`) over fetching large pages and filtering in SQL.
4. Use `LIMIT` on every inventory query in production realms.
5. Inspect `coral.columns` for exact column names:
   `SELECT column_name, data_type FROM coral.columns WHERE schema_name = 'keycloak' AND table_name = 'users'`.

## Limitations

- **Read-only.** No create, update, or delete via this source.
- **Realm roles only.** `roles` lists realm roles; client roles and user/client
  role mappings are not exposed in v1.
- **Groups top-level only.** `groups` exposes top-level `id` and `name` from
  the Admin REST list endpoint. Nested `subGroups`, membership, hierarchy
  fields (`path`, `parentId`), and server-side `search` / `q` group filters are
  not modeled in v1.
- **Admin events require configuration.** The realm must record admin events;
  otherwise the table is legitimately empty.
- **Token scope and TTL.** Access is limited to the bearer token’s roles and
  lifetime; re-issue tokens when you see `401` responses.
- **Pagination.** Keycloak uses `first` and `max` (max 100 per request). Very
  large result sets need tight `LIMIT` and server-side filters.
- **`realms` is unpaginated.** Coral fetches the full realm list once per query;
  use SQL `LIMIT` for display only.
