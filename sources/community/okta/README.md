# Okta Community Source

Query Okta users, groups, applications, app assignments, and System Log events
through Coral SQL using the Okta Management APIs.

## Setup

### 1. Create an Okta API token

Create an API token in the Okta Admin Console:

1. Open **Security > API > Tokens**
2. Create a new token
3. Copy the token value

The token inherits the permissions of the admin user who creates it. Use a
read-only admin role where possible.

### 2. Add the source

```bash
export OKTA_ORG_URL="https://dev-123456.okta.com"
export OKTA_API_TOKEN="<your-token>"
coral source add --file sources/community/okta/manifest.yaml
```

Do not include a trailing slash in `OKTA_ORG_URL`.

### 3. Verify

```bash
coral source test okta
```

The built-in test query reads `okta.current_user`, which verifies that the
organization URL and API token are usable.

## Tables

### `okta.current_user`

Returns the Okta user associated with the API token.

### `okta.users`

Lists users visible to the API token.

**Optional filters:** `q`, `search`, `filter`

### `okta.groups`

Lists groups visible to the API token.

**Optional filters:** `q`, `search`, `filter`

### `okta.apps`

Lists applications visible to the API token.

**Optional filters:** `q`, `filter`

### `okta.app_users`

Lists users assigned to an Okta application.

**Required filter:** `app_id`
**Optional filter:** `q`

### `okta.system_log`

Lists recent Okta System Log events.

**Optional filters:** `since`, `until`, `filter`, `q`

Use ISO 8601 timestamps for `since` and `until`, for example
`2026-05-01T00:00:00Z`.

## Example Queries

```sql
-- Verify the token identity
SELECT id, login, email, status
FROM okta.current_user;

-- List recently active users
SELECT login, email, status, last_login_at
FROM okta.users
ORDER BY last_login_at DESC
LIMIT 20;

-- Search for users
SELECT id, login, email, status
FROM okta.users
WHERE q = 'alice'
LIMIT 10;

-- Inventory groups
SELECT name, type, last_membership_updated_at
FROM okta.groups
ORDER BY name;

-- Inventory applications
SELECT label, name, status, sign_on_mode
FROM okta.apps
ORDER BY label;

-- List assigned users for an application
SELECT id, status, external_id, profile
FROM okta.app_users
WHERE app_id = '0oa123example'
LIMIT 50;

-- Review recent sign-in and admin activity
SELECT uuid, published_at, event_type, actor__alternate_id, outcome__result
FROM okta.system_log
ORDER BY published_at DESC
LIMIT 20;
```

## Validation

```bash
coral source lint sources/community/okta/manifest.yaml
export OKTA_ORG_URL="https://dev-123456.okta.com"
export OKTA_API_TOKEN="<your-token>"
coral source add --file sources/community/okta/manifest.yaml
coral source test okta
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'okta'"
coral sql "SELECT id, login, email, status FROM okta.current_user"
```

## Limitations

- **Read-only.** This source does not create, update, activate, suspend, or
  delete Okta resources.
- **Token permissions apply.** Tables only return objects visible to the admin
  user who created the API token.
- **System Log pagination is capped.** Okta System Log feeds can paginate
  continuously, so v1 fetches one page per query. Use `since`, `until`, and
  `LIMIT` to keep queries bounded.
- **No policy, factor, hook, or OAuth client detail tables in v1.** The first
  version focuses on identity inventory, app assignments, and audit events.
