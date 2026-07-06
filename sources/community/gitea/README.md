# Gitea (Community)

**Version:** 0.1.0
**Backend:** HTTP (Gitea REST API v1)
**Tables:** 3
**Base URL:** `{{input.GITEA_BASE_URL}}/api/v1`

Query Gitea repositories, organizations, and user accounts directly through Coral SQL using the Gitea REST API v1.

This integration provides read-only access to the Gitea REST API v1 for repository inventory visibility, organization auditing, administrator account reviews, repository metadata inspection, and operational access audits.

Coral exposes read-only `GET` tables. Write operations (creating repositories, modifying organizations, deleting users, managing hooks) are out of scope for v1.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export GITEA_BASE_URL=https://gitea.example.com
export GITEA_API_TOKEN=your_token_here
coral source add --file sources/community/gitea/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Gitea API access requires a Personal Access Token. Coral authenticates with `Authorization: token <token>`.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `GITEA_BASE_URL` | variable | yes | Root Gitea URL without trailing slash and without `/api/v1`, for example `https://gitea.example.com` |
| `GITEA_API_TOKEN` | secret | yes | Personal Access Token generated from Gitea User Settings → Applications |

Generate a token from **User Settings → Applications → Generate New Token**. Grant it read access for repositories, organizations, and users. Copy it immediately and store it securely — Gitea does not display it again.

The `gitea.users` table calls `/admin/users` and requires **administrator privileges**. Without them, requests to that table return `403 Forbidden`. Returned data across all tables is restricted by the permissions of the supplied token.

Official docs:

- [Gitea API Usage](https://docs.gitea.com/development/api-usage)
- [Gitea Swagger API Reference](https://gitea.example.com/api/swagger)

## Tables

| Table | API Endpoint | Pushdown filters | Notes |
| --- | --- | --- | --- |
| `gitea.repositories` | `GET /repos/search` | `query` | Repository search inventory (rows under `data`) |
| `gitea.organizations` | `GET /user/orgs` | — | Organizations associated with the authenticated user |
| `gitea.users` | `GET /admin/users` | `query`, `sort`, `order`, `visibility`, `is_active`, `is_admin`, `is_restricted`, `is_2fa_enabled`, `is_prohibit_login` | Requires administrator privileges |

All tables use Gitea's page-based pagination with the `page` and `limit` query parameters. Coral injects these automatically (`mode: page`, 1-indexed, `limit` capped at 50, matching Gitea's default `MAX_RESPONSE_ITEMS`). Use a SQL `LIMIT` to bound scans.

### `gitea.repositories`

Repositories visible through the Gitea repository search API.

| Column | Type | Description |
| --- | --- | --- |
| `query` | Utf8 | Repository search query filter (virtual) |
| `id` | Int64 | Internal repository identifier |
| `name` | Utf8 | Repository name |
| `full_name` | Utf8 | Full repository name (`owner/repo`) |
| `owner_username` | Utf8 | Repository owner username |
| `private` | Boolean | Whether the repository is private |
| `empty` | Boolean | Whether the repository has no commits/content |
| `clone_url` | Utf8 | HTTP clone URL |
| `ssh_url` | Utf8 | SSH clone URL |
| `stars_count` | Int64 | Repository star count |
| `forks_count` | Int64 | Repository fork count |
| `created_at` | Timestamp | Repository creation timestamp |

#### Pushdown filters

| SQL filter | Gitea param | Description |
| --- | --- | --- |
| `query` | `q` | Repository search query |

```sql
SELECT id, full_name
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 20;
```

### `gitea.organizations`

Organizations associated with the authenticated user.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal organization identifier |
| `username` | Utf8 | Organization username |
| `full_name` | Utf8 | Organization display name |
| `description` | Utf8 | Organization description |
| `location` | Utf8 | Organization location |
| `website` | Utf8 | Organization website URL |

### `gitea.users`

User accounts registered on the Gitea instance. **Requires administrator privileges.**

| Column | Type | Description |
| --- | --- | --- |
| `query` | Utf8 | Search query filter (virtual) |
| `sort` | Utf8 | Sort attribute filter (virtual) |
| `order` | Utf8 | Sort order filter (virtual) |
| `is_2fa_enabled` | Boolean | 2FA filter (virtual) |
| `id` | Int64 | Internal user identifier |
| `username` | Utf8 | User login name |
| `full_name` | Utf8 | User display name |
| `email` | Utf8 | User email address |
| `visibility` | Utf8 | Account visibility (also a pushdown filter) |
| `is_admin` | Boolean | Whether the user has administrator privileges (also a pushdown filter) |
| `is_active` | Boolean | Whether the account is active (also a pushdown filter) |
| `is_restricted` | Boolean | Whether the account is restricted (also a pushdown filter) |
| `is_prohibit_login` | Boolean | Whether login is prohibited (also a pushdown filter) |
| `last_login` | Timestamp | Last successful login timestamp |
| `created_at` | Timestamp | Account creation timestamp |

#### Pushdown filters

All map to Gitea's `adminSearchUsers` query parameters and are pushed to the server, so audits don't require scanning every user locally.

| SQL filter | Gitea param | Description |
| --- | --- | --- |
| `query` | `q` | Search term (username, full name, or email) |
| `sort` | `sort` | Sort attribute: `name`, `created`, `updated`, `id` (default `name`) |
| `order` | `order` | Sort order: `asc` or `desc` |
| `visibility` | `visibility` | `public`, `limited`, or `private` |
| `is_active` | `is_active` | Restrict to active accounts |
| `is_admin` | `is_admin` | Restrict to administrator accounts |
| `is_restricted` | `is_restricted` | Restrict to restricted accounts |
| `is_2fa_enabled` | `is_2fa_enabled` | Restrict to accounts with 2FA enabled |
| `is_prohibit_login` | `is_prohibit_login` | Restrict to login-prohibited accounts |

```sql
SELECT username, email, last_login
FROM gitea.users
WHERE is_admin = true
  AND is_active = true
ORDER BY last_login DESC
LIMIT 20;
```

## Example queries

### Search repositories

```sql
SELECT
  full_name,
  stars_count,
  forks_count
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 20;
```

### Organization inventory

```sql
SELECT
  username,
  website
FROM gitea.organizations
ORDER BY username;
```

### Audit administrator accounts

```sql
SELECT
  username,
  email,
  last_login
FROM gitea.users
WHERE is_admin = true;
```

### Audit inactive or login-prohibited accounts

```sql
SELECT
  username,
  is_active,
  is_prohibit_login,
  last_login
FROM gitea.users
WHERE is_prohibit_login = true
ORDER BY last_login;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/gitea/manifest.yaml
Coral manifest schema validation: passed for sources/community/gitea/manifest.yaml
make lint-sources: passed
Live API tests: passed with a Gitea admin token
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/gitea/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export GITEA_BASE_URL=https://gitea.example.com
export GITEA_API_TOKEN=your_token_here
coral source add --file sources/community/gitea/manifest.yaml
coral source test gitea
```

Validate table access with representative SQL:

```bash
coral sql "SELECT id, name FROM gitea.repositories LIMIT 5"
coral sql "SELECT full_name, stars_count FROM gitea.repositories WHERE query = 'platform' LIMIT 5"
coral sql "SELECT username, website FROM gitea.organizations LIMIT 5"
coral sql "SELECT username, is_admin FROM gitea.users WHERE is_admin = true LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'gitea'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'gitea' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ gitea connected successfully

gitea (3 tables)
├─ repositories
├─ organizations
└─ users

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT id, name FROM gitea.repositories LIMIT 1
  1 row
```

Representative query:

```sql
SELECT full_name, owner_username, private, stars_count, forks_count
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 3;
```

Example output:

```text
full_name              | owner_username | private | stars_count | forks_count
platform/api-gateway   | platform       | false   | 34          | 8
platform/auth-service  | platform       | true    | 12          | 3
platform/docs-site     | platform       | false   | 5           | 1
```

## Limitations

- Read-only source; no repository creation or deletion, and no webhook or organization management.
- `gitea.users` requires administrator privileges; without them the `/admin/users` endpoint returns `403 Forbidden`.
- Access depends on the permissions of the supplied Gitea token.
- All tables use page-based pagination (`limit` capped at 50); use a SQL `LIMIT` to bound scans.
- Only REST API-visible metadata is modeled.
