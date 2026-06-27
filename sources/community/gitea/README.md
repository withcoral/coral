# Gitea (Community)

**Version:** 0.1.0
**Backend:** HTTP (Gitea REST API v1)
**Tables:** 3
**Base URL:** `{{input.GITEA_BASE_URL}}/api/v1`

Query Gitea repositories, organizations, and user accounts directly through Coral SQL using the Gitea REST API v1.

Use this source for:
- repository inventory visibility
- organization auditing
- administrator account reviews
- repository metadata inspection
- operational access audits

Coral exposes read-only `GET` tables. Write operations (creating repositories, modifying organizations, deleting users, managing hooks) are out of scope for v1.

---

# Install

Community sources are not bundled with the Coral binary.

```bash
coral source add --file sources/community/gitea/manifest.yaml
```

You may also copy `manifest.yaml` locally and reference it directly.

---

# Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `GITEA_BASE_URL` | variable | yes | Root Gitea URL without trailing slash and without `/api/v1` |
| `GITEA_API_TOKEN` | secret | yes | Personal Access Token generated from Gitea User Settings → Applications |

---

# Authentication

Generate a Personal Access Token from **User Settings → Applications → Generate New Token**.

```bash
export GITEA_BASE_URL=https://gitea.example.com
export GITEA_API_TOKEN=<token>
```

Coral authenticates using:

```text
Authorization: token <token>
```

---

# Required Permissions

The token should include read access for repositories, organizations, and users.

The `gitea.users` table calls `/admin/users` and requires **administrator privileges**. Without them, requests may return:

```text
403 Forbidden
```

---

# Tables Overview

| Table | API Endpoint | Notes |
| --- | --- | --- |
| `repositories` | `GET /repos/search` | Repository search inventory (rows under `data`) |
| `organizations` | `GET /user/orgs` | Organizations associated with the authenticated user |
| `users` | `GET /admin/users` | Requires administrator privileges |

---

# Pagination

All tables use Gitea's page-based pagination with the `page` and `limit` query parameters. Coral injects these automatically (`mode: page`, 1-indexed, `limit` capped at 50, matching Gitea's default `MAX_RESPONSE_ITEMS`). Use a SQL `LIMIT` to bound scans.

---

# Table Reference

## `gitea.repositories`

Repositories visible through the Gitea repository search API.

### Supported filters

| Filter | Pushdown | Description |
| --- | --- | --- |
| `query` | `q` | Repository search query |

### Example pushdown

```sql
SELECT id, full_name
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 20;
```

### Columns

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

---

## `gitea.organizations`

Organizations associated with the authenticated user.

### Columns

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal organization identifier |
| `username` | Utf8 | Organization username |
| `full_name` | Utf8 | Organization display name |
| `description` | Utf8 | Organization description |
| `location` | Utf8 | Organization location |
| `website` | Utf8 | Organization website URL |

---

## `gitea.users`

User accounts registered on the Gitea instance.

> Requires administrator privileges.

### Supported filters

All map to Gitea's `adminSearchUsers` query parameters and are pushed to the server, so audits don't require scanning every user locally.

| Filter | Pushdown param | Description |
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

### Example pushdown

```sql
SELECT username, email, last_login
FROM gitea.users
WHERE is_admin = true
  AND is_active = true
ORDER BY last_login DESC
LIMIT 20;
```

### Columns

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

---

# Example Queries

## Search repositories

```sql
SELECT full_name, stars_count, forks_count
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 20;
```

## Organization inventory

```sql
SELECT username, website
FROM gitea.organizations
ORDER BY username;
```

## Audit administrator accounts

```sql
SELECT username, email, last_login
FROM gitea.users
WHERE is_admin = true;
```

## Audit inactive or login-prohibited accounts

```sql
SELECT username, is_active, is_prohibit_login, last_login
FROM gitea.users
WHERE is_prohibit_login = true
ORDER BY last_login;
```

---

# Validation

Run formatting and schema validation locally before opening a pull request:

```bash
make lint-sources
coral source lint sources/community/gitea/manifest.yaml
```

Execute a live connection test:

```bash
export GITEA_BASE_URL=https://gitea.example.com
export GITEA_API_TOKEN=<token>

coral source add --file sources/community/gitea/manifest.yaml
coral source test gitea
coral sql "SELECT username, is_admin FROM gitea.users WHERE is_admin = true LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test gitea`
> run against this manifest. Do not ship placeholder output.

```text
$ coral source test gitea

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

---

# Limitations

- Read-only source
- No repository creation or deletion support
- No webhook or organization management
- Access depends on Gitea token permissions
- `gitea.users` requires administrator privileges
- Only REST API-visible metadata is modeled
