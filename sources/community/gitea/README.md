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

Generate a Personal Access Token from:

- User Settings
- Applications
- Generate New Token

Example:

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

The token should include permissions for:

- repository read access
- organization read access
- user read access

The `gitea.users` table requires:
- administrator account privileges
- admin-level user visibility permissions

Without administrator permissions, requests against `/admin/users` may return:

```text
403 Forbidden
```

---

# Tables Overview

| Table | API Endpoint | Notes |
| --- | --- | --- |
| `repositories` | `GET /repos/search` | Repository search inventory |
| `organizations` | `GET /user/orgs` | Organizations associated with the authenticated user |
| `users` | `GET /admin/users` | Requires administrator privileges |

---

# Pagination

This source models Gitea page-based pagination using:

- `page`
- `limit`

Large result sets are automatically paginated through the Gitea REST API.

---

# Table Reference

## `gitea.repositories`

Repositories visible through the Gitea repository search API.

### Supported filters

| Filter | Description |
| --- | --- |
| `query` | Repository search query |

### Example pushdown

```sql
SELECT
  id,
  full_name
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 20;
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| `query` | Utf8 | Repository search query filter |
| `id` | Int64 | Internal repository identifier |
| `name` | Utf8 | Repository name |
| `full_name` | Utf8 | Full repository name (`owner/repo`) |
| `owner_username` | Utf8 | Repository owner username |
| `private` | Boolean | Whether the repository is private |
| `empty` | Boolean | Whether the repository contains commits |
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

| Filter | Description |
| --- | --- |
| `query` | User search query |
| `is_admin` | Restrict results to administrator accounts |

### Example pushdown

```sql
SELECT
  username,
  email
FROM gitea.users
WHERE is_admin = true
LIMIT 20;
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| `query` | Utf8 | User search query filter |
| `is_admin_filter` | Boolean | Administrator filter pushdown |
| `id` | Int64 | Internal user identifier |
| `username` | Utf8 | User login name |
| `full_name` | Utf8 | User display name |
| `email` | Utf8 | User email address |
| `is_admin` | Boolean | Whether the user has administrator privileges |
| `last_login` | Timestamp | Last successful login timestamp |

---

# Example Queries

## Search repositories

```sql
SELECT
  full_name,
  stars_count,
  forks_count
FROM gitea.repositories
WHERE query = 'platform'
LIMIT 20;
```

---

## Organization inventory

```sql
SELECT
  username,
  website
FROM gitea.organizations
ORDER BY username;
```

---

## Audit administrator accounts

```sql
SELECT
  username,
  email,
  last_login
FROM gitea.users
WHERE is_admin = true;
```

---

# Validation

Run formatting and schema mapping evaluations locally before generating your pull request:

```bash
# YAML and style verification
make lint-sources

# Validate schema structure types against Coral DSL engine rules
coral source lint sources/community/gitea/manifest.yaml
```

Execute a live target connection test locally:

```bash
export GITEA_BASE_URL=https://gitea.example.com
export GITEA_API_TOKEN=<token>

coral source add --file sources/community/gitea/manifest.yaml

coral source test gitea
```

---

# Representative Live Output Evidence

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

    +--------+------------------+
    | id     | name             |
    +--------+------------------+
    | 402914 | cloud-automation |
    +--------+------------------+

    1 row
```

---

# Representative Query Output

```text
$ coral sql "SELECT full_name, stars_count FROM gitea.repositories WHERE query = 'infra' LIMIT 5"

+--------------------------------+--------------+
| full_name                      | stars_count  |
+--------------------------------+--------------+
| platform/infra-core            | 182          |
| sre/infra-toolkit              | 94           |
+--------------------------------+--------------+

$ coral sql "SELECT username, is_admin FROM gitea.users WHERE is_admin = true LIMIT 5"

+------------+-----------+
| username   | is_admin  |
+------------+-----------+
| admin      | true      |
| platform   | true      |
+------------+-----------+
```

---

# Limitations

- Read-only source
- No repository creation or deletion support
- No webhook or organization management
- Access depends on Gitea token permissions
- `gitea.users` requires administrator privileges
- Only REST API-visible metadata is modeled

