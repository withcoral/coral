# Outline (Community)

**Version:** 0.1.0
**Backend:** HTTP (Outline RPC-style API Interface)
**Tables:** 3
**Base URL:** `{{input.OUTLINE_URL}}/api`

Query collaborative workspace metadata, document lifecycle states, and user access roles directly through Coral SQL using the Outline API.

This integration is intended for knowledge-management auditing, operational visibility, and workspace governance workflows.

Coral exposes read-only access patterns. Document mutations, collection management, publishing operations, and workspace administration actions are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/outline/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `OUTLINE_URL` | variable | yes | Outline instance base URL with protocol but without trailing slash, for example `https://app.getoutline.com` |
| `OUTLINE_API_TOKEN` | secret | yes | Long-lived personal API token or bot token generated in Outline user settings |

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `collections` | `POST /collections.list` | — | None (fetches collection array directly) |
| `documents` | `POST /documents.list` | — | None (fetches document array directly) |
| `users` | `POST /users.list` | — | None (fetches user array directly) |

---

# Table Reference

## outline.collections

List of permissioned collections and workspace catalog groupings.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Unique collection identifier |
| `name` | Utf8 | Collection display name |
| `description` | Utf8 | Collection summary description |
| `permission` | Utf8 | Default workspace permission model |
| `created_at` | Utf8 | Collection creation timestamp |

---

## outline.documents

Wiki articles and knowledge-base documentation resources.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Unique document identifier |
| `collection_id` | Utf8 | Parent collection identifier |
| `title` | Utf8 | Document title |
| `status` | Utf8 | Publishing state (`draft`, `published`, etc.) |
| `revision` | Int64 | Document revision number |
| `updated_at` | Utf8 | Timestamp of the most recent document update |

---

## outline.users

Workspace membership directory and access metadata.

> Visibility of user records depends on the permissions associated with the provided API token.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Unique internal user identifier |
| `name` | Utf8 | User display name |
| `email` | Utf8 | Primary email address |
| `role` | Utf8 | Workspace role assigned to the user |
| `is_suspended` | Bool | Indicates whether the user account is suspended |

---

# Example Queries

## Audit Stale Workspace Drafts

```sql
SELECT
  title,
  collection_id,
  updated_at
FROM outline.documents
WHERE status = 'draft'
ORDER BY updated_at ASC;
```

---

## Track Suspended User Accounts

```sql
SELECT
  name,
  email,
  role
FROM outline.users
WHERE is_suspended = true
ORDER BY name ASC;
```

---

# Validation

Run formatting and schema validation locally before opening a pull request.

## Lint Sources

```bash
make lint-sources
```

## Validate Coral Source Schema

```bash
coral source lint sources/community/outline/manifest.yaml
```

## Execute Live Connection Test

```bash
export OUTLINE_URL=https://app.getoutline.com
export OUTLINE_API_TOKEN=your_secret_api_token_here

coral source add --file sources/community/outline/manifest.yaml
coral source test outline
```

---

# Representative Live Output

```text
$ coral source test outline

✓ outline connected successfully

  outline (3 tables)
  ├─ collections
  ├─ documents
  └─ users

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT name FROM outline.collections LIMIT 1

+-------------------+
| name              |
+-------------------+
| Engineering Wiki  |
+-------------------+

1 row
```

---

# Limitations

- Read-only retrieval scope
- Does not support document mutations, collection administration, or publishing operations
- Returned data visibility depends entirely on the permissions associated with the provided `OUTLINE_API_TOKEN`
- The Outline API exposes list operations through RPC-style POST endpoints rather than traditional REST collection GET endpoints
- Query filtering is evaluated by Coral SQL after upstream API retrieval
```
