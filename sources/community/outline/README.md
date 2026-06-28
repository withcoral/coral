# Outline (Community)

**Version:** 0.1.0
**Backend:** HTTP (Outline API)
**Tables:** 3
**Base URL:** `{{input.OUTLINE_URL}}/api`

Query Outline collections, documents, and user directories directly through Coral SQL.

This source provides visibility into workspace organization, document lifecycle metadata, and user access information for operational auditing and knowledge-base inventory.

Coral exposes read-only access to Outline resources. Creating, updating, publishing, archiving, or deleting content is out of scope.

---

## Install

Community sources are not bundled with the Coral binary.

```bash
coral source add --file sources/community/outline/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and reference it directly:

```bash
coral source add --file <path-to-manifest>
```

---

## Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `OUTLINE_URL` | variable | yes | Outline instance URL with protocol and without a trailing slash (for example, `https://app.getoutline.com`) |
| `OUTLINE_API_TOKEN` | secret | yes | Personal API token or bot token generated in Outline user settings |

Coral sends the token as `Authorization: Bearer <token>`.

---

## Tables Overview

| Table | Endpoint | Pagination |
| --- | --- | --- |
| `collections` | `POST /collections.list` | Offset (`limit` / `offset`) |
| `documents` | `POST /documents.list` | Offset (`limit` / `offset`) |
| `users` | `POST /users.list` | Offset (`limit` / `offset`) |

Outline's list methods are RPC-style POST endpoints that paginate with `limit` and `offset`. Coral advances `offset` automatically (page size `limit=100`, Outline's maximum) and stops when a short page is returned, so full result sets are fetched page by page rather than just the first page.

---

## Table Reference

### outline.collections

Collections used to organize documents within a workspace.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Collection identifier |
| `name` | Utf8 | Collection name |
| `description` | Utf8 | Collection description |
| `permission` | Utf8 | Collection permission model |
| `created_at` | Timestamp | Collection creation timestamp |

### outline.documents

Documents and knowledge-base content stored within Outline.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Document identifier |
| `collection_id` | Utf8 | Parent collection identifier (null for documents not in a collection) |
| `title` | Utf8 | Document title |
| `revision` | Int64 | Revision number |
| `updated_at` | Timestamp | Last modification timestamp |
| `published_at` | Timestamp | Publication timestamp |
| `archived_at` | Timestamp | Archive timestamp |
| `deleted_at` | Timestamp | Soft-deletion timestamp |

### outline.users

Workspace users visible to the authenticated API token.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | User identifier |
| `name` | Utf8 | Display name |
| `email` | Utf8 | Email address |
| `role` | Utf8 | Workspace role |
| `is_suspended` | Boolean | Whether the user account is suspended |

---

## Example Queries

### Find Unpublished Documents

```sql
SELECT title, collection_id, updated_at
FROM outline.documents
WHERE published_at IS NULL
  AND archived_at IS NULL
ORDER BY updated_at ASC;
```

### Audit Suspended Users

```sql
SELECT name, email, role
FROM outline.users
WHERE is_suspended = true
ORDER BY name ASC;
```

---

## Validation

```bash
make lint-sources
coral source lint sources/community/outline/manifest.yaml
```

```bash
export OUTLINE_URL=https://app.getoutline.com
export OUTLINE_API_TOKEN=<token>

coral source add --file sources/community/outline/manifest.yaml
coral source test outline
coral sql "SELECT title, published_at FROM outline.documents WHERE archived_at IS NULL LIMIT 5"
```

---

## Live Output

> Replace the block below with the actual output from your own `coral source test outline`
> run against this manifest. Do not ship placeholder output.

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
    1 row
```

---

## Limitations

- Read-only source; workspace and collection mutations are not supported.
- Returned data depends on the permissions granted to `OUTLINE_API_TOKEN`.
- Outline list operations are RPC-style POST endpoints.
- Pagination uses Outline's `limit`/`offset` parameters (`limit` capped at 100); Coral advances `offset` until a short page is returned.
- `documents.collection_id` may be null for documents not associated with a collection.
