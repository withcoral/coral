# Outline (Community)

**Version:** 0.1.0
**Backend:** HTTP (Outline API)
**Tables:** 3
**Base URL:** `{{input.OUTLINE_URL}}/api`

Query Outline collections, documents, and user directories through Coral SQL. Read-only access for knowledge-base inventory and user auditing.

## Install

```bash
export OUTLINE_URL=https://app.getoutline.com
export OUTLINE_API_TOKEN=your_api_token_here
coral source add --file sources/community/outline/manifest.yaml
```

## Authentication

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `OUTLINE_URL` | variable | yes | Outline instance URL, with protocol and no trailing slash (e.g. `https://app.getoutline.com`) |
| `OUTLINE_API_TOKEN` | secret | yes | An Outline API key or OAuth 2.0 access token with read access |

Outline accepts two kinds of bearer credential:

- **API key** — created under **Settings → API Keys**. Best for scripts and local use.
- **OAuth 2.0 access token** — issued to an OAuth app acting on a user's behalf.

Both are sent as `Authorization: Bearer <token>`. They can be scope-limited (a space-separated list of endpoints like `documents.list`, or wildcards like `documents.*`); a key with no scope has the same access as the user who created it. For this source, `collections.list documents.list users.list` is enough.

## Tables

| Table | Endpoint | Pagination |
| --- | --- | --- |
| `outline.collections` | `POST /collections.list` | Offset (`limit` / `offset`) |
| `outline.documents` | `POST /documents.list` | Offset (`limit` / `offset`) |
| `outline.users` | `POST /users.list` | Offset (`limit` / `offset`) |

Coral advances `offset` automatically (page size `limit=100`, Outline's max) until a short page is returned.

### `outline.collections`

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Collection identifier |
| `name` | Utf8 | Collection name |
| `description` | Utf8 | Collection description |
| `permission` | Utf8 | Collection permission model |
| `created_at` | Timestamp | Creation timestamp |

### `outline.documents`

Returned by `POST /documents.list` with an empty body (no `statusFilter`), so this covers **published documents plus the current user's own drafts** — not other users' drafts, and not archived or deleted documents. `archived_at` and `deleted_at` are normally null.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Document identifier |
| `collection_id` | Utf8 | Parent collection ID (null if not in a collection) |
| `title` | Utf8 | Document title |
| `revision` | Int64 | Revision number |
| `updated_at` | Timestamp | Last modification timestamp |
| `published_at` | Timestamp | Publication timestamp |
| `archived_at` | Timestamp | Archive timestamp |
| `deleted_at` | Timestamp | Soft-deletion timestamp |

### `outline.users`

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | User identifier |
| `name` | Utf8 | Display name |
| `email` | Utf8 | Email address |
| `role` | Utf8 | Workspace role |
| `is_suspended` | Boolean | Whether the account is suspended |

## Example queries

Your own draft documents (an unpublished row here is one of your own drafts, not a workspace-wide view):

```sql
SELECT title, collection_id, updated_at
FROM outline.documents
WHERE published_at IS NULL
ORDER BY updated_at ASC;
```

Suspended users:

```sql
SELECT name, email, role
FROM outline.users
WHERE is_suspended = true
ORDER BY name ASC;
```

Collections overview:

```sql
SELECT name, permission, created_at
FROM outline.collections
ORDER BY created_at ASC
LIMIT 25;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/outline/manifest.yaml
coral source test outline
```

Live output:

```text
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

## Limitations

- Read-only; no create, update, publish, archive, or delete.
- Results are limited by the token's scope and the user's permissions.
- Pagination uses `limit`/`offset` (`limit` capped at 100).
- `documents.collection_id` may be null for documents not in a collection.
- `outline.documents` returns only what `documents.list` gives with no `statusFilter`: published documents plus the authenticated user's own drafts. Archived and deleted documents are served by other Outline endpoints this table does not call.
