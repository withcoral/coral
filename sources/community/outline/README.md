# Outline (Community)

**Version:** 0.1.0
**Backend:** HTTP (Outline API)
**Tables:** 3
**Base URL:** `{{input.OUTLINE_URL}}/api`

Query Outline collections, documents, and user directories directly through Coral SQL.

This integration provides read-only access to Outline's API for workspace organization visibility, document lifecycle auditing, knowledge-base inventory, and user access reporting.

Coral does not support content creation, updates, publishing, archiving, or deletion. All access is read-only.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export OUTLINE_URL=https://app.getoutline.com
export OUTLINE_API_TOKEN=your_api_token_here
coral source add --file sources/community/outline/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Outline API access requires a valid bearer token. Outline supports **two token types**, and both work with this source. They use bearer authentication and grant the same API access — choose based on whether the token should be tied to your account or to an automated integration.

Coral sends the token as `Authorization: Bearer <token>`.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `OUTLINE_URL` | variable | yes | Outline instance URL with protocol and without a trailing slash (for example, `https://app.getoutline.com`) |
| `OUTLINE_API_TOKEN` | secret | yes | A personal API token or bot token authorized to read the Outline API |

### Personal API Token

A personal API token is tied to your individual Outline account. It is the right choice for local development, personal scripts, and IDE/MCP use.

1. Open your Outline account **Settings** and go to the **API Tokens** section.
2. Add a token name/description and click **Create Token**.
3. Copy the token immediately and store it securely. Outline does not display it again.

### Bot Token

A bot token is generated from an Outline OAuth application (integration) and is **not** tied to an individual user. Prefer it for CI/CD pipelines, shared integrations, and team tooling that shouldn't depend on a single person's account.

1. Create (or open) an OAuth application under **Settings → Applications**.
2. Generate an application/bot token for the integration.
3. Copy the token immediately and store it securely. Outline does not display it again.

Returned resources are restricted by the permissions associated with the supplied token. Content not visible to the token cannot be queried through Coral.

Official docs:

- [Outline API Reference](https://www.getoutline.com/developers)
- [Outline API Authentication](https://www.getoutline.com/developers#section/Authentication)

## Tables

| Table | Endpoint | Pagination |
| --- | --- | --- |
| `outline.collections` | `POST /collections.list` | Offset (`limit` / `offset`) |
| `outline.documents` | `POST /documents.list` | Offset (`limit` / `offset`) |
| `outline.users` | `POST /users.list` | Offset (`limit` / `offset`) |

Outline's list methods are RPC-style POST endpoints that paginate with `limit` and `offset`. Coral advances `offset` automatically (page size `limit=100`, Outline's maximum) and stops when a short page is returned, so full result sets are fetched page by page rather than just the first page.

### `outline.collections`

Collections used to organize documents within a workspace.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Collection identifier |
| `name` | Utf8 | Collection name |
| `description` | Utf8 | Collection description |
| `permission` | Utf8 | Collection permission model |
| `created_at` | Timestamp | Collection creation timestamp |

### `outline.documents`

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

### `outline.users`

Workspace users visible to the authenticated API token.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | User identifier |
| `name` | Utf8 | Display name |
| `email` | Utf8 | Email address |
| `role` | Utf8 | Workspace role |
| `is_suspended` | Boolean | Whether the user account is suspended |

## Example queries

### Find unpublished documents

```sql
SELECT
  title,
  collection_id,
  updated_at
FROM outline.documents
WHERE published_at IS NULL
  AND archived_at IS NULL
ORDER BY updated_at ASC;
```

### Audit suspended users

```sql
SELECT
  name,
  email,
  role
FROM outline.users
WHERE is_suspended = true
ORDER BY name ASC;
```

### Collections overview

```sql
SELECT
  name,
  permission,
  created_at
FROM outline.collections
ORDER BY created_at ASC
LIMIT 25;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/outline/manifest.yaml
Coral manifest schema validation: passed for sources/community/outline/manifest.yaml
make lint-sources: passed
Live API tests: passed with an Outline API token
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/outline/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export OUTLINE_URL=https://app.getoutline.com
export OUTLINE_API_TOKEN=your_api_token_here
coral source add --file sources/community/outline/manifest.yaml
coral source test outline
```

Validate table access with representative SQL:

```bash
coral sql "SELECT name FROM outline.collections LIMIT 5"
coral sql "SELECT title, revision, updated_at FROM outline.documents WHERE archived_at IS NULL LIMIT 5"
coral sql "SELECT title, collection_id, updated_at FROM outline.documents WHERE published_at IS NULL AND archived_at IS NULL LIMIT 5"
coral sql "SELECT name, email, role FROM outline.users WHERE is_suspended = true LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'outline'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'outline' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

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

Representative query:

```sql
SELECT title, revision, updated_at
FROM outline.documents
WHERE archived_at IS NULL
LIMIT 3;
```

Example output:

```text
title                      | revision | updated_at
Engineering Onboarding     | 12       | 2026-05-14T09:32:00Z
Incident Response Runbook  | 47       | 2026-06-02T17:08:00Z
Q2 Planning Notes          | 5        | 2026-04-21T13:45:00Z
```

## Limitations

- Read-only source; workspace, collection, and document mutations are not supported.
- Content creation, publishing, archiving, and deletion are not supported.
- Query results are limited by the permissions granted to `OUTLINE_API_TOKEN`.
- Outline list operations are RPC-style POST endpoints.
- Pagination uses Outline's `limit`/`offset` parameters (`limit` capped at 100); Coral advances `offset` until a short page is returned.
- `documents.collection_id` may be null for documents not associated with a collection.
- Returned data reflects the current visibility scope of the authenticated account.
