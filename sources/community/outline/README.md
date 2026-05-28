# Outline (Community)

**Version:** 0.1.0
**Backend:** HTTP (Outline API)
**Tables:** 3
**Base URL:** `{{input.OUTLINE_URL}}/api`

Query Outline collections, documents, and user directories directly through Coral SQL.

This source provides visibility into workspace organization, document lifecycle metadata, and user access information for operational auditing and knowledge-base inventory use cases.

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

| Input               | Kind     | Required | Description                                                                               |
| ------------------- | -------- | -------- | ----------------------------------------------------------------------------------------- |
| `OUTLINE_URL`       | variable | yes      | Outline instance URL without a trailing slash (for example, `https://app.getoutline.com`) |
| `OUTLINE_API_TOKEN` | secret   | yes      | Personal API token or bot token generated within Outline                                  |

---

## Tables Overview

| Table         | Endpoint                 | Pagination   |
| ------------- | ------------------------ | ------------ |
| `collections` | `POST /collections.list` | Offset-based |
| `documents`   | `POST /documents.list`   | Offset-based |
| `users`       | `POST /users.list`       | Offset-based |

---

## Table Reference

### outline.collections

Collections used to organize documents within a workspace.

| Column        | Type      | Description                   |
| ------------- | --------- | ----------------------------- |
| `id`          | Utf8      | Collection identifier         |
| `name`        | Utf8      | Collection name               |
| `description` | Utf8      | Collection description        |
| `permission`  | Utf8      | Collection permission model   |
| `created_at`  | Timestamp | Collection creation timestamp |

### outline.documents

Documents and knowledge-base content stored within Outline.

| Column          | Type      | Description                  |
| --------------- | --------- | ---------------------------- |
| `id`            | Utf8      | Document identifier          |
| `collection_id` | Utf8      | Parent collection identifier |
| `title`         | Utf8      | Document title               |
| `revision`      | Int64     | Revision number              |
| `updated_at`    | Timestamp | Last modification timestamp  |
| `published_at`  | Timestamp | Publication timestamp        |
| `archived_at`   | Timestamp | Archive timestamp            |
| `deleted_at`    | Timestamp | Soft-deletion timestamp      |

### outline.users

Workspace users visible to the authenticated API token.

| Column         | Type    | Description                           |
| -------------- | ------- | ------------------------------------- |
| `id`           | Utf8    | User identifier                       |
| `name`         | Utf8    | Display name                          |
| `email`        | Utf8    | Email address                         |
| `role`         | Utf8    | Workspace role                        |
| `is_suspended` | Boolean | Whether the user account is suspended |

---

## Example Queries

### Find Unpublished Documents

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

### Audit Suspended Users

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

### Representative Live Output

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

### Representative Query Output

```text
$ coral sql --query "SELECT title, published_at FROM outline.documents WHERE archived_at IS NULL LIMIT 2"

+------------------------------------+----------------------+
| title                              | published_at         |
+------------------------------------+----------------------+
| Runbooks: Kubernetes Cluster Loss  | 2026-04-12T08:14:22Z |
| Incident Lifecycle Management      | 2026-05-30T14:22:05Z |
+------------------------------------+----------------------+
```

---

## Limitations

* Read-only source
* Workspace mutations are not supported
* Collection administration operations are not supported
* Returned data depends on the permissions granted to `OUTLINE_API_TOKEN`
* Outline list operations use RPC-style POST endpoints
* Pagination is implemented using provider-side `offset` and `limit` parameters
