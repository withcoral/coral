# Immich (Community)

**Version:** 0.1.0
**Backend:** HTTP (Immich Server Core API Interface)
**Tables:** 3
**Base URL:** `{{input.IMMICH_URL}}/api`

Query user storage quotas, album structures, and uploaded media asset metadata directly through Coral SQL using the Immich API.

This integration acts as a self-hosted media infrastructure auditor, allowing administrators and operators to analyze storage growth, identify oversized media assets, inventory album structures, and inspect account resource utilization patterns.

Coral exposes read-only access patterns. Asset uploads, user modifications, album management actions, sharing workflows, and deletion operations are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/immich/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input            | Kind     | Required | Description                                                                                                                        |
| ---------------- | -------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `IMMICH_URL`     | variable | yes      | Immich instance base URL with port and without trailing slash, for example `http://localhost:2283` or `https://immich.infra.local` |
| `IMMICH_API_KEY` | secret   | yes      | API key generated from user account settings within the Immich web UI                                                              |

---

# Tables Overview

| Table    | API Endpoint   | Required Filters | Pagination                                         |
| -------- | -------------- | ---------------- | -------------------------------------------------- |
| `users`  | `GET /users`   | —                | None (returns flat JSON array payload)             |
| `albums` | `GET /albums`  | —                | None (returns flat JSON array payload)             |
| `assets` | `POST /search` | —                | Incremental page traversal using `size` and `page` |

---

# Table Reference

## immich.users

Registered account identities and storage allocation parameters.

> User visibility depends entirely on the permissions associated with the provided API key. Non-admin accounts may not be permitted to enumerate all users.

| Column        | Type  | Description                                       |
| ------------- | ----- | ------------------------------------------------- |
| `id`          | Utf8  | Unique user identifier                            |
| `email`       | Utf8  | Primary email address associated with the account |
| `name`        | Utf8  | Display name of the user                          |
| `role`        | Utf8  | Administrative access level tier                  |
| `quota_bytes` | Int64 | Storage quota allocation in bytes                 |
| `created_at`  | Utf8  | Timestamp when the account profile was created    |

---

## immich.albums

Shared or personal media collection folders.

| Column        | Type  | Description                                |
| ------------- | ----- | ------------------------------------------ |
| `id`          | Utf8  | Album identifier                           |
| `album_name`  | Utf8  | Album display title                        |
| `owner_id`    | Utf8  | User identifier of the album owner         |
| `asset_count` | Int64 | Number of assets associated with the album |
| `created_at`  | Utf8  | Album creation timestamp                   |

---

## immich.assets

Media asset inventory metadata for uploaded photos and videos.

| Column            | Type  | Description                               |
| ----------------- | ----- | ----------------------------------------- |
| `id`              | Utf8  | Asset identifier                          |
| `owner_id`        | Utf8  | Identifier of the uploading user          |
| `file_name`       | Utf8  | Original file name                        |
| `type`            | Utf8  | Media classification (`IMAGE` or `VIDEO`) |
| `file_size_bytes` | Int64 | File size in bytes                        |
| `file_created_at` | Utf8  | Timestamp when the asset was created      |

---

# Example Queries

## Audit Largest Video Assets

```sql
SELECT
  file_name,
  type,
  file_size_bytes,
  owner_id
FROM immich.assets
WHERE type = 'VIDEO'
ORDER BY file_size_bytes DESC
LIMIT 5;
```

---

## Find Empty Albums

```sql
SELECT
  album_name,
  owner_id,
  created_at
FROM immich.albums
WHERE asset_count = 0
   OR asset_count IS NULL
ORDER BY created_at ASC;
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
coral source lint sources/community/immich/manifest.yaml
```

## Execute Live Connection Test

```bash
export IMMICH_URL=http://localhost:2283
export IMMICH_API_KEY=your_secret_api_key_here

coral source add --file sources/community/immich/manifest.yaml
coral source test immich
```

---

# Representative Live Output

```text
$ coral source test immich

✓ immich connected successfully

  immich (3 tables)
  ├─ users
  ├─ albums
  └─ assets

  Query tests
  1 declared · 1 passed · 0 failed
```

Example query execution:

```text
$ coral sql --query "SELECT name, email FROM immich.users WHERE role = 'admin' LIMIT 1"

+-------------------+--------------------+
| name              | email              |
+-------------------+--------------------+
| SRE HomeLab Admin | admin@infra.local  |
+-------------------+--------------------+
```

---

# Limitations

* Read-only retrieval scope
* Asset uploads, album modifications, and user administration workflows are unsupported
* User enumeration visibility depends on the permissions associated with the provided `IMMICH_API_KEY`
* `users` and `albums` endpoints currently retrieve full response payloads without incremental pagination
* Asset inventory traversal is implemented using incremental `page` and `size` request semantics to avoid single-response truncation on large media libraries
* Query filtering is evaluated by Coral SQL after upstream API retrieval
