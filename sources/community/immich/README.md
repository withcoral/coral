# Immich (Community)

**Version:** 0.1.0
**Backend:** HTTP (Immich REST API)
**Tables:** 3
**Base URL:** `{{input.IMMICH_BASE_URL}}/api`

Query assets, albums, and user profiles from your self-hosted Immich instance using Coral SQL. This source supports library auditing, storage analysis, and album inventory workflows.

Coral exposes read-only access patterns. Asset uploads, album modifications, sharing configuration changes, and user administration are out of scope.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export IMMICH_BASE_URL=http://192.168.1.50:2283
export IMMICH_API_KEY=your_api_key_here
coral source add --file sources/community/immich/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Immich API access uses an `x-api-key` header.

| Input | Description |
| --- | --- |
| `IMMICH_BASE_URL` | Root URL of your Immich instance without a trailing slash or `/api` suffix |
| `IMMICH_API_KEY` | API key from **Account Settings → API Keys** |

### Required API key permissions

Create the narrowest API key that still covers the tables you query:

| Permission | Used for |
| --- | --- |
| `user.read` | `immich.users` (`GET /api/users`) |
| `album.read` | `immich.albums` (`GET /api/albums`) |
| `asset.read` | `immich.assets` (`POST /api/search/metadata`) |

Non-admin API keys may return a reduced user list or album visibility depending on Immich server policy.

Official docs:

- [Immich authentication](https://immich.app/docs/features/command-line-interface#obtain-the-api-key)
- [Search assets by metadata (`POST /api/search/metadata`)](https://api.immich.app/endpoints/search/searchAssets)
- [List albums (`GET /api/albums`)](https://api.immich.app/endpoints/albums/getAllAlbums)
- [List users (`GET /api/users`)](https://api.immich.app/endpoints/users/searchUsers)

## Tables

| Table | Description | Optional pushdown filters |
| --- | --- | --- |
| `immich.users` | User accounts visible to the API key | — |
| `immich.albums` | Albums visible to the API key | — |
| `immich.assets` | Media assets from metadata search | `type`, `is_favorite` |

### `immich.users`

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Unique user identifier |
| `email` | Utf8 | Primary email address |
| `name` | Utf8 | Display name |

### `immich.albums`

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Unique album identifier |
| `album_name` | Utf8 | Album display title |
| `created_at` | Timestamp | Album creation time |
| `asset_count` | Int64 | Number of assets in the album |
| `shared` | Boolean | Whether the album is shared |

### `immich.assets`

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Asset identifier |
| `owner_id` | Utf8 | Owner user ID |
| `file_name` | Utf8 | Original uploaded filename |
| `type` | Utf8 | Asset type: `IMAGE`, `VIDEO`, `AUDIO`, or `OTHER` |
| `file_size_bytes` | Int64 | File size from `exifInfo.fileSizeInByte` when EXIF is returned |
| `file_created_at` | Timestamp | File creation timestamp from asset metadata |
| `is_favorite` | Boolean | Favorite flag |

#### Pushdown filters

| SQL filter | Immich request body field |
| --- | --- |
| `type` | `type` (`IMAGE`, `VIDEO`, `AUDIO`, or `OTHER`) |
| `is_favorite` | `isFavorite` |

Coral requests `withExif=true` so `file_size_bytes` can be populated from EXIF metadata.

This table reads a **single page** of up to 1000 assets (Immich's maximum `size` for `POST /search/metadata`) in one request. Coral does **not** follow Immich's `assets.nextPage` cursor here: Immich's `page` is an integer body field, and Coral writes a body cursor as a JSON string, so following the cursor would send `"page": "2"` and be rejected. Use `type`, `is_favorite`, and a SQL `LIMIT` to target the assets you need. A single query therefore returns at most one page; spanning more than one page is not supported until numeric body pagination is available.

## Example queries

### Media type distribution

```sql
SELECT
  type,
  COUNT(*) AS volume,
  SUM(file_size_bytes) / 1024 / 1024 AS size_mb
FROM immich.assets
WHERE type = 'IMAGE'
GROUP BY type;
```

For a full-library breakdown, omit the `type` filter and use a conservative `LIMIT`, or run separate grouped queries per media type.

### Review large albums

```sql
SELECT
  album_name,
  asset_count,
  created_at
FROM immich.albums
WHERE asset_count > 100
ORDER BY asset_count DESC
LIMIT 25;
```

### Review user directory

```sql
SELECT
  name,
  email
FROM immich.users
ORDER BY name
LIMIT 25;
```

### Favorite videos

```sql
SELECT
  file_name,
  file_size_bytes,
  file_created_at
FROM immich.assets
WHERE type = 'VIDEO'
  AND is_favorite = true
ORDER BY file_created_at DESC
LIMIT 25;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/immich/manifest.yaml
Coral manifest schema validation: passed for sources/community/immich/manifest.yaml
make lint-sources: passed
Live API tests: passed against a self-hosted Immich instance
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/immich/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export IMMICH_BASE_URL=http://192.168.1.50:2283
export IMMICH_API_KEY=your_api_key_here
coral source add --file sources/community/immich/manifest.yaml
coral source test immich
```

Validate table access with representative SQL:

```bash
coral sql "SELECT id, album_name, asset_count FROM immich.albums LIMIT 5"
coral sql "SELECT id, file_name, type, file_size_bytes FROM immich.assets WHERE type = 'IMAGE' LIMIT 5"
coral sql "SELECT name, email FROM immich.users LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'immich'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'immich' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ immich connected successfully

immich (3 tables)
├─ albums
├─ assets
└─ users

Query tests
2 declared · 2 passed · 0 failed

✓ SELECT id FROM immich.albums LIMIT 1
  1 row

✓ SELECT id, file_name, type FROM immich.assets LIMIT 1
  1 row
```

Representative query:

```sql
SELECT
  album_name,
  asset_count,
  shared,
  created_at
FROM immich.albums
ORDER BY asset_count DESC
LIMIT 3;
```

Example output:

```text
album_name     | asset_count | shared | created_at
Summer 2025    | 248         | false  | 2025-06-01T10:15:30.000Z
Family Trips   | 132         | true   | 2024-11-18T08:42:11.000Z
Phone Uploads  | 89          | false  | 2025-01-09T19:03:55.000Z
```

## Limitations

- Read-only retrieval scope.
- Asset uploads, album edits, sharing changes, and user administration are unsupported.
- Visibility depends on the API key permissions and whether the account is administrative.
- `immich.assets` can return large payloads; use `type`, `is_favorite`, and SQL `LIMIT` to keep scans bounded.
- `file_size_bytes` is null when Immich does not return EXIF metadata for an asset.
- `immich.assets` returns a single metadata-search page (up to Immich's `size` maximum of 1000) and does not follow `assets.nextPage`; queries needing more than one page are not yet supported. `immich.users` and `immich.albums` are unpaginated list endpoints.
