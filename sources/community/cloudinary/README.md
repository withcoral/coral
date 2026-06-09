# Cloudinary

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://api.cloudinary.com/v1_1/<cloud_name>`

Query assets, folders, upload presets, usage details, and metadata fields from Cloudinary — the media management platform for image and video optimization, transformation, and delivery.

## Authentication

Requires a `CLOUDINARY_CLOUD_NAME` variable and a `CLOUDINARY_BASIC_AUTH` secret (base64-encoded `api_key:api_secret`).

### Generating the Basic Auth Token

```bash
printf '%s:%s' YOUR_API_KEY YOUR_API_SECRET | base64
```

### Finding Your Credentials

1. Log in to the [Cloudinary Console](https://console.cloudinary.com).
2. Navigate to **Settings → Access Keys** (or **Programmable Media Dashboard** for the cloud name).
3. Copy the **API Key** and **API Secret** (or generate a new key pair if none exists).
4. Base64-encode `api_key:api_secret` as shown above.

### Recommended Permissions

Standard API Key/Secret credentials provide read access to all Admin API endpoints listed below. No additional scopes or permissions configuration is required.

```bash
CLOUDINARY_CLOUD_NAME=your_cloud_name
CLOUDINARY_BASIC_AUTH=$(printf '%s:%s' YOUR_API_KEY YOUR_API_SECRET | base64)
export CLOUDINARY_CLOUD_NAME CLOUDINARY_BASIC_AUTH
coral source add --file sources/community/cloudinary/manifest.yaml
```

API docs: https://cloudinary.com/documentation/admin_api

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `resources` | Assets (images, videos, raw files) with metadata | — | `resource_type`, `type`, `prefix`, `start_at`, `direction`, `tags`, `context`, `metadata` |
| `folders` | Top-level asset folders | — | — |
| `upload_presets` | Upload presets with default settings | — | — |
| `usage` | Current plan usage (credits, storage, bandwidth, transformations) | — | — |
| `metadata_fields` | Structured metadata field definitions | — | — |

### Key design notes

- **`resources` is the richest table.** It supports filtering by resource type, folder prefix, upload date, and can include tags, context metadata, and structured metadata fields in the response.
- **`folders` returns top-level folders only.** Use the `prefix` filter on `resources` to browse assets in subfolder paths.
- **`usage` returns exactly one row.** It includes nested fields for credits, storage, bandwidth, transformations, and object counts with their limits.
- **`tags` endpoint (GET /tags) is not available** on Free plan accounts — it has been excluded from this source. Use the `tags` filter on `resources` to retrieve tag data per asset.
- **No pagination for `metadata_fields`.** The Cloudinary Admin API returns all metadata fields in a single response.
- **`usage` has no pagination.** It is a single-object response, not an array.

### resources filter values

| Filter | Description |
|---|---|
| `resource_type` | Filter by type (`image`, `video`, `raw`, `auto`) |
| `type` | Filter by delivery type (`upload`, `private`, `authenticated`) |
| `prefix` | Filter by folder prefix (e.g. `myfolder/subfolder`) |
| `start_at` | Filter by minimum `created_at` date (ISO 8601, e.g. `2024-01-01T00:00:00Z`) |
| `direction` | Sort direction (`asc` or `desc`, default: `desc` by date) |
| `tags` | Set to `true` to include `tags` JSON column |
| `context` | Set to `true` to include `context` JSON column |
| `metadata` | Set to `true` to include `metadata_fields` JSON column |

### Rate Limits & Fetch Limits

Cloudinary enforces plan-based rate limits on its Admin API. Free plan accounts have a limit of **500 credits/hour** for Admin API calls. The `resources` table has a default fetch limit of **100 assets**. You can override this limit in your SQL query by specifying a `LIMIT` clause (e.g. `LIMIT 500`).

## Quick start

```bash
# Step 1 — list recent assets
coral sql "
  SELECT public_id, resource_type, format, bytes, width, height, created_at
  FROM cloudinary.resources
  LIMIT 20
"

# Step 2 — list all top-level folders
coral sql "SELECT name, path FROM cloudinary.folders"

# Step 3 — view current usage and plan limits
coral sql "
  SELECT plan, credits_usage, credits_limit, storage_bytes,
         storage_limit_bytes, bandwidth_bytes
  FROM cloudinary.usage
"

# Step 4 — list all upload presets
coral sql "SELECT name, unsigned, settings FROM cloudinary.upload_presets"

# Step 5 — list all metadata field definitions
coral sql "
  SELECT external_id, label, type, mandatory, default_value
  FROM cloudinary.metadata_fields
"
```

## Example queries

### All images with dimensions

```sql
SELECT
  public_id,
  format,
  width,
  height,
  bytes,
  secure_url,
  created_at
FROM cloudinary.resources
WHERE resource_type = 'image'
LIMIT 50;
```

### Largest assets by file size

```sql
SELECT
  public_id,
  resource_type,
  format,
  bytes,
  width,
  height,
  created_at
FROM cloudinary.resources
ORDER BY bytes DESC
LIMIT 20;
```

### Assets in a specific folder

```sql
SELECT
  public_id,
  format,
  resource_type,
  bytes,
  created_at
FROM cloudinary.resources
WHERE prefix = 'myfolder/subfolder'
LIMIT 50;
```

### Assets with tags and context metadata

```sql
SELECT
  public_id,
  format,
  resource_type,
  tags,
  context,
  created_at
FROM cloudinary.resources
WHERE tags = true AND context = true
LIMIT 20;
```

### Current storage and bandwidth usage

```sql
SELECT
  plan,
  credits_usage,
  credits_limit,
  credits_used_percentage,
  storage_bytes,
  storage_limit_bytes,
  bandwidth_bytes,
  bandwidth_limit_bytes,
  transformations_used,
  transformations_limit,
  objects_count,
  objects_limit
FROM cloudinary.usage;
```

### Upload presets

```sql
SELECT
  name,
  unsigned,
  settings
FROM cloudinary.upload_presets;
```

### Metadata field definitions

```sql
SELECT
  external_id,
  label,
  type,
  mandatory,
  default_value,
  validation
FROM cloudinary.metadata_fields;
```
