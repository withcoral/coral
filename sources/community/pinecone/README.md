# Pinecone

Query vector indexes, collections, and backups from Pinecone via the
Pinecone Control Plane API.

## Setup

### 1. Get your API key

1. Open the [Pinecone console](https://app.pinecone.io).
2. Go to **API Keys** in the left sidebar.
3. Copy an existing key or create a new one.

The key is a string starting with `pcsk_`. It requires at least read access
to the project.

See [Pinecone authentication](https://docs.pinecone.io/guides/get-started/authentication)
for more details.

### 2. Add the source

```bash
export PINECONE_API_KEY="pcsk_..."
coral source add --file sources/community/pinecone/manifest.yaml
```

### 3. Verify

```bash
coral source test pinecone
```

## Tables

### `pinecone.indexes`

All vector indexes in the project. One row per index.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Unique index name within the project |
| `metric` | Utf8 | Similarity metric -- cosine, euclidean, or dotproduct |
| `dimension` | Int64 | Vector dimension |
| `vector_type` | Utf8 | Vector type -- dense or sparse |
| `status_ready` | Boolean | Whether the index is ready to serve requests |
| `status_state` | Utf8 | Lifecycle state -- Initializing, ScalingUp, ScalingDown, Terminating, Ready |
| `host` | Utf8 | Data-plane host URL (unique per index) |
| `spec_cloud` | Utf8 | Cloud provider for serverless indexes (aws, gcp, azure) |
| `spec_region` | Utf8 | Cloud region for serverless indexes |
| `deletion_protection` | Utf8 | Whether deletion protection is enabled or disabled |
| `embed_model` | Utf8 | Integrated embedding model name; NULL for bring-your-own-vector indexes |
| `embed_dimension` | Int64 | Output dimension of the integrated embedding model |
| `embed_metric` | Utf8 | Similarity metric used by the integrated embedding model |

Use `name` to identify indexes and `host` to reference the data-plane endpoint.

### `pinecone.collections`

Static snapshots of pod-based indexes. One row per collection.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Unique collection name |
| `status` | Utf8 | Status -- Initializing, Ready, or Terminating |
| `size` | Int64 | Storage size in bytes |
| `dimension` | Int64 | Vector dimension of the source index |
| `vector_count` | Int64 | Number of vectors stored |
| `source_index` | Utf8 | Name of the index this collection was created from |
| `environment` | Utf8 | Pod environment the collection was created in |

Collections are only available for pod-based indexes. Serverless indexes
use backups instead.

### `pinecone.backups`

Point-in-time snapshots of serverless indexes. One row per backup.

| Column | Type | Description |
|---|---|---|
| `backup_id` | Utf8 | Unique backup identifier |
| `name` | Utf8 | Optional user-provided backup name |
| `description` | Utf8 | Optional user-provided description |
| `index_name` | Utf8 | Name of the source index |
| `index_id` | Utf8 | ID of the source index |
| `status` | Utf8 | Status -- Initializing, Ready, Terminating, or Failed |
| `cloud` | Utf8 | Cloud provider where the backup is stored |
| `region` | Utf8 | Cloud region where the backup is stored |
| `dimension` | Int64 | Vector dimension of the source index at backup time |
| `metric` | Utf8 | Similarity metric of the source index at backup time |
| `record_count` | Int64 | Number of records in the backup |
| `size_bytes` | Int64 | Storage size in bytes |
| `created_at` | Timestamp | When the backup was created (UTC) |

## Authentication

The source uses the Pinecone Control Plane API with API key authentication:

```text
Api-Key: <PINECONE_API_KEY>
X-Pinecone-Api-Version: 2026-04
```

## Example Queries

### List all indexes with their status and embedding model

```sql
SELECT name, metric, dimension, status_state, embed_model, spec_cloud, spec_region
FROM pinecone.indexes
ORDER BY name
```

### Find indexes that are not yet ready

```sql
SELECT name, status_state, status_ready
FROM pinecone.indexes
WHERE status_ready = false
```

### Indexes using integrated embedding vs bring-your-own-vector

```sql
SELECT
  CASE WHEN embed_model IS NOT NULL THEN 'integrated' ELSE 'bring-your-own' END AS embedding_mode,
  COUNT(*) AS index_count
FROM pinecone.indexes
GROUP BY embedding_mode
```

### Backup inventory with size

```sql
SELECT backup_id, index_name, status, record_count, size_bytes, created_at
FROM pinecone.backups
ORDER BY created_at DESC
```

## Limitations

- Read-only. This source does not create, update, or delete any Pinecone
  resources.
- This source covers the Control Plane API only. Data-plane operations
  (vector upsert, query, fetch, list) require per-index host URLs and are
  out of scope for a static source spec.
- `collections` is only relevant for pod-based indexes. Serverless indexes
  use `backups` instead.
- The Pinecone API does not paginate the indexes or collections list.
  All results are returned in a single response.
- `backups` uses cursor pagination via `paginationToken`.

## Out of scope for v1

- Data-plane vector listing and stats (requires per-index host routing)
- Namespace inventory per index (data-plane only)
- Import job history
- Write operations of any kind
