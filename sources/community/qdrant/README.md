# Qdrant

Query collections, points, cluster information, snapshots, locks, and perform vector similarity search, recommendations, and point discovery in Qdrant.

## Setup

### Run Qdrant Locally

If you don't have a Qdrant instance running, you can start one using Docker:

```bash
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

### Add the Source

Add the Qdrant community source to Coral:

```bash
coral source add --file sources/community/qdrant/manifest.yaml
```

When prompted:
1. Provide your Qdrant host URL as `QDRANT_HOST` (defaults to `http://localhost:6333`).
2. Provide your API Key as `QDRANT_API_KEY`. If your Qdrant instance is running locally without an API key, enter a dummy value such as `none`.

## Tables

### `collections`
List names of all Qdrant collections.

### `collection_details`
Detailed configuration and status for a specific collection.
- **Requires filter:** `collection_name`

### `collection_cluster_info`
Cluster shard details for a specific collection.
- **Requires filter:** `collection_name`

### `aliases`
List all collection aliases globally.

### `collection_aliases`
List aliases for a specific collection.
- **Requires filter:** `collection_name`

### `snapshots`
List all storage snapshots in Qdrant.

### `collection_snapshots`
List snapshots for a specific collection.
- **Requires filter:** `collection_name`

### `cluster_info`
Check the cluster configuration and state.

### `locks`
Get active lock configurations for the node.

### `telemetry`
Fetch node telemetry data.

### `points`
Scroll points from a collection with pagination support.
- **Requires filter:** `collection_name`

### `points_by_id`
Fetch points by specific list of IDs.
- **Requires filters:** `collection_name`, `ids`

### `points_search`
Query vector similarity search as a SQL table.
- **Requires filters:** `collection_name`, `vector`
- **Optional filters:** `limit`, `score_threshold`, `filter`

### `points_recommend`
Query points recommendation as a SQL table based on positive and negative example IDs or vectors.
- **Requires filters:** `collection_name`, `positive`
- **Optional filters:** `negative`, `limit`, `strategy`, `filter`

### `points_discover`
Query points discovery as a SQL table (find point closest to target while keeping away from context).
- **Requires filters:** `collection_name`, `target`, `context`
- **Optional filters:** `limit`, `filter`

### `points_count`
Get the total count of points matching a payload filter.
- **Requires filter:** `collection_name`
- **Optional filters:** `filter`, `exact`

## Functions

### `search_points`
Vector similarity search on Qdrant points.
- **Arguments:**
  - `collection_name` (string, required)
  - `vector` (array, required)
  - `limit` (integer, optional)
  - `score_threshold` (float, optional)

### `recommend_points`
Point recommendation based on positive and negative examples.
- **Arguments:**
  - `collection_name` (string, required)
  - `positive` (array, required)
  - `negative` (array, optional)
  - `limit` (integer, optional)
  - `strategy` (string, optional)

## Authentication

The source accesses the Qdrant REST API using header-based authentication:

```text
api-key: <QDRANT_API_KEY>
```

For local development without security enabled, supply `none` as the API key.

## Limits

- This source exposes read-only API endpoints.
- Collection management (creating collections, deleting collections, updating vector configuration) and point modification (upserting, deleting points) are out of scope.
- Timestamps and JSON fields are parsed in their native types.

## Example Queries

### List collections

```sql
SELECT name FROM qdrant.collections
```

### Inspect collection details

```sql
SELECT status, optimizer_status, points_count, config__params__vectors__distance
FROM qdrant.collection_details
WHERE collection_name = 'test_collection'
```

### Vector search using points_search table

```sql
SELECT id, score, payload, payload__title
FROM qdrant.points_search
WHERE collection_name = 'test_collection'
  AND vector = '[0.05, 0.61, 0.76, 0.15]'
  AND limit = 5
```

### Vector search using search_points function

```sql
SELECT id, score, payload
FROM qdrant.search_points(
  collection_name => 'test_collection',
  vector => '[0.05, 0.61, 0.76, 0.15]',
  limit => 5
)
```

### Recommend points based on positive examples

```sql
SELECT id, score, payload
FROM qdrant.points_recommend
WHERE collection_name = 'test_collection'
  AND positive = '["1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"]'
  AND limit = 3
```

### Count points in a collection

```sql
SELECT count
FROM qdrant.points_count
WHERE collection_name = 'test_collection'
```
