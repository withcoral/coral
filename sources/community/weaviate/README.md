# Weaviate

Query Weaviate server metadata, collection (class) schema, and per-node
cluster health and object counts from a self-hosted or Weaviate Cloud vector
database, through the Weaviate REST API.

## Setup

### Requirements

- Network access to a Weaviate REST endpoint (default port `8080`).
- A Weaviate API key for the target cluster.

### Add the Source

Set the inputs as environment variables, then add the source from this
manifest:

```bash
export WEAVIATE_URL=http://localhost:8080
export WEAVIATE_API_KEY=your_weaviate_api_key
coral source add --file sources/community/weaviate/manifest.yaml
```

Inputs:

- `WEAVIATE_URL` — base URL including scheme and port, e.g.
  `http://localhost:8080` or `https://my-cluster.weaviate.network`.
  No trailing slash.
- `WEAVIATE_API_KEY` — API key sent as `Authorization: Bearer <key>`.

## Tables

### `meta`
Single-row server metadata from `/v1/meta`.

**Useful for:**
- Connectivity checks and version reporting
- Listing enabled modules

### `collections`
One row per collection (class) from `/v1/schema`.

**Useful for:**
- Inventorying collections and their vectorizer
- Auditing vector index type and distance metric
- Reviewing replication factor, shard count, and multi-tenancy

### `nodes`
Per-node cluster health and object counts from `/v1/nodes` (verbose).

**Useful for:**
- Node health monitoring (`status = 'HEALTHY'`)
- Tracking total object and shard counts
- Watching batch ingestion queue length and indexing status

## Authentication

Weaviate uses bearer-token API keys. This source sends:

```text
Authorization: Bearer <WEAVIATE_API_KEY>
```

Username / password and OIDC flows are not configured by this source.

## Limits

- This source is **read-only**. It exposes metadata, schema, and node
  endpoints only — no vector search, object reads/writes, or schema
  changes.
- The table covering collections is named `collections` (Weaviate's current
  term); the underlying REST endpoint is `/v1/schema` and still calls them
  classes.
- Nested structures (`modules`, `properties`, `shards`) are exposed as
  `Json` columns; query them with the JSON accessor functions, e.g.
  `json_length(properties)`.
- No server-side filtering: filter with SQL `WHERE` after fetching.

## Example Queries

### Server version and modules

```sql
SELECT version, hostname, modules FROM weaviate.meta
```

### Collections with their vector configuration

```sql
SELECT name, vectorizer, vector_index_type, vector_distance,
       shard_count, json_length(properties) AS property_count
FROM weaviate.collections
ORDER BY name
```

### Node health and object counts

```sql
SELECT name, status, version, object_count, shard_count,
       batch_queue_length
FROM weaviate.nodes
ORDER BY name
```

### Unhealthy nodes

```sql
SELECT name, status, version
FROM weaviate.nodes
WHERE status <> 'HEALTHY'
```

## Notes

- Verified against Weaviate 1.27. The `/v1/meta`, `/v1/schema`, and
  `/v1/nodes` endpoints are stable across recent releases.
- For per-object inspection or vector search, use a regular Weaviate client
  — that is intentionally outside this source's read-only observability
  scope.
