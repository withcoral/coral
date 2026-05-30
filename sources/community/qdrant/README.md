# Qdrant (qdrant)

**Version:** 1.0.0
**Backend:** HTTP
**Tables:** 16
**Functions:** 2
**Base URL:** Configurable (`QDRANT_HOST`, default `http://localhost:6333`)

Query collections, points, cluster information, snapshots, locks, and perform
vector similarity search, recommendations, and point discovery in
[Qdrant](https://qdrant.tech/) — the open-source vector database.

```bash
coral source add --file sources/community/qdrant/manifest.yaml
```

## Configuration

| Input | Kind | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `QDRANT_HOST` | variable | No | `http://localhost:6333` | Host URL of your Qdrant instance |
| `QDRANT_API_KEY` | secret | Yes | — | API key (use `none` for local without auth) |

### Run Qdrant locally

```bash
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

## Tables

| Table | Description | Key filters |
| --- | --- | --- |
| `qdrant.collections` | List all collection names | — |
| `qdrant.collection_details` | Config, status, and vector dimensions | `collection_name` |
| `qdrant.collection_cluster_info` | Cluster shard details | `collection_name` |
| `qdrant.aliases` | List all collection aliases | — |
| `qdrant.collection_aliases` | Aliases for a specific collection | `collection_name` |
| `qdrant.snapshots` | List all storage snapshots | — |
| `qdrant.collection_snapshots` | Snapshots for a specific collection | `collection_name` |
| `qdrant.cluster_info` | Cluster configuration and state | — |
| `qdrant.locks` | Active lock configurations | — |
| `qdrant.telemetry` | Node telemetry data | — |
| `qdrant.points` | Scroll points with cursor pagination | `collection_name` |
| `qdrant.points_by_id` | Fetch specific points by ID | `collection_name`, `ids` |
| `qdrant.points_search` | Vector similarity search | `collection_name`, `vector` |
| `qdrant.points_recommend` | Point recommendation | `collection_name`, `positive` |
| `qdrant.points_discover` | Point discovery | `collection_name`, `target`, `context` |
| `qdrant.points_count` | Count points matching a filter | `collection_name` |

## Functions

| Function | Description | Key args |
| --- | --- | --- |
| `qdrant.search_points()` | Vector similarity search | `collection_name`, `vector` |
| `qdrant.recommend_points()` | Point recommendation | `collection_name`, `positive` |

## Example queries

```sql
-- List all collections
SELECT name FROM qdrant.collections;

-- Inspect collection configuration
SELECT status, points_count, config__params__vectors__distance
FROM qdrant.collection_details
WHERE collection_name = 'my_collection';

-- Vector similarity search (table syntax)
SELECT id, score, payload
FROM qdrant.points_search
WHERE collection_name = 'my_collection'
  AND vector = '[0.05, 0.61, 0.76, 0.15]'
  AND limit = 5;

-- Vector similarity search (function syntax)
SELECT id, score, payload
FROM qdrant.search_points(
  collection_name => 'my_collection',
  vector => '[0.05, 0.61, 0.76, 0.15]',
  limit => 5
);

-- Search with payload filter
SELECT id, score, payload
FROM qdrant.points_search
WHERE collection_name = 'my_collection'
  AND vector = '[0.05, 0.61, 0.76, 0.15]'
  AND filter = '{"must":[{"key":"city","match":{"value":"Berlin"}}]}';

-- Recommend similar points
SELECT id, score, payload
FROM qdrant.points_recommend
WHERE collection_name = 'my_collection'
  AND positive = '["point-uuid-1", "point-uuid-2"]'
  AND limit = 3;

-- Count points in a collection
SELECT count FROM qdrant.points_count
WHERE collection_name = 'my_collection';
```

## Pagination

The `points` table uses cursor-based pagination, handled automatically
by Coral. Default page size is 100 (max 1000). Other tables return
fixed result sets and do not require pagination.

## Notes

- **Read-only.** This source only exposes read endpoints. Collection
  management and point modification are out of scope.
- **JSON inputs.** Vector search requires passing vectors as JSON arrays
  (e.g. `'[0.05, 0.61, 0.76]'`) and payload filters as JSON objects.
  The engine parses these automatically.
- **Point IDs.** Qdrant supports both integer and UUID point IDs. The
  `id` column uses `Json` type to handle both formats.
- **Payload fields.** The `points` and `points_search` tables expose
  common payload fields (`payload__name`, `payload__title`, etc.) for
  convenience, alongside the full `payload` JSON column.
- **Cloud compatible.** Works with both self-hosted Qdrant and
  [Qdrant Cloud](https://cloud.qdrant.io/) — just set `QDRANT_HOST`
  to your cloud endpoint.

## Validation

```bash
coral source lint sources/community/qdrant/manifest.yaml
coral source add --file sources/community/qdrant/manifest.yaml
coral source test qdrant
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'qdrant'"
coral sql "SELECT name FROM qdrant.collections LIMIT 5"
```
