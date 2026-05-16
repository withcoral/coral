# Elasticsearch

Inspect Elasticsearch cluster health, nodes, indices, shards, aliases, index
templates, and pending cluster tasks through the native REST and `_cat` APIs.

## Setup

### Requirements

- Network access to an Elasticsearch HTTP endpoint (default port `9200`).
- A user with sufficient privileges (see [Required privileges](#required-privileges)).
  The built-in `elastic` superuser works for every table.

### Add the Source

```bash
coral source add elasticsearch
```

Provide:

- `ELASTICSEARCH_URL` — base URL including scheme and port, e.g.
  `http://localhost:9200` or
  `https://<deployment>.es.<region>.cloud.es.io:9243`. No trailing slash.
- `ELASTICSEARCH_USER` — username (defaults to `elastic`).
- `ELASTICSEARCH_PASSWORD` — password for that user.

Authentication is HTTP Basic. A non-empty password is required because Coral
cannot install a source with a blank secret.

## Tables

### `cluster_health`
Single-row cluster-wide health summary from `/_cluster/health`.

**Useful for:**
- Alerting on `status = 'red'` or `'yellow'`
- Tracking `unassigned_shards` and `number_of_pending_tasks`
- Cluster capacity overviews

### `nodes`
Per-node heap, RAM, CPU, load, and disk usage from `_cat/nodes`.

**Useful for:**
- Finding nodes under heap or disk pressure
- Identifying the elected master (`master = '*'`)
- Version drift checks across the cluster

### `indices`
Per-index document counts and storage from `_cat/indices`.

**Useful for:**
- Largest-index and document-count reporting
- Finding `red`/`yellow` indices
- Storage growth analysis

### `shards`
Per-shard allocation and state from `_cat/shards`.

**Useful for:**
- Explaining why a cluster is `yellow` or `red`
- Listing `UNASSIGNED` shards and their reasons
- Spotting long-running `RELOCATING` shards

### `aliases`
Alias-to-index mappings from `_cat/aliases`.

**Useful for:**
- Mapping logical aliases to concrete indices
- Finding the write index for rollover aliases

### `templates`
Index templates from `_cat/templates`.

**Useful for:**
- Auditing which patterns are covered by templates
- Reviewing composed component templates

### `pending_tasks`
Cluster state update tasks waiting in the master queue.

**Useful for:**
- Detecting master node pressure
- Debugging slow cluster state changes

## Authentication

HTTP Basic against the Elasticsearch security realm:

```text
Authorization: Basic base64(ELASTICSEARCH_USER:ELASTICSEARCH_PASSWORD)
```

## Required privileges

The built-in `elastic` superuser works out of the box. For a least-privilege
user, the `monitor` **cluster** privilege alone is **not** enough — it covers
cluster-level APIs only. The `indices`, `shards`, and `aliases` tables read
the `_cat` index APIs and additionally require an **index-level** privilege
on the indices you want visible.

| Table | Endpoint | Required privilege |
| ----- | -------- | ------------------ |
| `cluster_health` | `/_cluster/health` | cluster `monitor` |
| `nodes` | `/_cat/nodes` | cluster `monitor` |
| `templates` | `/_cat/templates` | cluster `monitor` |
| `pending_tasks` | `/_cat/pending_tasks` | cluster `monitor` |
| `indices` | `/_cat/indices` | index `monitor` (or `view_index_metadata`) |
| `shards` | `/_cat/shards` | index `monitor` (or `view_index_metadata`) |
| `aliases` | `/_cat/aliases` | index `view_index_metadata` (or `monitor`) |

A role that covers every table:

```json
{
  "cluster": ["monitor"],
  "indices": [
    { "names": ["*"], "privileges": ["monitor", "view_index_metadata"] }
  ]
}
```

Narrow `names` to the index patterns you actually need to expose.

## Limits

- This source is **read-only**. It exposes monitoring and `_cat` endpoints
  only — no document search, indexing, mapping changes, or cluster settings
  mutations.
- `_cat` endpoints return all values as JSON strings. Numeric-looking
  columns (counts, sizes, percentages) are typed as `Utf8`. Use SQL `CAST`
  for arithmetic, e.g. `CAST("docs_count" AS BIGINT)`.
- `cluster_health` returns proper JSON numbers and is typed accordingly.
- No server-side filtering: filter with SQL `WHERE` after the rows are
  fetched.
- On clusters with very many indices or shards, `indices` and `shards`
  return large result sets — apply `LIMIT` for exploratory queries.

## Example Queries

### Cluster health at a glance

```sql
SELECT cluster_name, status, number_of_nodes, unassigned_shards,
       active_shards_percent_as_number
FROM elasticsearch.cluster_health
```

### Largest indices by document count

```sql
SELECT index, health, CAST("docs_count" AS BIGINT) AS docs,
       CAST(store_size AS BIGINT) AS bytes
FROM elasticsearch.indices
WHERE index NOT LIKE '.%'
ORDER BY docs DESC
LIMIT 20
```

### Unassigned shards and why

```sql
SELECT index, shard, prirep, unassigned_reason
FROM elasticsearch.shards
WHERE state = 'UNASSIGNED'
ORDER BY index
```

### Nodes under heap pressure

```sql
SELECT name, ip, CAST(heap_percent AS INTEGER) AS heap_pct,
       CAST(disk_used_percent AS DOUBLE) AS disk_pct
FROM elasticsearch.nodes
WHERE CAST(heap_percent AS INTEGER) > 85
ORDER BY heap_pct DESC
```

## Notes

- Works with self-managed Elasticsearch and Elastic Cloud. OpenSearch
  exposes the same `_cat` and `_cluster/health` shapes and is largely
  compatible, but is not officially targeted here.
- See [Required privileges](#required-privileges) for the least-privilege
  role; `monitor` cluster privilege alone does not cover the index-level
  tables (`indices`, `shards`, `aliases`).
