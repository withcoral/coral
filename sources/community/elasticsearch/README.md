# Elasticsearch — Coral Source

Query your Elasticsearch cluster with SQL using [Coral](https://withcoral.com). This source exposes six tables covering cluster health, index management, node stats, shard allocation, aliases, and running tasks — all via read-only GET endpoints.

---

## Prerequisites

- Coral CLI installed — see the [installation guide](https://withcoral.com/docs/getting-started/installation)
- An Elasticsearch cluster reachable from your machine (self-hosted, Elastic Cloud, or ECE)
- An API key with at minimum `monitor` cluster privilege

### Minimum Elasticsearch version

Elasticsearch **7.x or later**. All six endpoints used here (`/_cluster/health`, `/_cat/indices`, `/_cat/nodes`, `/_cat/shards`, `/_cat/aliases`, `/_tasks`) have been stable since 7.0.

---

## Authentication

This source uses **API Key** auth — the recommended approach for Elastic Cloud and modern self-hosted clusters.

```
Authorization: ApiKey <base64-encoded id:api_key>
```

To create a key: **Kibana → Stack Management → API Keys → Create API key**. Grant it the `monitor` cluster privilege and `read` on whichever indices you need.

If you need **Basic Auth** instead, replace the `auth` block in `manifest.yaml` with:

```yaml
auth:
  type: BasicAuth
  username: "{{input.ELASTICSEARCH_USERNAME}}"
  password: "{{input.ELASTICSEARCH_PASSWORD}}"
```

and add the corresponding `kind: variable` / `kind: secret` inputs.

---

## Installation

```bash
# 1. Export your inputs as environment variables
export ELASTICSEARCH_BASE_URL=https://my-cluster.es.io:9200
export ELASTICSEARCH_API_KEY=<your-api-key>

# 2. Lint the spec locally — no credentials required, no workspace changes
coral source lint ./manifest.yaml

# 3. Add the source (reads env vars automatically)
coral source add --file ./manifest.yaml

# 4. Validate end-to-end against your cluster
coral source test elasticsearch
```

**Interactive mode** — prompts for each input instead of reading env vars:

```bash
coral source add --file ./manifest.yaml --interactive
```

---

## Tables

| #   | Table            | Endpoint               | Description                                                           |
| --- | ---------------- | ---------------------- | --------------------------------------------------------------------- |
| 1   | `cluster_health` | `GET /_cluster/health` | Overall cluster status, shard counts, node counts. One row.           |
| 2   | `indices`        | `GET /_cat/indices`    | All indices with health, status, shard config, doc count, store size. |
| 3   | `nodes`          | `GET /_cat/nodes`      | All nodes with roles, heap, CPU, load average, and disk usage.        |
| 4   | `shards`         | `GET /_cat/shards`     | Per-shard allocation state, node assignment, and store size.          |
| 5   | `aliases`        | `GET /_cat/aliases`    | All index aliases with target index, routing, and write-index flag.   |
| 6   | `tasks`          | `GET /_tasks`          | All tasks currently running across the cluster.                       |

---

## Column Reference

### `cluster_health`

| Column                            | Type    | Description                                                                |
| --------------------------------- | ------- | -------------------------------------------------------------------------- |
| `cluster_name`                    | Utf8    | Name of the cluster                                                        |
| `status`                          | Utf8    | `green`, `yellow`, or `red`                                                |
| `timed_out`                       | Boolean | Whether the request timed out before the expected shard status was reached |
| `number_of_nodes`                 | Int64   | Total nodes in the cluster                                                 |
| `number_of_data_nodes`            | Int64   | Data nodes only                                                            |
| `active_primary_shards`           | Int64   | Active primary shards                                                      |
| `active_shards`                   | Int64   | Total active shards (primary + replica)                                    |
| `relocating_shards`               | Int64   | Shards currently moving between nodes                                      |
| `initializing_shards`             | Int64   | Shards currently initializing                                              |
| `unassigned_shards`               | Int64   | Shards with no node assignment                                             |
| `delayed_unassigned_shards`       | Int64   | Unassigned shards held back by delay timeout                               |
| `number_of_pending_tasks`         | Int64   | Cluster-level tasks waiting to execute                                     |
| `active_shards_percent_as_number` | Float64 | Percentage of active shards cluster-wide                                   |

### `indices`

Numeric fields (`docs__count`, `store__size`, etc.) are returned as `Utf8` by the CAT API. Cast with `CAST(docs__count AS BIGINT)` when arithmetic is needed.

| Column             | Type | Description                                                         |
| ------------------ | ---- | ------------------------------------------------------------------- |
| `index`            | Utf8 | Index name                                                          |
| `health`           | Utf8 | `green`, `yellow`, or `red`                                         |
| `status`           | Utf8 | `open` or `close`                                                   |
| `uuid`             | Utf8 | Index UUID                                                          |
| `pri`              | Utf8 | Number of primary shards                                            |
| `rep`              | Utf8 | Number of replica shards                                            |
| `docs__count`      | Utf8 | Available document count (maps to `docs.count`)                     |
| `docs__deleted`    | Utf8 | Deleted documents not yet merged away (maps to `docs.deleted`)      |
| `store__size`      | Utf8 | Total store size in bytes, primary + replica (maps to `store.size`) |
| `pri__store__size` | Utf8 | Primary shards store size in bytes (maps to `pri.store.size`)       |

### `nodes`

All numeric fields (`heap__percent`, `cpu`, etc.) are `Utf8` from the CAT API.

| Column               | Type | Description                                                                  |
| -------------------- | ---- | ---------------------------------------------------------------------------- |
| `id`                 | Utf8 | Short node ID                                                                |
| `name`               | Utf8 | Node name                                                                    |
| `ip`                 | Utf8 | Node IP address                                                              |
| `role`               | Utf8 | Role flags: `d`=data, `m`=master eligible, `i`=ingest, `c`=coordinating only |
| `master`             | Utf8 | `*` if this is the elected master, `-` otherwise                             |
| `heap__percent`      | Utf8 | Heap used as % of max                                                        |
| `heap__current`      | Utf8 | Current heap used in bytes                                                   |
| `heap__max`          | Utf8 | Maximum heap size in bytes                                                   |
| `cpu`                | Utf8 | CPU usage %                                                                  |
| `load_1m`            | Utf8 | 1-minute OS load average                                                     |
| `disk__used_percent` | Utf8 | Disk used %                                                                  |
| `disk__used`         | Utf8 | Disk used in bytes                                                           |
| `disk__total`        | Utf8 | Total disk size in bytes                                                     |

### `shards`

`docs`, `store`, `ip`, and `node` are nullable — unassigned shards have no node and no stats.

| Column   | Type | Description                                              |
| -------- | ---- | -------------------------------------------------------- |
| `index`  | Utf8 | Index this shard belongs to                              |
| `shard`  | Utf8 | Shard number                                             |
| `prirep` | Utf8 | `p` = primary, `r` = replica                             |
| `state`  | Utf8 | `STARTED`, `UNASSIGNED`, `RELOCATING`, or `INITIALIZING` |
| `docs`   | Utf8 | Document count in this shard (null if unassigned)        |
| `store`  | Utf8 | Store size in bytes (null if unassigned)                 |
| `ip`     | Utf8 | Node IP (null if unassigned)                             |
| `node`   | Utf8 | Node name (null if unassigned)                           |

### `aliases`

`filter`, `routing__index`, `routing__search`, and `is_write_index` are nullable — most simple aliases don't set these.

| Column            | Type | Description                                      |
| ----------------- | ---- | ------------------------------------------------ |
| `alias`           | Utf8 | Alias name                                       |
| `index`           | Utf8 | Target index name                                |
| `filter`          | Utf8 | Filter query on the alias, if any                |
| `routing__index`  | Utf8 | Index routing value (maps to `routing.index`)    |
| `routing__search` | Utf8 | Search routing value (maps to `routing.search`)  |
| `is_write_index`  | Utf8 | `true` if this is the write target for the alias |

### `tasks`

The `key` column is the raw dict key (`nodeId:taskNumber`) injected by `dict_entries`. All other columns are nullable since task detail varies by action type.

| Column                  | Type    | Description                                   |
| ----------------------- | ------- | --------------------------------------------- |
| `key`                   | Utf8    | Task identifier in `node:number` format       |
| `node`                  | Utf8    | Node ID running the task                      |
| `id`                    | Int64   | Numeric task ID within that node              |
| `type`                  | Utf8    | Task type (e.g. `transport`, `direct`)        |
| `action`                | Utf8    | Task action (e.g. `indices:data/read/search`) |
| `description`           | Utf8    | Human-readable task description               |
| `start_time_in_millis`  | Int64   | Start time as Unix epoch milliseconds         |
| `running_time_in_nanos` | Int64   | Elapsed time in nanoseconds                   |
| `cancellable`           | Boolean | Whether the task can be cancelled             |
| `parent_task_id`        | Utf8    | Parent task ID, if this is a child task       |

---

## Example Queries

**Cluster status at a glance**

```sql
SELECT status, number_of_nodes, active_shards,
       unassigned_shards, active_shards_percent_as_number
FROM elasticsearch.cluster_health;
```

**Find unhealthy indices**

```sql
SELECT index, health, status, pri, rep, docs__count, store__size
FROM elasticsearch.indices
WHERE health != 'green'
ORDER BY health DESC, index;
```

**Nodes under heap pressure**

```sql
SELECT name, ip, role, master,
       heap__percent, cpu, load_1m,
       disk__used_percent
FROM elasticsearch.nodes
ORDER BY CAST(heap__percent AS INT) DESC;
```

**Unassigned shards and why they are stuck**

```sql
SELECT index, shard, prirep, state, node
FROM elasticsearch.shards
WHERE state = 'UNASSIGNED'
ORDER BY index, shard;
```

**Aliases pointing to a specific index**

```sql
SELECT alias, index, is_write_index, filter
FROM elasticsearch.aliases
WHERE index = 'my-logs-000001';
```

**Long-running cancellable tasks**

```sql
SELECT key, action, description,
       running_time_in_nanos / 1000000 AS running_ms,
       cancellable
FROM elasticsearch.tasks
WHERE cancellable = true
ORDER BY running_time_in_nanos DESC;
```

**Find active reindex or delete-by-query operations**

```sql
SELECT key, action, description,
       running_time_in_nanos / 1000000 AS running_ms
FROM elasticsearch.tasks
WHERE action LIKE '%reindex%'
   OR action LIKE '%delete-by-query%'
   OR action LIKE '%update-by-query%';
```

---

## Inspect the source from within Coral

```sql
-- All columns for this source
SELECT table_name, column_name, data_type, description
FROM coral.columns
WHERE schema_name = 'elasticsearch'
ORDER BY table_name, ordinal_position;

-- Check which inputs are configured
SELECT key, kind, value, is_set, hint
FROM coral.inputs
WHERE schema_name = 'elasticsearch';
```

---

## Troubleshooting

| Symptom                              | Likely cause                   | Fix                                                                                              |
| ------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------ |
| `401 Unauthorized`                   | Wrong or expired API key       | Regenerate the key, re-export `ELASTICSEARCH_API_KEY`, and re-add the source                     |
| `403 Forbidden`                      | Missing privilege              | Grant `monitor` cluster privilege to the API key                                                 |
| `Connection refused`                 | Wrong `ELASTICSEARCH_BASE_URL` | Confirm the URL and port; Elastic Cloud uses port `443`                                          |
| `tasks` table returns no rows        | No tasks running               | Normal — the table is empty when the cluster is idle                                             |
| `shards` `node` column is null       | Shard is unassigned            | Expected — check `state` and investigate allocation with Kibana or `_cluster/allocation/explain` |
| CAT numeric values look like strings | CAT API always returns strings | Use `CAST(docs__count AS BIGINT)` for arithmetic                                                 |
