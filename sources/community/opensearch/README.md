# OpenSearch (Community)

**Version:** 0.1.0
**Backend:** HTTP (OpenSearch CAT & Snapshot APIs)
**Tables:** 3
**Base URL:** `{{input.OPENSEARCH_URL}}`

Query OpenSearch cluster topology, index storage telemetry, and snapshot metadata directly through Coral SQL using the OpenSearch REST APIs.

This integration is designed for infrastructure observability, operational auditing, and cluster health monitoring workflows across OpenSearch environments.

Coral exposes read-only `GET` tables. Document indexing, search execution (`/_search`), mapping mutations, or cluster administration operations are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/opensearch/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `OPENSEARCH_URL` | variable | yes | OpenSearch base cluster URL with port, for example `https://localhost:9200` |
| `OPENSEARCH_USERNAME` | variable | yes | Basic authentication username |
| `OPENSEARCH_PASSWORD` | secret | yes | Basic authentication password |

Coral authenticates with HTTP Basic auth. Use a read-scoped account where possible.

---

# Tables Overview

| Table | API Endpoint | Filters | Pagination |
|---|---|---|---|
| `nodes` | `GET /_cat/nodes` | — | None (JSON array response) |
| `indices` | `GET /_cat/indices[/{index}]` | optional pushdown: `index`, `health` | None (CAT listing) |
| `snapshots` | `GET /_snapshot/{repository}/_all` | **required:** `repository` | None (maps the `snapshots` array) |

---

# Table Reference

## opensearch.nodes

Runtime node telemetry and cluster topology metadata.

| Column | Type | Description |
|---|---|---|
| `node_name` | Utf8 | Unique node name |
| `ip` | Utf8 | Node IP address |
| `role` | Utf8 | Allocated cluster node roles |
| `cluster_manager_status` | Utf8 | Cluster manager election state (`*`, `-`, or `m`) |
| `heap_percent` | Int64 | JVM heap utilization percentage |
| `ram_percent` | Int64 | Physical memory utilization percentage |
| `cpu_percent` | Int64 | CPU utilization percentage |

---

## opensearch.indices

Index catalog with sizing and shard-health telemetry.

### Pushdown filters

These are sent to the OpenSearch CAT API so the cluster filters the response **before** it reaches Coral:

| Filter | Pushdown | Description |
|---|---|---|
| `index` | request path `/_cat/indices/{index}` | Index name or pattern, e.g. `logs-*` |
| `health` | query `health=` | Restrict to `green`, `yellow`, or `red` |

Without these filters the source fetches the full CAT index listing and any other `WHERE` predicates are applied locally by Coral.

| Column | Type | Description |
|---|---|---|
| `index` | Utf8 | Index name/pattern pushdown filter (virtual) |
| `index_name` | Utf8 | Index identifier |
| `health` | Utf8 | Shard allocation health state (also a pushdown filter) |
| `status` | Utf8 | Index lifecycle status (`open` or `closed`) |
| `doc_count` | Int64 | Indexed document count |
| `store_size` | Utf8 | Total storage footprint used by the index |

---

## opensearch.snapshots

Snapshot lifecycle metadata for a specific backup repository.

> Queries against this table **require** a `repository` filter; it is used in the request path (`/_snapshot/{repository}/_all`). OpenSearch snapshot objects do not contain a repository field, so the `repository` column is derived from the required filter rather than read from each row.

| Column | Type | Description |
|---|---|---|
| `repository` | Utf8 | Repository the snapshots belong to (from the required filter) |
| `snapshot_name` | Utf8 | Snapshot name |
| `state` | Utf8 | Snapshot execution state (e.g., SUCCESS, FAILED, IN_PROGRESS) |
| `version` | Utf8 | OpenSearch version that produced the snapshot |
| `start_time` | Utf8 | Snapshot start timestamp |

---

# Example Queries

## Audit High Resource Utilization Nodes

```sql
SELECT node_name, role, cluster_manager_status, cpu_percent, heap_percent
FROM opensearch.nodes
WHERE cpu_percent > 80
   OR heap_percent > 85
ORDER BY cpu_percent DESC;
```

## Track Unhealthy Index Allocations (health pushed down)

```sql
SELECT index_name, health, status, doc_count, store_size
FROM opensearch.indices
WHERE health = 'yellow'
ORDER BY doc_count DESC;
```

## Inspect a Specific Index Pattern (index pushed down to the path)

```sql
SELECT index_name, health, doc_count
FROM opensearch.indices
WHERE index = 'logs-*'
ORDER BY index_name;
```

## Retrieve Repository Snapshots

```sql
SELECT repository, snapshot_name, state, start_time
FROM opensearch.snapshots
WHERE repository = 's3-cold-backup'
ORDER BY start_time DESC;
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
coral source lint sources/community/opensearch/manifest.yaml
```

## Execute Live Connection Test

```bash
export OPENSEARCH_URL=https://localhost:9200
export OPENSEARCH_USERNAME=admin
export OPENSEARCH_PASSWORD=your_secure_password

coral source add --file sources/community/opensearch/manifest.yaml
coral source test opensearch
coral sql "SELECT index_name, health FROM opensearch.indices WHERE health = 'green' LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test opensearch`
> run against this manifest. Do not ship placeholder output.

```text
$ coral source test opensearch

✓ opensearch connected successfully

  opensearch (3 tables)
  ├─ nodes
  ├─ indices
  └─ snapshots

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT node_name, role FROM opensearch.nodes LIMIT 1
  1 row
```

---

# Limitations

- Read-only retrieval scope.
- Does not expose document search execution APIs (`/_search`).
- Does not support index mutation or cluster administration operations.
- `indices` supports `index` and `health` pushdown to the CAT API; other predicates are filtered locally after the CAT listing is fetched.
- Node queries pin CAT response headers via the `h=` selector for compatibility across OpenSearch versions.
- `snapshots` requires an explicit `repository` filter because OpenSearch snapshot responses do not embed a repository field; the `repository` column echoes the requested repository.
