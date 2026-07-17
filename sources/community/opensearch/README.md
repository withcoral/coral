# OpenSearch (Community)

**Version:** 0.1.0
**Backend:** HTTP (OpenSearch CAT & Snapshot APIs)
**Tables:** 3
**Base URL:** `{{input.OPENSEARCH_URL}}`

Query OpenSearch cluster topology, index storage telemetry, and snapshot metadata through Coral SQL. Read-only access for infrastructure observability and cluster health monitoring.

Coral exposes read-only `GET` tables. Document indexing, search execution (`/_search`), mapping mutations, and cluster administration are out of scope.

## Install

```bash
export OPENSEARCH_URL=https://localhost:9200
export OPENSEARCH_USERNAME=admin
export OPENSEARCH_PASSWORD=your_secure_password
coral source add --file sources/community/opensearch/manifest.yaml
```

## Authentication

OpenSearch access uses HTTP Basic authentication. Coral sends the username and password on each request.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `OPENSEARCH_URL` | variable | yes | Base cluster URL with port, no trailing slash (e.g. `https://localhost:9200`) |
| `OPENSEARCH_USERNAME` | variable | yes | Basic authentication username |
| `OPENSEARCH_PASSWORD` | secret | yes | Basic authentication password |

Results are limited by the permissions of the supplied account. Prefer an account scoped to the CAT and snapshot read permissions your workflow needs.

Docs: [CAT API](https://opensearch.org/docs/latest/api-reference/cat/index/) · [Snapshot API](https://opensearch.org/docs/latest/api-reference/snapshots/index/) · [Users and roles](https://opensearch.org/docs/latest/security/access-control/users-roles/)

## Tables

| Table | Endpoint | Filters | Pagination |
| --- | --- | --- | --- |
| `opensearch.nodes` | `GET /_cat/nodes` | — | None |
| `opensearch.indices` | `GET /_cat/indices[/{index}]` | optional: `index`, `health` | None |
| `opensearch.snapshots` | `GET /_snapshot/{repository}/_all` | **required:** `repository` | None |

### `opensearch.nodes`

Runtime node telemetry and cluster topology. Response headers are pinned via the CAT `h=` selector for compatibility across OpenSearch versions.

| Column | Type | Description |
| --- | --- | --- |
| `node_name` | Utf8 | Unique node name |
| `ip` | Utf8 | Node IP address |
| `role` | Utf8 | Allocated cluster node roles |
| `cluster_manager_status` | Utf8 | Cluster manager state (`*`, `-`, or `m`) |
| `heap_percent` | Int64 | JVM heap utilization percentage |
| `ram_percent` | Int64 | Physical memory utilization percentage |
| `cpu_percent` | Int64 | CPU utilization percentage |

### `opensearch.indices`

Index catalog with sizing and shard-health telemetry.

| Column | Type | Description |
| --- | --- | --- |
| `index` | Utf8 | Index name/pattern pushdown filter (virtual) |
| `index_name` | Utf8 | Index identifier |
| `health` | Utf8 | Shard allocation health (also a pushdown filter) |
| `status` | Utf8 | Index status (`open` or `closed`) |
| `doc_count` | Int64 | Indexed document count |
| `store_size` | Utf8 | Total storage footprint for primary and replica shards |

Two filters are pushed to the CAT API so the cluster filters before Coral sees the response:

| SQL filter | Mapping | Description |
| --- | --- | --- |
| `index` | request path `/_cat/indices/{index}` | Index name or pattern, e.g. `logs-*` |
| `health` | query `health=` | Restrict to `green`, `yellow`, or `red` |

Without them the full CAT listing is fetched and other `WHERE` predicates are applied locally.

### `opensearch.snapshots`

Snapshot metadata for one backup repository. A `repository` filter is **required** — it forms the request path (`/_snapshot/{repository}/_all`). OpenSearch snapshot objects carry no repository field, so the `repository` column echoes the filter rather than reading from each row.

| Column | Type | Description |
| --- | --- | --- |
| `repository` | Utf8 | Repository the snapshots belong to (from the required filter) |
| `snapshot_name` | Utf8 | Snapshot name |
| `state` | Utf8 | Snapshot state (e.g. SUCCESS, FAILED, IN_PROGRESS) |
| `version` | Utf8 | OpenSearch version that produced the snapshot |
| `start_time` | Utf8 | Snapshot start timestamp (ISO-8601 string from the API) |

## Example queries

Nodes under resource pressure:

```sql
SELECT node_name, role, cluster_manager_status, cpu_percent, heap_percent
FROM opensearch.nodes
WHERE cpu_percent > 80
   OR heap_percent > 85
ORDER BY cpu_percent DESC;
```

Unhealthy index allocations (health pushed down):

```sql
SELECT index_name, health, status, doc_count, store_size
FROM opensearch.indices
WHERE health = 'yellow'
ORDER BY doc_count DESC;
```

A specific index pattern (index pushed down to the path):

```sql
SELECT index_name, health, doc_count
FROM opensearch.indices
WHERE index = 'logs-*'
ORDER BY index_name;
```

Snapshots in a repository:

```sql
SELECT repository, snapshot_name, state, start_time
FROM opensearch.snapshots
WHERE repository = 's3-cold-backup'
ORDER BY start_time DESC;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/opensearch/manifest.yaml
coral source test opensearch
```

Live output:

```text
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

## Limitations

- Read-only; no index mutation or cluster administration.
- Does not expose document search execution (`/_search`).
- Results are limited by the permissions of the supplied account.
- `indices` pushes down `index` and `health` only; other predicates are filtered locally after the CAT listing is fetched.
- `snapshots` requires an explicit `repository` filter, and the `repository` column echoes that filter rather than reading it from the response.
