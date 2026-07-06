# CockroachDB

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** your CockroachDB node's HTTP interface (set via `COCKROACHDB_URL`)

Inspect a self-hosted CockroachDB cluster's node liveness, per-node status and
build information, known databases, and selected node metrics through the
read-only [Cluster API v2](https://www.cockroachlabs.com/docs/stable/cluster-api)
(`/api/v2`).

This source is **observability only**. It uses the documented HTTP Cluster API
and never runs SQL — it does not connect over the Postgres wire protocol and
does not expose `crdb_internal` tables.

## Why the Cluster API v2

The original design proposal (#600) suggested the `/_status` and `/_admin/v1`
endpoints plus the `/_status/vars` Prometheus endpoint. This source instead
targets the **Cluster API v2**, because:

- `/api/v2/nodes/`, `/api/v2/databases/`, and `/api/v2/health/` are the
  documented, **stable** HTTP endpoints; `/_status/*` and `/_admin/v1/*` are
  internal and explicitly undocumented
  ([cockroachdb/docs#8602](https://github.com/cockroachdb/docs/issues/8602)).
- `/_status/vars` returns Prometheus text, which Coral's JSON mapper cannot
  read. The `metrics` table instead reads the per-node `metrics` map that the
  v2 nodes endpoint already returns as JSON.

## Setup

### Requirements

- Network access to a CockroachDB node's HTTP interface (default port `8080`).
- A Cluster API session token (secure clusters). The Cluster API v2 only
  authenticates SQL users that are members of the `admin` role and have a
  password and the `LOGIN` privilege.

### Get a session token (secure clusters)

```bash
curl -k --data "username=<user>&password=<password>" \
  https://localhost:8080/api/v2/login/
# => {"session":"<token>"}
```

Copy the `session` value. Tokens are revoked on logout and expire per the
cluster's `server.web_session_timeout` setting.

### Add the source

```bash
export COCKROACHDB_URL=https://localhost:8080
export COCKROACHDB_SESSION=<token>
coral source add --file sources/community/cockroachdb/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
coral source add --file sources/community/cockroachdb/manifest.yaml --interactive
```

Inputs:

- `COCKROACHDB_URL` — base URL including scheme and port, e.g.
  `https://localhost:8080`. No trailing slash and no `/api/v2` suffix; Coral
  appends the API path.
- `COCKROACHDB_SESSION` — session token sent in the `X-Cockroach-API-Session`
  header.

## Authentication

The Cluster API v2 authenticates with a **session token** in the
`X-Cockroach-API-Session` header (created via `/api/v2/login/`). That is the
method this source uses.

- **Secure clusters**: set `COCKROACHDB_SESSION` to a real token.
- **Insecure clusters**: the Cluster API is served without authentication and
  ignores the header, so set `COCKROACHDB_SESSION` to any non-empty placeholder
  value (Coral requires a non-empty secret).
- **TLS**: secure clusters serve the HTTP API over TLS. The node's HTTP
  certificate must be trusted by the host running Coral, or front the cluster
  with a proxy that terminates TLS with a trusted certificate.

## Tables

| Table | Description | Endpoint |
|---|---|---|
| `health` | Per-node liveness/readiness (`liveness_status`) | `/api/v2/nodes/` |
| `nodes` | Per-node status, addresses, and build information | `/api/v2/nodes/` |
| `databases` | Databases known to the cluster | `/api/v2/databases/` |
| `metrics` | Selected per-node metrics plus the full metrics maps | `/api/v2/nodes/` |

No table requires or accepts SQL filters; filter with a `WHERE` clause after
fetching. `/api/v2/nodes/` returns all nodes in a single response, so `health`,
`nodes`, and `metrics` are not paginated; `databases` pages through results with
`limit`/`offset`, which Coral drives automatically.

## Notes

- `health.liveness_status` is CockroachDB's `NodeLivenessStatus` numeric code:
  `0` UNKNOWN, `1` DEAD, `2` UNAVAILABLE, `3` LIVE, `4` DECOMMISSIONING,
  `5` DECOMMISSIONED. The same column is also on the `nodes` table.
- `nodes.started_at` and `nodes.updated_at` are epoch **nanoseconds**
  (Int64). Divide by 1,000,000,000 for Unix seconds.
- `metrics` named columns cover common node-level health signals. Metric keys
  vary across major versions, so a named column may be NULL while the value is
  still present in the `metrics` JSON column — read any gauge with
  `json_get_float(metrics, 'sql.conns')`.
- `store_metrics` is a **best-effort** column. It carries per-store gauges
  (ranges, replicas, capacity, livebytes) keyed by store ID, e.g.
  `json_get_float(store_metrics, '1', 'ranges')`, but it is **not** part of the
  documented [Cluster API v2 response schema](https://www.cockroachlabs.com/docs/api/cluster/v2.html)
  (which guarantees the node `metrics` map). Treat it as nullable and
  version-dependent; it may be absent on some CockroachDB versions.
- `databases` lists names only. The Cluster API does not return per-database
  sizes; use a SQL client and `SHOW DATABASES` for storage details.

## Example Queries

### Cluster membership and liveness

```sql
SELECT node_id, liveness_status
FROM cockroachdb.health
ORDER BY node_id
```

### Node inventory with versions

```sql
SELECT node_id, address, sql_address, build_tag, num_cpus, total_system_memory
FROM cockroachdb.nodes
ORDER BY node_id
```

### Nodes not running the expected version

```sql
SELECT node_id, build_tag
FROM cockroachdb.nodes
WHERE build_tag <> 'v24.1.0'
```

### Open SQL connections per node

```sql
SELECT node_id, sql_conns, sql_query_count, sys_goroutines
FROM cockroachdb.metrics
ORDER BY sql_conns DESC
```

### Read an arbitrary gauge from the raw metrics map

```sql
SELECT node_id,
       json_get_float(metrics, 'sys.uptime') AS uptime_seconds,
       json_get_float(metrics, 'sql.txn.commit.count') AS txn_commits
FROM cockroachdb.metrics
ORDER BY node_id
```

### Per-store ranges, replicas, and capacity (store ID 1)

```sql
SELECT node_id,
       json_get_float(store_metrics, '1', 'ranges') AS ranges,
       json_get_float(store_metrics, '1', 'replicas') AS replicas,
       json_get_float(store_metrics, '1', 'capacity.available') AS capacity_available
FROM cockroachdb.metrics
ORDER BY node_id
```

### List databases

```sql
SELECT name
FROM cockroachdb.databases
ORDER BY name
```
