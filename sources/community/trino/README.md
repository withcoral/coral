# Trino

Query Trino coordinator information and the cluster's active and recently
completed query history (state, errors, timings, driver counts, memory usage)
through the Trino cluster REST API.

## Setup

### Requirements

- Network access to a Trino coordinator's HTTP endpoint (default port
  `8080`, or `8443` for TLS clusters).
- An identity your cluster's access control allows to read query state. On
  clusters without authentication, any non-empty value works.

### Add the Source

Set the inputs as environment variables, then add the source from this
manifest:

```bash
export TRINO_URL=http://localhost:8080
export TRINO_USER=coral
coral source add --file sources/community/trino/manifest.yaml
```

Inputs:

- `TRINO_URL` — coordinator base URL including scheme and port, e.g.
  `http://localhost:8080` or `https://coordinator.example.com:8443`.
  No trailing slash.
- `TRINO_USER` — identity sent in the `X-Trino-User` header
  (defaults to `coral`).

## Tables

### `info`
Single-row coordinator server information from `/v1/info`.

**Useful for:**
- Version and environment reporting
- Readiness checks (`starting = false`, `state = 'ACTIVE'`)

### `queries`
Active and recently completed queries from `/v1/query`.

**Useful for:**
- Finding long-running or stuck queries (`state = 'RUNNING'`)
- Triaging failures by `error_type` / `error_code_name`
- Spotting expensive queries via CPU time, input rows, and memory
- Per-user and per-catalog query attribution

## Authentication

Trino requires a user identity on requests to `/v1/query`. This source sends
it as the `X-Trino-User` header:

```text
X-Trino-User: <TRINO_USER>
```

Username/password (`BasicAuth`) and OAuth2 authentication are **not**
configured by this source. It targets clusters that accept header-based user
identity (the common default), or clusters fronted by a proxy that injects
auth.

## Limits

- This source is **read-only** and intentionally does **not** run SQL.
  It does not use the `/v1/statement` query-submission protocol, so it adds
  no query-execution load and needs no result pagination.
- `queries` reflects only what the coordinator still holds in memory:
  in-flight queries plus a bounded recent history. Evicted older queries are
  not returned — this is a live operational view, not a complete audit log.
- The `/v1/node` endpoint was removed in modern Trino, so a worker-node
  table is intentionally not included. Inspect nodes through the
  `system.runtime.nodes` table with a regular Trino SQL client instead.
- Duration and data-size fields (`elapsed_time`, `total_cpu_time`,
  `physical_input_data_size`, `peak_user_memory_reservation`, etc.) are
  human-readable strings exactly as Trino returns them (e.g. `439.69ms`,
  `0B`, `4.5GB`), typed as `Utf8`. `create_time` and `end_time` are real
  `Timestamp` columns.
- `error_type`, `error_code_name`, and `error_code` are populated only when
  `state = 'FAILED'`.
- No server-side filtering: filter with SQL `WHERE` after fetching.

## Example Queries

### Coordinator readiness

```sql
SELECT version, environment, state, starting, uptime
FROM trino.info
```

### Currently running queries, oldest first

```sql
SELECT query_id, user, state, elapsed_time, query
FROM trino.queries
WHERE state = 'RUNNING'
ORDER BY create_time ASC
```

### Recent failures grouped by error code

```sql
SELECT error_code_name, error_type, COUNT(*) AS failures
FROM trino.queries
WHERE state = 'FAILED'
GROUP BY error_code_name, error_type
ORDER BY failures DESC
```

### Highest-throughput finished queries by input rows

```sql
SELECT user, query_id, processed_input_positions, total_cpu_time
FROM trino.queries
WHERE state = 'FINISHED'
ORDER BY processed_input_positions DESC
LIMIT 20
```

## Notes

- Verified against Trino 481. The `/v1/info` and `/v1/query` endpoints back
  the Trino Web UI and are stable across recent releases; `/v1/node` is not
  used because it no longer exists.
- For deep per-query analysis, use the `self_uri` value (the coordinator's
  `/v1/query/{queryId}` document) with a regular HTTP client.
