# ClickHouse source

Query ClickHouse databases, tables, columns, query history, running processes,
server metrics, storage parts, and replication health via the ClickHouse HTTP
interface (port 8123).

## Setup

### 1. Prerequisites

- ClickHouse instance accessible over HTTP or HTTPS
- A ClickHouse user with `SELECT` access to the `system` database

### 2. Create a dedicated user (recommended)

```sql
CREATE USER coral_reader IDENTIFIED BY 'your_password';
GRANT SELECT ON system.* TO coral_reader;
```

### 3. Password requirement

Coral requires a non-empty password for secret inputs. If your default user
has no password, set one first:

```sql
ALTER USER default IDENTIFIED BY 'your_password';
```

### 4. Install the source

```bash
CLICKHOUSE_HOST=http://localhost:8123 \
CLICKHOUSE_USER=coral_reader \
CLICKHOUSE_PASSWORD=your_password \
coral source add --file manifest.yaml
```

For ClickHouse Cloud, use `https://<host>.clickhouse.cloud:8443` as the host.
Do not include a trailing slash.

## Tables

| Table | Description |
|---|---|
| `databases` | All databases visible to the configured user |
| `tables` | Tables across all databases with engine, row count, size, and key expressions |
| `columns` | Column names, types, key membership, and compression stats |
| `query_log` | Last 1000 query log entries across all event types including failures |
| `processes` | Live snapshot of currently executing queries |
| `metrics` | Instantaneous server counters (connections, merges, memory) |
| `parts` | Active MergeTree data parts with partition and storage statistics |
| `replicas` | Replication queue depth and health (returns 0 rows on standalone instances) |

## Example queries

```sql
-- Find the largest tables by size
SELECT database, name, engine, total_rows, total_bytes
FROM clickhouse.tables
WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
ORDER BY total_bytes DESC
LIMIT 20;

-- Slowest recent queries
SELECT query, user, query_duration_ms, read_rows, memory_usage
FROM clickhouse.query_log
WHERE type = 'QueryFinish'
ORDER BY query_duration_ms DESC
LIMIT 10;

-- Failed queries with errors
SELECT event_time, user, query, exception
FROM clickhouse.query_log
WHERE type = 'ExceptionWhileProcessing'
ORDER BY event_time DESC
LIMIT 20;

-- Correlate query start and finish rows
SELECT
  s.query_id,
  s.query,
  s.event_time AS started_at,
  f.event_time AS finished_at,
  f.query_duration_ms
FROM clickhouse.query_log s
JOIN clickhouse.query_log f ON s.query_id = f.query_id
WHERE s.type = 'QueryStart'
  AND f.type = 'QueryFinish'
LIMIT 10;

-- Tables with the most parts (merge pressure indicator)
SELECT database, table, COUNT(*) AS part_count, SUM(rows) AS total_rows
FROM clickhouse.parts
WHERE database NOT IN ('system')
GROUP BY database, table
ORDER BY part_count DESC
LIMIT 20;

-- Check replication lag
SELECT database, table, absolute_delay, queue_size, active_replicas, total_replicas
FROM clickhouse.replicas
WHERE absolute_delay > 0
ORDER BY absolute_delay DESC;

-- Live running queries
SELECT query_id, user, elapsed, read_rows, memory_usage, query
FROM clickhouse.processes
ORDER BY elapsed DESC;
```

## Notes

- **`query_log`** is only populated when `log_queries = 1` is set in the
  ClickHouse server configuration. An empty result may mean logging is disabled.
- **`parts`** can return very large result sets on busy production clusters.
  Always filter by `database` or `table` in WHERE clauses for specific lookups,
  and use LIMIT for exploratory queries.
- **`replicas`** returns 0 rows on standalone (non-replicated) instances —
  this is expected, not an error.
- **`processes`** includes the Coral query fetching the table as one of the
  returned rows.
- Statistics columns (`viewCount`, `total_rows`, `total_bytes`) are typed
  `Int64` — very large tables (petabyte scale) could theoretically overflow,
  but this is not a practical concern for most deployments.
