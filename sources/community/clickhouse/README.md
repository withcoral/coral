# ClickHouse (Community)

**Version:** 0.1.0
**Backend:** HTTP (ClickHouse System Tables Interface API)
**Tables:** 4
**Base URL:** `{{input.CLICKHOUSE_URL}}`

Query ClickHouse database inventory, storage telemetry, merge activity, and replication health directly through Coral SQL using the ClickHouse HTTP interface.

This integration is intended for infrastructure observability and operational auditing workflows across ClickHouse deployments.

Coral exposes read-only `GET` tables. Data mutations, cluster configuration changes, and analytical query execution against arbitrary business datasets are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/clickhouse/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `CLICKHOUSE_URL` | variable | yes | ClickHouse HTTP interface URL with port, for example `http://localhost:8123` |
| `CLICKHOUSE_USER` | variable | yes | Database user with access to system tables |
| `CLICKHOUSE_PASSWORD` | secret | yes | Password for the specified ClickHouse user |

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `databases` | `GET /?query=...` | — | None (maps from JSON `data` array) |
| `tables` | `GET /?query=...` | — | None (maps from JSON `data` array) |
| `merges` | `GET /?query=...` | — | None (maps from JSON `data` array) |
| `replicas` | `GET /?query=...` | — | None (maps from JSON `data` array) |

---

# Table Reference

## clickhouse.databases

Metadata inventory of configured ClickHouse databases.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Logical database name |
| `engine` | Utf8 | Database engine type |
| `data_path` | Utf8 | Disk path containing database data |
| `metadata_path` | Utf8 | File system path containing metadata definitions |

---

## clickhouse.tables

Storage sizing and telemetry metrics for database tables.

| Column | Type | Description |
|---|---|---|
| `database` | Utf8 | Parent database namespace |
| `table_name` | Utf8 | Table identifier |
| `engine` | Utf8 | Storage engine type |
| `total_rows` | Int64 | Total number of rows stored in the table |
| `total_bytes` | Int64 | Total storage size consumed by the table |

---

## clickhouse.merges

Real-time diagnostics for active background merges and mutations.

| Column | Type | Description |
|---|---|---|
| `database` | Utf8 | Database containing the active merge |
| `table_name` | Utf8 | Table currently undergoing a merge operation |
| `elapsed` | Float64 | Merge execution time in seconds |
| `progress` | Float64 | Merge completion percentage |
| `num_parts` | Int64 | Number of source parts participating in the merge |
| `result_part_name` | Utf8 | Name of the resulting merged part |

---

## clickhouse.replicas

Replication lag and synchronization health telemetry.

| Column | Type | Description |
|---|---|---|
| `database` | Utf8 | Database containing the replica |
| `table_name` | Utf8 | Replicated table name |
| `is_leader` | Int64 | Indicates whether the replica is the current leader |
| `can_become_leader` | Int64 | Indicates whether the replica can become leader |
| `absolute_delay` | Int64 | Replication lag in seconds |
| `queue_size` | Int64 | Number of queued replication operations |

---

# Example Queries

## Top 5 Tables by Storage Footprint

```sql
SELECT
  database,
  table_name,
  engine,
  total_bytes
FROM clickhouse.tables
ORDER BY total_bytes DESC
LIMIT 5;
```

---

## Monitor High Replication Delay

```sql
SELECT
  database,
  table_name,
  absolute_delay,
  queue_size
FROM clickhouse.replicas
WHERE absolute_delay > 60
   OR queue_size > 10
ORDER BY absolute_delay DESC;
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
coral source lint sources/community/clickhouse/manifest.yaml
```

## Execute Live Connection Test

```bash
export CLICKHOUSE_URL=http://localhost:8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=your_secure_password

coral source add --file sources/community/clickhouse/manifest.yaml
coral source test clickhouse
```

---

# Representative Live Output

```text
$ coral source test clickhouse

✓ clickhouse connected successfully

  clickhouse (4 tables)
  ├─ databases
  ├─ tables
  ├─ merges
  └─ replicas

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT name FROM clickhouse.databases LIMIT 1

+---------+
| name    |
+---------+
| default |
+---------+

1 row
```

---

# Limitations

- Read-only retrieval scope
- Does not execute analytical queries against arbitrary business datasets
- Focuses exclusively on ClickHouse system tables and operational telemetry
- Does not expose query logs, distributed query traces, or custom monitoring extensions
- Uses ClickHouse `FORMAT JSON` responses and extracts rows from the JSON `data` array
- Large deployments with extensive table inventories may require targeted SQL filtering for optimal performance
