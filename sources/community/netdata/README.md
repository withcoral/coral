# Netdata (Community)

**Version:** 0.1.0
**Backend:** HTTP (Netdata Agent v1 REST API)
**Tables:** 3
**Base URL:** `{{input.NETDATA_URL}}/api/v1`

Query Netdata host metadata, active alarms, and metrics catalog definitions directly through Coral SQL using the Netdata agent API.

This integration is intended for infrastructure observability and operational auditing workflows across monitored systems.

Coral exposes read-only `GET` tables. Modifying alarm definitions, collector configuration, telemetry pipelines, or querying raw historical timeseries streams is out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/netdata/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `NETDATA_URL` | variable | yes | Netdata agent base URL with port, for example `http://localhost:19999` |
| `NETDATA_TOKEN` | secret | no | Optional bearer token if the Netdata endpoint is protected behind an authentication proxy |

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `nodes` | `GET /info` | — | None (returns a single object response) |
| `alarms` | `GET /alarms?all=true` | — | None (iterates over alarm dictionary entries) |
| `metrics_metadata` | `GET /charts` | — | None (iterates over chart dictionary entries) |

---

# Table Reference

## netdata.nodes

Host runtime metadata and operating system information.

| Column | Type | Description |
|---|---|---|
| `version` | Utf8 | Netdata agent version |
| `os_name` | Utf8 | Operating system distribution name |
| `os_version` | Utf8 | Operating system version |
| `kernel_version` | Utf8 | Running kernel version |
| `cpu_cores` | Int64 | Total monitored CPU cores |

---

## netdata.alarms

Active and configured system alarm definitions.

| Column | Type | Description |
|---|---|---|
| `alarm_id` | Utf8 | Alarm dictionary entry key |
| `alarm_name` | Utf8 | Technical alarm name |
| `chart` | Utf8 | Chart associated with the alarm |
| `status` | Utf8 | Current alarm state |
| `value` | Float64 | Most recent evaluated alarm value |
| `family` | Utf8 | Alarm subsystem classification |
| `recipient` | Utf8 | Notification routing target |

---

## netdata.metrics_metadata

Chart catalog and metric metadata definitions.

| Column | Type | Description |
|---|---|---|
| `chart_id` | Utf8 | Unique chart identifier |
| `chart_name` | Utf8 | Human-readable chart name |
| `title` | Utf8 | Chart display title |
| `unit` | Utf8 | Metric measurement unit |
| `chart_type` | Utf8 | Visualization chart type |

---

# Example Queries

## Find Active Warning or Critical Alarms

```sql
SELECT
  alarm_id,
  alarm_name,
  chart,
  family,
  status,
  value
FROM netdata.alarms
WHERE status IN ('WARNING', 'CRITICAL')
ORDER BY value DESC;
```

---

## Inventory Percentage-Based Metrics

```sql
SELECT
  chart_id,
  chart_name,
  title,
  chart_type
FROM netdata.metrics_metadata
WHERE unit = 'percentage'
ORDER BY chart_id ASC;
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
coral source lint sources/community/netdata/manifest.yaml
```

## Execute Live Connection Test

```bash
export NETDATA_URL=http://localhost:19999
export NETDATA_TOKEN=your_optional_bearer_token

coral source add --file sources/community/netdata/manifest.yaml
coral source test netdata
```

---

# Representative Live Output

```text
$ coral source test netdata

✓ netdata connected successfully

  netdata (3 tables)
  ├─ nodes
  ├─ alarms
  └─ metrics_metadata

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT os_name, kernel_version FROM netdata.nodes LIMIT 1

+---------+--------------------+
| os_name | kernel_version     |
+---------+--------------------+
| ubuntu  | 5.15.0-101-generic |
+---------+--------------------+

1 row
```

---

# Limitations

- Read-only retrieval scope
- Does not expose raw historical timeseries data (`/data`) APIs
- Intended for metadata inventory and operational monitoring workflows
- `/alarms` and `/charts` endpoints are modeled using Coral `dict_entries` response traversal because Netdata returns dictionary-based payloads rather than flat arrays
- Large monitoring environments with extensive chart catalogs may require targeted SQL filtering for optimal performance
