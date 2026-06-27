# Netdata (Community)

**Version:** 0.1.0
**Backend:** HTTP (Netdata Agent REST API)
**Tables:** 3
**Base URL:** `{{input.NETDATA_URL}}`

Query Netdata host metadata, active alarms, and the metric context catalog directly through Coral SQL using the Netdata **Agent** API.

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
| `NETDATA_URL` | variable | yes | Netdata **Agent** base URL with port, for example `http://localhost:19999`. Point it at the Agent's HTTP endpoint, not at Netdata Cloud. |

---

# Authentication

**None is required, and this source sends no credentials.**

This source targets a Netdata **Agent**. The Agent's data APIs (`/api/v1`, `/api/v3`) are public by default — they are governed by IP-based access control (`allow dashboard from` in `netdata.conf`), not by a token.

A few clarifications, since this is easy to get wrong:

- **Netdata Cloud API tokens** (created at app.netdata.cloud) authenticate against the Netdata **Cloud** API, a different product surface. They do **not** apply to a self-hosted Agent and are not used here.
- The Agent can optionally enable **bearer protection** (via `/api/v3/bearer_protection`) or sit behind an authenticating reverse proxy. Coral does not currently send a bearer token to the Agent, so for those setups point `NETDATA_URL` at an endpoint Coral can reach without app-level authentication (for example, an internal address allowed by the Agent's ACL). Token-based Agent access may be added in a future revision.

---

# Tables Overview

| Table | API Endpoint | Pagination |
|---|---|---|
| `nodes` | `GET /api/v1/info` | None (single object response) |
| `alarms` | `GET /api/v1/alarms?all=true` | None (iterates the alarms dictionary) |
| `metrics_metadata` | `GET /api/v3/contexts?options=titles` | None (iterates the contexts dictionary) |

> Netdata's current API is **v3**. `metrics_metadata` uses `/api/v3/contexts`, which replaces the deprecated `/api/v1/charts` endpoint for context/metric metadata. `nodes` and `alarms` continue to use their `/api/v1` endpoints, which remain functional; they can move to v3 equivalents in a later revision.

Rows from `alarms` and `metrics_metadata` come from JSON **dictionaries** (keyed objects), so Coral uses the `dict_entries` row strategy: each entry's fields are read directly, and the dictionary key is exposed via the `_key` field (surfaced as `alarm_id` / `context_id`).

---

# Table Reference

## netdata.nodes

Host runtime metadata and operating system information (`/api/v1/info`).

| Column | Type | Description |
|---|---|---|
| `version` | Utf8 | Netdata agent version |
| `os_name` | Utf8 | Operating system distribution name |
| `os_version` | Utf8 | Operating system version |
| `kernel_version` | Utf8 | Running kernel version |
| `cpu_cores` | Int64 | Number of monitored CPU cores |

---

## netdata.alarms

Active and configured health alarm states (`/api/v1/alarms?all=true`).

| Column | Type | Description |
|---|---|---|
| `alarm_id` | Utf8 | Alarm dictionary key |
| `alarm_name` | Utf8 | Health alert rule name |
| `chart` | Utf8 | Chart the alarm is attached to |
| `status` | Utf8 | Current alarm state (e.g., WARNING, CRITICAL, CLEAR) |
| `value` | Float64 | Most recent evaluated value |
| `family` | Utf8 | Subsystem classification |
| `recipient` | Utf8 | Notification routing target |

---

## netdata.metrics_metadata

Metric context catalog from the current v3 contexts API (`/api/v3/contexts?options=titles`).

| Column | Type | Description |
|---|---|---|
| `context_id` | Utf8 | Unique context identifier (e.g., `system.cpu`, `disk.io`) |
| `title` | Utf8 | Human-readable context title |
| `units` | Utf8 | Measurement units (e.g., percentage, MiB/s) |
| `family` | Utf8 | Subsystem family |
| `priority` | Int64 | Relative display priority (lower sorts higher) |
| `live` | Boolean | Whether the context is currently collecting |

Per-context dimensions are available from the same endpoint via the `dimensions` option but are not modeled as a separate table in this revision.

---

# Example Queries

## Find Active Warning or Critical Alarms

```sql
SELECT alarm_id, alarm_name, chart, family, status, value
FROM netdata.alarms
WHERE status IN ('WARNING', 'CRITICAL')
ORDER BY value DESC;
```

## Inventory Percentage-Based Metric Contexts

```sql
SELECT context_id, title, family, priority
FROM netdata.metrics_metadata
WHERE units = 'percentage'
ORDER BY context_id ASC;
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

coral source add --file sources/community/netdata/manifest.yaml
coral source test netdata
coral sql "SELECT context_id, title, units FROM netdata.metrics_metadata LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test netdata`
> run against this manifest. Do not ship placeholder output.

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
  1 row
```

---

# Limitations

- Read-only retrieval scope.
- No credentials are sent; the Agent API must be reachable under its IP-based ACL. Agent bearer protection and authenticating proxies are not yet supported.
- Does not expose raw historical timeseries data (`/api/v3/data`).
- `metrics_metadata` uses the current `/api/v3/contexts` endpoint; `nodes` and `alarms` use `/api/v1` endpoints, which Netdata marks deprecated but still serves.
- `alarms` and `metrics_metadata` are modeled with Coral's `dict_entries` strategy because Netdata returns keyed dictionaries rather than arrays.
- Large monitoring environments with extensive context catalogs may need targeted SQL filtering.
