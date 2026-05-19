# SigNoz

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** your SigNoz instance URL (set via `SIGNOZ_URL`)

Query services, logs, traces, dashboards, and alerts from SigNoz
(Cloud or self-hosted).

## Authentication

Requires a `SIGNOZ_API_KEY` from a service account with the **SigNoz-Viewer** role.

To create one:

1. Open **Settings > Service Accounts** in your SigNoz instance.
2. Click **New Service Account**, give it a name, and click **Create**.
3. In the **Overview** tab, assign the **SigNoz-Viewer** role and click **Save**.
4. Switch to the **Keys** tab, click **Add Key**, enter a name, and click **Create**.
5. Copy the key immediately -- it is shown only once.

See the [SigNoz service accounts docs](https://signoz.io/docs/manage/administrator-guide/iam/service-accounts/).

```bash
SIGNOZ_URL=https://signoz.example.com \
SIGNOZ_API_KEY=<your-key> \
  coral source add --file sources/community/signoz/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
SIGNOZ_URL=https://signoz.example.com \
SIGNOZ_API_KEY=<your-key> \
  coral source add --file sources/community/signoz/manifest.yaml --interactive
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `services` | APM services reporting to SigNoz | -- |
| `logs` | Log query results via query_range API | `start_time`, `end_time` |
| `traces` | Trace query results via query_range API | `start_time`, `end_time` |
| `dashboards` | Dashboards configured in SigNoz | -- |
| `alerts` | Alert rules configured in SigNoz | -- |

### Time filter note

`start_time` and `end_time` on the `logs` and `traces` tables are **epoch milliseconds**.
Each query returns result envelopes from `data.data.results`; the `rows` column
contains the actual log or span objects as JSON. Returns an empty result set when
no data exists for the given time range.

## Quick start

```bash
# List all instrumented services
coral sql "
  SELECT service_name, p99, avg_duration, num_calls, error_rate
  FROM signoz.services
  ORDER BY error_rate DESC
"

# Search recent logs (supply epoch-ms timestamps)
coral sql "
  SELECT query_name, next_cursor, rows
  FROM signoz.logs
  WHERE start_time = 1700000000000
    AND end_time   = 1700003600000
"

# Search recent trace spans
coral sql "
  SELECT query_name, next_cursor, rows
  FROM signoz.traces
  WHERE start_time = 1700000000000
    AND end_time   = 1700003600000
"

# List all dashboards
coral sql "
  SELECT uuid, title, description, created_by, updated_at
  FROM signoz.dashboards
"

# List all alert rules
coral sql "
  SELECT id, alert, alert_type, state, severity, disabled
  FROM signoz.alerts
  ORDER BY state, alert
"
```

## Discovery order

```text
services
  -> service_name

logs   (WHERE start_time = ... AND end_time = ...)
  -> rows (JSON array of log objects)

traces (WHERE start_time = ... AND end_time = ...)
  -> rows (JSON array of span objects)

dashboards
  -> uuid

alerts
  -> id
```
