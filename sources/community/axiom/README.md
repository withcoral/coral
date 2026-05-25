# Axiom

Query datasets, annotations, monitors, and notifiers from
[Axiom](https://axiom.co/) — the cloud-native log management and observability
platform.

> **Note:** This source exposes Axiom **configuration metadata** only —
> datasets, annotations, monitors, and notifiers. It does not query the event
> or log data stored inside datasets. To run APL queries over log/event data,
> use Axiom's own query interface.

## Authentication

Requires an **Axiom API token**. Personal Access Tokens (PATs) work for
development but for production workloads use a dedicated API token scoped to
the minimum required permissions.

1. Log in to Axiom → **Settings** → **API Tokens** → **New API Token**.
2. Grant **Read** access to datasets, annotations, monitors, and notifiers.
3. Copy the generated token.

**US region (default):**

```sh
export AXIOM_API_TOKEN="xaat-..."
coral source add --file sources/community/axiom/manifest.yaml
```

**EU region:** Axiom workspaces on the EU data-residency region must also set
`AXIOM_API_BASE`. The default is the US endpoint; EU workspaces are unreachable
through it.

```sh
export AXIOM_API_TOKEN="xaat-..."
export AXIOM_API_BASE="https://api.eu.axiom.co"
coral source add --file sources/community/axiom/manifest.yaml
```

See [Axiom API authentication docs](https://axiom.co/docs/restapi/introduction)
for details on token types, scopes, rate limits, and rotation.

## Tables

| Table | Description | Optional filters |
|---|---|---|
| `axiom.datasets` | All datasets accessible to the API token | — |
| `axiom.annotations` | Deployment, incident, and custom event annotations | `datasets`, `start`, `end` |
| `axiom.monitors` | Alert monitor definitions with threshold and query config | — |
| `axiom.notifiers` | Notification channel configurations (Slack, email, webhook, etc.) | — |

### `axiom.annotations` filters

The `datasets`, `start`, and `end` filters are all optional and are pushed
down to the Axiom API as query parameters.

| Filter | Type | Description |
|---|---|---|
| `datasets` | `Utf8` | Filter by dataset name (e.g. `WHERE datasets = 'my-app'`) |
| `start` | `Utf8` | RFC3339 lower bound (e.g. `'2024-01-01T00:00:00Z'`) |
| `end` | `Utf8` | RFC3339 upper bound (e.g. `'2024-12-31T23:59:59Z'`) |

`start` and `end` are **virtual** — they are sent to the API but do not appear
as columns in query results.

## Example queries

### List all datasets with retention info

```sql
SELECT
  name,
  description,
  kind,
  retention_days,
  can_write,
  created
FROM axiom.datasets
ORDER BY created DESC;
```

### List recent deploy annotations

```sql
SELECT
  id,
  title,
  type,
  time,
  end_time,
  url,
  datasets
FROM axiom.annotations
WHERE start = '2024-01-01T00:00:00Z'
ORDER BY time DESC
LIMIT 50;
```

### Filter annotations by dataset name

```sql
SELECT
  id,
  title,
  type,
  time,
  description
FROM axiom.annotations
WHERE datasets = 'production-api'
  AND start = '2024-06-01T00:00:00Z'
  AND end   = '2024-06-30T23:59:59Z'
ORDER BY time DESC;
```

### List all active monitors

```sql
SELECT
  id,
  name,
  type,
  operator,
  threshold,
  interval_minutes,
  range_minutes,
  alert_on_no_data,
  resolvable
FROM axiom.monitors
WHERE COALESCE(disabled, false) = false
ORDER BY name;
```

### Find threshold monitors above a value

```sql
SELECT
  id,
  name,
  apl_query,
  column_name,
  operator,
  threshold,
  notifier_ids
FROM axiom.monitors
WHERE type      = 'Threshold'
  AND operator  = 'Above'
  AND threshold > 100
ORDER BY threshold DESC;
```
