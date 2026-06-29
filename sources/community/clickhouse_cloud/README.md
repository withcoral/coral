# ClickHouse Cloud MCP Connector

**Version:** 0.2.0
**Source:** ClickHouse Cloud remote MCP server
**Backend:** MCP (Streamable HTTP, native)
**Server URL:** `https://mcp.clickhouse.cloud/mcp`
**Surface:** 7 tables + 6 functions wrapping 13 MCP tools

This connector exposes the Cloud management API (organizations, services,
ClickPipes, backups, billing) and a SQL passthrough (`run_select_query`)
against any service in your organization.

## Setup

Coral talks to `mcp.clickhouse.cloud` directly over the MCP Streamable HTTP
transport and drives the OAuth flow itself — no `mcp-remote` proxy and no
manual token handling required.

### Register the source

```bash
coral source add --file sources/community/clickhouse_cloud/manifest.yaml --interactive
```

When prompted for `CLICKHOUSE_ACCESS_TOKEN`, choose **Connect with ClickHouse
Cloud**. Coral binds a loopback callback on port 53683, opens your browser
to ClickHouse's authorization page, exchanges the resulting code for an
access token, and stores it as the source's secret. The catalog prints
immediately after.

### Verify

```bash
coral sql "SELECT id, name FROM clickhouse_cloud.organizations"
```

You should see your accessible ClickHouse Cloud organizations.

### Re-authenticating

Access tokens expire after one hour. Automatic refresh is on the MCP-HTTP
follow-up plan; until then, when queries start failing with
`MCP_AUTH_REQUIRED`, re-run the same `coral source add` command above to
mint a fresh access token. The browser may complete the flow without
re-prompting if the session at ClickHouse is still valid.

## Tables

All tables that require a filter fail planning if the filter is missing —
you'll get a clear error rather than a runaway scan.

### No filter required

| Table | Description |
|---|---|
| `organizations` | All ClickHouse Cloud organizations accessible to the authenticated user |

### Filter: `organization_id`

| Table | Description |
|---|---|
| `services` | All services in an organization |
| `organization_costs` | Daily, per-entity organization usage cost records. Optional `from_date`, `to_date` filters |

### Filter: `organization_id` + `service_id`

| Table | Description |
|---|---|
| `clickpipes` | All ClickPipes configured for a specific service |
| `service_backups` | Backups for a service, most recent first |

### Filter: `service_id`

| Table | Description |
|---|---|
| `databases` | All databases in a ClickHouse service (each row is a database name) |

### Filter: `service_id` + `database_filter`

| Table | Description |
|---|---|
| `tables` | All tables in a database, including engine, primary key, and size metadata |

> Note: the filter is named `database_filter` rather than `database` to
> avoid colliding with the `database` field returned in each row.

## Functions

All functions require **named arguments**, not positional:

```sql
SELECT * FROM clickhouse_cloud.run_select_query(
  query => 'SELECT 1',
  service_id => '...'
)
```

| Function | Required args | Returns |
|---|---|---|
| `run_select_query` | `query`, `service_id` | Each row of the SQL result wrapped as a `row: Json` column. Use JSON accessors to project specific fields. |
| `get_organization_details` | `organization_id` | One row with org metadata, private endpoints, BYOC config |
| `get_service_details` | `organization_id`, `service_id` | One row with service config (provider, region, memory, replicas, endpoints, ...) |
| `get_clickpipe` | `organization_id`, `service_id`, `clickpipe_id` | One row with full ClickPipe definition (source, destination, mappings) |
| `get_service_backup_details` | `organization_id`, `service_id`, `backup_id` | One row with backup metadata |
| `get_service_backup_configuration` | `organization_id`, `service_id` | One row: `backupPeriodInHours`, `backupRetentionPeriodInHours`, `backupStartTime` |

## Discovery flow

Most queries need IDs from a parent resource. Walk the tree:

```text
organizations.id
  → services.id (WHERE organization_id = ...)
      → databases.name (WHERE service_id = ...)
          → tables.name (WHERE service_id = ... AND database_filter = ...)
              → run_select_query(query, service_id)
      → clickpipes.id (WHERE organization_id = ... AND service_id = ...)
          → get_clickpipe(organization_id, service_id, clickpipe_id)
      → service_backups.id (WHERE organization_id = ... AND service_id = ...)
          → get_service_backup_details(organization_id, service_id, backup_id)
```

## Quick start

```bash
# 1. Register the source (browser opens automatically for OAuth)
coral source add --file sources/community/clickhouse_cloud/manifest.yaml --interactive

# 2. List orgs
coral sql "SELECT id, name FROM clickhouse_cloud.organizations"

# 3. List services in an org
coral sql "
  SELECT id, name, provider, region, state, \"clickhouseVersion\"
  FROM clickhouse_cloud.services
  WHERE organization_id = '<org-id>'
"

# 4. List databases in a service
coral sql "
  SELECT name
  FROM clickhouse_cloud.databases
  WHERE service_id = '<service-id>'
"

# 5. List tables in a database
coral sql "
  SELECT name, engine, primary_key
  FROM clickhouse_cloud.tables
  WHERE service_id = '<service-id>' AND database_filter = 'default'
  LIMIT 20
"

# 6. Run SQL against the service
coral sql "
  SELECT row
  FROM clickhouse_cloud.run_select_query(
    query => 'SELECT version() AS v, currentDatabase() AS db',
    service_id => '<service-id>'
  )
"

# 7. Inspect a single service
coral sql "
  SELECT id, name, region, \"clickhouseVersion\", \"numReplicas\"
  FROM clickhouse_cloud.get_service_details(
    organization_id => '<org-id>',
    service_id => '<service-id>'
  )
"

# 8. Aggregate costs
coral sql "
  SELECT SUM(json_get_float(row, 'totalCHC')) AS total
  FROM clickhouse_cloud.organization_costs
  WHERE organization_id = '<org-id>'
    AND from_date = '2026-04-01'
    AND to_date = '2026-05-01'
"
```

## Gotchas

### camelCase columns

The Cloud API returns camelCase fields (`clickhouseVersion`, `sizeInBytes`,
`numReplicas`, `createdAt`, ...). DataFusion lowercases unquoted
identifiers, so you must double-quote them in SQL:

```sql
SELECT "sizeInBytes" FROM clickhouse_cloud.service_backups WHERE ...
```

snake_case columns from the manifest (`organization_id`, `service_id`,
`database_filter`) work without quoting.

### Function args are named, not positional

```sql
-- WRONG: errors with "requires named arguments"
SELECT * FROM clickhouse_cloud.run_select_query('SELECT 1', '<id>')

-- RIGHT
SELECT * FROM clickhouse_cloud.run_select_query(
  query => 'SELECT 1',
  service_id => '<id>'
)
```

### Dynamic-shape rows from `run_select_query`

The result column is a single `row: Json` — the result row from the
ClickHouse SQL query. To project a specific field:

```sql
SELECT
  json_get_str(row, 'name')   AS name,
  json_get_str(row, 'engine') AS engine
FROM clickhouse_cloud.run_select_query(
  query => 'SELECT name, engine FROM system.tables LIMIT 10',
  service_id => '<id>'
)
```

### Each tool call opens a fresh MCP session

Coral creates a new Streamable HTTP MCP session for every underlying
`tools/call` — not once per SQL query. A single query that scans several
tables, or that joins across two MCP tables, runs the initialize +
notifications/initialized handshake for each tool call, adding a few
hundred ms of latency per call. Session pooling is tracked in the MCP
backend follow-up plan.

### Error responses surface as `MCP_TOOL_RETURNED_ERROR`

`run_select_query` and most other tools return a success/error union:

```json
{ "result": { "status": "error", "message": "..." } }
```

Each table and function in this manifest sets
`response.error_path: [result, message]`, so an error branch is converted
into a structured `MCP_TOOL_RETURNED_ERROR` carrying the upstream message
instead of silently producing zero rows.

Service-idle responses surface the same way — when a service is woken on
demand, you'll see `Service ... is currently idle. A wake command has been
sent. Please try again shortly.` Retry after a few seconds.
