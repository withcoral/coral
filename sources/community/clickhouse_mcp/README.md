# ClickHouse Cloud MCP Connector

**Version:** 0.1.0
**Source:** ClickHouse Cloud remote MCP server
**Backend:** MCP (stdio, proxied through `mcp-remote`)
**Server URL:** `https://mcp.clickhouse.cloud/mcp`
**Surface:** 7 tables + 6 functions wrapping 13 MCP tools

This connector exposes the Cloud management API (organizations, services,
ClickPipes, backups, billing) and a SQL passthrough (`run_select_query`)
against any service in your organization.

## Setup

The Cloud MCP server is HTTP-only with OAuth, but `coral-engine`'s MCP
backend only speaks stdio. We bridge with `mcp-remote` — it runs as a
stdio child process and proxies traffic to the HTTPS endpoint.

### One-time authentication

Run `mcp-remote` once interactively to complete the OAuth flow. A browser
window opens; sign in with your ClickHouse Cloud account.

```bash
npx -y mcp-remote@0.1.37 https://mcp.clickhouse.cloud/mcp
# Browser opens automatically.
# After you authorize, the proxy prints "Proxy established successfully".
# Press Ctrl+C.
```

Tokens are cached to `~/.mcp-auth/mcp-remote-<version>/`. The access token
lasts 1 hour and refreshes automatically via the cached refresh token, so
day-to-day querying needs no further interaction.

If the refresh token ever expires, re-run the command above to re-auth.

### Register the source

```bash
coral source add --file sources/community/clickhouse_mcp/manifest.yaml
```

This registers the source and prints its catalog. The MCP child process is
not spawned here — `mcp-remote` runs (and any cached OAuth tokens are read
or refreshed) on the first real `coral sql` query. Run the `Verify` query
below to exercise the end-to-end path.

### Verify

```bash
coral sql "SELECT id, name FROM clickhouse_mcp.organizations"
```

You should see your accessible ClickHouse Cloud organizations.

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
SELECT * FROM clickhouse_mcp.run_select_query(
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
# 1. Authenticate once
npx -y mcp-remote@0.1.37 https://mcp.clickhouse.cloud/mcp   # browser flow, then Ctrl+C

# 2. Register the source
coral source add --file sources/community/clickhouse_mcp/manifest.yaml

# 3. List orgs
coral sql "SELECT id, name FROM clickhouse_mcp.organizations"

# 4. List services in an org
coral sql "
  SELECT id, name, provider, region, state, \"clickhouseVersion\"
  FROM clickhouse_mcp.services
  WHERE organization_id = '<org-id>'
"

# 5. List databases in a service
coral sql "
  SELECT name
  FROM clickhouse_mcp.databases
  WHERE service_id = '<service-id>'
"

# 6. List tables in a database
coral sql "
  SELECT name, engine, primary_key
  FROM clickhouse_mcp.tables
  WHERE service_id = '<service-id>' AND database_filter = 'default'
  LIMIT 20
"

# 7. Run SQL against the service
coral sql "
  SELECT row
  FROM clickhouse_mcp.run_select_query(
    query => 'SELECT version() AS v, currentDatabase() AS db',
    service_id => '<service-id>'
  )
"

# 8. Inspect a single service
coral sql "
  SELECT id, name, region, \"clickhouseVersion\", \"numReplicas\"
  FROM clickhouse_mcp.get_service_details(
    organization_id => '<org-id>',
    service_id => '<service-id>'
  )
"

# 9. Aggregate costs
coral sql "
  SELECT SUM(json_get_float(row, 'totalCHC')) AS total
  FROM clickhouse_mcp.organization_costs
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
SELECT "sizeInBytes" FROM clickhouse_mcp.service_backups WHERE ...
```

snake_case columns from the manifest (`organization_id`, `service_id`,
`database_filter`) work without quoting.

### Function args are named, not positional

```sql
-- WRONG: errors with "requires named arguments"
SELECT * FROM clickhouse_mcp.run_select_query('SELECT 1', '<id>')

-- RIGHT
SELECT * FROM clickhouse_mcp.run_select_query(
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
FROM clickhouse_mcp.run_select_query(
  query => 'SELECT name, engine FROM system.tables LIMIT 10',
  service_id => '<id>'
)
```

### `mcp-remote` chatter in stderr

Every scan spawns a fresh `mcp-remote` child; its handshake logs (port
discovery, OAuth check, connection established) print to stderr. That's
noise, not errors.

### Each scan spawns a new process

Coral spawns `mcp-remote` per query, so every scan does the
OAuth-token-read + connect handshake (~1s overhead). Fine for interactive
use; less ideal for many small queries in a loop. Tracked in
the MCP backend follow-up plan.

### Error responses surface as `MCP_TOOL_RETURNED_ERROR`

`run_select_query` and most other tools return a success/error union:

```json
{ "result": { "status": "error", "message": "..." } }
```

Each table and function in this manifest sets
`response.error_path: [result, message]`, so an error branch is converted
into a structured `MCP_TOOL_RETURNED_ERROR` carrying the upstream message
instead of silently producing zero rows.
