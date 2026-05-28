# CockroachDB Coral source

Query CockroachDB Cluster API metadata with Coral. The source is read-only and
targets DB Console inventory, schema, session, event, user, and health use
cases.

## Setup

```bash
COCKROACHDB_BASE_URL=http://localhost:8080 \
COCKROACHDB_SESSION_TOKEN=... \
coral source add --file sources/community/cockroachdb/manifest.yaml
```

Create a session token with `cockroach auth-session login` or by calling
`POST /api/v2/login/` and copying the returned `session` value.

Run validation:

```bash
coral source test cockroachdb
```

## Tables

| Table | Description |
| --- | --- |
| `cockroachdb.health` | Cluster health response. |
| `cockroachdb.nodes` | Cluster nodes and build metadata. |
| `cockroachdb.databases` | Cluster databases. |
| `cockroachdb.database_details` | Descriptor metadata for a required database. |
| `cockroachdb.database_grants` | Privileges granted on a required database. |
| `cockroachdb.database_tables` | Tables in a required database. |
| `cockroachdb.table_details` | Table schema, grants, indexes, ranges, and zone config. |
| `cockroachdb.events` | Recent cluster events. |
| `cockroachdb.sessions` | Active SQL sessions, optionally filtered by username. |
| `cockroachdb.users` | SQL users and roles. |

## Example queries

```sql
SELECT node_id, address, sql_address, build_info__tag, started_at
FROM cockroachdb.nodes
ORDER BY node_id;
```

```sql
SELECT timestamp, event_type, target_id, reporting_id
FROM cockroachdb.events
ORDER BY timestamp DESC
LIMIT 50;
```

```sql
SELECT database, table, range_count, indexes_json
FROM cockroachdb.table_details
WHERE database = 'defaultdb' AND table = 'orders';
```

```sql
SELECT username, application_name, client_address, active_queries_json
FROM cockroachdb.sessions
WHERE username = 'app_user'
LIMIT 50;
```

## Notes

- This source does not execute arbitrary SQL.
- The Cluster API uses `X-Cockroach-API-Session` session-token authentication
  for all endpoints except `/health` and `/login`.
- Keep the base URL and session token scoped to a DB Console API endpoint.

## API references

- https://www.cockroachlabs.com/docs/stable/cluster-api
- https://cockroachlabs.com/docs/api/cluster/v2.html
