# CockroachDB Coral source

Query CockroachDB Cluster API metadata with Coral. The source is read-only and
targets DB Console inventory, schema, session, event, and user use cases.

## Setup

```bash
COCKROACHDB_BASE_URL=http://localhost:8080 \
COCKROACHDB_SESSION_TOKEN=... \
coral source add --file sources/community/cockroachdb/manifest.yaml
```

Create a session token with `cockroach auth-session login` or by calling
`POST /api/v2/login/` and copying the returned `session` value. Authenticated
Cluster API access requires a user with the CockroachDB `admin` role.

Run validation:

```bash
coral source test cockroachdb
```

## Tables

| Table | Description |
| --- | --- |
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
SELECT node_id, address, sql_address, build_tag, started_at
FROM cockroachdb.nodes
ORDER BY node_id;
```

```sql
SELECT database_name
FROM cockroachdb.databases
ORDER BY database_name;
```

```sql
SELECT table_name
FROM cockroachdb.database_tables
WHERE database = 'defaultdb'
ORDER BY table_name;
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
- `cockroachdb.databases` models the documented `databases: string[]`
  response as one `database_name` column.
- `cockroachdb.database_tables` models the documented `table_names: string[]`
  response. Values are schema-qualified, for example `public.orders`.
- `cockroachdb.nodes.started_at` is exposed as the raw Unix nanosecond integer
  returned by the API.
- The `/health/` endpoint is not exposed as a table because the published API
  documents status codes, not a stable JSON response body.
- Keep the base URL and session token scoped to a DB Console API endpoint.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/cockroachdb/manifest.yaml
make lint-sources
yamllint sources/community/cockroachdb/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed `coral source add --file`, `coral source test cockroachdb`, and
representative live queries require an admin-role CockroachDB session token and
were not run in this workspace.

## API references

- https://www.cockroachlabs.com/docs/stable/cluster-api
- https://cockroachlabs.com/docs/api/cluster/v2.html
