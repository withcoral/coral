# CouchDB Coral source

Query CouchDB server, database inventory, task, and replication scheduler
metadata with Coral. This source intentionally stays read-only and
observability-focused.

## Setup

Use credentials for a CouchDB admin or server-operator account when querying
server-level observability surfaces such as `/_active_tasks`,
`/_scheduler/jobs`, and `/_scheduler/docs`. Lower-privilege database users may
only be able to query narrower database metadata.

```bash
COUCHDB_BASE_URL=http://localhost:5984 \
COUCHDB_USERNAME=admin \
COUCHDB_PASSWORD=... \
coral source add --file sources/community/couchdb/manifest.yaml
```

Run validation:

```bash
coral source test couchdb
```

## Tables

| Table | Description |
| --- | --- |
| `couchdb.server` | Server version and vendor metadata. |
| `couchdb.active_tasks` | Active compaction, indexing, and replication tasks. |
| `couchdb.database_infos` | Paginated metadata for all databases. |
| `couchdb.database_info` | Database metadata for a required `db`. |
| `couchdb.database_info_legacy` | Database metadata fallback for CouchDB servers without `/_dbs_info`. |
| `couchdb.scheduler_jobs` | Active replication scheduler jobs. |
| `couchdb.scheduler_docs` | Replication document states, including completed and failed states. |

## Example queries

```sql
SELECT id, type, database, progress, started_on
FROM couchdb.active_tasks
ORDER BY started_on DESC;
```

```sql
SELECT db_name, doc_count, sizes__active, sizes__external, compact_running
FROM couchdb.database_infos
ORDER BY sizes__file DESC
LIMIT 20;
```

```sql
SELECT db_name, doc_count, sizes__active, sizes__external
FROM couchdb.database_info
WHERE db = 'users';
```

```sql
SELECT db_name, doc_count, sizes__active, sizes__external
FROM couchdb.database_info_legacy
WHERE db_path = 'users';
```

```sql
SELECT id, database, doc_id, state, error_count, last_updated
FROM couchdb.scheduler_docs
WHERE state IN ('error', 'failed', 'crashing')
ORDER BY last_updated DESC
LIMIT 50;
```

## Notes

- This source does not expose arbitrary document scans in v1.
- Several tables query server-level/admin observability endpoints. Use an
  account with sufficient privileges for active tasks, scheduler jobs, and
  scheduler docs.
- `couchdb.database_infos` uses the documented `GET /_dbs_info` endpoint added
  in CouchDB 3.2. For older servers, use `couchdb.database_info_legacy` with a
  URL path-safe database name in `db_path`.
- Use database-scoped filters for metadata that would otherwise require a
  path parameter.

## API references

- https://docs.couchdb.org/en/stable/api/index.html
