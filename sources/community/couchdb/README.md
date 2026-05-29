# CouchDB

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** your CouchDB HTTP endpoint (set via `COUCHDB_URL`)

Inspect a CouchDB instance's server info, per-database document counts and
sizes, running background tasks, and replication scheduler jobs through
CouchDB's HTTP-native REST API.

This source is **observability only**. It maps read-only REST endpoints to
queryable tables and never reads or writes documents or modifies design
documents.

## Setup

### Requirements

- Network access to a CouchDB HTTP endpoint (default port `5984`).
- A CouchDB user. The `server` table only needs valid credentials, but the
  `databases`, `active_tasks`, and `scheduler` tables read server-level
  metadata endpoints that require **server-admin** access (or a user granted
  the relevant role).
- The `databases` table uses the bodyless `GET /_dbs_info`, which requires
  **CouchDB 3.2 or newer**.

### Add the source

```bash
export COUCHDB_URL=http://localhost:5984
export COUCHDB_USERNAME=admin
export COUCHDB_PASSWORD=password
coral source add --file sources/community/couchdb/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
coral source add --file sources/community/couchdb/manifest.yaml --interactive
```

Inputs:

- `COUCHDB_URL` — base URL including scheme and port, e.g.
  `http://localhost:5984`. No trailing slash.
- `COUCHDB_USERNAME` — CouchDB username.
- `COUCHDB_PASSWORD` — password for the user.

## Authentication

This source uses HTTP **Basic authentication** (`Authorization: Basic ...`),
the standard CouchDB credential scheme. Provide a server admin for full
coverage, or a less-privileged user if you only need the `server` table. See
the [CouchDB authentication docs](https://docs.couchdb.org/en/stable/api/server/authn.html).

## Tables

| Table | Description | Endpoint |
|---|---|---|
| `server` | Server welcome banner, version, and vendor info | `GET /` |
| `databases` | Per-database document counts and storage sizes | `GET /_dbs_info` |
| `active_tasks` | Running indexing, replication, and compaction tasks | `GET /_active_tasks` |
| `scheduler` | Replication scheduler jobs and their current state | `GET /_scheduler/jobs` |

No table requires or accepts SQL filters; filter with a `WHERE` clause after
fetching. `scheduler` uses `skip`/`limit` page-based pagination; the other
tables return all results in a single request.

## Notes

- `databases` sizes: `disk_size` is the on-disk file size, `active_size` is the
  live data and index size, and `external_size` is the uncompressed JSON size.
  A large `disk_size` relative to `active_size` indicates space reclaimable by
  compaction.
- `active_tasks` returns an empty result on an idle node. Columns like
  `source`, `target`, `docs_read`, and `docs_written` are populated only for
  replication tasks; `design_document` only for indexer tasks.
- `scheduler.history` is ordered most-recent-first, so `latest_event` and
  `latest_event_at` reflect each job's current state (`started`, `crashed`,
  `added`, etc.). Live progress is in the `info` JSON column.
- `started_on`/`updated_on` (active tasks) and `start_time`/`latest_event_at`
  (scheduler) are real `Timestamp` columns.

## Example Queries

### Confirm connectivity and version

```sql
SELECT couchdb, version, vendor_name
FROM couchdb.server
```

### Largest databases by document count

```sql
SELECT name, doc_count, doc_del_count, disk_size, active_size
FROM couchdb.databases
ORDER BY doc_count DESC
LIMIT 20
```

### Databases with reclaimable space (compaction candidates)

```sql
SELECT name, disk_size, active_size, disk_size - active_size AS reclaimable
FROM couchdb.databases
WHERE disk_size > active_size
ORDER BY reclaimable DESC
```

### What is the node doing right now?

```sql
SELECT type, database, progress, changes_done, total_changes
FROM couchdb.active_tasks
ORDER BY progress ASC
```

### Replication jobs that are not healthy

```sql
SELECT doc_id, source, target, node, latest_event, latest_event_at
FROM couchdb.scheduler
WHERE latest_event <> 'started'
ORDER BY latest_event_at DESC
```

### Replication progress

```sql
SELECT doc_id,
       json_get_int(info, 'docs_read') AS docs_read,
       json_get_int(info, 'docs_written') AS docs_written,
       json_get_int(info, 'changes_pending') AS changes_pending
FROM couchdb.scheduler
```
