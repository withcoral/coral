# Splunk Community Source

Query Splunk Enterprise or Splunk Cloud management REST API inventory through
Coral SQL.

## Setup

### 1. Create a Splunk authentication token

Create a Splunk authentication token for a user or service account with read
access to the resources you plan to inspect. Splunk REST API tokens can be used
with an `Authorization: Bearer <token>` header.

### 2. Configure the API base URL

Set `SPLUNK_API_BASE` to the Splunk management REST API URL. The default is
`https://localhost:8089`.

```bash
export SPLUNK_API_BASE="https://splunk.example.com:8089"
export SPLUNK_TOKEN="<your-token>"
```

### 3. Add the source

```bash
coral source add --file sources/community/splunk/manifest.yaml
```

### 4. Verify

```bash
coral source test splunk
```

The default test query reads `splunk.server_info`.

## Tables

### `splunk.server_info`

Basic Splunk server metadata.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Entry name |
| `server_name` | Utf8 | Splunk server name |
| `host` | Utf8 | Server host |
| `version` | Utf8 | Splunk version |
| `build` | Utf8 | Splunk build |
| `guid` | Utf8 | Server GUID |
| `license_signature` | Utf8 | License signature |
| `os_name` | Utf8 | Operating system name |
| `os_version` | Utf8 | Operating system version |
| `cpu_arch` | Utf8 | CPU architecture |

### `splunk.indexes`

Indexes visible to the authenticated token.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Index name |
| `title` | Utf8 | Index title |
| `disabled` | Boolean | Whether the index is disabled |
| `datatype` | Utf8 | Index data type |
| `home_path` | Utf8 | Hot/warm bucket path |
| `cold_path` | Utf8 | Cold bucket path |
| `thawed_path` | Utf8 | Thawed bucket path |
| `current_db_size_mb` | Int64 | Current size in MB |
| `total_event_count` | Int64 | Total event count |
| `max_total_data_size_mb` | Int64 | Max total data size in MB |
| `frozen_time_period_in_secs` | Int64 | Retention period before freezing |
| `updated` | Timestamp | Entry update time |
| `acl` | Json | Access control metadata |

**Optional filter:** `search`

### `splunk.saved_searches`

Saved searches, reports, and alert searches across Splunk namespaces.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Saved search name |
| `title` | Utf8 | Saved search title |
| `author` | Utf8 | Author |
| `app` | Utf8 | Splunk app namespace |
| `owner` | Utf8 | Owner namespace |
| `sharing` | Utf8 | Sharing scope |
| `disabled` | Boolean | Whether disabled |
| `is_scheduled` | Boolean | Whether scheduled |
| `cron_schedule` | Utf8 | Cron schedule |
| `search` | Utf8 | SPL search string |
| `alert_type` | Utf8 | Alert type |
| `alert_comparator` | Utf8 | Alert comparator |
| `alert_threshold` | Utf8 | Alert threshold |
| `actions` | Utf8 | Comma-separated alert actions |
| `dispatch_earliest_time` | Utf8 | Earliest dispatch time |
| `dispatch_latest_time` | Utf8 | Latest dispatch time |
| `updated` | Timestamp | Entry update time |

**Optional filter:** `search`

### `splunk.apps`

Installed Splunk apps.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | App name |
| `label` | Utf8 | App label |
| `version` | Utf8 | App version |
| `author` | Utf8 | App author |
| `disabled` | Boolean | Whether disabled |
| `visible` | Boolean | Whether visible in launcher |
| `configured` | Boolean | Whether setup completed |
| `state` | Utf8 | App state |
| `updated` | Timestamp | Entry update time |

**Optional filter:** `search`

### `splunk.users`

Users visible to the authenticated token.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Username |
| `realname` | Utf8 | Real name |
| `email` | Utf8 | Email |
| `default_app` | Utf8 | Default app |
| `type` | Utf8 | User type |
| `roles` | Json | Assigned roles |
| `updated` | Timestamp | Entry update time |

**Optional filter:** `search`

### `splunk.search_jobs`

Search jobs visible to the authenticated token.

| Column | Type | Description |
|---|---|---|
| `sid` | Utf8 | Search job ID |
| `label` | Utf8 | Search job label |
| `dispatch_state` | Utf8 | Dispatch state |
| `is_done` | Boolean | Whether done |
| `is_failed` | Boolean | Whether failed |
| `is_paused` | Boolean | Whether paused |
| `event_count` | Int64 | Matching event count |
| `result_count` | Int64 | Result count |
| `scan_count` | Int64 | Scanned event count |
| `done_progress` | Float64 | Completion progress |
| `run_duration` | Float64 | Run duration in seconds |
| `earliest_time` | Timestamp | Earliest event time |
| `latest_time` | Timestamp | Latest event time |
| `updated` | Timestamp | Entry update time |
| `search` | Utf8 | Search string |

**Optional filter:** `search`

## Example Queries

```sql
-- Check Splunk version and host metadata
SELECT server_name, host, version, build
FROM splunk.server_info;

-- Find large indexes
SELECT name, current_db_size_mb, total_event_count, frozen_time_period_in_secs
FROM splunk.indexes
ORDER BY current_db_size_mb DESC
LIMIT 20;

-- Review enabled scheduled searches and alerts
SELECT app, owner, name, cron_schedule, alert_type, actions
FROM splunk.saved_searches
WHERE disabled = false AND is_scheduled = true
ORDER BY app, name;

-- Inventory installed apps
SELECT name, label, version, disabled, visible
FROM splunk.apps
ORDER BY name;

-- Find failed or incomplete search jobs
SELECT sid, dispatch_state, is_failed, run_duration, result_count
FROM splunk.search_jobs
WHERE is_done = false OR is_failed = true
ORDER BY updated DESC;
```

## Validation

```bash
export SPLUNK_API_BASE="https://splunk.example.com:8089"
export SPLUNK_TOKEN="<your-token>"
coral source lint sources/community/splunk/manifest.yaml
coral source add --file sources/community/splunk/manifest.yaml
coral source test splunk
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'splunk'"
coral sql "SELECT * FROM coral.columns WHERE schema_name = 'splunk'"
coral sql "SELECT name, version, server_name FROM splunk.server_info"
```

## Limitations

- **Read-only.** This source does not create search jobs, edit saved searches,
  mutate indexes, manage apps, or update users.
- **Management API required.** The Splunk management REST API is commonly
  exposed on port `8089`. Network and role restrictions may prevent access.
- **Token permissions.** Results depend on the authenticated token's Splunk
  capabilities and namespace access.
- **No event export in v1.** This source focuses on inventory and operational
  metadata. Search result export requires form-encoded POST behavior and is
  left for a future source revision.

## Out of scope for v1

- Creating or exporting ad hoc searches
- Raw event/result retrieval
- Knowledge objects beyond saved searches
- Cluster manager and indexer cluster administration
- License usage and license pool management
- Write operations
