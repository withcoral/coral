# Apache Airflow

Query DAGs, DAG runs, task instances, tasks, connections, pools, and providers
from Apache Airflow (self-hosted) via the Airflow REST API v2.

## Setup

### 1. Generate an API token

1. Open the Airflow UI and log in as an admin.
2. Go to **Admin > API Tokens**.
3. Click **Create token**, give it a name, and copy the full JWT string.

The token is a signed JWT. Copy the complete string including header, payload,
and signature. Requires at least the **Viewer** role for read-only access.

See [Airflow API authentication](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html)
for more details.

### 2. Add the source

```bash
export AIRFLOW_BASE_URL="http://localhost:8080"
export AIRFLOW_API_TOKEN="<your-token>"
coral source add --file sources/community/airflow/manifest.yaml
```

`AIRFLOW_BASE_URL` defaults to `http://localhost:8080`. Set it to your
instance's full base URL for remote or cloud-managed deployments
(e.g. `https://airflow.example.com`). Do not include `/api/v2`.

### 3. Verify

```bash
coral source test airflow
```

## Tables

### `airflow.dags`

DAG definitions registered in Airflow. One row per DAG.

| Column | Type | Description |
|---|---|---|
| `dag_id` | Utf8 | Unique DAG identifier |
| `dag_display_name` | Utf8 | Human-readable display name |
| `description` | Utf8 | Description from the Python file |
| `is_paused` | Boolean | Whether the DAG is currently paused |
| `is_stale` | Boolean | Whether the DAG file is no longer parsed |
| `timetable_summary` | Utf8 | Cron expression or timetable string |
| `timetable_description` | Utf8 | Human-readable schedule description |
| `owners` | Utf8 | Comma-joined owner names |
| `tags` | Utf8 | Comma-joined tag names |
| `has_import_errors` | Boolean | Whether the DAG file has import errors |
| `last_parsed_time` | Timestamp | Last successful parse time (UTC) |
| `next_dagrun_run_after` | Timestamp | Next scheduled run time (UTC) |
| `max_active_runs` | Int64 | Max concurrent active runs |
| `max_active_tasks` | Int64 | Max concurrent active tasks |
| `file_location` | Utf8 | Absolute path to the DAG file on the server |
| `relative_fileloc` | Utf8 | Relative path within the DAG bundle |
| `bundle_name` | Utf8 | DAG bundle name |

Use `dag_id` to join to `dag_runs`, `tasks`, and `task_instances`.

### `airflow.dag_runs`

Execution history for all DAGs. One row per run.

| Column | Type | Description |
|---|---|---|
| `dag_run_id` | Utf8 | Unique run identifier |
| `dag_id` | Utf8 | DAG this run belongs to |
| `dag_display_name` | Utf8 | Human-readable DAG display name |
| `state` | Utf8 | Run state: success, failed, running, queued |
| `run_type` | Utf8 | How the run was created: manual, scheduled, backfill, dataset_triggered |
| `triggered_by` | Utf8 | What initiated the run: ui, cli, timetable |
| `triggering_user_name` | Utf8 | Username of the person who triggered the run |
| `logical_date` | Timestamp | Data interval anchor date (UTC) |
| `run_after` | Timestamp | Earliest wall-clock time the scheduler considers this run (UTC) |
| `queued_at` | Timestamp | When the run was queued (UTC) |
| `start_date` | Timestamp | When the run started executing (UTC) |
| `end_date` | Timestamp | When the run finished (UTC) |
| `last_scheduling_decision` | Timestamp | When the scheduler last evaluated this run (UTC) |
| `duration` | Float64 | Run duration in seconds |
| `data_interval_start` | Timestamp | Start of the data interval (UTC) |
| `data_interval_end` | Timestamp | End of the data interval (UTC) |
| `note` | Utf8 | Optional user note |

Optional filters: `dag_id`, `state`.

Note: `logical_date` is the data interval anchor, not the wall-clock start
time. Use `start_date` for actual execution start.

### `airflow.task_instances`

Task-level execution records across all DAGs and runs. One row per task attempt.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Unique task instance UUID (Airflow 3.x) |
| `task_id` | Utf8 | Task identifier within the DAG |
| `task_display_name` | Utf8 | Human-readable task display name |
| `dag_id` | Utf8 | DAG this task instance belongs to |
| `dag_run_id` | Utf8 | Run this task instance belongs to |
| `dag_display_name` | Utf8 | Human-readable DAG display name |
| `state` | Utf8 | State: success, failed, running, queued, up_for_retry, skipped, deferred, removed |
| `operator_name` | Utf8 | Airflow operator class name |
| `try_number` | Int64 | Attempt number, starting at 1 |
| `max_tries` | Int64 | Maximum attempts configured |
| `map_index` | Int64 | Map index for dynamic tasks; -1 for non-mapped |
| `logical_date` | Timestamp | Data interval anchor date (UTC) |
| `start_date` | Timestamp | When this instance started (UTC) |
| `end_date` | Timestamp | When this instance finished (UTC) |
| `duration` | Float64 | Duration in seconds |
| `pool` | Utf8 | Pool this instance draws slots from |
| `pool_slots` | Int64 | Pool slots occupied |
| `queue` | Utf8 | Queue submitted to |
| `priority_weight` | Int64 | Scheduling priority weight |
| `hostname` | Utf8 | Worker hostname |
| `pid` | Int64 | Worker process ID |
| `queued_when` | Timestamp | When this instance was queued (UTC) |
| `note` | Utf8 | Optional user note |

Optional filters: `dag_id`, `dag_run_id`, `state`.

Uses the cross-DAG wildcard endpoint `/api/v2/dags/~/dagRuns/~/taskInstances`.

### `airflow.tasks`

Task definitions for a specific DAG. One row per task.

| Column | Type | Description |
|---|---|---|
| `task_id` | Utf8 | Unique task identifier within the DAG |
| `task_display_name` | Utf8 | Human-readable task display name |
| `dag_id` | Utf8 | DAG this task belongs to |
| `operator_name` | Utf8 | Airflow operator class name |
| `owner` | Utf8 | Task owner |
| `trigger_rule` | Utf8 | Trigger rule (e.g. all_success, one_failed, all_done) |
| `retries` | Float64 | Number of retries configured |
| `pool` | Utf8 | Pool this task draws slots from |
| `pool_slots` | Float64 | Pool slots occupied when running |
| `queue` | Utf8 | Queue submitted to |
| `priority_weight` | Float64 | Priority weight for scheduling |
| `depends_on_past` | Boolean | Whether previous run's instance must succeed |
| `is_mapped` | Boolean | Whether this is a dynamically mapped task |
| `downstream_task_ids` | Utf8 | Comma-joined downstream task IDs |
| `start_date` | Timestamp | Earliest date this task can be scheduled (UTC) |

**Required filter:** `dag_id`

### `airflow.connections`

External system connections configured in the Airflow metadata database.

| Column | Type | Description |
|---|---|---|
| `connection_id` | Utf8 | Unique connection identifier (conn_id) |
| `conn_type` | Utf8 | Connection type (e.g. postgres, http, aws, gcp) |
| `description` | Utf8 | Connection description |
| `host` | Utf8 | Connection host or endpoint |
| `schema` | Utf8 | Database schema or bucket name |
| `port` | Int64 | Connection port number |
| `login` | Utf8 | Login username |

Note: Passwords are never returned by the Airflow API.

### `airflow.pools`

Worker pool slot capacity and current utilization.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Pool name |
| `slots` | Int64 | Total slot capacity |
| `description` | Utf8 | Pool description |
| `occupied_slots` | Int64 | Total slots in use |
| `running_slots` | Int64 | Slots occupied by running tasks |
| `queued_slots` | Int64 | Slots reserved by queued tasks |
| `scheduled_slots` | Int64 | Slots reserved by scheduled tasks |
| `open_slots` | Int64 | Slots available for new tasks |
| `deferred_slots` | Int64 | Slots occupied by deferred tasks |
| `include_deferred` | Boolean | Whether deferred tasks count against capacity |

### `airflow.providers`

Installed Airflow provider packages.

| Column | Type | Description |
|---|---|---|
| `package_name` | Utf8 | PyPI package name (e.g. apache-airflow-providers-amazon) |
| `description` | Utf8 | Provider description |
| `version` | Utf8 | Installed version |
| `documentation_url` | Utf8 | Official documentation URL |

## Authentication

The source uses Bearer token authentication via the Airflow REST API v2:

```text
Authorization: Bearer <AIRFLOW_API_TOKEN>
```

API tokens are generated from **Admin > API Tokens** in the Airflow UI
(Airflow 3.x). Requires at least the Viewer role for read-only access.

This source does not support username/password BasicAuth. If you are running
Airflow 2.x, the API token flow may not be available and you would need to
adapt the auth block to use BasicAuth.

## Example Queries

### List all DAGs with their schedule and pause state

```sql
SELECT dag_id, timetable_description, is_paused, tags, owners
FROM airflow.dags
ORDER BY dag_id
```

### Find failed DAG runs

```sql
SELECT dag_id, dag_run_id, start_date, duration
FROM airflow.dag_runs
WHERE state = 'failed'
ORDER BY start_date DESC
LIMIT 20
```

### Task failure breakdown by operator across all runs

```sql
SELECT operator_name, COUNT(*) AS failures
FROM airflow.task_instances
WHERE state = 'failed'
GROUP BY operator_name
ORDER BY failures DESC
```

### Slowest task instances across all DAGs

```sql
SELECT dag_id, task_id, dag_run_id, duration, operator_name
FROM airflow.task_instances
WHERE state = 'success'
ORDER BY duration DESC
LIMIT 10
```

### Task dependency graph for a specific DAG

```sql
SELECT task_id, operator_name, trigger_rule, downstream_task_ids
FROM airflow.tasks
WHERE dag_id = 'my_dag'
ORDER BY task_id
```

### Pool utilization summary

```sql
SELECT name, slots, open_slots, running_slots, queued_slots
FROM airflow.pools
ORDER BY occupied_slots DESC
```

### Audit external connections by type

```sql
SELECT conn_type, COUNT(*) AS count
FROM airflow.connections
GROUP BY conn_type
ORDER BY count DESC
```

### Installed provider versions

```sql
SELECT package_name, version
FROM airflow.providers
ORDER BY package_name
```

## Limitations

- Read-only. This source does not trigger runs, pause DAGs, or modify any
  Airflow state.
- API tokens require Airflow 3.x. Airflow 2.x uses a different auth model.
- The `variables` endpoint is not included. It returns a 500 error on some
  Airflow 3.x deployments and variable values may contain secrets.
- `tasks` requires a `dag_id` filter. There is no cross-DAG task definition
  listing endpoint in the Airflow REST API.
- Passwords and sensitive extras are never returned by the connections endpoint.
- Pagination is capped at 100 rows per page for all list endpoints.

## Out of scope for v1

- Airflow Variables table (endpoint instability, potential secret exposure)
- DAG source code retrieval
- Event logs
- Import errors detail table
- Write operations of any kind
