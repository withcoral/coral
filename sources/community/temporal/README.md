# Temporal

Query self-hosted Temporal Server workflow runtime data — namespaces, workflow executions, and schedules — via the Temporal HTTP API (self-hosted only).

## Requirements

- **Temporal Server v1.24 or later** with the HTTP API enabled (the HTTP API frontend is separate from the gRPC frontend and typically runs on port 7243). The default source test validates namespace discovery only; namespace-scoped tables and version-gated tables are documented as manual checks below.
- **Self-hosted clusters only.** Temporal Cloud namespace endpoints expose gRPC, not the workflow-service HTTP API, and are not supported by this source.
- **Open (unauthenticated) clusters only.** Auth-enabled clusters (those configured with an authorization plugin) are not supported until the source-spec DSL can express conditional auth.

To confirm the HTTP API is reachable, run:

```bash
curl http://localhost:7243/api/v1/namespaces?pageSize=1
```

A JSON response (even `{"namespaces":[]}`) confirms the HTTP API is up.

## Setup

### Self-hosted Temporal Server (local or cloud-hosted)

This source works with any self-hosted Temporal Server — whether running locally, on Kubernetes, or on a cloud VM — as long as the HTTP API frontend is enabled and reachable.

**Enable the HTTP frontend** in your `temporal.yaml` config (or equivalent):

```yaml
services:
  frontend:
    rpc:
      httpPort: 7243
```

Restart the server if you changed this setting. For `temporal server start-dev`, pass `--http-port 7243` instead.

### Add the Source

```bash
coral source add --file sources/community/temporal/manifest.yaml
```

When prompted, provide:

- `TEMPORAL_ADDRESS`: Base URL of the Temporal HTTP API. Do not include a trailing slash.
  - Examples: `http://localhost:7243`, `http://temporal.internal:7243`, `https://temporal.mycompany.com:7243`

### Verify Setup

```bash
coral sql "SELECT name, state FROM temporal.namespaces LIMIT 5"
```

If the source is configured correctly, the query returns registered namespaces.

## Tables

### `namespaces`

All namespaces registered on the Temporal cluster. Use the `name` column as the required `namespace` filter when querying `workflows` and `schedules`.

Useful for namespace discovery, retention policy review, cluster topology inspection, and finding namespace names.

Columns include: `name`, `state`, `description`, `owner_email`, `workflow_execution_retention_ttl`, `active_cluster_name`, `is_global_namespace`.

### `workflows`

Workflow executions in a Temporal namespace. Each row is one execution instance — running or closed. The `namespace` filter is required.

Optionally narrow results with a [Temporal Visibility query](https://docs.temporal.io/visibility) using the `query` filter. Visibility queries support filtering by `ExecutionStatus`, `WorkflowType`, `TaskQueue`, custom search attributes, and more.

Useful for workflow monitoring, failure analysis, latency inspection, task queue auditing, and finding specific executions.

Columns include: `namespace` (virtual), `workflow_id`, `run_id`, `workflow_type`, `task_queue`, `status`, `start_time`, `close_time`, `execution_time`, `history_length`, `history_size_bytes`, `parent_workflow_id`, `parent_run_id`, `search_attributes`, `memo`.

### `schedules`

Schedules in a Temporal namespace. Each row is one schedule that triggers workflow executions on a cron or interval basis. The `namespace` filter is required.

Useful for schedule inventory, cron expression review, next-trigger inspection, and identifying paused or broken schedules.

Columns include: `namespace` (virtual), `schedule_id`, `workflow_type`, `paused`, `notes`, `spec`, `recent_actions`, `future_action_times`.

### `archived_workflows`

Closed workflow executions moved to the archival store. The `namespace` filter is required.

> **Requires Temporal Server v1.24+** with archival configured on the cluster.

Useful for long-term retention analysis and retrieving executions purged from the primary visibility store.

Columns include: `namespace` (virtual), `workflow_id`, `run_id`, `workflow_type`, `task_queue`, `status`, `start_time`, `close_time`, `history_length`, `history_size_bytes`, `search_attributes`, `memo`.

### `batch_operations`

Batch operations (bulk terminate, cancel, signal) submitted against a namespace. The `namespace` filter is required.

> **Requires Temporal Server v1.24+**.

Useful for auditing bulk actions and tracking progress of in-flight batch jobs.

Columns include: `namespace` (virtual), `job_id`, `state`, `start_time`, `close_time`.

### `nexus_endpoints`

Nexus endpoints registered on the cluster (cluster-scoped; no namespace filter required).

> **Requires Temporal Server v1.27+**.

Useful for Nexus topology discovery and endpoint configuration review.

Columns include: `id`, `name`, `target_namespace`, `target_task_queue`, `target_external_url`, `url_prefix`, `version`, `created_time`, `last_modified_time`.

### `nexus_operations`

Nexus operations running within a namespace. The `namespace` filter is required.

> **Requires Temporal Server v1.32+** (not yet GA as of this writing).

Useful for cross-namespace Nexus call tracing and status inspection.

Columns include: `namespace` (virtual), `operation_id`, `run_id`, `endpoint`, `service`, `operation`, `status`, `schedule_time`, `close_time`, `execution_duration`, `state_transition_count`, `search_attributes`.

### `workflow_rules`

Automation rules that match and act on workflows within a namespace. The `namespace` filter is required.

> **Requires Temporal Server v1.28+** with `frontend.workflowRulesAPIsEnabled` enabled.

Useful for governance auditing and understanding automated workflow management policies.

Columns include: `namespace` (virtual), `rule_id`, `description`, `visibility_query`, `actions`, `expiration_time`, `create_time`, `created_by_identity`.

### `activities`

Standalone activity executions within a namespace. The `namespace` filter is required.

> **Requires Temporal Server v1.31+** (public preview) with `activity.enableStandalone` enabled.

Useful for monitoring standalone activity health and latency.

Columns include: `namespace` (virtual), `activity_id`, `run_id`, `activity_type`, `status`, `task_queue`, `schedule_time`, `close_time`, `execution_duration`, `state_transition_count`, `search_attributes`.

### `worker_deployments`

Worker deployments registered on the cluster for versioned rollouts. The `namespace` filter is required.

> **Requires Temporal Server v1.27+** with `system.enableDeployments` enabled.

Useful for deployment tracking, version rollout monitoring, and canary analysis.

Columns include: `namespace` (virtual), `name`, `create_time`, `current_version`, `ramping_version`, `ramping_version_percentage`, `current_version_changed_time`, `ramping_version_changed_time`.

The source reads both RoutingConfig schemas: legacy string fields
(`currentVersion`, `rampingVersion`) and newer nested deployment-version
objects (`currentDeploymentVersion.buildId`,
`rampingDeploymentVersion.buildId`).

## Optional or Manual Checks

The following tables are not part of the default `coral source test temporal` path. Namespace-scoped tables require a namespace that varies per cluster, and newer tables are version-gated or feature-gated.

### v1.24+ (namespace-scoped)

These core tables work on any v1.24+ cluster but require a namespace filter. Replace `'default'` with a namespace from your cluster:

```sql
SELECT workflow_id, workflow_type, status
FROM temporal.workflows
WHERE namespace = 'default'
LIMIT 1
```

```sql
SELECT schedule_id, workflow_type, paused
FROM temporal.schedules
WHERE namespace = 'default'
LIMIT 1
```

```sql
SELECT job_id, state, start_time
FROM temporal.batch_operations
WHERE namespace = 'default'
LIMIT 1
```

### v1.27+

- `nexus_endpoints` (OperatorService):

```sql
SELECT id, name, target_namespace, target_task_queue
FROM temporal.nexus_endpoints
LIMIT 1
```

Unavailable behavior on older servers: endpoint may return 404/Unimplemented.

- `worker_deployments` (requires `system.enableDeployments`):

```sql
SELECT name, current_version, ramping_version
FROM temporal.worker_deployments
WHERE namespace = 'default'
LIMIT 1
```

Unavailable behavior when disabled/unsupported: endpoint may return 404/Unimplemented.

### v1.28+

- `workflow_rules` (requires `frontend.workflowRulesAPIsEnabled`):

```sql
SELECT rule_id, description, create_time
FROM temporal.workflow_rules
WHERE namespace = 'default'
LIMIT 1
```

Unavailable behavior when disabled/unsupported: endpoint may return 501 method not supported, Unimplemented, or 404.

### v1.31+

- `activities` (requires `activity.enableStandalone`):

```sql
SELECT activity_id, activity_type, status
FROM temporal.activities
WHERE namespace = 'default'
LIMIT 1
```

Unavailable behavior when disabled/unsupported: endpoint may return 404.

### v1.32+

- `nexus_operations`:

```sql
SELECT operation_id, endpoint, service, status
FROM temporal.nexus_operations
WHERE namespace = 'default'
LIMIT 1
```

Unavailable behavior on older servers: endpoint may return 404.

## Example Queries

### Namespace inventory

```sql
SELECT name, state, active_cluster_name, workflow_execution_retention_ttl
FROM temporal.namespaces
ORDER BY name
```

### Running workflows by type

```sql
SELECT workflow_type, COUNT(*) AS running_count
FROM temporal.workflows
WHERE namespace = 'default'
  AND query = 'ExecutionStatus="Running"'
GROUP BY workflow_type
ORDER BY running_count DESC
```

### Recently failed workflows

```sql
SELECT workflow_id, run_id, workflow_type, task_queue,
       start_time, close_time
FROM temporal.workflows
WHERE namespace = 'default'
  AND query = 'ExecutionStatus="Failed"'
ORDER BY close_time DESC
LIMIT 20
```

### Long-running workflows (by history size)

```sql
SELECT workflow_id, workflow_type, status, history_length, history_size_bytes,
       start_time
FROM temporal.workflows
WHERE namespace = 'default'
ORDER BY history_size_bytes DESC NULLS LAST
LIMIT 10
```

### Child workflows

```sql
SELECT workflow_id, workflow_type, status, parent_workflow_id, start_time
FROM temporal.workflows
WHERE namespace = 'default'
  AND parent_workflow_id IS NOT NULL
ORDER BY start_time DESC
LIMIT 20
```

### Schedule overview

```sql
SELECT schedule_id, workflow_type, paused, notes
FROM temporal.schedules
WHERE namespace = 'default'
ORDER BY schedule_id
```

### Schedules with upcoming trigger times

```sql
SELECT schedule_id, future_action_times
FROM temporal.schedules
WHERE namespace = 'default'
  AND future_action_times IS NOT NULL
ORDER BY schedule_id
```

### Paused schedules

```sql
SELECT schedule_id, workflow_type, notes
FROM temporal.schedules
WHERE namespace = 'default'
  AND paused = true
ORDER BY schedule_id
```

### Schedules by workflow type

```sql
SELECT workflow_type, COUNT(*) AS schedule_count
FROM temporal.schedules
WHERE namespace = 'default'
GROUP BY workflow_type
ORDER BY schedule_count DESC
```

### Workflow count by task queue

```sql
SELECT task_queue, status, COUNT(*) AS execution_count
FROM temporal.workflows
WHERE namespace = 'default'
GROUP BY task_queue, status
ORDER BY task_queue, status
```

## Verification

Add the source and verify it works against a running Temporal Server:

```bash
$ coral source add --file sources/community/temporal/manifest.yaml
# When prompted:
#   TEMPORAL_ADDRESS: http://localhost:7243
Added source temporal

  ✓ temporal connected successfully

    temporal (10 tables)
    ├─ activities
    ├─ archived_workflows
    ├─ batch_operations
    ├─ namespaces
    ├─ nexus_endpoints
    ├─ nexus_operations
    ├─ schedules
    ├─ worker_deployments
    ├─ workflow_rules
    └─ workflows
```

### Query Namespaces

```bash
$ coral sql "SELECT name, state FROM temporal.namespaces LIMIT 5"
+-----------------+----------------------------+
| name            | state                      |
+-----------------+----------------------------+
| default         | NAMESPACE_STATE_REGISTERED |
| temporal-system | NAMESPACE_STATE_REGISTERED |
+-----------------+----------------------------+
```

### Query Workflows

```bash
$ coral sql "SELECT workflow_id, workflow_type, task_queue, status FROM temporal.workflows WHERE namespace = 'default' LIMIT 3"
+-------------------------------+-----------------+------------+-------------------------------------+
| workflow_id                   | workflow_type   | task_queue | status                              |
+-------------------------------+-----------------+------------+-------------------------------------+
| order-1234                    | OrderWorkflow   | orders     | WORKFLOW_EXECUTION_STATUS_COMPLETED |
| payment-5678                  | PaymentWorkflow | payments   | WORKFLOW_EXECUTION_STATUS_RUNNING   |
+-------------------------------+-----------------+------------+-------------------------------------+
```

### Query Schedules

```bash
$ coral sql "SELECT schedule_id, workflow_type, paused, notes FROM temporal.schedules WHERE namespace = 'default' LIMIT 5"
+----------------+-----------------+--------+-------+
| schedule_id    | workflow_type   | paused | notes |
+----------------+-----------------+--------+-------+
| daily-cleanup  | CleanupWorkflow | false  |       |
+----------------+-----------------+--------+-------+
```

## Limits

- Requires Temporal Server v1.24+ with the HTTP frontend (`httpPort`) enabled for core tables. Newer tables require higher versions; see the Tables section. Earlier versions are unsupported and unverified by this source.
- Temporal Cloud namespace endpoints are not supported. This source targets the self-hosted Temporal Server HTTP API only.
- The `workflows` and `schedules` tables require the `namespace` filter. Omitting it causes a query error; use the `namespaces` table to discover available namespaces first.
- Workflow payload contents (input, output, activity results) are not included in the `raw` column by default — the list API returns execution metadata only, not full history. Use the Temporal SDK or CLI to retrieve workflow histories.
- The `query` filter uses [Temporal Visibility query language](https://docs.temporal.io/visibility); it requires an Elasticsearch or SQL visibility store to be configured on the server. Basic visibility (without Elasticsearch) may return errors for complex query expressions.
- Write operations (start workflow, signal, terminate, create schedule) are intentionally excluded.
