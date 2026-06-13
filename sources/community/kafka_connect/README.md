# Kafka Connect

Query Kafka Connect connector inventory, config, and runtime state using SQL
via the Kafka Connect REST API.

## Local Setup

The example below starts Kafka and Kafka Connect locally with no auth so you
can test this source quickly.

```bash
# 1. Create a network

docker network create kafka-net

# 2. Start Kafka in KRaft mode

docker run -d \
  --network kafka-net \
  --name kafka-mq \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka-mq:9092" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka-mq:9093" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:4.2.0

# 3. Start Kafka Connect REST API (port 8083)

docker run -d \
  --network kafka-net \
  --name kafka-connect \
  -p 8083:8083 \
  -e CONNECT_BOOTSTRAP_SERVERS=kafka-mq:9092 \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect \
  -e CONNECT_REST_PORT=8083 \
  -e CONNECT_GROUP_ID=connect-cluster \
  -e CONNECT_CONFIG_STORAGE_TOPIC=connect-configs \
  -e CONNECT_OFFSET_STORAGE_TOPIC=connect-offsets \
  -e CONNECT_STATUS_STORAGE_TOPIC=connect-status \
  -e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
  -e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
  -e CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka-connect:8.2.1
```

Kafka Connect REST API will be available at `http://localhost:8083`.

## Configuration

| Input                    | Kind     | Required | Default                 | Description |
|--------------------------|----------|----------|-------------------------|-------------|
| `KAFKA_CONNECT_URL`      | variable | no       | `http://localhost:8083` | Kafka Connect API base URL reachable without adding auth headers in this source |

## Required Permissions

This source requires read-only access to Kafka Connect REST API endpoints:

| Endpoint | Required Permission | Typical Role |
|----------|-------------------|--------------|
| `GET /connectors` | Read connector list | Connector Viewer |
| `GET /connectors/{name}/config` | Read connector config | Connector Viewer |
| `GET /connectors/{name}/status` | Read connector status | Connector Viewer |

For self-hosted Kafka Connect clusters, ensure the calling user or service account has HTTP read permissions on these endpoints.
This v1 source is intended for endpoints reachable without extra auth headers,
such as local no-auth Kafka Connect, or an authenticating reverse proxy/gateway
in front of Kafka Connect.

## Tables Reference

| Table | Description | Required Filters | Contains |
|-------|-------------|------------------|----------|
| `connectors` | Connector inventory with types, classes, and runtime state | none | Connector metadata and current state across the worker |
| `connector_configs` | Key-value configuration pairs for a connector | `connector_name` (required) | Config keys and values as flat key-value rows |
| `connector_statuses` | Runtime status including state and worker assignment | `connector_name` (required) | High-level connector state, worker info, and error traces |
| `connector_tasks` | Task-level status for each connector task | `connector_name` (required) | Per-task state, worker assignment, and error traces |

## Cross-table Relationships

- **connectors.connector_name** ↔ **connector_configs.connector_name**: Get full config for a connector
- **connectors.connector_name** ↔ **connector_statuses.connector_name**: Get detailed status and worker assignment
- **connectors.connector_name** ↔ **connector_tasks.connector_name**: Get per-task status and assignment

## Validation

After following the Local Setup steps above, you can verify the source is working:

```bash
# 1. Lint the manifest
cargo run -p coral-cli -- source lint sources/community/kafka_connect/manifest.yaml
# Output: Manifest is valid

# 2. Add the source to Coral
export KAFKA_CONNECT_URL=http://localhost:8083
cargo run -p coral-cli -- source add --file sources/community/kafka_connect/manifest.yaml

# 3. Run source tests explicitly
cargo run -p coral-cli -- source test kafka_connect

# 4. Create a test connector to query real data
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-source-connector",
    "config": {
      "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
      "source.cluster.alias": "src",
      "target.cluster.alias": "tgt",
      "source.cluster.bootstrap.servers": "kafka-mq:9092",
      "target.cluster.bootstrap.servers": "kafka-mq:9092",
      "topics": "test-topic",
      "groups": "test-group",
      "tasks.max": "2"
    }
  }' | jq .

# 5. Run representative SQL queries
cargo run -p coral-cli -- sql "SELECT connector_name, connector_type, state FROM kafka_connect.connectors ORDER BY connector_name LIMIT 5"
cargo run -p coral-cli -- sql "SELECT connector_name, state, worker_id FROM kafka_connect.connector_statuses WHERE connector_name = 'test-source-connector'"
cargo run -p coral-cli -- sql "SELECT config_key, config_value FROM kafka_connect.connector_configs WHERE connector_name = 'test-source-connector' ORDER BY config_key LIMIT 8"

```

Current-head captured output (2026-06-13):

```text
$ cargo run -p coral-cli -- source add --file sources/community/kafka_connect/manifest.yaml
Added source kafka_connect (secrets: none)

  ✓ kafka_connect connected successfully
  Secrets: none

    kafka_connect (4 tables)
    ├─ connector_configs
    ├─ connector_statuses
    ├─ connector_tasks
    └─ connectors
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT connector_name, connector_type, state FROM kafka_connect.connectors LIMIT 1
      0 rows

$ cargo run -p coral-cli -- source test kafka_connect

  ✓ kafka_connect connected successfully
  Secrets: none

    kafka_connect (4 tables)
    ├─ connector_configs
    ├─ connector_statuses
    ├─ connector_tasks
    └─ connectors
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT connector_name, connector_type, state FROM kafka_connect.connectors LIMIT 1
      0 rows

$ cargo run -p coral-cli -- sql "SELECT connector_name, connector_type, state FROM kafka_connect.connectors ORDER BY connector_name LIMIT 5"
+-----------------------+----------------+-------+
| connector_name        | connector_type | state |
+-----------------------+----------------+-------+
| test-source-connector |                |       |
+-----------------------+----------------+-------+

$ cargo run -p coral-cli -- sql "SELECT connector_name, state, worker_id FROM kafka_connect.connector_statuses WHERE connector_name = 'test-source-connector'"
+-----------------------+---------+--------------------+
| connector_name        | state   | worker_id          |
+-----------------------+---------+--------------------+
| test-source-connector | RUNNING | kafka-connect:8083 |
+-----------------------+---------+--------------------+

$ cargo run -p coral-cli -- sql "SELECT config_key, config_value FROM kafka_connect.connector_configs WHERE connector_name = 'test-source-connector' ORDER BY config_key LIMIT 8"
+----------------------------------+--------------------------------------------------------------+
| config_key                       | config_value                                                 |
+----------------------------------+--------------------------------------------------------------+
| connector.class                  | org.apache.kafka.connect.mirror.MirrorSourceConnector        |
| groups                           | test-group                                                   |
| name                             | test-source-connector                                        |
| source.cluster.alias             | src                                                          |
| source.cluster.bootstrap.servers | kafka-mq:9092                                                |
| target.cluster.alias             | tgt                                                          |
| target.cluster.bootstrap.servers | kafka-mq:9092                                                |
| tasks.max                        | 2                                                            |
+----------------------------------+--------------------------------------------------------------+
```

## Schema

### `connectors`

One row per connector with connector type, class, and current runtime state.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `connector_name` | Utf8 | no | Connector name |
| `connector_type` | Utf8 | yes | `source` or `sink` |
| `connector_class` | Utf8 | yes | Implementation class (e.g. `org.apache.kafka.connect.mirror.MirrorSourceConnector`) |
| `tasks_max` | Utf8 | yes | Configured max task count |
| `state` | Utf8 | yes | Runtime state: `RUNNING`, `PAUSED`, `FAILED`, etc. |
| `worker_id` | Utf8 | yes | Worker hostname:port currently managing this connector |
| `trace` | Utf8 | yes | Error trace if connector is in failed state |

**Example output:**
```
connector_name          | connector_type | state   | worker_id
------------------------+----------------+---------+---------------------
test-source-connector   | source         | RUNNING | kafka-connect:8083
```

### `connector_configs`

One row per config key-value pair. **Requires** `connector_name` filter.

Warning: Kafka Connect masks sensitive/password configuration fields by default.
If `connect.password.field.masking.disable=true` is set on the Kafka Connect
worker, sensitive config values may be returned in clear text by
`GET /connectors/{name}/config` and therefore appear in this table. Review
access and output handling before querying configs in shared environments.
Reference: https://docs.confluent.io/platform/current/connect/references/restapi.html#connectors

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `connector_name` | Utf8 | no | Connector name (from filter) |
| `config_key` | Utf8 | no | Configuration property name |
| `config_value` | Utf8 | yes | Configuration property value |

**Example output:**
```
connector_name          | config_key                      | config_value
------------------------+---------------------------------+-------------------------------------
test-source-connector   | connector.class                 | org.apache.kafka.connect.mirror.MirrorSourceConnector
test-source-connector   | source.cluster.alias            | src
test-source-connector   | target.cluster.alias            | tgt
test-source-connector   | source.cluster.bootstrap.servers| kafka-mq:9092
test-source-connector   | target.cluster.bootstrap.servers| kafka-mq:9092
test-source-connector   | topics                          | test-topic
test-source-connector   | groups                          | test-group
test-source-connector   | tasks.max                       | 2
```

### `connector_statuses`

Current runtime status for one connector. **Requires** `connector_name` filter.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `connector_name` | Utf8 | no | Connector name |
| `connector_type` | Utf8 | yes | `source` or `sink` |
| `state` | Utf8 | yes | Runtime state |
| `worker_id` | Utf8 | yes | Worker managing this connector |
| `trace` | Utf8 | yes | Error details if applicable |
| `requested_connector_name` | Utf8 | yes | Virtual column echoing filter value |

**Example output:**
```
connector_name          | connector_type | state   | worker_id          | trace
------------------------+----------------+---------+--------------------+-------
test-source-connector   | source         | RUNNING | kafka-connect:8083 | NULL
```

### `connector_tasks`

One row per task in a connector. **Requires** `connector_name` filter.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `connector_name` | Utf8 | no | Connector name (from filter) |
| `task_id` | Int64 | no | Task ID (0-based index) |
| `state` | Utf8 | yes | Task state: `RUNNING`, `PAUSED`, `FAILED`, etc. |
| `worker_id` | Utf8 | yes | Worker running this task |
| `trace` | Utf8 | yes | Error trace if task failed |

**Example output (empty tasks, normal for status queries):**
```
connector_name          | task_id | state | worker_id | trace
------------------------+---------+-------+-----------+-------
```

## Example Queries

### Basic Inventory and Status

```sql
-- List all connectors with their current state
SELECT connector_name, connector_type, state, worker_id
FROM kafka_connect.connectors
ORDER BY connector_name;

-- Get config for a specific connector
SELECT config_key, config_value
FROM kafka_connect.connector_configs
WHERE connector_name = 'my-connector'
ORDER BY config_key;

-- Check connector-level status
SELECT connector_name, connector_type, state, worker_id
FROM kafka_connect.connector_statuses
WHERE connector_name = 'my-connector';

-- List tasks for a connector
SELECT task_id, state, worker_id
FROM kafka_connect.connector_tasks
WHERE connector_name = 'my-connector'
ORDER BY task_id;
```

### Troubleshooting Patterns

```sql
-- Find failed or unhealthy connectors
SELECT connector_name, state, trace
FROM kafka_connect.connectors
WHERE state != 'RUNNING'
LIMIT 10;

-- Check if a connector has failed tasks
SELECT connector_name, task_id, state, trace
FROM kafka_connect.connector_tasks
WHERE connector_name = 'my-connector'
  AND state NOT IN ('RUNNING', 'PAUSED')
LIMIT 10;

-- Step 1: Find all failed connectors
SELECT connector_name, state
FROM kafka_connect.connectors
WHERE state = 'FAILED'
LIMIT 10;

-- Step 2: Get config for one specific failed connector
SELECT config_key, config_value
FROM kafka_connect.connector_configs
WHERE connector_name = 'my-failed-connector'
ORDER BY config_key;

-- Check status of a specific connector
SELECT connector_name, state, worker_id
FROM kafka_connect.connector_statuses
WHERE connector_name = 'my-connector';
```

## Notes

- Kafka Connect REST endpoints used here are read-only and non-paginated; they return complete result sets.
- This source focuses on connector inventory, configuration, and status discovery (v1 scope).
- Available connector classes depend on your Kafka Connect image and installed plugins.
- This source does not add auth headers itself; for secured clusters, use a
  base URL where authentication is handled upstream (for example, an
  authenticating reverse proxy or gateway).

## Out of Scope for v1

- Creating, modifying, or deleting connectors (use Kafka Connect REST API directly or CLI)
- Restarting connectors or tasks
- Worker metrics or logs
- Producer/consumer lag monitoring (use Kafka metrics sources or Prometheus)
- Plugin discovery or installation
- Connector classification or type filtering

## Performance Considerations

- Kafka Connect REST API `/connectors` endpoint returns all connectors in one request (no server-side pagination).
- SQL `LIMIT` reduces rows returned by Coral, but does not reduce provider-side work for `/connectors` because Kafka Connect returns the full expanded connector map before Coral applies projection/limit.
- Default result fetch: Full connector list is returned; Coral respects manifest `pagination.mode: none`.
- Queries are read-only and do not modify cluster state.
- Each query translates to 1-2 HTTP calls to Kafka Connect (varies by table).
