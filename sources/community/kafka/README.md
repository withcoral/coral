# Kafka

Query Apache Kafka cluster metadata — brokers, topics, consumer groups, and
per-partition lag — using SQL via the
[Confluent Kafka REST Proxy](https://github.com/confluentinc/kafka-rest)
(open source).

Kafka speaks a custom binary protocol on port 9092 that HTTP clients cannot
use directly. The Confluent Kafka REST Proxy wraps that protocol and exposes
a standard REST API over the Kafka Admin Client — this source queries that API.

## Authentication

The source uses HTTP Basic authentication for every request. `KAFKA_REST_USER`
is optional (defaults to empty). `KAFKA_REST_PASSWORD` is required at install
time — the DSL does not support optional secrets.

For Confluent REST Proxy deployments fronted by RBAC or a reverse proxy with
Basic auth enabled, set both to your real credentials.

For unauthenticated local instances (the default Docker setup), leave
`KAFKA_REST_USER` at its default and set `KAFKA_REST_PASSWORD` to any
non-empty placeholder (e.g. `none`). The REST Proxy ignores the Authorization
header when no auth is configured.

## Local Setup

The recommended setup runs both containers on a shared Docker network so the
REST Proxy can reach Kafka by container name.

```bash
# 1. Create a shared network (skip if it already exists)
docker network create kafka-net

# 2. Start Kafka (KRaft mode, no ZooKeeper)
docker run -d \
  --network kafka-net \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka:9092" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:4.2.0

# 3. Start Confluent Kafka REST Proxy pointing to the kafka container
docker run -d \
  --network kafka-net \
  --name kafka-rest \
  -p 8082:8082 \
  -e KAFKA_REST_HOST_NAME=kafka-rest \
  -e KAFKA_REST_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_REST_LISTENERS=http://0.0.0.0:8082 \
  confluentinc/cp-kafka-rest:8.2.1
```

Kafka REST Proxy will be available at `http://localhost:8082`.

### Create a topic and produce test messages (optional)

```bash
# Open a shell inside the Kafka container
docker exec --workdir /opt/kafka/bin/ -it kafka sh

# Create a topic
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic

# Verify
./kafka-topics.sh --bootstrap-server localhost:9092 --list

# Produce messages (type messages, press Ctrl+C to stop)
./kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
```

## Configuration

| Input                 | Kind     | Required | Default                 | Description                                                   |
|-----------------------|----------|----------|-------------------------|---------------------------------------------------------------|
| `KAFKA_REST_URL`      | variable | no       | `http://localhost:8082` | Base URL of your Confluent Kafka REST Proxy                   |
| `KAFKA_REST_USER`     | variable | no       | (empty)                 | Username for Basic auth. Leave empty for unauthenticated setup.|
| `KAFKA_REST_PASSWORD` | secret   | yes      | —                       | Password for Basic auth. Use `none` for unauthenticated setup. |

The cluster ID is not a global config — it is discovered at query time via
`SELECT * FROM kafka.cluster` and passed as the `cluster_id` filter to all
other tables.

## Schema

### `cluster`

One row representing the Kafka cluster the REST Proxy is connected to.
Start here to get the `cluster_id` — it is required by all other tables.

### `brokers`

One row per broker. Requires `cluster_id`. Fields: `broker_id`, `host`,
`port`, `rack`.

### `topics`

One row per topic. Requires `cluster_id`. Internal topics (e.g.
`__consumer_offsets`) are included. Use `topic_name` as the `topic_name`
filter in `topic_configs`.

### `topic_configs`

One row per configuration key for a specific topic. Requires `cluster_id`
and `topic_name`. The `source` column indicates where the value originates
(`DEFAULT_CONFIG`, `DYNAMIC_TOPIC_CONFIG`, `DYNAMIC_DEFAULT_BROKER_CONFIG`,
etc.). `is_default` is true when the value matches the broker default.

### `consumer_groups`

One row per consumer group. Requires `cluster_id`. Use `consumer_group_id`
as the `group_id` filter in `consumer_group_lags`.

### `consumer_group_lags`

One row per topic-partition assignment for a specific consumer group.
Requires `cluster_id` and `group_id`. `lag` is `log_end_offset -
current_offset`. Use this to drill into per-partition lag after identifying
a lagging group in `consumer_groups`.

## Example Queries

```sql
-- Discover the cluster ID (needed for all other queries)
SELECT cluster_id FROM kafka.cluster;

-- List all brokers
SELECT broker_id, host, port, rack
FROM kafka.brokers
WHERE cluster_id = 'your-cluster-id';

-- List all topics (excluding internal)
SELECT topic_name, partitions_count, replication_factor
FROM kafka.topics
WHERE cluster_id = 'your-cluster-id'
  AND is_internal = false;

-- Topics with highest partition count
SELECT topic_name, partitions_count, replication_factor
FROM kafka.topics
WHERE cluster_id = 'your-cluster-id'
ORDER BY partitions_count DESC
LIMIT 20;

-- Retention config for a topic
SELECT name, value, source, is_default
FROM kafka.topic_configs
WHERE cluster_id = 'your-cluster-id'
  AND topic_name = 'test-topic'
  AND name IN ('retention.ms', 'cleanup.policy', 'max.message.bytes');

-- All consumer groups and their state
SELECT consumer_group_id, state, partition_assignor
FROM kafka.consumer_groups
WHERE cluster_id = 'your-cluster-id';

-- Per-partition lag for a consumer group
SELECT topic_name, partition_id, current_offset, log_end_offset, lag
FROM kafka.consumer_group_lags
WHERE cluster_id = 'your-cluster-id'
  AND group_id = 'my-consumer-group'
ORDER BY lag DESC NULLS LAST;
```

## Limitations

### Pagination not yet supported

- The Confluent Kafka REST Proxy v3 paginates list responses (topics, consumer groups, lags, etc.) using a `metadata.next` URL in the response body.
- This source does not yet follow those pagination links, so results are
  currently limited to the REST Proxy's default page size (approximately
  100 items per request).
- As a result, large Kafka clusters with many topics or consumer groups may return truncated results.
- Full pagination support will be added once the Coral DSL supports URL-cursor pagination (`cursor_body_url` mode).

## Notes

- The REST Proxy is always connected to exactly one Kafka cluster. The
  `cluster` table always returns a single row.
- `cluster_id` is a stable identifier assigned by Kafka at cluster creation.
  It does not change unless the cluster is re-created.
- The `consumer_group_lags` table requires the `/lags` endpoint which returns
  per-partition lag. This endpoint is available in open-source Kafka REST
  Proxy (not Confluent Server only).
- Unlike Kafka UI, the REST Proxy does not expose throughput metrics
  (bytes/sec) — those are available via JMX or a metrics system like
  Prometheus.
