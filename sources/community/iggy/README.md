# Apache Iggy

Query [Apache Iggy](https://iggy.apache.org) message streaming server — streams,
topics, consumer groups, users, connected clients, and server statistics via the HTTP API.

Apache Iggy is a persistent message streaming platform with support for HTTP, TCP, and QUIC
transports. This source targets the HTTP API (default port 3000).

## Authentication

Iggy uses Personal Access Tokens (PATs) for API authentication. `IGGY_URL` is optional
(defaults to `http://localhost:3000`). `IGGY_PAT` is required.

## Local Setup

```bash
# Run Iggy with Docker
docker run -d \
  --name iggy-server \
  -p 3000:3000 \
  -p 8090:8090 \
  -p 8080:8080 \
  apache/iggy:latest

# Create a Personal Access Token using the Iggy CLI
cargo install iggy-cli
iggy login                                  # default: root / secret
iggy personal-access-token create coral     # copy the token shown
```

### Create a stream and topic for testing (optional)

```bash
iggy stream create 1 test-stream
iggy topic create 1 1 3 none test-topic     # stream_id=1, topic_id=1, partitions=3
```

## Configuration

| Input       | Kind     | Required | Default                   | Description                        |
|-------------|----------|----------|---------------------------|------------------------------------|
| `IGGY_URL`  | variable | no       | `http://localhost:3000`   | Base URL of your Iggy HTTP API     |
| `IGGY_PAT`  | secret   | yes      | —                         | Personal Access Token              |

## Schema

### `streams`

One row per stream. Start here — the `id` column is required by `topics` and `consumer_groups`.

### `topics`

One row per topic. Requires `stream_id`. Use `id` as the `topic_id` in `consumer_groups`.

### `consumer_groups`

One row per consumer group. Requires both `stream_id` and `topic_id`.

### `users`

One row per registered user. Requires admin privileges. `id` matches `user_id` in `clients`.

### `clients`

One row per connected client. Shows transport type (`http`, `tcp`, `quic`), address, and user.

### `stats`

Single row with server-wide counters and health metrics — CPU, memory, disk, message counts,
and version. Use for monitoring and capacity planning.

## Example Queries

```sql
-- List all streams
SELECT id, name, topics_count, messages_count
FROM iggy.streams;

-- Topics in a stream
SELECT id, name, partitions_count, messages_count, compression_algorithm
FROM iggy.topics
WHERE stream_id = '1';

-- Find high-traffic topics
SELECT name, messages_count, partitions_count
FROM iggy.topics
WHERE stream_id = '1'
ORDER BY messages_count DESC
LIMIT 10;

-- Consumer groups for a topic
SELECT id, name, members_count, partitions_count
FROM iggy.consumer_groups
WHERE stream_id = '1' AND topic_id = '1';

-- Server health overview
SELECT streams_count, topics_count, messages_count, clients_count,
       cpu_usage, hostname, iggy_server_version
FROM iggy.stats;

-- Disk usage
SELECT hostname,
       messages_size_bytes,
       free_disk_space,
       total_disk_space,
       ROUND(100.0 * messages_size_bytes / total_disk_space, 2) AS pct_used
FROM iggy.stats;

-- Clients by transport type
SELECT transport, COUNT(*) AS client_count
FROM iggy.clients
GROUP BY transport;

-- Cross-source: streams joined with topic counts
SELECT s.name AS stream_name, s.topics_count, s.messages_count
FROM iggy.streams s
ORDER BY s.messages_count DESC;
```

## Notes

- `stream_id` and `topic_id` filters accept both numeric IDs and stream/topic names
- Timestamps (`created_at`, `start_time`) are microseconds since epoch
- `run_time` is server uptime in microseconds
- `cpu_usage` is the percentage for the Iggy process only (not total system CPU)
- The HTTP API is best suited for observability and ops queries; use TCP or QUIC for high-throughput message production/consumption
