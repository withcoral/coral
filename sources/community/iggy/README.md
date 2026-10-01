# Apache Iggy

Query [Apache Iggy](https://iggy.apache.org) message streaming server — streams,
topics, consumer groups, users, connected clients, and server statistics via the HTTP API.

Apache Iggy is a persistent message streaming platform with support for HTTP, TCP, and QUIC
transports. This source targets the HTTP API (default port 3000).

## Authentication

Iggy's HTTP API requires a **JWT access token** in the `Authorization: Bearer` header.
A Personal Access Token (PAT) cannot be used directly — it must first be exchanged for a
JWT via `POST /personal-access-tokens/login`.

**Option A — exchange a PAT for a JWT (recommended for production):**

```bash
# 1. Create a PAT (requires iggy CLI)
iggy -u iggy -p iggy pat create coral 365d
# Copy the token shown

# 2. Exchange it for a JWT
curl -s -X POST http://localhost:3000/personal-access-tokens/login \
  -H "Content-Type: application/json" \
  -d '{"token":"<your-PAT>"}' | jq -r '.access_token.token'
```

**Option B — login with username/password (quick start):**

```bash
curl -s -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"username":"iggy","password":"iggy"}' | jq -r '.access_token.token'
```

Use the resulting JWT as `IGGY_ACCESS_TOKEN` when running `coral source add`.

> **Note:** JWT tokens expire (default 1 hour). Re-run the exchange/login command
> to get a fresh token when re-adding the source.

## Local Setup

```bash
# Run Iggy with Docker. A root user ("iggy") is always created on first boot,
# but gets a randomly generated password (printed to the container logs)
# unless IGGY_ROOT_USERNAME/IGGY_ROOT_PASSWORD are set — set both to "iggy"
# for the well-known iggy/iggy credentials used below.
#
# IGGY_HTTP_ADDRESS/IGGY_TCP_ADDRESS must be set to 0.0.0.0 — by default Iggy
# binds these to 127.0.0.1 inside the container, so the -p port mappings
# below would otherwise be unreachable from the host.
docker run -d \
  --name iggy-server \
  --cap-add SYS_NICE \
  --security-opt seccomp=unconfined \
  --ulimit memlock=-1:-1 \
  -p 3000:3000 \
  -p 8090:8090 \
  -p 8080:8080 \
  -e IGGY_ROOT_USERNAME=iggy \
  -e IGGY_ROOT_PASSWORD=iggy \
  -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
  apache/iggy:latest

# Verify it is running (/ping doesn't require auth)
curl http://localhost:3000/ping
```

### Get an access token

```bash
# Login and capture the JWT
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"username":"iggy","password":"iggy"}' | jq -r '.access_token.token')
echo $TOKEN
```

### Create a stream and topic for testing (optional)

```bash
iggy -u iggy -p iggy stream create test-stream
iggy -u iggy -p iggy topic create test-stream test-topic 3 none
# stream create: <name>
# topic create: <stream_id> <name> <partitions> <compression_algorithm>
# stream_id/topic_id can be a numeric ID or a name. IDs are auto-assigned by
# the server starting at 0 for the first stream/topic — check iggy.streams
# and iggy.topics for the assigned numeric ID (the -s/-t flags for explicit
# IDs are not honored by iggy-cli 0.10.0).
```

## Configuration

| Input                | Kind     | Required | Default                 | Description                                  |
|----------------------|----------|----------|-------------------------|----------------------------------------------|
| `IGGY_URL`           | variable | no       | `http://localhost:3000` | Base URL of your Iggy HTTP API               |
| `IGGY_ACCESS_TOKEN`  | secret   | yes      | —                       | JWT access token (see Authentication above)  |

## Required Permissions

The examples below use the default `iggy` root account which bypasses permission checks.
For non-root tokens, grant the following permissions:

| Table             | Required Permission                                |
|-------------------|-----------------------------------------------------|
| `streams`         | `read_streams` global permission, or per-stream read access |
| `topics`          | `read_topics` global permission, or per-stream/topic read access |
| `consumer_groups` | `read_topics` global permission, or per-stream/topic read access |
| `users`           | `read_users` global permission                       |
| `clients`         | `read_servers` global permission                     |
| `stats`           | `read_servers` global permission, plus a valid JWT (`/stats` is not public) |

See [Iggy security docs](https://iggy.apache.org/docs/server/security) for details.

## Schema

### `streams`

One row per stream. Start here — the `id` column is required by `topics` and `consumer_groups`.

### `topics`

One row per topic. Requires `stream_id`. Use `id` as the `topic_id` in `consumer_groups`.

### `consumer_groups`

One row per consumer group. Requires both `stream_id` and `topic_id`.

### `users`

One row per registered user. Requires `read_users` global permission (root bypasses).

### `clients`

One row per connected client. Shows transport type (`http`, `tcp`, `quic`), address, and user.
Requires `read_servers` global permission (root bypasses).

### `stats`

Single row with server-wide counters and health metrics. Requires `read_servers` global
permission (root bypasses) and a valid JWT - `/stats` is not a public endpoint.

## Example Queries

```sql
-- List all streams
SELECT id, name, topics_count, messages_count
FROM iggy.streams;

-- Topics in a stream (0 is the ID of the first stream created above)
SELECT id, name, partitions_count, messages_count, compression_algorithm
FROM iggy.topics
WHERE stream_id = '0';

-- Consumer groups for a topic (0 is the ID of the first stream/topic created above)
SELECT id, name, members_count, partitions_count
FROM iggy.consumer_groups
WHERE stream_id = '0' AND topic_id = '0';

-- Server health overview
SELECT streams_count, topics_count, messages_count, clients_count,
       cpu_usage, hostname, iggy_server_version
FROM iggy.stats;

-- Clients by transport type
SELECT transport, COUNT(*) AS client_count
FROM iggy.clients
GROUP BY transport;
```

## Notes

- `stream_id` and `topic_id` filters accept both numeric IDs and names
- Timestamps (`created_at`, `start_time`) are microseconds since epoch
- `run_time` is server uptime in microseconds
- The HTTP API is best suited for observability; use TCP or QUIC for high-throughput production/consumption
