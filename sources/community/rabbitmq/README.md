# RabbitMQ

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 10
**Base URL:** your RabbitMQ management API URL (set via `RABBITMQ_URL`)

Query vhosts, queues, exchanges, bindings, consumers, connections, nodes,
users, and policies from RabbitMQ (self-hosted or Cloud).

## Authentication

Requires a RabbitMQ user with the `monitoring` or `administrator` tag.
The default `guest` user has administrator access but is restricted to
`localhost` connections by default.

```bash
RABBITMQ_URL=http://localhost:15672 \
RABBITMQ_USERNAME=guest \
RABBITMQ_PASSWORD=guest \
  coral source add --file sources/community/rabbitmq/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
RABBITMQ_URL=http://localhost:15672 \
RABBITMQ_USERNAME=guest \
RABBITMQ_PASSWORD=guest \
  coral source add --file sources/community/rabbitmq/manifest.yaml --interactive
```

The management HTTP API must be enabled. It ships enabled by default in
most RabbitMQ distributions. For self-hosted instances, verify the
`rabbitmq_management` plugin is active:

```bash
rabbitmq-plugins enable rabbitmq_management
```

See the [RabbitMQ management plugin docs](https://www.rabbitmq.com/docs/management)
for full setup instructions.

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `overview` | Cluster-wide summary (version, node, message totals) | -- | -- |
| `vhosts` | Virtual hosts with message totals | -- | -- |
| `nodes` | Cluster nodes with resource and health metrics | -- | -- |
| `queues` | Queues with depth, consumer, and memory stats | -- | -- |
| `exchanges` | Exchanges with type and durability | -- | -- |
| `bindings` | Bindings between exchanges and queues | -- | -- |
| `connections` | Active AMQP connections | -- | -- |
| `consumers` | Active consumers subscribed to queues | -- | -- |
| `users` | Users in the internal authentication database | -- | -- |
| `policies` | Policies applied to queues and exchanges | -- | -- |

### Pagination note

`queues`, `exchanges`, `bindings`, `connections`, and `consumers` use
page-based pagination with a page size of 500. `overview`, `vhosts`,
`nodes`, `users`, and `policies` return all results in a single response.

## Quick start

```bash
# Cluster summary
coral sql "
  SELECT rabbitmq_version, cluster_name, node, rates_mode
  FROM rabbitmq.overview
"

# List virtual hosts
coral sql "
  SELECT name, description, default_queue_type, messages, messages_ready
  FROM rabbitmq.vhosts
"

# Node health
coral sql "
  SELECT name, running, mem_used, mem_alarm, disk_free, disk_free_alarm, uptime
  FROM rabbitmq.nodes
"

# Queue depths -- find queues with unacknowledged messages
coral sql "
  SELECT name, vhost, type, messages, messages_ready, messages_unacknowledged, consumers
  FROM rabbitmq.queues
  ORDER BY messages_unacknowledged DESC
  LIMIT 20
"

# All exchanges
coral sql "
  SELECT name, vhost, type, durable, internal
  FROM rabbitmq.exchanges
  ORDER BY vhost, name
"

# Routing topology
coral sql "
  SELECT vhost, source, destination, destination_type, routing_key
  FROM rabbitmq.bindings
  ORDER BY vhost, source
"

# Active consumers
coral sql "
  SELECT queue_name, queue_vhost, consumer_tag, ack_required, prefetch_count
  FROM rabbitmq.consumers
"

# Active connections
coral sql "
  SELECT name, vhost, user, protocol, state, channels, peer_host
  FROM rabbitmq.connections
"

# Users and roles
coral sql "
  SELECT name, tags
  FROM rabbitmq.users
"

# Policies
coral sql "
  SELECT name, vhost, pattern, apply_to, priority, definition
  FROM rabbitmq.policies
  ORDER BY vhost, priority DESC
"
```

## Discovery order

```text
overview
  -> cluster_name, node

vhosts
  -> name (use as vhost context for queues, exchanges, bindings)

nodes
  -> name

queues
  -> name, vhost
  -> consumers (count)
  -> policy

exchanges
  -> name, vhost

bindings
  -> source (exchange name)
  -> destination (queue or exchange name)
  -> routing_key

connections
  -> name, vhost, user

consumers
  -> queue_name, queue_vhost
  -> consumer_tag

users
  -> name

policies
  -> name, vhost
  -> apply_to (queues, exchanges, all)
```
