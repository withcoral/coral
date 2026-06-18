# Webhooks

**Version:** 0.1.0
**Backend:** file
**Tables:** 2
**Default data directory:** `~/.webhooks/`

Query incoming webhook deliveries as a Coral SQL table. Each row stores a
JSONL event written by your own receiver or pipeline. Coral's file backend
reads that file and makes it queryable immediately.

This enables federated SQL queries over local event data (GitHub pushes,
Sentry alerts, Stripe payment failures, PagerDuty incidents) without polling
any external API — and without any data leaving the machine.

**Authentication:** None for reads (Coral reads a local file). If you run a
receiver that validates HMAC signatures, keep those secrets in the receiver,
not in this source spec.

---

## Tables

| Table | Key Columns |
|---|---|
| `deliveries` | `id`, `source`, `event_type`, `payload`, `received_at` |
| `subscriptions` | `source`, `receiver_url`, `secret_alias`, `events`, `active`, `registered_at` |

---

## Inputs

- `WEBHOOKS_PATH`: Path to the JSONL file containing webhook deliveries (default: `~/.webhooks/deliveries.jsonl`).
- `WEBHOOK_SUBSCRIPTIONS_PATH`: Path to the JSONL file containing subscription configurations (default: `~/.webhooks/subscriptions.jsonl`).

---

## Quick start

### 1. Copy fixture data to the default path

```bash
mkdir -p ~/.webhooks
cp sources/community/webhooks/fixtures/*.jsonl ~/.webhooks/
```

### 2. Add the source to Coral

```bash
coral source add --file ./sources/community/webhooks/manifest.yaml
```

### 3. Query deliveries via Coral SQL

```bash
coral sql "
SELECT
    id,
    source,
    event_type,
    received_at,
    json_get_str(payload, 'repository', 'full_name') AS repo
FROM webhooks.deliveries
WHERE source = 'github'
  AND event_type = 'push'
ORDER BY received_at DESC
"
```

Expected output:

```text
+--------------------------------------+--------+------------+----------------------+-----------------+
| id                                   | source | event_type | received_at          | repo            |
+--------------------------------------+--------+------------+----------------------+-----------------+
| 550e8400-e29b-41d4-a716-446655440000 | github | push       | 2024-06-18T12:00:00Z | withcoral/coral |
+--------------------------------------+--------+------------+----------------------+-----------------+
```

---

## JSONL row schemas

### Deliveries (`WEBHOOKS_PATH`)

Each row written to `WEBHOOKS_PATH` must be a JSON object with these fields:

| Field | Type | Description |
|---|---|---|
| `id` | string (UUID) | Unique delivery ID |
| `source` | string | Originating service name, lowercase (e.g. `github`) |
| `event_type` | string | Provider event type (e.g. `push`, `issues.opened`) |
| `payload` | string | Raw JSON body as a string |
| `received_at` | ISO 8601 string | UTC timestamp when the delivery was received |
| `hmac_valid` | boolean | Set by the producer to indicate whether HMAC signature was verified |
| `delivery_id` | string | Provider-assigned delivery ID, if present |
| `content_length_bytes` | integer | Byte length of the raw payload |

### Subscriptions (`WEBHOOK_SUBSCRIPTIONS_PATH`)

Each row written to `WEBHOOK_SUBSCRIPTIONS_PATH` configures a webhook source:

| Field | Type | Description |
|---|---|---|
| `source` | string | Service name this subscription is for, e.g. `github` |
| `receiver_url` | string | Public URL for receiving webhooks from this source |
| `secret_alias` | string | Name of the credential alias for the HMAC secret, if any |
| `events` | string | Comma-separated list of event types this subscription covers |
| `active` | boolean | Whether this subscription is currently active |
| `registered_at` | ISO 8601 string | UTC timestamp when this subscription was registered |

---

## Example: correlate push events with deploys and errors

```sql
SELECT
    w.received_at                                           AS push_time,
    json_get_str(w.payload, 'pusher', 'name')        AS pusher,
    json_get_str(w.payload, 'repository', 'name')    AS repo,
    json_get_str(w.payload, 'after')                 AS new_sha,
    d.id                                                     AS deploy_id,
    d.status                                                 AS deploy_status,
    d.created_at                                             AS deploy_time,
    s.title                                                  AS sentry_error
FROM webhooks.deliveries AS w
INNER JOIN github.deployments AS d
    ON d.sha = json_get_str(w.payload, 'after')
LEFT JOIN sentry.issues AS s
    ON s.first_seen >= w.received_at
    AND s.first_seen <= w.received_at + INTERVAL '15 minutes'
WHERE w.source = 'github'
  AND w.event_type = 'push'
  AND w.received_at >= NOW() - INTERVAL '2 hours'
ORDER BY
    w.received_at DESC
LIMIT 20
```

---

## Security

- The JSONL files are local only — they never leave the machine.
- HMAC secrets belong in your receiver/ingestion pipeline, not in this source spec.
- Treat the JSONL files as sensitive if payloads contain private data.
