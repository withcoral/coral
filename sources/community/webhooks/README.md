# sources/webhooks — Webhook Deliveries as SQL

Coral source spec that exposes incoming webhook deliveries as a queryable SQL
table. A local HMAC-validating receiver listens on an HTTP port, verifies
per-source signatures, and appends each accepted delivery as a JSONL row to
disk. Coral's file backend makes that file queryable immediately.

This design enables federated SQL queries over real-time event data (GitHub
pushes, Sentry alerts, Stripe payment failures, PagerDuty incidents) without
polling any external API — and without any data leaving the machine.

**Authentication:** None for reads (Coral reads a local file). Per-source HMAC
secrets are required by the receiver for ingestion.

**Backend:** File (Coral file backend reads JSONL written by the receiver).

---

## Tables

| Table | Key Columns |
|---|---|
| `deliveries` | `id`, `source`, `event_type`, `payload`, `received_at` |
| `subscriptions` | `source`, `receiver_url`, `secret_alias`, `events`, `active`, `registered_at` |

---

## Inputs

- `WEBHOOKS_PATH`: Path to the JSONL file containing webhook deliveries (default: `~/.kraken/webhooks.jsonl`).
- `WEBHOOK_SUBSCRIPTIONS_PATH`: Path to the JSONL file containing subscription configurations (default: `~/.kraken/webhooks-subscriptions.jsonl`).

---

## Quick start

### 1. Create sample webhook data

Create a local `webhooks.jsonl` file with sample deliveries:

```bash
echo '{"id": "550e8400-e29b-41d4-a716-446655440000", "source": "github", "event_type": "push", "payload": "{\"repository\": {\"full_name\": \"withcoral/coral\"}}", "received_at": "2023-10-24T12:00:00Z", "hmac_valid": true, "delivery_id": "123", "content_length_bytes": 50}' > webhooks.jsonl
```

### 2. Add the source to Coral

```bash
coral source add --file ./sources/community/webhooks/manifest.yaml --WEBHOOKS_PATH ./webhooks.jsonl
```

### 3. Query deliveries via Coral SQL

```sql
SELECT
    id,
    source,
    event_type,
    received_at,
    json_get_str(payload, '$.repository.full_name') AS repo
FROM webhooks.deliveries
WHERE source = 'github'
  AND event_type = 'push'
  AND received_at >= NOW() - INTERVAL '1 hour'
ORDER BY received_at DESC
```

---

## Supported sources

| Source | Signature header | Algorithm |
|---|---|---|
| `github` | `X-Hub-Signature-256` | HMAC-SHA256 |
| `sentry` | `Sentry-Hook-Signature` | HMAC-SHA256 |
| `stripe` | `Stripe-Signature` | Timestamp + HMAC-SHA256 |
| `pagerduty` | `X-PagerDuty-Signature` / `X-Webhook-Subscription` | HMAC-SHA256 |
| `linear` | `Linear-Signature` | Timestamp replay checks + HMAC-SHA256 |

Add new sources by extending your receiver's signature header map and setting
the corresponding secret environment variable.

---

## JSONL row schemas

### Deliveries (`WEBHOOKS_PATH`)

Each row written to `WEBHOOKS_PATH` must be a JSON object with these fields:

| Field | Type | Description |
|---|---|---|
| `id` | string (UUID) | Unique delivery ID assigned by the receiver |
| `source` | string | Originating service name, lowercase (e.g. `github`) |
| `event_type` | string | Provider event type (e.g. `push`, `issues.opened`) |
| `payload` | string | Raw JSON body as a string |
| `received_at` | ISO 8601 string | UTC timestamp when the receiver accepted the delivery |
| `hmac_valid` | boolean | Whether HMAC signature was verified before writing |
| `delivery_id` | string | Provider-assigned delivery ID, if present |
| `content_length_bytes` | integer | Byte length of the raw payload |

### Subscriptions (`WEBHOOK_SUBSCRIPTIONS_PATH`)

Each row written to `WEBHOOK_SUBSCRIPTIONS_PATH` configures a webhook source:

| Field | Type | Description |
|---|---|---|
| `source` | string | Service name this subscription is for, e.g. `github` |
| `receiver_url` | string | Public URL where the provider will deliver webhooks |
| `secret_alias` | string | Name of the credential alias that holds the HMAC secret for this source |
| `events` | string | Comma-separated list of event types this subscription covers |
| `active` | boolean | Whether this subscription is currently active |
| `registered_at` | ISO 8601 string | UTC timestamp when this subscription was registered |

---

## Example: correlate push events with deploys and errors

```sql
SELECT
    w.received_at                                           AS push_time,
    json_get_str(w.payload, '$.pusher.name')        AS pusher,
    json_get_str(w.payload, '$.repository.name')    AS repo,
    json_get_str(w.payload, '$.after')              AS new_sha,
    d.id                                                    AS deploy_id,
    d.status                                                AS deploy_status,
    d.created_at                                            AS deploy_time,
    s.title                                                 AS sentry_error
FROM webhooks.deliveries AS w
INNER JOIN github.deployments AS d
    ON d.sha = json_get_str(w.payload, '$.after')
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

- The receiver rejects all deliveries that fail HMAC validation with HTTP 403.
- Secrets are never logged or included in responses.
- HMAC comparisons should use constant-time comparison to prevent timing attacks.
- The JSONL file is local only — it never leaves the machine.