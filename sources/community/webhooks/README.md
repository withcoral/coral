# sources/webhooks — Webhook Deliveries as SQL

Coral source spec that exposes incoming webhook deliveries as a queryable SQL
table. A lightweight FastAPI receiver (`kraken/webhook_receiver.py`) runs
locally on port 9000, validates per-source HMAC signatures, and appends each
valid delivery to a local JSONL file. Coral's file backend makes that file
queryable immediately.

This design enables KRAKEN voyages to JOIN real-time event data (GitHub pushes,
Sentry alerts, Stripe payment failures, PagerDuty incidents) directly into
federated SQL queries without polling any external API — and without any data
leaving the laptop.

**Authentication:** None for reads (Coral reads a local file). Per-source HMAC
secrets are required by the receiver for ingestion.

**Backend:** File (Coral file backend reads JSONL written by the receiver).

**Bounty eligible:** Yes — $100 + $50 charity (see PROPOSAL.md)

---

## Tables

| Table | Key Columns |
|---|---|
| `deliveries` | `id`, `source`, `event_type`, `payload`, `received_at` |
| `subscriptions` | `source`, `receiver_url`, `secret_alias`, `active` |

---

## Quick start

### 1. Start the receiver

```bash
# Via kraken CLI
kraken webhook:serve

# Or directly with uvicorn
uvicorn kraken.webhook_receiver:app --port 9000
```

### 2. Configure secrets (per source)

```bash
# Set the HMAC secret for GitHub webhooks
export KRAKEN_WEBHOOK_SECRET_GITHUB="your-github-webhook-secret"

# Or use a single shared secret for local testing
export KRAKEN_WEBHOOK_SECRET="dev-secret"
```

### 3. Expose publicly (for external providers)

Use ngrok or a similar tunnel so GitHub / Sentry / Stripe can reach port 9000:

```bash
ngrok http 9000
# Then configure https://<ngrok-id>.ngrok.io/webhook/github in GitHub
```

### 4. Query deliveries via Coral SQL

```sql
SELECT
    id,
    source,
    event_type,
    received_at,
    json_extract_string(payload, '$.repository.full_name') AS repo
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
| `stripe` | `Stripe-Signature` | HMAC-SHA256 |
| `pagerduty` | `X-Webhook-Signature` | HMAC-SHA256 |
| `linear` | `Linear-Signature` | HMAC-SHA256 |

Add new sources by extending `_SOURCE_SIG_HEADERS` in
`kraken/webhook_receiver.py` and setting the corresponding
`KRAKEN_WEBHOOK_SECRET_<SOURCE>` environment variable.

---

## Example: Hot Deploy voyage with webhook trigger

```sql
SELECT
    w.received_at                                           AS push_time,
    json_extract_string(w.payload, '$.pusher.name')        AS pusher,
    json_extract_string(w.payload, '$.repository.name')    AS repo,
    json_extract_string(w.payload, '$.after')              AS new_sha,
    d.id                                                    AS deploy_id,
    d.status                                                AS deploy_status,
    d.created_at                                            AS deploy_time,
    s.title                                                 AS sentry_error
FROM webhooks.deliveries AS w
JOIN github.deployments AS d
    ON d.sha = json_extract_string(w.payload, '$.after')
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
- HMAC comparisons use `hmac.compare_digest` to prevent timing attacks.
- The JSONL file is local only — it never leaves the laptop unless you
  explicitly expose it (which would violate CLAUDE.md Rule 6).
