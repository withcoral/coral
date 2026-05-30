# Knock Coral source

Query Knock notification infrastructure with Coral. The source focuses on
delivery debugging and message observability through Knock's public API.

## Setup

Create a Knock secret API key in the Knock dashboard for the environment you
want to inspect, such as development or production. Public API keys are not
sufficient for these server-side endpoints. Use a key with read access to
messages, recipients/users, tenants, and message observability data.

```bash
KNOCK_API_KEY=... coral source add --file sources/community/knock/manifest.yaml
```

Run validation:

```bash
coral source test knock
```

First-success query after setup:

```sql
SELECT id, source__key, source__type, status, channel__type, inserted_at
FROM knock.messages
ORDER BY inserted_at DESC
LIMIT 5;
```

## Tables

| Table | Description |
| --- | --- |
| `knock.messages` | Notification messages and delivery status. |
| `knock.users` | Knock user records. |
| `knock.tenants` | Tenant records. |
| `knock.message_events` | Events for a required `message_id`. |
| `knock.message_delivery_logs` | Downstream provider delivery logs for a required `message_id`. |
| `knock.message_activities` | Trigger activities for a required `message_id`. |

## Example queries

```sql
SELECT id, source__key, status, channel__type, tenant, inserted_at
FROM knock.messages
WHERE status != 'delivered'
ORDER BY inserted_at DESC
LIMIT 20;
```

```sql
SELECT id, source__key, workflow_run_id, workflow_recipient_run_id, status
FROM knock.messages
WHERE inserted_at_gte = '2026-05-01T00:00:00Z'
ORDER BY inserted_at DESC
LIMIT 20;
```

```sql
SELECT id, name, email, created_at
FROM knock.users
LIMIT 20;
```

```sql
SELECT type, inserted_at
FROM knock.message_events
WHERE message_id = 'message_id_here'
ORDER BY inserted_at DESC;
```

```sql
SELECT service_name, response__status, request__host, inserted_at
FROM knock.message_delivery_logs
WHERE message_id = 'message_id_here'
ORDER BY inserted_at DESC;
```

## Notes

- This source is read-only.
- The public Knock API exposes notification runtime data such as messages,
  users, tenants, events, delivery logs, and activities.
- The Knock API key selects the environment being queried. If the same
  recipient/message exists in multiple environments, add or use the key from
  the exact environment you want to inspect.
- Operational workflow debugging starts from `knock.messages`: `source__key`
  identifies the workflow, broadcast, or guide that generated the message;
  `status`, `channel__type`, `tenant`, and `inserted_at` support delivery
  triage.
- `workflow_run_id` and `workflow_recipient_run_id` are read from the nested
  `source` object in Knock message responses.
- List endpoints use Knock cursor pagination with `page_size` up to 50. For
  routine monitoring, prefer recent `inserted_at_gte` / `inserted_at_lte`
  windows instead of broad historical scans. Knock rate limits apply by API
  tier and endpoint.
- Workflow template management lives in Knock's separate Management API at
  `https://control.knock.app` and requires a service token, so it is
  intentionally not mixed into this runtime source.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/knock/manifest.yaml
make lint-sources
yamllint sources/community/knock/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed `coral source add --file`, `coral source test knock`, and
representative live queries require a Knock API key and were not run in this
workspace.

## API references

- https://docs.knock.app/api-reference
