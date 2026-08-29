# Zendesk source

This community source lets Coral query core Zendesk Support data with an API
token.

The first version is intentionally narrow and read-only. It focuses on the
objects that matter most for support queue analysis:

- users
- organizations
- groups
- tickets
- comments for one ticket

## Authentication

Create a Zendesk API token in Admin Center under:

- Apps and integrations
- APIs
- Zendesk API

Then export:

```sh
export ZENDESK_BASE_URL="https://acme.zendesk.com"
export ZENDESK_EMAIL="you@example.com"
export ZENDESK_API_TOKEN="your_zendesk_api_token"
```

Coral uses Zendesk's documented token auth format:

- username: `your_email/token`
- password: `your_api_token`

## Quick start

```sh
coral source add zendesk
coral source test zendesk
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'zendesk' ORDER BY table_name"
```

If you update the token later, run `coral source add zendesk` again so Coral
refreshes the stored credentials.

## Inspect the installed shape

After adding the source, inspect what Coral sees:

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'zendesk'
ORDER BY table_name;
```

```sql
SELECT table_name, column_name, data_type, is_nullable
FROM coral.columns
WHERE schema_name = 'zendesk'
ORDER BY table_name, ordinal_position;
```

```sql
SELECT key, kind, required, is_set, default_value
FROM coral.inputs
WHERE schema_name = 'zendesk'
ORDER BY key;
```

This is useful for confirming required filters and seeing which nested Zendesk
payloads stay as JSON.

## Rate Limits

Zendesk API enforces rate limits that vary by plan:

- **Team**: 200 requests/minute
- **Professional/Growth**: 400 requests/minute
- **Enterprise**: 700 requests/minute
- **Enterprise Plus**: 2500 requests/minute

This source uses cursor-based pagination with a default page size of 100, which
efficiently respects these limits. If you encounter rate limit errors, consider
reducing query frequency or filtering by time ranges when available. Refer to the
[Zendesk rate limits documentation](https://developer.zendesk.com/api-reference/introduction/rate-limits/)
for endpoint-specific limits.

## Tables

| Table | Notes |
|---|---|
| `users` | User directory for the Zendesk account |
| `organizations` | Organization metadata |
| `groups` | Group metadata for ticket assignment |
| `tickets` | Core ticket queue table |
| `ticket_comments` | Comments for one ticket; requires `ticket_id` |

## How to query it

List users:

```sql
SELECT id, name, email, role
FROM zendesk.users
LIMIT 20;
```

List organizations:

```sql
SELECT id, name, shared_tickets, shared_comments
FROM zendesk.organizations
LIMIT 20;
```

List groups:

```sql
SELECT id, name, is_public
FROM zendesk.groups
LIMIT 20;
```

List recent tickets:

```sql
SELECT id, subject, status, priority, assignee_id, requester_id, updated_at
FROM zendesk.tickets
ORDER BY updated_at DESC
LIMIT 20;
```

Inspect comments for one ticket:

```sql
SELECT created_at, author_id, public, body
FROM zendesk.ticket_comments
WHERE ticket_id = '123456'
ORDER BY created_at DESC
LIMIT 20;
```

## Table behavior notes

- `ticket_comments` is a lookup-style table requiring a `ticket_id` filter to fetch comments for one ticket.
- `custom_fields` is stored as JSON because field structure is account-specific. Refer to your Zendesk admin panel for your field definitions.
- `tags`, `via`, `attachments`, `domain_names`, and `raw` remain JSON so the source stays stable across different Zendesk accounts.
- This source intentionally does not expose Zendesk search or export APIs in v1.

## Validation

If you are developing this source in the Coral repo, run:

```sh
cargo run --locked -p coral-cli -- source lint ./sources/community/zendesk/manifest.yaml
make lint-sources
make docs-generate
make docs-check
```

Then add and validate the source:

```sh
coral source add zendesk
coral source test zendesk
```

Inspect the installed shape:

```sh
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'zendesk' ORDER BY table_name"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'zendesk' ORDER BY table_name, ordinal_position"
coral sql "SELECT key, kind, required, is_set, default_value FROM coral.inputs WHERE schema_name = 'zendesk' ORDER BY key"
```

Then verify the data flow with a few real queries:

```sh
coral sql "SELECT id, name, email, role FROM zendesk.users LIMIT 20"
coral sql "SELECT id, name FROM zendesk.organizations LIMIT 20"
coral sql "SELECT id, name FROM zendesk.groups LIMIT 20"
coral sql "SELECT id, subject, status, updated_at FROM zendesk.tickets ORDER BY updated_at DESC LIMIT 20"
coral sql "SELECT created_at, author_id, public, body FROM zendesk.ticket_comments WHERE ticket_id = 'YOUR_TICKET_ID' ORDER BY created_at DESC LIMIT 20"
```
