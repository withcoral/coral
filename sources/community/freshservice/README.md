# Freshservice

Query Freshservice ITSM and asset metadata from Coral. The source covers
tickets, agents, requesters, departments, assets, and locations.

## Authentication

Create or copy a Freshservice API key from your Freshservice profile and
provide:

| Input | Description |
| --- | --- |
| `FRESHSERVICE_DOMAIN` | Account URL, for example `https://example.freshservice.com`. |
| `FRESHSERVICE_API_KEY` | Freshservice API key. |

Freshservice API keys are modeled as secrets and sent using HTTP Basic auth as
the username with `X` as the placeholder password. Use a least-privilege agent
or admin account whose API key can read only the records Coral agents need.

Official docs:

- <https://api.freshservice.com/>
- <https://api.freshservice.com/#tickets>
- <https://api.freshservice.com/v2/#view_all_assets_for_freshservice_itam>

## Tables

| Table | Description |
| --- | --- |
| `freshservice.tickets` | Ticket metadata. Supports `updated_since`, `requester_id`, `email`, and `workspace_id`. |
| `freshservice.agents` | Agents. |
| `freshservice.requesters` | Requesters. |
| `freshservice.departments` | Departments. Supports `workspace_id`. |
| `freshservice.assets` | Current Freshservice ITAM assets. Supports `last_updated_gt` and `last_updated_lt`. |
| `freshservice.locations` | Locations. Supports `workspace_id`. |

## Examples

List recently updated tickets:

```sql
SELECT id, subject, status, priority, requester_id, updated_at
FROM freshservice.tickets
WHERE updated_since = '2026-05-01T00:00:00Z'
ORDER BY updated_at DESC
LIMIT 25;
```

Map tickets to agents:

```sql
SELECT t.id, t.subject, t.status, a.email AS responder_email
FROM freshservice.tickets AS t
LEFT JOIN freshservice.agents AS a
  ON t.responder_id = a.id
LIMIT 25;
```

Review asset ownership:

```sql
SELECT id, display_id, name, user_id, department_id, location_id
FROM freshservice.assets
WHERE last_updated_gt = '2026-05-01T00:00:00Z'
LIMIT 25;
```

## Notes

- Freshservice list endpoints are modeled with `page` and `per_page`
  pagination. Page size is capped at 100.
- Ticket conversations, notes, and asset custom-field payloads are intentionally
  omitted from v1 to keep the source read-only and avoid exposing sensitive
  text or credentials.
- `freshservice.assets` uses the current Freshservice ITAM endpoint
  `/api/v2/itam/assets`. The legacy `/api/v2/assets` endpoint for older
  signups is not modeled by this source.
- Ticket and asset tables have conservative default fetch limits. Use provider
  filters and SQL `LIMIT` for production service desks.
- Freshservice workspace and MSP accounts may default departments and locations
  to a primary workspace when `workspace_id` is omitted. Provide `workspace_id`
  to query a specific workspace.
- Results depend on the API key owner's Freshservice role and workspace access.

## Validation

- YAML parsing: passed
- Coral manifest schema validation: passed
- `git diff --check`: passed
- `make lint-sources`: passed

Manual Freshservice tenant validation:

```text
API verified: Freshservice tenant and tickets endpoint responded as expected.
Tenant: https://opensourcecontributor.freshservice.com
Endpoint checked: GET /api/v2/tickets

Verified:
- Tenant is reachable.
- Freshservice API v2 tickets endpoint exists.
- Unauthenticated API access is blocked with 403 access_denied.
- A live test incident was created in the tenant (#INC-1).
- Authenticated browser-session API access returned ticket data.
```
