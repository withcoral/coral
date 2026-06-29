# Calendly source

Query Calendly event types, scheduled meetings, organization members,
routing forms, webhook subscriptions, and invitees through SQL.

## Authentication

This source uses a **Personal Access Token** (PAT). PATs give access to the
Calendly account that generated them; org admin tokens also give access to
org-wide data.

1. Go to [Calendly Integrations & API](https://calendly.com/integrations/api_webhooks)
2. Under **API & Webhooks → Personal Access Tokens**, click **Generate New Token**
3. Give the token a name and copy it — it is only shown once

You also need your **organization URI**. Export your token first, then retrieve
the org URI with:

```bash
export CALENDLY_API_TOKEN='your_token'

curl -s -H "Authorization: Bearer $CALENDLY_API_TOKEN" \
  https://api.calendly.com/users/me \
  | jq -r '.resource.current_organization'
```

If `jq` prints `null`, the token is missing/invalid or the response was an error.
Inspect the full payload with `curl ... | jq .` before filtering.

The value looks like `https://api.calendly.com/organizations/AAAAAAAAAAAAAAAA`.

For the `organization_invitations` table you also need the **organization UUID**
— the trailing segment of the org URI, e.g. `AAAAAAAAAAAAAAAA`.

### API token scopes

New [Personal Access Tokens and OAuth apps](https://developer.calendly.com/scopes)
must be granted explicit scopes. Legacy tokens created before scoped permissions
may still have full API access until they are rotated.

**Minimum read scopes for this Coral source** (query all tables and table
functions in the manifest):

| Scope | Used for |
|-------|----------|
| `users:read` | Manual setup curl (`GET /users/me`) to discover org URI — not used by Coral at query time |
| `event_types:read` | `calendly.event_types` |
| `scheduled_events:read` | `calendly.scheduled_events`, `calendly.event_invitees`, `calendly.scheduled_event_hosts` |
| `organizations:read` | `calendly.organization_memberships`, `calendly.organization_invitations` |
| `routing_forms:read` | `calendly.routing_forms`, `calendly.routing_form_submissions` (Professional, Teams, or Enterprise) |
| `webhooks:read` | `calendly.webhook_subscriptions` (paid plan: Professional, Standard, Teams, or Enterprise; owner/admin token for org scope) |

Those six scopes are **sufficient for read-only Coral usage**.

## Install

```bash
# From the coral repo root:
CALENDLY_API_TOKEN=your_token \
CALENDLY_ORG_URI=https://api.calendly.com/organizations/AAAAAAAAAAAAAAAA \
coral source add --file sources/community/calendly/manifest.yaml

# Optional: set CALENDLY_ORG_UUID only when querying organization_invitations
coral source test calendly
```

Or interactively:

```bash
coral source add --interactive --file sources/community/calendly/manifest.yaml
coral source test calendly
```

## Tables

| Table | Description | Plan required |
|---|---|---|
| `calendly.event_types` | Scheduling page templates (event type definitions) | Free |
| `calendly.scheduled_events` | Booked meetings — active and canceled | Free |
| `calendly.organization_memberships` | Members of the organization with role and contact details | Free |
| `calendly.routing_forms` | Routing forms for the organization | Professional, Teams, or Enterprise |
| `calendly.webhook_subscriptions` | Active webhook endpoints and subscribed event types (`events` column) | Paid plan (Professional, Standard, Teams, or Enterprise); owner/admin token |
| `calendly.organization_invitations` | Pending and accepted invitations to join the org | Free (requires `CALENDLY_ORG_UUID`; queries without it fail) |

## Table functions

| Function | Description | Plan required |
|---|---|---|
| `calendly.event_invitees(event_uuid => '...')` | Invitees for one specific scheduled event | Free |
| `calendly.routing_form_submissions(form_uuid => '...')` | Submissions for one specific routing form | Professional, Teams, or Enterprise |
| `calendly.scheduled_event_hosts(event_uuid => '...')` | Hosts assigned to one specific scheduled event | Free |

Call them with named arguments, for example
`calendly.event_invitees(event_uuid => 'YOUR_EVENT_UUID')`.

The `event_uuid` and `form_uuid` arguments are the trailing segments of the
respective URIs. For an event at
`https://api.calendly.com/scheduled_events/AAAAAAAAAAAAAAAA`, pass
`AAAAAAAAAAAAAAAA`.

## Filters

`calendly.scheduled_events` supports provider-side filters pushed directly to
the API rather than evaluated locally:

| Filter | Type | Description |
|---|---|---|
| `status` | `Utf8` | `'active'` or `'canceled'` |
| `invitee_email` | `Utf8` | Filter events that include a specific invitee |
| `min_start_time` | `Utf8` | ISO 8601 lower bound on event start time |
| `max_start_time` | `Utf8` | ISO 8601 upper bound on event start time |
| `sort_order` | `Utf8` | `'start_time:asc'` (default) or `'start_time:desc'` — controls API-side page ordering; add SQL `ORDER BY` after fetching for a different final order |

Use `=` in SQL WHERE clauses for these filters. Always provide
`min_start_time`/`max_start_time` for large orgs to avoid scanning all pages.

Coral stops paging at **500 total rows by default** on `scheduled_events` so a
bare `SELECT *` does not walk the full org history. That cap is separate from
Calendly's per-request `count` parameter (1–100 rows per page; this table
requests up to 100 per page). Narrow with time-range or other pushdown filters,
or add an explicit `LIMIT` greater than 500 when you need more rows.

## Example queries

```sql
-- All active event types with their scheduling URLs
SELECT name, scheduling_url, duration, active, profile__name
FROM calendly.event_types
WHERE active = true
ORDER BY name;

-- Adjust min_start_time / max_start_time to your org's data window
-- Events in a date range (provider-side min_start_time and max_start_time filters)
SELECT name, start_time, end_time, status, location__type, location__location
FROM calendly.scheduled_events
WHERE min_start_time = '2025-12-01T00:00:00Z'
  AND max_start_time = '2025-12-31T23:59:59Z'
ORDER BY start_time;

-- Newest bookings first (push sort_order to the API, not SQL ORDER BY)
SELECT name, start_time, status, invitees_counter__total
FROM calendly.scheduled_events
WHERE min_start_time = '2025-01-01T00:00:00Z'
  AND max_start_time = '2026-12-31T23:59:59Z'
  AND sort_order = 'start_time:desc'
LIMIT 25;

-- Canceled events with cancellation details
SELECT name, start_time,
       cancellation__canceled_by,
       cancellation__canceler_type,
       cancellation__reason
FROM calendly.scheduled_events
WHERE status = 'canceled'
  AND min_start_time = '2025-01-01T00:00:00Z'
  AND max_start_time = '2026-12-31T23:59:59Z'
ORDER BY start_time DESC;

-- Meetings that include a specific invitee email (API filter)
SELECT name, start_time, status
FROM calendly.scheduled_events
WHERE invitee_email = 'someone@example.com'
  AND min_start_time = '2025-01-01T00:00:00Z'
ORDER BY start_time DESC;

-- Organization members and their roles
SELECT user__name, user__email, role, user__timezone
FROM calendly.organization_memberships
ORDER BY role, user__name;

-- Bookings per event type (join on event type URI)
SELECT t.name AS event_type_name, COUNT(*) AS bookings
FROM calendly.scheduled_events s
JOIN calendly.event_types t ON t.uri = s.event_type
WHERE s.status = 'active'
  AND s.min_start_time = '2025-01-01T00:00:00Z'
  AND s.max_start_time = '2026-12-31T23:59:59Z'
GROUP BY t.name
ORDER BY bookings DESC;

-- Discover a scheduled event UUID first (use the trailing segment of uri)
SELECT uri, name, start_time
FROM calendly.scheduled_events
WHERE min_start_time = '2025-01-01T00:00:00Z'
  AND max_start_time = '2026-12-31T23:59:59Z'
ORDER BY start_time DESC
LIMIT 5;

-- Invitees for one scheduled event (replace YOUR_EVENT_UUID)
SELECT name, email, status, timezone, cancel_url,
       json_get_str(questions_and_answers, '0', 'answer') AS first_answer
FROM calendly.event_invitees(event_uuid => 'YOUR_EVENT_UUID');

-- Invitees enriched with event metadata (same YOUR_EVENT_UUID as above)
SELECT i.name, i.email, i.status, se.name AS event_name, se.start_time
FROM calendly.event_invitees(event_uuid => 'YOUR_EVENT_UUID') i
JOIN calendly.scheduled_events se ON se.uri = i.event
WHERE se.min_start_time = '2025-01-01T00:00:00Z'
  AND se.max_start_time = '2026-12-31T23:59:59Z';

-- Routing forms (Professional, Teams, or Enterprise); status should be published, not draft
SELECT name, status, created_at
FROM calendly.routing_forms
ORDER BY name;

-- Submissions for one routing form (published form on a Routing Forms plan)
SELECT uri,
       tracking__utm_source,
       tracking__utm_campaign,
       tracking__utm_content,
       tracking__utm_term,
       created_at
FROM calendly.routing_form_submissions(form_uuid => 'AAAAAAAAAAAAAAAA')
ORDER BY created_at DESC;

-- Webhook subscriptions (paid plan + owner/admin token; empty if none configured)
SELECT callback_url, state, scope, events, created_at
FROM calendly.webhook_subscriptions
ORDER BY created_at DESC;

-- Pending organization invitations
SELECT email, status, last_sent_at, created_at
FROM calendly.organization_invitations
WHERE status = 'pending'
ORDER BY created_at DESC;

-- Hosts for one event (from event_memberships on GET /scheduled_events/{uuid})
SELECT user, user_email, user_name, buffered_start_time, buffered_end_time
FROM calendly.scheduled_event_hosts(event_uuid => 'AAAAAAAAAAAAAAAA');

-- Hosts enriched with org membership (join user URI columns)
SELECT h.user_email, h.user_name, m.role, m.user__timezone
FROM calendly.scheduled_event_hosts(event_uuid => 'AAAAAAAAAAAAAAAA') h
JOIN calendly.organization_memberships m ON m.user__uri = h.user;
```

## Notes

**Rate limits.** Calendly enforces per-minute, per-token rate limits. Coral
reads the `X-RateLimit-Remaining` and `X-RateLimit-Reset` response headers to
back off automatically. Always use `min_start_time`/`max_start_time` filters
on `scheduled_events` for large orgs to avoid scanning all pages.

**Admin vs. member tokens.** A token belonging to an org admin can query
org-wide `scheduled_events` and `organization_memberships`. Member tokens see
only their own events. `event_types` is always scoped to the org.

**Timestamps.** All `Timestamp` columns are UTC. The `min_start_time` and
`max_start_time` filters accept ISO 8601 strings with a `Z` or `+00:00` suffix.

**UUID extraction.** Table functions take the UUID portion of a URI. Strip the
prefix (`https://api.calendly.com/scheduled_events/` or
`https://api.calendly.com/routing_forms/`) to get the UUID string.

**`CALENDLY_ORG_UUID`.** Optional at install (defaults to empty). Set it only
when querying `organization_invitations`; it is the final path segment of
`CALENDLY_ORG_URI`. Querying `organization_invitations` without it set produces
a request error.

**Plan requirements.** `routing_forms` and `routing_form_submissions` require a
Calendly plan with [Routing Forms](https://calendly.com/help/how-to-create-a-routing-form)
enabled (Professional, Teams, or Enterprise; owner/admin). See also
[manage routing forms](https://calendly.com/help/how-to-manage-routing-forms).
`webhook_subscriptions` requires a
[paid plan with API/webhook access](https://calendly.com/help/calendly-api-overview)
(Professional, Standard, Teams, or Enterprise, depending on account plan naming;
see [Getting started](https://developer.calendly.com/getting-started)). This table
queries organization-scoped subscriptions (`scope=organization`) and requires an
[owner/admin token](https://developer.calendly.com/receive-data-from-scheduled-events-in-real-time-with-webhook-subscriptions);
members with only `webhooks:read` will not see org-wide subscriptions. All other
tables and functions work on the free plan.

**`webhook_subscriptions` and admin scope.** This table queries
organization-scoped webhook subscriptions (`scope=organization`). Only org
owner and admin tokens can list org-level webhooks; member tokens return an
empty result set even on a paid plan with `webhooks:read` granted.

**Routing form submissions.** Calendly lists submissions at
`GET /routing_form_submissions` with the `form` query parameter set to the full
routing form URI (Coral builds that URI from `form_uuid`). The form must be
**published** (not `draft`), and submissions appear after someone completes the
public form. Tracking columns include `tracking__utm_source`, `tracking__utm_medium`,
`tracking__utm_campaign`, `tracking__utm_content`, `tracking__utm_term`, and
`tracking__salesforce_uuid`. The API also returns `submitter` and `submitter_type`
when a submission results in a booking; those fields are not mapped in this source
yet, so join to `scheduled_events` via invitee or webhook workflows instead.

**`questions_and_answers` columns.** Both `event_invitees` and
`routing_form_submissions` expose a `questions_and_answers` column of type
`Json`. Invitee booking Q&A elements have `question`, `answer`, and `position`.
Routing form submission Q&A elements also include `question_uuid`. Use the built-in
JSON functions to extract individual answers, for example
`json_get_str(questions_and_answers, '0', 'answer')` for the first question's answer.
