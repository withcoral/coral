# Zendesk Source Spec

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 16
**Base URL:** `https://{subdomain}.zendesk.com/api/v2`

Query the entirety of your Zendesk Support operation via SQL — tickets, users, organizations, groups, ticket comments, ticket metrics, satisfaction ratings, SLA policies, macros, triggers, automations, views, brands, ticket fields, ticket forms, and tags. Turn your support data into a queryable analytics layer for CSAT dashboards, SLA compliance auditing, agent performance tracking, and workflow automation analysis.

## Authentication

Requires three inputs:

| Input | Kind | Description |
|---|---|---|
| `ZENDESK_SUBDOMAIN` | variable | Your Zendesk subdomain (e.g., if URL is `https://mycompany.zendesk.com`, enter `mycompany`) |
| `ZENDESK_EMAIL` | variable | Email address of a Zendesk admin or agent account |
| `ZENDESK_API_TOKEN` | secret | API token generated from **Admin Center → Apps & Integrations → Zendesk API → Add API Token** |

Authentication uses Basic Auth with the format `{email}/token:{api_token}` per the [Zendesk API authentication docs](https://developer.zendesk.com/api-reference/introduction/security-and-auth/).

## Available Tables

### Core Support Data

| Table | Required Filter | Description |
|---|---|---|
| `tickets` | None | All support tickets — subject, status, priority, type, assignee, group, org, tags, CSAT score, channel, and timestamps. |
| `users` | None | All end-users, agents, and admins — name, email, role, organization, timezone, activity status. |
| `organizations` | None | Customer organizations (companies) — name, domain mapping, shared ticket settings, tags. |
| `groups` | None | Agent groups (teams) — name, description, default flag. Tickets must be assigned to a group. |

### Ticket Drill-Down

| Table | Required Filter | Description |
|---|---|---|
| `ticket_comments` | `ticket_id` | Full conversation thread — body, html_body, public/private flag, author, channel, attachment count. |
| `ticket_metrics` | None | Time-based performance data — reply time, first/full resolution time, wait times, reopens, replies (business & calendar hours). |

### Quality & Compliance

| Table | Required Filter | Description |
|---|---|---|
| `satisfaction_ratings` | None | CSAT scores (good/bad/offered), customer comments, reason codes, per-ticket and per-agent. |
| `sla_policies` | None | SLA policy definitions — conditions, metric targets, evaluation order. |

### Workflow Automation

| Table | Required Filter | Description |
|---|---|---|
| `macros` | None | Pre-built agent actions — title, actions JSON, usage stats (7d/30d), restrictions. |
| `triggers` | None | Event-driven automation rules — conditions, actions, execution order. |
| `automations` | None | Time-based automation rules — conditions, actions, execution order. |
| `views` | None | Saved ticket queries (agent sidebar) — conditions, output columns, restrictions. |

### Configuration & Schema

| Table | Required Filter | Description |
|---|---|---|
| `brands` | None | Multi-brand configuration — subdomain, brand URL, host mapping, Help Center status. |
| `ticket_fields` | None | All system and custom ticket fields — type, title, required, dropdown options. |
| `ticket_forms` | None | Ticket form templates — field IDs, brand restrictions, end-user visibility. |
| `tags` | None | Global tag inventory with usage counts — essential for tag hygiene audits. |

## Quick Start

```bash
# Step 1 — add the source spec to your workspace
coral source add --file sources/community/zendesk/manifest.yaml --interactive
# You will be prompted for ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, and ZENDESK_API_TOKEN
```

## Example Queries

### Basic Ticket Analytics

```sql
-- List recent open tickets with priority and assignee
SELECT id, subject, status, priority, assignee_id, group_id, created_at
FROM zendesk.tickets
WHERE status = 'open'
LIMIT 25;

-- Count tickets by status
SELECT status, COUNT(*) as ticket_count
FROM zendesk.tickets
GROUP BY status;

-- Find unassigned urgent tickets
SELECT id, subject, created_at
FROM zendesk.tickets
WHERE priority = 'urgent' AND assignee_id IS NULL;
```

### Agent Performance & Workload

```sql
-- Tickets per agent with average resolution time
SELECT t.assignee_id, u.name AS agent_name, COUNT(*) AS ticket_count,
       AVG(m.full_resolution_time_business) AS avg_resolution_mins
FROM zendesk.tickets t
JOIN zendesk.users u ON t.assignee_id = u.id
JOIN zendesk.ticket_metrics m ON t.id = m.ticket_id
GROUP BY t.assignee_id, u.name;

-- Workload distribution across groups
SELECT g.name AS group_name, COUNT(*) AS open_tickets
FROM zendesk.tickets t
JOIN zendesk.groups g ON t.group_id = g.id
WHERE t.status IN ('new', 'open', 'pending')
GROUP BY g.name;
```

### SLA Compliance Auditing

```sql
-- Tickets with first reply time exceeding 60 business minutes
SELECT m.ticket_id, t.subject, t.priority,
       m.reply_time_business AS first_reply_mins
FROM zendesk.ticket_metrics m
JOIN zendesk.tickets t ON m.ticket_id = t.id
WHERE m.reply_time_business > 60;

-- Review SLA policy targets
SELECT id, title, description, position, policy_metrics
FROM zendesk.sla_policies;
```

### Customer Satisfaction (CSAT) Analysis

```sql
-- CSAT breakdown by score
SELECT score, COUNT(*) AS rating_count
FROM zendesk.satisfaction_ratings
GROUP BY score;

-- Bad ratings with customer comments and agent context
SELECT sr.ticket_id, t.subject, sr.score, sr.comment, sr.reason,
       u.name AS agent_name
FROM zendesk.satisfaction_ratings sr
JOIN zendesk.tickets t ON sr.ticket_id = t.id
JOIN zendesk.users u ON sr.assignee_id = u.id
WHERE sr.score = 'bad';

-- CSAT by group (team-level quality tracking)
SELECT g.name AS group_name, sr.score, COUNT(*) AS count
FROM zendesk.satisfaction_ratings sr
JOIN zendesk.groups g ON sr.group_id = g.id
GROUP BY g.name, sr.score;
```

### Ticket Conversation Drill-Down

```sql
-- Get full conversation thread for a specific ticket
SELECT id, author_id, body, public, channel, created_at
FROM zendesk.ticket_comments
WHERE ticket_id = 12345;
```

### Organization & B2B Analytics

```sql
-- Ticket volume per customer organization
SELECT o.name AS org_name, COUNT(*) AS ticket_count
FROM zendesk.tickets t
JOIN zendesk.organizations o ON t.organization_id = o.id
GROUP BY o.name
ORDER BY ticket_count DESC
LIMIT 20;

-- Organizations with shared ticket access enabled
SELECT id, name, domain_names, shared_tickets
FROM zendesk.organizations
WHERE shared_tickets = true;
```

### Workflow Automation Audit

```sql
-- Active triggers ordered by execution priority
SELECT id, title, position, conditions, actions
FROM zendesk.triggers
WHERE active = true
ORDER BY position;

-- Most-used macros in the last 7 days
SELECT id, title, usage_7d, usage_30d
FROM zendesk.macros
WHERE active = true
ORDER BY usage_7d DESC
LIMIT 10;

-- Active automations (time-based rules)
SELECT id, title, conditions, actions
FROM zendesk.automations
WHERE active = true
ORDER BY position;
```

### Configuration & Schema Inspection

```sql
-- List all custom ticket fields
SELECT id, title, type, active, required
FROM zendesk.ticket_fields
WHERE removable = true;

-- Ticket forms with their field composition
SELECT id, name, display_name, active, ticket_field_ids
FROM zendesk.ticket_forms;

-- Brand overview for multi-brand instances
SELECT id, name, subdomain, brand_url, active, is_default, has_help_center
FROM zendesk.brands;

-- Tag hygiene: find most-used and least-used tags
SELECT name, count
FROM zendesk.tags
ORDER BY count DESC
LIMIT 50;
```

### Multi-Channel Analytics

```sql
-- Ticket volume by submission channel
SELECT channel, COUNT(*) AS ticket_count
FROM zendesk.tickets
GROUP BY channel
ORDER BY ticket_count DESC;
```

## Rate Limits

Zendesk enforces rate limits based on your plan tier. This source spec reads the `X-Rate-Limit-Remaining` and `Retry-After` headers for automatic rate-limit handling. Typical limits:

| Plan | Rate Limit |
|---|---|
| Team | 200 requests/minute |
| Professional | 400 requests/minute |
| Enterprise | 700 requests/minute |

Use `LIMIT` clauses on large tables (`tickets`, `users`, `ticket_metrics`) to avoid excessive API calls.

## Pagination

All list endpoints use **cursor-based pagination** (`page[size]` + `page[after]`) for optimal performance and to avoid the 10,000-record limit of offset pagination. Default page size is 100 (Zendesk maximum).

## Join Reference

Key relationships between tables for cross-referencing:

```
tickets.assignee_id      → users.id
tickets.requester_id     → users.id
tickets.submitter_id     → users.id
tickets.group_id         → groups.id
tickets.organization_id  → organizations.id
tickets.brand_id         → brands.id
tickets.ticket_form_id   → ticket_forms.id
ticket_comments.author_id → users.id
ticket_metrics.ticket_id → tickets.id
satisfaction_ratings.ticket_id → tickets.id
satisfaction_ratings.assignee_id → users.id
satisfaction_ratings.group_id → groups.id
users.organization_id    → organizations.id
users.default_group_id   → groups.id
organizations.group_id   → groups.id
```
