# PagerDuty Connector

**API Version:** v2/v3
**Source:** OpenAPI-generated from PagerDuty's REST API spec
**Backend:** HTTP
**Tables:** 112
**Base URL:** `https://api.pagerduty.com`

## Authentication

Requires a `PAGERDUTY_API_TOKEN` credential. Add the source and provide your token when prompted:

```bash
coral source add pagerduty
```

### Token scopes

| Type | Coverage | Notes |
|---|---|---|
| General access token | Most tables | Recommended; generate from **My Profile > User Settings** |
| Account-level API key | All tables | Admin-only; found under **Account Settings > API Access** |

### Rate limiting

PagerDuty allows 960 API requests per minute (16/sec) for most endpoints. Analytics endpoints have lower limits.

## Table categories

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| No filter | 52 | `SELECT * FROM pagerduty.incidents` |
| `id` | 25 | `WHERE id = 'P1234AB'` |
| `service_id` | 3 | `WHERE service_id = 'PSVC123'` |
| `id` + secondary filter | 10 | `WHERE id = 'P1234AB' AND post_id = 'PT5678'` |
| Other filters | 22 | Various combinations |

### No filter required (52 tables)

| Table | Description |
|---|---|
| `incidents` | List incidents |
| `services` | List services |
| `users` | List users |
| `list_teams` | List teams |
| `escalation_policies` | List escalation policies |
| `schedules` | List schedules (v2) |
| `list_schedules_v3` | List schedules (v3) |
| `oncalls` | List on-calls |
| `log_entries` | List log entries |
| `maintenance_windows` | Maintenance windows |
| `me` | Current user profile |
| `abilities` | Account abilities |
| `priorities` | List priorities |
| `vendors` | List vendors |
| `notifications` | List notifications |
| `tags` | List tags |

### Incidents & alerts (15 tables)

| Table | Notes |
|---|---|
| `incidents` | List/search incidents |
| `incident_alerts` | Alerts on an incident (`id` required) |
| `incident_custom_fields` | Custom field definitions |
| `incident_custom_field_values` | Field values for an incident (`id` required) |
| `incident_type_custom_fields` | Custom fields by incident type |
| `incident_status_update_subscribers` | Status update subscribers (`id` required) |
| `incident_workflows` | Incident workflows |
| `incident_workflow_actions` | Available workflow actions |
| `analytic_raw_incidents` | Raw analytics data |
| `notes` | Incident notes (`id` required) |
| `outlier_incident` | Outlier incident for a service (`id` required) |
| `past_incidents` | Related past incidents (`id` required) |
| `related_incidents` | Related incidents (`id` required) |
| `related_change_events` | Related change events (`id` required) |
| `paused_incident_report_alerts` | Paused incident report alerts |

### Services (15 tables)

| Table | Notes |
|---|---|
| `services` | List services |
| `service_integrations` | Service integrations (`id` + `integration_id`) |
| `service_rules` | Service event rules (`id` required) |
| `service_custom_fields` | Service custom field definitions |
| `service_custom_field_values` | Field values for a service (`id` required) |
| `service_custom_field_field_options` | Field options (`field_id` required) |
| `service_dependency_business_services` | Business service dependencies (`id` required) |
| `service_impacts` | Service impacts (`url_slug` required) |
| `business_services` | List business services |
| `business_service_subscribers` | Subscribers (`id` required) |
| `business_service_supporting_service_impacts` | Supporting service impacts (`id` required) |
| `alert_grouping_settings` | Alert grouping settings |
| `active` | Service orchestration active status (`service_id` required) |
| `global` | Global orchestration for a service (`service_id` required) |
| `event_orchestration_services` | Service orchestration rules (`service_id` required) |

### Users & teams (16 tables)

| Table | Notes |
|---|---|
| `users` | List users |
| `me` | Current authenticated user |
| `list_teams` | List teams |
| `members` | Team members (`id` required) |
| `contact_methods` | User contact methods (`id` required) |
| `notification_rules` | User notification rules (`id` required) |
| `notification_subscriptions` | User notification subscriptions (`id` required) |
| `oncall_handoff_notification_rules` | On-call handoff rules (`id` required) |
| `user_session` | Single user session (`id` + `type` + `session_id`) |
| `user_sessions` | User active sessions (`id` required) |
| `licenses` | List licenses |
| `license` | Single license (`id` required) |
| `license_allocations` | License allocations |
| `status_update_notification_rules` | Status update notification rules (`id` required) |
| `oauth_clients` | OAuth clients |
| `oauth_delegations` | OAuth delegations (`id` required) |

### Schedules & on-call (7 tables)

| Table | Notes |
|---|---|
| `schedules` | List schedules (v2) |
| `list_schedules_v3` | List schedules (v3) |
| `oncalls` | List on-calls |
| `list_overrides` | Schedule overrides (v3, `id` required) |
| `rotations` | Schedule rotations (`id` required) |
| `custom_shifts` | Schedule custom shifts (`id` + `rotation_id`) |
| `events` | Schedule rotation events (`id` + `rotation_id`) |

### Event orchestrations (9 tables)

| Table | Notes |
|---|---|
| `event_orchestrations` | List event orchestrations |
| `event_orchestration_integrations` | Orchestration integrations (`id` required) |
| `event_orchestration_services` | Service orchestration rules (`service_id`) |
| `active` | Orchestration active status (`service_id`) |
| `global` | Global orchestration (`service_id`) |
| `router` | Orchestration router (`id` required) |
| `unrouted` | Unrouted orchestration rules (`id` required) |
| `cache_variables` | Cache variables (`service_id`) |
| `data` | Cache variable data (`id` + `cache_variable_id`) |

### Status pages & dashboards (13 tables)

| Table | Notes |
|---|---|
| `status_pages` | List status pages |
| `status_dashboards` | List status dashboards |
| `statuses` | Page statuses (`id` required) |
| `posts` | Status page posts (`id` required) |
| `post_updates` | Post updates (`id` + `post_id`) |
| `postmortem` | Post postmortem (`id` + `post_id`) |
| `status_page_services` | Page services (`id` required) |
| `status_page_impacts` | Page impacts (`id` required) |
| `severities` | Page severities (`id` required) |
| `subscriptions` | Page subscriptions (`id` required) |
| `url_slugs` | Page URL slugs (`url_slug` required) |
| `service_impacts` | Service impacts (`url_slug` required) |
| `enablements` | Enablements (`id` required) |

### Automation & workflows (9 tables)

| Table | Notes |
|---|---|
| `automation_action_actions` | Automation actions |
| `automation_action_action_services` | Action service associations (`id` required) |
| `runners` | Automation runners |
| `invocations` | Action invocations |
| `triggers` | Workflow triggers |
| `workflow_integrations` | Workflow integrations |
| `workflow_integration_connections` | Integration connections |
| `workflows_integration_connections` | Connections for a specific integration (`integration_id`) |
| `session_configurations` | Session configurations |

### Other tables

| Table | Notes |
|---|---|
| `escalation_policies` | List escalation policies |
| `rulesets` | List rulesets |
| `ruleset_rules` | Rules for a ruleset (`id` required) |
| `addons` | Installed add-ons |
| `extensions` | Installed extensions |
| `extension_schemas` | Extension schemas |
| `webhook_subscriptions` | Webhook subscriptions |
| `log_entries` | Log entries |
| `records` | Audit records |
| `change_events` | Change events |
| `maintenance_windows` | Maintenance windows |
| `templates` | Notification templates |
| `standards` | Standards definitions |
| `standard_scores` | Resource standard scores (`ids` + `resource_type`) |
| `standards_score` | Standards for a resource (`id` + `resource_type`) |
| `responses` | Incident responses |
| `fields` | Custom fields |
| `field_options` | Field options (`field_id` required) |
| `counts` | Incident counts |
| `memories` | Intelligent alert grouping memories |
| `abilities` | Account abilities |
| `types` | Incident types |
| `vendors` | Vendors |
| `notifications` | Notifications |
| `tags` | Tags |
| `tag` | Entities connected to a tag (`id` + `entity_type`) |
| `priority_thresholds` | Priority thresholds |
| `impacts` | Impacts |
| `impactors` | Impactors |

## Example queries

```sql
-- List all open incidents
SELECT id, title, status, urgency, created_at
FROM pagerduty.incidents
WHERE status = 'triggered';

-- Services and their status
SELECT id, name, status, description
FROM pagerduty.services;

-- Who is currently on-call
SELECT user__name, escalation_policy__name, start, end
FROM pagerduty.oncalls;

-- Incident alerts
SELECT id, summary, severity, status, created_at
FROM pagerduty.incident_alerts
WHERE id = 'P1234AB';

-- Team members
SELECT user__name, user__email, role
FROM pagerduty.members
WHERE id = 'PTEAM01';

-- Escalation policies
SELECT id, name, num_loops, description
FROM pagerduty.escalation_policies;

-- Recent log entries
SELECT id, type, summary, created_at
FROM pagerduty.log_entries;

-- Schedule overrides (v3)
SELECT id, start, end, user__summary
FROM pagerduty.list_overrides
WHERE id = 'PSCHED1';
```

## Quick start

```bash
# Add the source
coral source add pagerduty

# Discover tables
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'pagerduty'"

# Find required filters
coral sql \
  "SELECT table_name, column_name FROM coral.columns \
   WHERE schema_name = 'pagerduty' AND is_required_filter = true \
   ORDER BY table_name"

# Query
coral sql \
  "SELECT id, title, status FROM pagerduty.incidents LIMIT 10"
```
