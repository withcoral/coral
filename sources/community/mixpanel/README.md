# Mixpanel Source

Query analytics metadata from [Mixpanel](https://mixpanel.com) — projects,
cohorts, annotations, annotation tags, Lexicon schemas (all, event, profile),
and dashboards.

## Setup

### 1. Create a service account

Go to **Organization Settings > Service Accounts** in the Mixpanel dashboard
and create a new service account. Save the **username** and **secret**.

See: [Mixpanel Service Accounts](https://docs.mixpanel.com/docs/orgs-and-projects/managing-projects#service-accounts)

### 2. Find your project ID

Go to **Project Settings > Overview** in the Mixpanel dashboard. The numeric
project ID is displayed at the top.

### 3. Configure environment variables

```bash
export MIXPANEL_SERVICE_ACCOUNT_USERNAME="your-service-account-username"
export MIXPANEL_SERVICE_ACCOUNT_SECRET="your-service-account-secret"
export MIXPANEL_PROJECT_ID="1234567"
```

For EU or India data residency, also set:

```bash
export MIXPANEL_BASE_URL="https://eu.mixpanel.com"   # EU
export MIXPANEL_BASE_URL="https://in.mixpanel.com"   # India
```

### 4. Add the source

```bash
coral source add --file sources/community/mixpanel/manifest.yaml
```

## Authentication

| Input | Kind | Description |
|---|---|---|
| `MIXPANEL_SERVICE_ACCOUNT_USERNAME` | Secret | Service account username |
| `MIXPANEL_SERVICE_ACCOUNT_SECRET` | Secret | Service account secret |
| `MIXPANEL_PROJECT_ID` | Variable | Numeric project ID |
| `MIXPANEL_BASE_URL` | Variable | API base URL (default: `https://mixpanel.com`) |

Uses HTTP Basic Auth with the service account credentials. The project ID is
passed as a path or query parameter to project-scoped endpoints.

## Tables

### projects

Projects accessible by the authenticated service account.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique project ID |
| `name` | Utf8 | Project display name |
| `timezone` | Utf8 | Project timezone |
| `token` | Utf8 | Project token (for client-side tracking) |
| `created_at` | Utf8 | Project creation timestamp |

---

### cohorts

Saved cohorts for the configured project. **Requires a paid Mixpanel plan.**

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique cohort ID |
| `name` | Utf8 | Cohort name |
| `description` | Utf8 | Cohort description |
| `count` | Int64 | Number of users in the cohort |
| `created` | Utf8 | Cohort creation date |
| `is_visible` | Int64 | Cohort visibility flag (0 hidden, 1 visible) |

---

### annotations

Project annotations marking significant events (releases, campaigns).

| Column | Type | Description |
|---|---|---|
| `from_date` | Utf8 | Start date filter (virtual, optional) |
| `to_date` | Utf8 | End date filter (virtual, optional) |
| `id` | Int64 | Unique annotation ID |
| `date` | Utf8 | Annotation date (YYYY-MM-DD HH:mm:ss) |
| `description` | Utf8 | Annotation text |
| `user_id` | Int64 | ID of the annotation creator |
| `user_first_name` | Utf8 | Creator first name |
| `user_last_name` | Utf8 | Creator last name |

**Optional filters:** `from_date`, `to_date` (YYYY-MM-DD format)

---

### annotation_tags

Tags that have been added to annotations.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique tag ID |
| `name` | Utf8 | Tag name |
| `project_id` | Int64 | Project ID |
| `has_annotations` | Boolean | Whether attached to any annotations |

---

### schemas

All Lexicon schema entries (events and profile properties).

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Event or property name |
| `entity_type` | Utf8 | Entity type (event or profile) |
| `description` | Utf8 | Human-readable description |
| `schema_json` | Json | Full schema definition |

---

### event_schemas

Lexicon schemas filtered to event entities only.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Event name |
| `entity_type` | Utf8 | Entity type (always event) |
| `description` | Utf8 | Event description |
| `schema_json` | Json | Full event schema definition |

---

### profile_schemas

Lexicon schemas filtered to profile entities only.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Profile property name |
| `entity_type` | Utf8 | Entity type (always profile) |
| `description` | Utf8 | Property description |
| `schema_json` | Json | Full profile schema definition |

---

### dashboards

Dashboards defined in the project.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique dashboard ID |
| `title` | Utf8 | Dashboard title |
| `description` | Utf8 | Dashboard description |
| `creator_name` | Utf8 | Name of the dashboard creator |
| `creator_email` | Utf8 | Email of the dashboard creator |
| `creator_id` | Int64 | ID of the creator |
| `is_private` | Boolean | Whether the dashboard is private |
| `created` | Utf8 | Creation timestamp |
| `last_modified` | Utf8 | Last modification timestamp |

## Example Queries

```sql
-- List all projects
SELECT id, name, timezone
FROM mixpanel.projects;

-- List cohorts with user counts (requires paid plan)
SELECT id, name, description, count
FROM mixpanel.cohorts;

-- View all annotations
SELECT id, date, description, user_first_name, user_last_name
FROM mixpanel.annotations;

-- Annotations in a date range
SELECT id, date, description
FROM mixpanel.annotations
WHERE from_date = '2024-01-01'
  AND to_date = '2024-12-31';

-- List annotation tags
SELECT id, name, has_annotations
FROM mixpanel.annotation_tags;

-- Browse all Lexicon schemas
SELECT name, entity_type, description
FROM mixpanel.schemas;

-- Event schemas only
SELECT name, description
FROM mixpanel.event_schemas;

-- Profile property schemas
SELECT name, description
FROM mixpanel.profile_schemas;

-- Dashboards
SELECT id, title, creator_name, creator_email, is_private
FROM mixpanel.dashboards;
```

## Pagination

No endpoints in this source use pagination. All tables return complete
result sets in a single request.

## Notes

- **Read-only**: This source is read-only; no create, update, or delete
  operations.
- **Data residency**: `MIXPANEL_BASE_URL` defaults to `https://mixpanel.com`
  (US). For EU projects, use `https://eu.mixpanel.com`. For India projects,
  use `https://in.mixpanel.com`.
- **Paid plan required for cohorts**: The cohorts endpoint requires a paid
  Mixpanel plan. Free-tier accounts will receive a 402 error.
- **Service account permissions**: The service account must have at least
  Analyst role on the project to read annotations and schemas.
- **Rate limits**: The Query API has a limit of approximately 60 queries
  per hour. Metadata endpoints (projects, schemas) have higher limits.
- **Schemas response**: The `schema_json` column contains the full
  Lexicon schema definition as a JSON value, which may include
  `required_properties`, `metadata`, and `tags`.
