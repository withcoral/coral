# SAP SuccessFactors

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 23 · **Functions:** 12
**Base URL:** `https://{SF_API_SERVER}` — your SuccessFactors API data center host

Query SAP SuccessFactors: Role-Based Permissions (roles, assignments,
groups, people pools), SCIM identity, GenAI-assisted RBP search helpers,
data privacy consent, proxy management, new hire onboarding, Time Off,
Employee Compensation, Continuous Performance Management, Interview
Scheduling, Onboarding extension tasks, and — generically — any of the
~1,000 OData v2 entities (Employee Central, recruiting, compensation,
etc.). Everything in this source authenticates with a single set of Basic
Auth credentials.

## Authentication

Requires `SF_API_SERVER`, `SF_USERNAME`, and `SF_PASSWORD` — standard
SuccessFactors Basic Authentication.

- `SF_API_SERVER` is your API data center host, **without scheme**
  (e.g. `api10.successfactors.com`). This is often a different host than
  the one you log into interactively — check with your SuccessFactors
  admin or implementation partner if unsure.
- `SF_USERNAME` is in the form `username@companyId` (e.g. `api_user@acme001`).
- `SF_PASSWORD` is that API user's password.

```bash
SF_API_SERVER=api10.successfactors.com \
SF_USERNAME=api_user@acme001 \
SF_PASSWORD=... \
  coral source add --file sources/community/sap_successfactors/manifest.yaml
```

Or interactively:

```bash
coral source add --file sources/community/sap_successfactors/manifest.yaml --interactive
```

### A note on auth methods

SAP's own OpenAPI specs for different API groups declare different
security schemes: `BasicAuth` for RBP/SCIM/GenAI/DPCS/Proxy/New Hire
Journey, and `sfOauth` (OAuth 2.0 SAML Bearer Assertion) or `BearerAuth`
for Time Off, Employee Compensation, Continuous Performance, Performance
review routing, Interview Scheduling, and Onboarding extension tasks. In
practice, many tenants accept Basic Auth across all of these regardless
of which scheme the spec documents as default — so this source uses Basic
Auth everywhere for a single, simple credential. **If your tenant enforces
OAuth-only on the `sfOauth`-labeled groups**, the tables/functions listed
under "Time Off, Compensation, Performance, Recruiting, Onboarding" below
will fail with an auth error; in that case those specific tables would
need to move to a separate bearer-token-authenticated source instead. Test
against your tenant before relying on this in production.

## Tables

**RBP Management**

| Table                | Description                                                | Optional filters                           |
| -------------------- | ---------------------------------------------------------- | ------------------------------------------ |
| `roles`            | Permission roles (collections of permissions)              | `search`, `odata_filter`, `order_by` |
| `role_assignments` | Assignments granting a role to an access/target population | `odata_filter`, `order_by`             |
| `groups`           | Permission groups (dynamic population definitions)         | `search`, `odata_filter`, `order_by` |

**Identity & access**

| Table                 | Description                                | Optional filters                         |
| --------------------- | ------------------------------------------ | ---------------------------------------- |
| `scim_users`        | SCIM 2.0 user identity records             | `filter`                               |
| `scim_groups`       | SCIM 2.0 view of permission groups         | `filter`                               |
| `genai_permissions` | Searchable RBP permission catalog          | `search`, `resource_type`            |
| `genai_users`       | Lightweight user search/autocomplete       | `search`, `odata_filter`, `action` |
| `proxy_grants`      | Proxy access grants (who can act for whom) | `odata_filter`                         |
| `proxy_scopes`      | Catalog of proxy scope definitions         | —                                       |

**Data privacy & onboarding**

| Table                     | Description                                      | Optional filters                                                            |
| ------------------------- | ------------------------------------------------ | --------------------------------------------------------------------------- |
| `dpcs_statements`       | Data privacy consent statement in effect (1 row) | `type`, `country`, `custom_field_value`, `language`                 |
| `dpcs_acknowledgements` | A subject's consent acknowledgement (1 row)      | `type`, `country`, `subject_id`, `custom_field_value`, `language` |

**Time Off**

| Table                                 | Description                            | Required filters                                           |
| ------------------------------------- | -------------------------------------- | ---------------------------------------------------------- |
| `available_time_types`              | Time-off types an employee can request | `as_of_date`                                             |
| `time_account_balances`             | Leave account balances                 | `as_of_date`                                             |
| `time_type_balances`                | Balances by time type                  | `as_of_date`                                             |
| `termination_time_account_balances` | Payable balances at termination        | —                                                         |
| `instructional_text`                | Localized ESS help text (1 row)        | `as_of_date`                                             |
| `time_off_events`                   | Absence events in a date range         | `assignment_id`, `types`, `start_date`, `end_date` |

**Compensation & Performance**

| Table                      | Description                                        | Required filters |
| -------------------------- | -------------------------------------------------- | ---------------- |
| `employee_compensations` | Comp worksheet entries (salary/stock/bonus/varpay) | `template_id`  |
| `activity_statuses`      | Continuous Performance status definitions          | —               |
| `activities`             | Continuous Performance activities                  | —               |
| `achievements`           | Continuous Performance achievements                | —               |
| `cpm_user_permissions`   | Caller's CPM view/edit permissions                 | —               |

**Recruiting**

| Table                   | Description                    | Required filters       |
| ----------------------- | ------------------------------ | ---------------------- |
| `interview_schedules` | Recruiting interview schedules | `job_requisition_id` |

### Filter syntax notes

`odata_filter` and `order_by` (RBP tables) pass their value straight
through as the provider's raw `$filter` / `$orderby` OData expressions (see
each table's `description` in `coral.filters` for the fields each endpoint
supports). `search` maps to the provider's `$search` free-text parameter.
`filter` (SCIM tables) passes a raw SCIM 2.0 filter expression through
instead — different syntax, same idea (e.g. `userName eq "jsmith"`).

`scim_users` and `scim_groups` are a second, independently-routed view of
identity: `scim_groups.id` is the *same* permission-group ID as
`groups.id` from RBP Management, just returned through the SCIM API —
useful for combining `scim_groups.member_ids`/`type` with RBP's
people-pool detail from `group_people_pools`. Both SCIM tables, and
several Time Off / Compensation / Continuous Performance tables, keep a
`raw` JSON column with the full record for nested detail that isn't worth
flattening into typed columns up front — pull fields with
`json_get_str(raw, '<field>')` etc.

`genai_permissions`/`genai_users` are lookup/autocomplete-style endpoints
SAP built for a GenAI-assisted role-authoring UI, not full data exports —
useful for fuzzy search but not for "give me every permission" (use
`genai_permissions` with no filter for that; it does paginate fully).

`dpcs_statements`, `dpcs_acknowledgements`, and `instructional_text` each
return **at most one row** — there's no list-all endpoint, only "the
record matching this type/country/language/date". `dpcs_acknowledgements .status` is a raw numeric code (0=DECLINE, 1=ACCEPT, 2=REVOKE, 3=NOT
PRESENTED).

## Functions (scoped child collections and single-item lookups)

These are source-scoped table functions rather than tables, since each one
needs an ID or other parameter to call:

| Function                                       | Description                                          | Required arg           | Optional args                        |
| ---------------------------------------------- | ---------------------------------------------------- | ---------------------- | ------------------------------------ |
| `role_members(role_id)`                      | Users with access via a given role                   | `role_id`            | —                                   |
| `assignment_members(assignment_id)`          | Users with access via a given role assignment        | `assignment_id`      | —                                   |
| `group_members(group_id, active_only)`       | Users in a given group's people pools                | `group_id`           | `active_only`                      |
| `group_people_pools(group_id)`               | Include/exclude filter definitions for a given group | `group_id`           | —                                   |
| `newhire_journey(journey_id)`                | Onboarding journey status for a new hire             | `journey_id`         | —                                   |
| `genai_groups_for_permission(permission_id)` | Top 5 static/dynamic groups holding a permission     | `permission_id`      | —                                   |
| `odata_entity(entity)`                       | Generic OData v2 entity access (see below)           | `entity`             | `select`, `filter`, `order_by` |
| `interview_schedule(id)`                     | A single interview schedule                          | `id`                 | `expand`                           |
| `activity_status(status_record_id)`          | A single CPM status definition                       | `status_record_id`   | —                                   |
| `activity_updates(activity_record_id)`       | Comment history on one activity                      | `activity_record_id` | —                                   |
| `review_route_map(review_id)`                | Modify/signoff routing for one performance review    | `review_id`          | `locale`                           |
| `onboarding_extension_tasks(process_id)`     | Extension task statuses for one onboarding process   | `process_id`         | `odata_filter`                     |

```sql
SELECT display_name, email, status
FROM sap_successfactors.role_members(role_id => 500);

SELECT display_name, status
FROM sap_successfactors.group_members(group_id => 100, active_only => true);

SELECT people_pool_id, type, filters
FROM sap_successfactors.group_people_pools(group_id => 100);

SELECT status, user_full_name, hire_status
FROM sap_successfactors.newhire_journey(journey_id => 'J-12345');
```

`group_members.status` uses the provider's raw single-character codes
(`t`/`T` active internal, `f`/`F` inactive internal, `e` active external,
`d` inactive external) — different from the `ACTIVE`/`INACTIVE` vocabulary
used by `roles`, `role_assignments`, and `role_members`/`assignment_members`.

`genai_groups_for_permission` is capped at the provider's top 5 static +
top 5 dynamic groups by role count — it's a search-assist endpoint, not a
complete listing; use `groups` for the full, unbounded list.

### `odata_entity` — generic OData v2 access

SuccessFactors also exposes a separate OData v2 API (Employee Central,
recruiting, compensation, and hundreds of other modules) at
`/odata/v2/<EntityName>`. A tenant's service document can list 1,000+
entity sets — far too many to hand-model as typed tables one at a time, and
each entity's *properties* live in that tenant's own `$metadata`, not in
the service document, so column names can't be derived generically. Until
specific entities get dedicated typed tables (see roadmap), `odata_entity`
is a raw passthrough: give it an entity name, get back full JSON rows.

```sql
-- Raw User records
SELECT json_get_str(row, 'userId') AS user_id,
       json_get_str(row, 'firstName') AS first_name,
       json_get_str(row, 'lastName') AS last_name
FROM sap_successfactors.odata_entity(entity => 'User')
LIMIT 10;

-- With $select / $filter / $orderby passthrough
SELECT row
FROM sap_successfactors.odata_entity(
  entity => 'EmpEmployment',
  select => 'userId,startDate,personIdExternal',
  filter => "userId eq '12345'"
);
```

Check your tenant's real `$metadata` (`GET /odata/v2/$metadata`) for the
exact, case-sensitive property names of whichever entity you're querying —
they aren't standardized across entities and this function can't validate
them for you.

## Quick start

```bash
# All active roles
coral sql "SELECT id, name, status, user_type FROM sap_successfactors.roles WHERE odata_filter = \"status eq 'ACTIVE'\""

# Assignments for a given role
coral sql "
  SELECT id, name, relationship_type, access_group_names, target_group_names
  FROM sap_successfactors.role_assignments
  WHERE odata_filter = 'roleId eq 500'
"

# Groups with more than 50 members
coral sql "
  SELECT id, name, total_member_count, active_member_count
  FROM sap_successfactors.groups
  ORDER BY total_member_count DESC
  LIMIT 20
"

# Active SCIM users, with a couple of fields pulled from the raw record
coral sql "
  SELECT id, user_name, display_name, active,
         json_get_str(raw, 'title') AS job_title
  FROM sap_successfactors.scim_users
  WHERE filter = 'active eq true'
  LIMIT 20
"

# Search the permission catalog for GenAI-assisted role authoring
coral sql "
  SELECT id, label, category_label, action_label
  FROM sap_successfactors.genai_permissions
  WHERE search = 'compensation'
"

# Who's currently proxying for whom
coral sql "
  SELECT proxy_display_name, principal_display_name, end_date, is_valid
  FROM sap_successfactors.proxy_grants
  WHERE odata_filter = 'isValid eq true'
"

# Time off types available today
coral sql "SELECT * FROM sap_successfactors.available_time_types WHERE as_of_date = '2026-08-29'"

# This employee's leave balances
coral sql "
  SELECT time_account_type_name, available_balance_formatted
  FROM sap_successfactors.time_account_balances
  WHERE as_of_date = '2026-08-29' AND assignment_id = '12345'
"

# Compensation entries for a template, with salary detail pulled from raw
coral sql "
  SELECT entry_user_name, department,
         json_get_float(raw, 'salary', 'newSalary') AS new_salary
  FROM sap_successfactors.employee_compensations
  WHERE template_id = '500'
"

# Interview schedules for a requisition
coral sql "
  SELECT title, mode, communication_platform, duration_in_minutes
  FROM sap_successfactors.interview_schedules
  WHERE job_requisition_id = '78910'
"
```

## Discovery order

```text
roles
  → id (role_id) → role_assignments (WHERE odata_filter = 'roleId eq <id>')
  → id (role_id) → role_members(role_id => id)

role_assignments
  → id (assignment_id) → assignment_members(assignment_id => id)
  → access_group_names / target_group_names → groups.name (name match;
    the assignment payload only carries group names in list form, not ids)

groups
  → id → group_members(group_id => id), group_people_pools(group_id => id)
  → id (as a string) → scim_groups.id (same object, SCIM view)

scim_users
  → id → (raw JSON) group memberships, extension schema attributes

scim_groups
  → id (as a number) → groups.id, group_people_pools(group_id => id)

genai_permissions
  → id (permission_id) → genai_groups_for_permission(permission_id => id)

activities
  → activity_record_id → achievements.activity_record_id,
    activity_updates(activity_record_id => id)

interview_schedules
  → id → interview_schedule(id => id)
```

## Scope and roadmap

The uploaded `SAP-SuccessFactorAPIs` bundle covers 26 API groups. This
source covers everything in it with a working GET-based read surface,
assuming your tenant accepts Basic Auth broadly (see "A note on auth
methods" above): RBP Management, SCIM identity, GenAI-assisted RBP search
helpers, Data Privacy Consent Statements, Proxy Management, New Hire
Journey lookups, Time Off, Employee Compensation, Continuous Performance
Management, Performance review routing, Interview Scheduling, Onboarding
extension tasks, and `odata_entity` as a generic escape hatch into the
separate OData v2 API. The `/roles/{id}`, `/assignments/{id}`, and
`/groups/{groupId}` single-item GET endpoints are intentionally left out —
the list tables already return full objects, so a detail table would be
redundant.

Not covered, and why:

- **`sap-sf-customTasks-v1`** — declares `BasicAuth` like everything else
  here, so it belongs in this source, but hasn't been built yet.
- **`i9AuditTrailRecords`**, **`sap-sf-customTasks-v2`** — mutual TLS
  (`Certificate`) auth, which doesn't fit a simple secret/variable input.
- **`sap-sf-PositionBudgetingControl-v1`** — its spec's server is a fixed
  `sandbox.api.sap.com` sandbox host rather than a tenant-scoped
  `{api-server}` variable.
- **`sap-sf-eco-growthPortfolios-v1`**, **`sap-sf-FMLARequest-v1`** — no
  GET endpoints in the bundled spec (write-only APIs).
- **Clock In/Clock Out** (`ClockInClockOut`,
  `ClockInClockOutTimeEventsRestAPI`,
  `sap-sf-ClockInClockOutExternalRestAPI-v1`,
  `sap-sf-tim-BulkClockInClockOutTimeEvents-v1`) — declares `ApiKeyAuth`
  (a header API key, not username/password), so it doesn't fit this
  source's Basic Auth model even under the "many tenants are lenient"
  assumption used elsewhere here; thin GET surface (mostly single-record
  lookups by ID) anyway. Candidate for a small separate source if needed.
- Typed tables for the highest-value OData v2 entities (`User`,
  `PerPerson`, `EmpJob`, `EmpEmployment`, `EmpCompensation`,
  `JobRequisition`, `Candidate`, `Position`, ...) — needs real per-entity
  `$metadata` (property lists), which the uploaded service document
  doesn't include. `odata_entity` covers all of them generically until then.

**If your tenant turns out to require OAuth-only** for the Time Off /
Compensation / Continuous Performance / Interview Scheduling / Onboarding
extension groups, those tables and functions would need to move to a
separate source with a bearer-token `auth` block — flag it and that split
can be reintroduced for just that subset.
