# Productboard Coral source

Query Productboard REST API v2 product-discovery data with Coral. The source
exposes product-management entities, notes, customer entities, workspace
members, teams, configuration metadata, and member activity analytics.

## Setup

Create a Productboard REST API token from your workspace's integrations
settings for internal tools, or use an OAuth access token for an installed
integration. The token must be authorized for the workspace you want to query
and needs read scopes for the surfaces you use, for example `entities:read`,
`notes:read`, `members:read`, `teams:read`, and `analytics:read`. Owner,
creator, member, and customer email/name fields require the relevant PII read
scope, such as `members:pii:read` or `users:pii:read`; otherwise Productboard
may return `[redacted]` values or reject email filters.

```bash
PRODUCTBOARD_API_TOKEN=... \
coral source add --file sources/community/productboard/manifest.yaml
```

Run validation:

```bash
coral source test productboard
```

First-success query after setup:

```sql
SELECT id, entity_type, name, status__name, updated_at
FROM productboard.features
LIMIT 5;
```

## Tables

| Table | Description |
| --- | --- |
| `productboard.entities` | Productboard PM entities with explicit `entity_type` and optional status/owner filters. |
| `productboard.features` | Feature entities from `/v2/entities?type[]=feature`. |
| `productboard.initiatives` | Initiative entities from `/v2/entities?type[]=initiative`. |
| `productboard.companies` | Company customer entities. |
| `productboard.users` | User customer entities. |
| `productboard.notes` | Customer feedback notes with owner, creator, source, and date filters. |
| `productboard.entity_configurations` | Entity field, relationship, and filter metadata. |
| `productboard.note_configurations` | Note field and relationship metadata. |
| `productboard.members` | Workspace members. |
| `productboard.teams` | Workspace teams. |
| `productboard.member_activities` | Member activity analytics. |

## Example queries

Feature and roadmap review:

```sql
SELECT name, status__name, owner__email, health__status, updated_at
FROM productboard.features
WHERE status__name = 'In Progress'
ORDER BY updated_at DESC
LIMIT 20;
```

Customer feedback triage:

```sql
SELECT name, source_system, owner__email, creator__email, processed, archived, created_at
FROM productboard.notes
WHERE archived = false AND source_system = 'zendesk'
ORDER BY created_at DESC
LIMIT 50;
```

Configuration discovery for plan/workspace-specific fields:

```sql
SELECT type, filters_json
FROM productboard.entity_configurations
WHERE entity_type = 'feature';
```

Workspace adoption window:

```sql
SELECT date, member_id, role, feature_created_count, note_created_count
FROM productboard.member_activities
WHERE date_from = '2025-10-01' AND date_to = '2025-10-31';
```

Manual Productboard pagination:

```sql
SELECT id, entity_type, name, updated_at
FROM productboard.features
WHERE page_cursor = 'cursor_from_links_next'
LIMIT 100;
```

## Notes

- This source targets Productboard REST API v2 under
  `https://api.productboard.com/v2`.
- Productboard list endpoints are cursor-paginated with `links.next` URLs and
  `pageCursor`. The current Coral HTTP DSL cannot safely extract `pageCursor`
  from a full JSON URL, so list tables expose one provider page at a time and
  include a pushed `page_cursor` filter mapped to Productboard's `pageCursor`
  query parameter. Copy the `pageCursor` value from Productboard's `links.next`
  URL when you need the next page, and use provider filters such as entity type,
  status, owner, note source, and date windows for repeatable workflows.
- Productboard entities and notes are configuration-driven. Standard fields are
  exposed as columns where stable, and dynamic/custom fields are preserved in
  `fields_json` and `raw_json`.
- API tokens need the relevant read scopes, such as `entities:read`,
  `notes:read`, `members:read`, `teams:read`, and `analytics:read`.
- Some PII fields, including owner/member emails, are returned as `[redacted]`
  unless the token has the required PII read scope.
- Available entity fields, filters, and customer/user fields can vary by
  Productboard plan and workspace configuration. Use
  `productboard.entity_configurations` and `productboard.note_configurations`
  to inspect the exact workspace schema before depending on custom fields.
- Productboard may return 429 rate-limit responses; keep validation queries
  bounded and prefer narrower provider filters over large local scans.
- Notes expose `created_at` and `updated_at` from Productboard's top-level
  note timestamps. Members and teams read stable values from their nested
  `fields` objects.
- `productboard.member_activities` exposes the documented activity metrics as
  first-class columns, including board, component, insight, product, feature,
  subfeature, note, note-state-change, and board-type counts.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/productboard/manifest.yaml
make lint-sources
yamllint sources/community/productboard/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed live validation against a Productboard workspace:

```text
Manifest is valid
Added source productboard (secrets: file (plaintext))

  ✓ productboard connected successfully
  Secrets: file (plaintext)

    productboard (11 tables)
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT id, entity_type, name FROM productboard.features LIMIT 1
      1 row
    ✓ SELECT id, note_type, name FROM productboard.notes LIMIT 1
      1 row
    ✓ SELECT type FROM productboard.entity_configurations LIMIT 1
      1 row
```

Representative live queries returned feature, note, and member rows, plus a
successful zero-row member-activity window for the test workspace:

```text
| entity_type | name                       | status__name | updated_at                  |
| feature     | Sample feature (e.g. Epic) | New idea     | 2026-05-30T11:58:35.855455Z |
| feature     | Sample feature (e.g. Epic) | In progress  | 2026-05-30T11:58:35.738001Z |

| name                                           | source_system | processed | archived |
| Sample Note: Customer call                     |               | false     | false    |
| Zendesk ticket #9                              | zendesk       | false     | false    |

| name               | role  |
| Saai Aravindh Raja | admin |

| date | date_from | date_to | feature_created_count | note_created_count |
|      |           |         |                       |                    |
```

## API references

- https://developer.productboard.com/reference/introduction
- https://developer.productboard.com/reference/listentities
- https://developer.productboard.com/reference/listnotes
- https://developer.productboard.com/reference/listmembers
- https://developer.productboard.com/reference/listteams
- https://developer.productboard.com/reference/listmemberactivities
