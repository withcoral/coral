# Productboard Coral source

Query Productboard REST API v2 product-discovery data with Coral. The source
exposes product-management entities, notes, customer entities, workspace
members, teams, configuration metadata, and member activity analytics.

## Setup

```bash
PRODUCTBOARD_API_TOKEN=... \
coral source add --file sources/community/productboard/manifest.yaml
```

Run validation:

```bash
coral source test productboard
```

## Tables

| Table | Description |
| --- | --- |
| `productboard.entities` | All Productboard PM entities with optional type/status/owner filters. |
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

```sql
SELECT name, status__name, owner__email, health__status, updated_at
FROM productboard.features
WHERE status__name = 'In Progress'
ORDER BY updated_at DESC
LIMIT 20;
```

```sql
SELECT name, owner__email, creator__email, processed, archived, created_at
FROM productboard.notes
WHERE archived = false AND source_system = 'zendesk'
ORDER BY created_at DESC
LIMIT 50;
```

```sql
SELECT type, filters_json
FROM productboard.entity_configurations
WHERE entity_type = 'feature';
```

```sql
SELECT date, member_id, role, feature_created_count, note_created_count
FROM productboard.member_activities
WHERE date_from = '2025-10-01' AND date_to = '2025-10-31';
```

## Notes

- This source targets Productboard REST API v2 under
  `https://api.productboard.com/v2`.
- Productboard entities and notes are configuration-driven. Standard fields are
  exposed as columns where stable, and dynamic/custom fields are preserved in
  `fields_json` and `raw_json`.
- API tokens need the relevant read scopes, such as `entities:read`,
  `notes:read`, `members:read`, `teams:read`, and `analytics:read`.
- Some PII fields, including owner/member emails, are returned as `[redacted]`
  unless the token has the required PII read scope.

## API references

- https://developer.productboard.com/reference/introduction
- https://developer.productboard.com/reference/listentities
- https://developer.productboard.com/reference/listnotes
- https://developer.productboard.com/reference/listmembers
- https://developer.productboard.com/reference/listteams
- https://developer.productboard.com/reference/listmemberactivities
