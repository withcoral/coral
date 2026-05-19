# Figma

Query projects, files, file versions, comments, published
components, component sets, styles, webhooks, dev resources,
and library analytics from Figma.

## Setup

### Get Your Access Token

1. Log in to [Figma](https://www.figma.com)
2. Go to **Settings > Account > Personal access tokens**
   or visit the [API docs](https://www.figma.com/developers/api#access-tokens)
3. Generate a new personal access token with the scopes needed
   for the tables you want to query
4. Copy the token

### Find Your Team ID

Your team ID is in the URL when viewing your team page:

```
https://www.figma.com/files/team/<TEAM_ID>/...
```

### Add the Source

```bash
export FIGMA_ACCESS_TOKEN="your_token"
export FIGMA_TEAM_ID="your_team_id"
coral source add --file sources/community/figma/manifest.yaml
```

## Tables

### `team_projects`

Lists all projects in the configured team (2 columns).

**Example:**

```sql
SELECT id, name
FROM figma.team_projects;
```

### `project_files`

Lists all files in a project with name, last modified time,
thumbnail URL, and branch metadata (6 columns).

**Requires:** `project_id` filter

**Example:**

```sql
SELECT key, name, last_modified, thumbnail_url
FROM figma.project_files
WHERE project_id = '12345';
```

### `file_versions`

Lists version history for a file with label, description,
creator, and creation time (8 columns).

**Requires:** `file_key` filter

**Example:**

```sql
SELECT id, label, description, created_at, user_handle
FROM figma.file_versions
WHERE file_key = 'abcXYZ123';
```

### `file_comments`

Lists all comments on a file with message, author, creation
time, resolution status, positioning metadata, and reactions
(12 columns).

**Requires:** `file_key` filter

**Example:**

```sql
SELECT id, message, user_handle, created_at, resolved_at
FROM figma.file_comments
WHERE file_key = 'abcXYZ123';
```

### `team_components`

Lists all published components in the team library with name,
description, file key, containing frame, and creator
(11 columns). Cursor-paginated with up to 1000 per page.

**Example:**

```sql
SELECT key, name, description, file_key, created_at
FROM figma.team_components
LIMIT 20;
```

### `team_component_sets`

Lists all published component sets (variant groups) in the
team library with name, description, file key, and creator
(11 columns). Cursor-paginated.

**Example:**

```sql
SELECT key, name, description, file_key
FROM figma.team_component_sets
LIMIT 20;
```

### `team_styles`

Lists all published styles in the team library with name,
type (PAINT, TEXT, EFFECT, GRID), file key, sort position,
and creator (12 columns). Cursor-paginated.

**Example:**

```sql
SELECT key, name, style_type, file_key, created_at
FROM figma.team_styles
LIMIT 20;
```

### `webhooks`

Lists all webhooks for the configured team with event type,
endpoint URL, status, context, and description (11 columns).

**Example:**

```sql
SELECT id, event_type, endpoint, status
FROM figma.webhooks;
```

### `file_dev_resources`

Lists all dev resources attached to nodes in a file
(5 columns). Returns resource ID, name, URL, and target
node ID.

**Requires:** `file_key` filter

**Example:**

```sql
SELECT id, name, url, node_id
FROM figma.file_dev_resources
WHERE file_key = 'abcXYZ123';
```

### `library_component_actions`

Lists library analytics for component actions (inserts,
detaches) in a library file (8 columns). Cursor-paginated.

> **Note:** Enterprise plan only.

**Requires:** `library_file_key` filter

**Example:**

```sql
SELECT component_name, week, insertions, detachments
FROM figma.library_component_actions
WHERE library_file_key = 'abcXYZ123';
```

### `library_component_usages`

Lists library analytics for component usages in a library
file (8 columns). Cursor-paginated.

> **Note:** Enterprise plan only.

**Requires:** `library_file_key` filter

**Example:**

```sql
SELECT component_name, usages, teams_using, files_using
FROM figma.library_component_usages
WHERE library_file_key = 'abcXYZ123';
```

## Authentication

The source uses the `X-Figma-Token` header with a personal
access token. Generate personal access tokens in your Figma
account settings.

Some tables require additional scopes or token types:

- `team_projects`, `project_files`, `file_versions`, and
  `file_comments` require access to the relevant team/project/file
- `team_components`, `team_component_sets`, and `team_styles`
  require `team_library_content:read`
- `webhooks` requires `webhooks:read`
- `file_dev_resources` requires `file_dev_resources:read`
- `library_component_actions` and `library_component_usages`
  require `library_analytics:read`

## Inputs

| Input | Kind | Description |
|---|---|---|
| `FIGMA_ACCESS_TOKEN` | secret | Personal access token |
| `FIGMA_TEAM_ID` | variable | Team ID from the URL |

## Pagination

Tables use different pagination strategies:

- **Cursor pagination** (`page_size` + `after` cursor):
  `team_components`, `team_component_sets`, `team_styles`
  (default 30, max 1000 per page)
- **Cursor pagination** (`cursor`):
  `library_component_actions`, `library_component_usages`
- **No pagination**: `team_projects`, `project_files`,
  `file_versions`, `file_comments`, `webhooks`,
  `file_dev_resources`

## Example Queries

### List all projects and their files

```sql
SELECT p.id AS project_id, p.name AS project_name
FROM figma.team_projects p;

-- Then for each project:
SELECT key, name, last_modified
FROM figma.project_files
WHERE project_id = '<project_id>';
```

### Find unresolved comments on a file

```sql
SELECT id, message, user_handle, created_at
FROM figma.file_comments
WHERE file_key = 'abcXYZ123'
  AND resolved_at IS NULL;
```

### Audit design system components

```sql
SELECT name, description, file_key, updated_at
FROM figma.team_components
WHERE description IS NOT NULL;
```

### List styles by type

```sql
SELECT name, style_type, file_key, updated_at
FROM figma.team_styles
WHERE style_type = 'PAINT';
```

### Review file version history

```sql
SELECT label, description, user_handle, created_at
FROM figma.file_versions
WHERE file_key = 'abcXYZ123';
```

### Audit active webhooks

```sql
SELECT event_type, endpoint, status, description
FROM figma.webhooks
WHERE status = 'ACTIVE';
```

### List dev resources on a file

```sql
SELECT name, url, node_id
FROM figma.file_dev_resources
WHERE file_key = 'abcXYZ123';
```

### Audit component adoption (Enterprise)

```sql
SELECT component_name, usages, teams_using, files_using
FROM figma.library_component_usages
WHERE library_file_key = 'abcXYZ123';
```

## Notes

- The source is read-only — no create, update, or delete operations
- All exposed timestamps are ISO 8601 strings
- The `FIGMA_TEAM_ID` input is used for team-scoped endpoints
  (projects, components, component sets, styles, webhooks)
- File-scoped tables (`project_files`, `file_versions`,
  `file_comments`, `file_dev_resources`) require a filter
  to specify which project or file to query
- The `containing_frame` column in components/component_sets
  is a JSON object with frame metadata
- The `client_meta` column in comments is a JSON object whose
  structure varies by comment type (Vector, FrameOffset, Region)
- The `reactions` column in comments is a JSON array of
  reaction objects
- Published components, component sets, and styles are
  team-library resources — they only include items published
  to the team library, not local/unpublished items
- **Enterprise-only tables**: `library_component_actions`
  and `library_component_usages` require an Enterprise plan
  and org admin access
- `activity_logs` is intentionally not included because Figma's
  Activity Logs API requires OAuth-style authentication, while
  this source is personal-access-token based
- `file_dev_resources` requires the `file_dev_resources:read`
  scope on the access token
- Figma for Government uses a different base URL
  (`https://api.figma-gov.com`) — update the manifest if needed
