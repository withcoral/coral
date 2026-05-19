# Todoist community source

The `todoist` community source exposes read-only Todoist task, project,
section, and label data through Coral SQL.

## Setup

Create or copy a Todoist token:

- Personal token: open Todoist web settings, go to **Integrations** >
  **Developer**, and copy the API token.
- OAuth token: request a token with the `data:read` scope.

Then install the source:

```sh
export TODOIST_API_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/todoist/manifest.yaml
```

## Tables

| Table | Purpose |
| --- | --- |
| `todoist.tasks` | Active tasks, optionally filtered by project, section, parent task, label, IDs, or goal. |
| `todoist.filtered_tasks` | Active tasks returned by a Todoist filter query such as `today` or `overdue`. |
| `todoist.projects` | Active projects accessible to the authenticated user. |
| `todoist.sections` | Active project sections, optionally scoped to one project. |
| `todoist.labels` | Personal labels for the authenticated user. |

All tables are read-only. This source does not create, update, complete, or
delete Todoist tasks.

## Example queries

Discover projects:

```sql
SELECT id, name, is_favorite, view_style
FROM todoist.projects
ORDER BY name;
```

List active tasks:

```sql
SELECT id, content, project_id, priority, due__date
FROM todoist.tasks
LIMIT 20;
```

List tasks in one project:

```sql
SELECT id, content, labels, due__date
FROM todoist.tasks
WHERE project_id = '<project_id>'
LIMIT 50;
```

Use Todoist filter syntax:

```sql
SELECT id, content, priority, due__date
FROM todoist.filtered_tasks
WHERE query = 'today | overdue'
LIMIT 20;
```

Find sections in a project:

```sql
SELECT id, name, section_order
FROM todoist.sections
WHERE project_id = '<project_id>'
ORDER BY section_order;
```

Discover labels:

```sql
SELECT id, name, color
FROM todoist.labels
ORDER BY name;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/todoist/manifest.yaml
```

Install and test with a real token:

```sh
export TODOIST_API_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/todoist/manifest.yaml
cargo run -p coral-cli -- source test todoist
```

Inspect the registered source:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description, required_filters FROM coral.tables WHERE schema_name = 'todoist'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, is_required_filter FROM coral.columns WHERE schema_name = 'todoist' ORDER BY table_name, ordinal_position"
```

## Notes

- Todoist API v1 uses cursor pagination with `cursor`, `limit`, and
  `next_cursor`.
- The default page size is 50 and the maximum page size is 200.
- `todoist.tasks` does not accept natural-language filter queries in API v1.
  Use `todoist.filtered_tasks` for Todoist filter syntax.
- Nested task fields such as `due`, `deadline`, and `duration` are preserved as
  JSON and selected common fields are also flattened into scalar columns.
