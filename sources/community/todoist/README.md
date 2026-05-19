# Todoist Source

Query your Todoist tasks, projects, sections, labels, and comments using SQL.

## Authentication

1. Go to [Todoist Settings](https://app.todoist.com/app/settings/integrations/developer)
2. Copy your **API token**
3. Add the source:

```bash
coral source add --interactive todoist
```

## Example Queries

List all projects:
```sql
SELECT id, name, color, is_favorite FROM todoist.projects
```

List all active tasks:
```sql
SELECT id, content, priority, due FROM todoist.tasks
```

Tasks from a specific project:
```sql
SELECT content, priority, due
FROM todoist.tasks
WHERE project_id = 'your_project_id'
```