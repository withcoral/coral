# Linear Connector

- **Version:** 2.3.0
- **Backend:** HTTP
- **Tables:** 10 plus 4 table functions
- **Base URL:** `https://api.linear.app`

## Authentication

Requires a `LINEAR_API_KEY` with Linear's Read permission. You can optionally
scope the key to specific teams.

```bash
coral source add linear
```

To rotate or update your key, run the same command again.

## Tables

| Table | Notes |
|---|---|
| `teams` | Workspace teams, cycle settings, and active cycle metadata |
| `users` | Workspace users |
| `issues` | Issues with team, state, assignee, project, milestone, label, and cycle fields |
| `issue_relations` | Issue relationships such as blockers, duplicates, related, and similar |
| `projects` | Projects with team and compact initiative fields |
| `initiative_projects` | Normalized links between initiatives and projects |
| `cycles` | Linear cycles, with optional team and active/previous/next filters |
| `initiatives` | Roadmap initiatives |
| `issue_labels` | Workspace labels |
| `attachments` | External links and pull requests attached to issues |

## Table Functions

| Function | Notes |
|---|---|
| `project_milestones(project_id)` | Milestones for one project |
| `project_updates(project_id)` | Updates for one project |
| `issue_comments(issue)` | Comments for one issue identifier, such as `SOURCE-496` |
| `team_issues(team_id)` | Issues for one team |

## Example Queries

Issues in the current active cycle:

```sql
WITH active_cycles AS (
  SELECT key AS team_key, active_cycle_id
  FROM linear.teams
  WHERE active_cycle_id IS NOT NULL
)
SELECT i.identifier, i.title, i.team_key, i.state_name, i.cycle_number
FROM linear.issues i
JOIN active_cycles c ON i.cycle_id = c.active_cycle_id
WHERE i.state_type NOT IN ('completed', 'canceled', 'duplicate')
ORDER BY i.team_key, i.priority ASC, i.updated_at DESC;
```

What a team shipped in its previous cycle:

```sql
SELECT i.identifier, i.title, i.completed_at, i.cycle_number
FROM linear.issues i
WHERE i.team_key = 'UI'
  AND i.cycle_is_previous = true
  AND i.state_type = 'completed'
ORDER BY i.completed_at DESC
LIMIT 50;
```

Cross-team blockers:

```sql
SELECT related_issue_identifier AS blocked_issue,
       related_issue_team_key AS blocked_team,
       issue_identifier AS blocking_issue,
       issue_team_key AS blocking_team
FROM linear.issue_relations
WHERE relation_type = 'blocks'
  AND related_issue_state_type NOT IN ('completed', 'canceled', 'duplicate')
  AND related_issue_team_key <> issue_team_key
ORDER BY updated_at DESC
LIMIT 50;
```

Initiative projects that are still incomplete:

```sql
SELECT initiative_name, project_name, project_progress, project_target_date
FROM linear.initiative_projects
WHERE project_progress < 1.0
ORDER BY project_target_date ASC NULLS LAST, project_progress ASC
LIMIT 25;
```

Open bug-labeled issues:

```sql
SELECT identifier, title, team_key, state_name, assignee_name, label_names
FROM linear.issues
WHERE state_type NOT IN ('completed', 'canceled', 'duplicate')
  AND lower(coalesce(label_names, '')) LIKE '%bug%'
ORDER BY team_key, created_at DESC;
```

## Notes

- `issue_relations.relation_type = 'blocks'` means `issue_*` is blocking
  `related_issue_*`.
- Linear workspaces can disable cycles per team. If `linear.cycles` is empty,
  check `linear.teams.cycles_enabled` and `active_cycle_id`.
- `projects.initiative_id` and `projects.initiative_name` expose the first
  linked initiative for convenience. Use `initiative_projects` when exact
  many-to-many membership matters.
