# Vercel source

Query your Vercel platform data — projects, deployments, domains, environment
variables, and team members — through SQL.

## Authentication

Create a [Vercel API token](https://vercel.com/account/tokens) and set it as
the `VERCEL_TOKEN` input. For team-scoped resources, also set `VERCEL_TEAM_ID`.

| Input             | Kind       | Required | Description                                           |
| ----------------- | ---------- | -------- | ----------------------------------------------------- |
| `VERCEL_TOKEN`    | **secret** | yes      | Vercel API access token                               |
| `VERCEL_TEAM_ID`  | variable   | no       | Team ID for team-scoped queries (personal if omitted) |

## Tables

| Table                          | Description                                          | Key filter                |
| ------------------------------ | ---------------------------------------------------- | ------------------------- |
| `vercel.projects`              | All projects with config and linked Git repo         | `search`, `repo`          |
| `vercel.deployments`           | Deployment history with status, URL, and creator     | `project_id`, `state`     |
| `vercel.domains`               | Registered domains with verification and DNS status  | —                         |
| `vercel.environment_variables` | Env vars for a project (values if token permits)     | `project_id` (**required**) |
| `vercel.team_members`          | Team members with roles (needs `VERCEL_TEAM_ID`)     | —                         |

## Example queries

```sql
-- List all projects with their framework
SELECT id, name, framework, created_at
FROM vercel.projects
LIMIT 10;

-- Recent production deployments
SELECT uid, name, state, url, target, created_at
FROM vercel.deployments
WHERE target = 'production'
LIMIT 10;

-- Failed deployments
SELECT uid, name, error_code, error_message, created_at
FROM vercel.deployments
WHERE state = 'ERROR'
LIMIT 10;

-- Domain overview
SELECT name, service_type, verified, expires_at
FROM vercel.domains
LIMIT 10;

-- Environment variables for a project
SELECT id, key, type, target, comment
FROM vercel.environment_variables
WHERE project_id = 'prj_YOUR_PROJECT_ID'
LIMIT 20;

-- Team members and roles
SELECT uid, email, username, role, created_at
FROM vercel.team_members
LIMIT 20;
```

## Pagination

Projects, deployments, domains, and team members use Vercel's cursor-based
pagination. Coral handles this automatically — just use `LIMIT` to control how
many rows you want. Environment variables are returned in a single response
(no pagination).

## Limitations

- **Read-only.** No create, update, or delete operations.
- **Environment variable values** may be `null` for encrypted or sensitive
  variables depending on token permissions.
- **Team members** requires `VERCEL_TEAM_ID` to be set. Querying
  `vercel.team_members` without it returns a 404 error.
- Deployment logs (streaming), DNS records, edge configs, webhooks, certs,
  and integrations are not included in this v1 source.
