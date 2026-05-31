# Railway source

Query Railway projects, environments, service instances, deployments, project
members, and deployment logs through Coral using Railway's public GraphQL API.

This source is aimed at operational questions: what projects exist, who can
change them, what is running in each environment, which deployment is latest,
and what the build or runtime logs say when something fails.

## Authentication

This source uses a Railway API token sent as:

```text
Authorization: Bearer <RAILWAY_API_TOKEN>
```

Create a token from [railway.com/account/tokens](https://railway.com/account/tokens).
An account token is recommended for the default inventory queries because it can
read the authenticated account and list visible projects.

Workspace and project-scoped tokens may have narrower permissions. The
`railway.me` and broad project inventory queries are designed for account-token
setup; scoped tokens may need more targeted queries depending on Railway's token
permissions.

## Setup

```bash
RAILWAY_API_TOKEN=<token> coral source add --file sources/community/railway/manifest.yaml
```

Or add it interactively:

```bash
coral source add --interactive --file sources/community/railway/manifest.yaml
```

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `me` | Authenticated Railway account | none |
| `projects` | Projects visible to the token | none |
| `environments` | Environments for a project | `project_id` |
| `project_members` | Project members and roles | `project_id` |
| `service_instances` | Services running in one environment, plus latest deploy status | `environment_id` |
| `deployments` | Deployments for one project, service, and environment | `project_id`, `service_id`, `environment_id` |
| `deployment_logs` | Runtime logs for one deployment | `deployment_id` |
| `build_logs` | Build logs for one deployment | `deployment_id` |

> **Pagination** is enabled for `projects`, `environments`, and `deployments`
> using Relay cursor pagination (50 rows per page). `service_instances`,
> `project_members`, `deployment_logs`, and `build_logs` return a single page
> per query — scoped by their required filters, this is sufficient for typical
> debugging workflows.

## Example queries

### Verify the token

```sql
SELECT id, name, email
FROM railway.me
LIMIT 1;
```

### List projects

```sql
SELECT id, name, description, created_at, updated_at
FROM railway.projects
ORDER BY created_at DESC
LIMIT 20;
```

### Find project owners and collaborators

```sql
SELECT role, user_name, user_email
FROM railway.project_members
WHERE project_id = 'project-id'
ORDER BY role, user_name;
```

### List environments for a project

```sql
SELECT id, name, created_at
FROM railway.environments
WHERE project_id = 'project-id'
ORDER BY created_at DESC;
```

### See running service instances in an environment

```sql
SELECT
  service_id,
  service_name,
  latest_deployment_id,
  latest_deployment_status,
  latest_deployment_created_at
FROM railway.service_instances
WHERE environment_id = 'environment-id'
ORDER BY service_name;
```

### Inspect recent deployments for one service

```sql
SELECT id, status, created_at, url, static_url
FROM railway.deployments
WHERE project_id = 'project-id'
  AND service_id = 'service-id'
  AND environment_id = 'environment-id'
ORDER BY created_at DESC
LIMIT 20;
```

Discover `service_id` from `railway.service_instances` first, then use it as a
constant filter in the deployment-history query.

### Diagnose a failed build

```sql
SELECT timestamp, severity, message
FROM railway.build_logs
WHERE deployment_id = 'deployment-id'
ORDER BY timestamp DESC
LIMIT 100;
```

### Correlate runtime errors with a deployment

```sql
SELECT timestamp, severity, message
FROM railway.deployment_logs
WHERE deployment_id = 'deployment-id'
ORDER BY timestamp DESC
LIMIT 100;
```

## Query flow

Start with `railway.projects`, then use the project ID to query
`railway.environments` and `railway.project_members`. Use an environment ID with
`railway.service_instances` to discover the latest deployment for each service.
Then use the deployment ID with `railway.build_logs` and
`railway.deployment_logs` when you need to debug failures.

For full deployment history, query `railway.deployments` with the project,
service, and environment IDs.

## Rate limits and result size

Railway applies API rate limits to GraphQL calls. Log tables can return many
rows, so this source requests a bounded number of log rows per query. Use
`LIMIT` in SQL and scope log queries by `deployment_id`.

Deployment history is scoped by project, service, and environment to avoid
accidentally walking every deployment in a Railway account.

## Known limitations

- This source uses Railway's GraphQL API at `https://backboard.railway.com/graphql/v2`.
- It focuses on read-only inventory and debugging workflows.
- It does not currently expose environment variables, generated domains, custom
  domains, volumes, billing, metrics, webhooks, or write operations.
- `service_instances`, `project_members`, and log tables are single-page.
- Log availability depends on Railway retaining logs for the selected
  deployment and on the token having permission to read them.
