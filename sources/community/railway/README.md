# Railway Community Source

Railway is a modern cloud deployment platform where developers can provision databases, microservices, and cron jobs with minimal configuration. This Coral community source allows teams to query their Railway infrastructure (projects, services, environments, and deployments) using SQL. Connecting Railway to Coral enables rich developer observability, such as monitoring deployment velocity, auditing environments, and joining deployment events with software delivery pipelines (e.g. GitHub Pull Requests).

## Installation

Add the Railway source to your Coral instance by referencing its local manifest:

```bash
coral source add --file sources/community/railway/manifest.yaml
```

## Setup & Authentication

The Railway source requires a Bearer Token to authenticate requests to the Public API.

### Step-by-Step API Token Generation:
1. Log in to your account at [railway.app](https://railway.app).
2. Navigate to **Account Settings** (click your profile avatar in the bottom left or top right, then settings).
3. Click on the **Tokens** tab.
4. Under **API Tokens**, click **New Token** or **Create Token**.
5. Give the token a descriptive name (e.g., `Coral Integration`).
6. Copy the generated token string.
7. Expose it to Coral as the `RAILWAY_API_TOKEN` environment variable or paste it when adding the source.

---

## Table Reference

### `railway.projects`
Lists all active projects associated with the authenticated account or workspace.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the project. |
| `name` | `Utf8` | `false` | Name of the project. |
| `description` | `Utf8` | `true` | Description of the project. |
| `created_at` | `Timestamp` | `true` | Timestamp when the project was created. |
| `updated_at` | `Timestamp` | `true` | Timestamp when the project was last updated. |
| `is_public` | `Boolean` | `true` | Whether the project is publicly visible. |
| `team_id` | `Utf8` | `true` | Team or workspace ID the project belongs to. |

### `railway.services`
Lists services defined within your projects.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the service. |
| `name` | `Utf8` | `false` | Name of the service. |
| `project_id` | `Utf8` | `true` | ID of the project the service belongs to. |
| `created_at` | `Timestamp` | `true` | Timestamp when the service was created. |
| `updated_at` | `Timestamp` | `true` | Timestamp when the service was last updated. |

* **Filters:**
  * `project_id` (`Utf8`, optional): Restrict results to a single project.

### `railway.deployments`
Lists the history of deployments across your services.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the deployment. |
| `status` | `Utf8` | `false` | Current status of the deployment (e.g. `SUCCESS`, `FAILED`, `CRASHED`). |
| `created_at` | `Timestamp` | `true` | Timestamp when the deployment was created. |
| `updated_at` | `Timestamp` | `true` | Timestamp when the deployment was last updated. |
| `url` | `Utf8` | `true` | Public live URL of the deployment (if generated). |
| `environment_id` | `Utf8` | `true` | ID of the environment deployed to. |
| `service_id` | `Utf8` | `true` | ID of the deployed service. |
| `creator_id` | `Utf8` | `true` | ID of the user who triggered the deployment. |

* **Filters:**
  * `project_id` (`Utf8`, optional): Scopes deployments list to a specific project.
  * `service_id` (`Utf8`, optional): Scopes deployments list to a specific service.

### `railway.environments`
Lists environments (e.g., `production`, `staging`, PR previews) configured inside projects.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the environment. |
| `name` | `Utf8` | `false` | Name of the environment. |
| `project_id` | `Utf8` | `true` | ID of the project the environment belongs to. |
| `created_at` | `Timestamp` | `true` | Timestamp when the environment was created. |
| `is_ephemeral` | `Boolean` | `true` | Whether this is a temporary/ephemeral preview environment. |

* **Filters:**
  * `project_id` (`Utf8`, optional): Restrict results to a single project.

---

## Example SQL Queries

### 1. Count projects and services per workspace
```sql
SELECT team_id, COUNT(DISTINCT id) as projects_count
FROM railway.projects
GROUP BY team_id;
```

### 2. Identify all ephemeral (preview) environments
```sql
SELECT id, name, project_id, created_at
FROM railway.environments
WHERE is_ephemeral = true
ORDER BY created_at DESC;
```

### 3. Check recent failed deployments
```sql
SELECT id, service_id, status, created_at, url
FROM railway.deployments
WHERE status NOT IN ('SUCCESS', 'BUILDING', 'INITIALIZING')
ORDER BY created_at DESC
LIMIT 10;
```

### 4. Cross-source JOIN with GitHub
Find deployments that occurred after a specific PR was merged to match deployment events with source changes:
```sql
-- Find deployments that happened after a PR was merged
SELECT d.id, d.status, d.created_at, g.title as pr_title
FROM railway.deployments d
JOIN github.pulls g
  ON d.created_at >= g.merged_at
WHERE g.owner = 'your-org'
  AND g.repo = 'your-repo'
  AND g.state = 'closed'
ORDER BY d.created_at DESC
LIMIT 10;
```

---

## Limitations

* **Filtering Requirements:** The Railway GraphQL API resolves child resources (like environments and services) nested within projects. While `project_id` is an optional filter in SQL, queries executed without it will fall back to querying the first project in your project list. Using explicit `project_id` equality filters is highly recommended to guarantee correct scoping.
* **Pagination:** The source uses a direct non-paginated fetch. If you have an exceptionally large deployment history or project list, some results beyond default page limits may be truncated.
