# Render Community Source

Render is a modern cloud hosting platform for developers to host web services, static sites, databases, cron jobs, and background workers. This Coral community source allows teams to query their Render projects, environments, services, deploys, and jobs using SQL. 

By exposing Render resources to Coral, developers can observably audit infrastructure configurations, monitor deployment success rates, check cron job statuses, and join deployment events with other tools in their stack (e.g. GitHub Pull Requests).

## Installation

Add the Render source to your Coral instance by referencing its local manifest:

```bash
coral source add --file sources/community/render/manifest.yaml
```

## Setup & Authentication

The Render source requires an API Key to authenticate requests to the Public API.

### Step-by-Step API Key Generation:
1. Log in to your dashboard at [dashboard.render.com](https://dashboard.render.com).
2. Navigate to **Account Settings** (click your profile avatar in the header, then settings).
3. Scroll down to the **API Keys** section.
4. Click **Create API Key**.
5. Give the key a descriptive name (e.g., `Coral Integration`).
6. Copy the generated key.
7. Expose it to Coral as the `RENDER_API_KEY` environment variable or paste it when prompted.

---

## Table Reference

### `render.projects`
Lists all active projects associated with the authenticated account.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the project. |
| `name` | `Utf8` | `false` | Name of the project. |
| `created_at` | `Timestamp` | `true` | Timestamp when the project was created. |
| `updated_at` | `Timestamp` | `true` | Timestamp when the project was last updated. |
| `owner_id` | `Utf8` | `true` | ID of the project owner. |
| `owner_name` | `Utf8` | `true` | Name of the project owner. |
| `owner_email` | `Utf8` | `true` | Email of the project owner. |
| `owner_type` | `Utf8` | `true` | Type of the owner (e.g. user, team). |
| `environment_ids` | `Json` | `true` | List of environment IDs associated with the project. |

### `render.environments`
Lists environments (e.g., `production`, `staging`) configured inside projects.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the environment. |
| `name` | `Utf8` | `false` | Name of the environment. |
| `project_id` | `Utf8` | `true` | ID of the project the environment belongs to. |
| `protected_status` | `Utf8` | `true` | Protected status of the environment (e.g. protected, unprotected). |
| `network_isolation_enabled` | `Boolean` | `true` | Whether network isolation is enabled for the environment. |
| `database_ids` | `Json` | `true` | List of database IDs in the environment. |
| `service_ids` | `Json` | `true` | List of service IDs in the environment. |
| `redis_ids` | `Json` | `true` | List of Redis instance IDs in the environment. |
| `env_group_ids` | `Json` | `true` | List of environment group IDs in the environment. |

* **Filters:**
  * `project_id` (`Utf8`, optional): Restrict results to a single project.
  * `id` (`Utf8`, optional): Retrieve a single environment by ID.

### `render.services`
Lists Web Services, Static Sites, Private Services, Background Workers, and Cron Jobs.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the service. |
| `name` | `Utf8` | `false` | Name of the service. |
| `type` | `Utf8` | `false` | Type of the service (e.g. `web_service`, `static_site`, `cron_job`). |
| `repo` | `Utf8` | `true` | Repository URL for Git-backed services. |
| `branch` | `Utf8` | `true` | Git branch deployed for the service. |
| `created_at` | `Timestamp` | `true` | Timestamp when the service was created. |
| `updated_at` | `Timestamp` | `true` | Timestamp when the service was last updated. |
| `dashboard_url` | `Utf8` | `true` | URL to the service details page in the Render Dashboard. |
| `environment_id` | `Utf8` | `true` | ID of the environment the service belongs to. |
| `owner_id` | `Utf8` | `true` | ID of the owner of the service. |
| `suspended` | `Boolean` | `true` | Whether the service is currently suspended. |
| `auto_deploy` | `Boolean` | `true` | Whether auto-deploys are enabled for the service. |
| `service_details` | `Json` | `true` | Service-type-specific details block. |

### `render.deploys`
Lists the history of deployments across a service.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the deployment. |
| `status` | `Utf8` | `false` | Current status of the deployment (e.g., `live`, `build_failed`, `pre_deploy_failed`). |
| `commit_id` | `Utf8` | `true` | Commit SHA for the deploy. |
| `commit_message` | `Utf8` | `true` | Commit message for the deploy. |
| `commit_created_at` | `Timestamp` | `true` | Timestamp when the commit was created. |
| `image_ref` | `Utf8` | `true` | Image reference used when creating the deploy. |
| `image_sha` | `Utf8` | `true` | SHA that the image reference resolved to. |
| `image_registry_credential` | `Utf8` | `true` | Name of credential used to pull the image. |
| `trigger` | `Utf8` | `true` | What triggered the deployment (e.g., `api`, `manual`, `new_commit`). |
| `created_at` | `Timestamp` | `true` | Timestamp when the deploy record was created. |
| `updated_at` | `Timestamp` | `true` | Timestamp when the deploy record was last updated. |
| `started_at` | `Timestamp` | `true` | Timestamp when the build or deploy started. |
| `finished_at` | `Timestamp` | `true` | Timestamp when the build or deploy finished. |
| `service_id` | `Utf8` | `false` | ID of the deployed service (echo column). |

* **Filters:**
  * `service_id` (`Utf8`, required): Restrict results to a single service.

### `render.jobs`
Lists execution history of cron jobs for a service.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `id` | `Utf8` | `false` | Unique identifier of the job run. |
| `start_command` | `Utf8` | `false` | Command executed by the job. |
| `plan_id` | `Utf8` | `false` | Plan ID associated with the job run. |
| `status` | `Utf8` | `true` | Execution status of the job run (e.g., `succeeded`, `failed`, `running`). |
| `created_at` | `Timestamp` | `true` | Timestamp when the job run was created. |
| `started_at` | `Timestamp` | `true` | Timestamp when the job run started. |
| `finished_at` | `Timestamp` | `true` | Timestamp when the job run finished. |
| `service_id` | `Utf8` | `false` | ID of the cron job service (echo column). |

* **Filters:**
  * `service_id` (`Utf8`, required): Restrict results to a cron job service.

---

## Example SQL Queries

### 1. Identify non-Git or custom image deployed services
```sql
SELECT id, name, type, repo, branch, auto_deploy
FROM render.services
WHERE repo IS NULL
ORDER BY name;
```

### 2. Count active environments in a project
```sql
SELECT COUNT(*) as environments_count
FROM render.environments
WHERE project_id = 'prj-xxxxxxxxxxxx';
```

### 3. Check recent failed deployments for a service
```sql
SELECT id, status, trigger, created_at, commit_message
FROM render.deploys
WHERE service_id = 'srv-xxxxxxxxxxxx'
  AND status NOT IN ('live', 'pre_deploy_in_progress', 'build_in_progress')
ORDER BY created_at DESC
LIMIT 5;
```

### 4. Cross-source JOIN with GitHub
Find deploys that occurred after a specific PR was merged to match deployment events with source changes:
```sql
-- Find deployments that happened after a PR was merged
SELECT d.id, d.status, d.created_at, g.title as pr_title
FROM render.deploys d
JOIN github.pulls g
  ON d.created_at >= g.merged_at
WHERE d.service_id = 'srv-xxxxxxxxxxxx'
  AND g.owner = 'your-org'
  AND g.repo = 'your-repo'
  AND g.state = 'closed'
ORDER BY d.created_at DESC
LIMIT 10;
```

---

## Limitations

* **Filtering Requirements & Recommendations:** Due to the nesting of resources in the Render REST API:
  * To list environments, `project_id` must be provided (or query by environment `id`).
  * `service_id` is a **required filter** in SQL for both the `render.deploys` and `render.jobs` tables.
* **Pagination:** The source automatically handles cursor-based pagination in pages of 20.
