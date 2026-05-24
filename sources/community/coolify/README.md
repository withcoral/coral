# Coolify (Community)

**Version:** 0.1.0
**Backend:** HTTP (Coolify REST API v1)
**Tables:** 5
**Base URL:** `{{input.COOLIFY_BASE_URL}}/api/v1`

Query Coolify projects, environments, servers, applications, and active deployments through Coral SQL using the [Coolify REST API](https://coolify.io/docs/api-reference). Use this data source for PaaS state auditing, deployment tracking, server health monitoring, and managing environment topologies across self-hosted instances or Coolify Cloud accounts.

Coral exposes read-only `GET` tables. Write operations (triggering deployments, stopping services, provisioning resources) are out of scope for v1.

## Install

Community sources are not bundled with the Coral binary. From the Coral repo root (or with a copied manifest):

```bash
coral source add --file sources/community/coolify/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to `coral source add --file`.

Set credentials via environment variables (recommended) or `coral source add --file ... --interactive`.

## Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `COOLIFY_BASE_URL` | variable | yes | Root URL of your instance with **no** trailing slash and **no** `/api/v1` path suffix (for example `https://coolify.example.com` or `http://localhost:8000`). |
| `COOLIFY_API_TOKEN` | secret | yes | Personal API token obtained from your Coolify panel under **Keys & Tokens → API tokens**. |

---

## Tables overview

| Table | API endpoint | Required filter | Pagination |
| --- | --- | --- | --- |
| `projects` | `GET /api/v1/projects` | — | Full array response |
| `environments` | `GET /api/v1/projects/{project_uuid}/environments` | `project_uuid` | Full array response |
| `servers` | `GET /api/v1/servers` | — | Full array response |
| `applications` | `GET /api/v1/applications` | — | Full array response |
| `deployments` | `GET /api/v1/deployments` | — | Full array response |

Project-scoped environment tables require an explicit SQL lookup condition, for example `WHERE project_uuid = 'string-uuid'`.

---

## Filters and API mapping

Coral maps declared SQL filters to native Coolify API query parameters. Only listed filters are pushed directly down to the REST endpoint; other clauses are filtered in-memory.

| SQL filter | Coolify query param | Tables |
| --- | --- | --- |
| `project_uuid` | path `{project_uuid}` | `environments` |
| `tag` | query `tag` | `applications` |

---

## Table reference

### `coolify.projects`

Top-level grouping for your environments and resources.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal database ID of the project |
| `uuid` | Utf8 | Project unique UUID |
| `name` | Utf8 | Project name |
| `description` | Utf8 | Optional project description |

### `coolify.environments`

Environments (e.g., production, staging) configured within a project.

| Column | Type | Description |
| --- | --- | --- |
| `project_uuid` | Utf8 | Parent project UUID from the required filter |
| `id` | Int64 | Internal database ID of the environment |
| `name` | Utf8 | Environment name |
| `description` | Utf8 | Optional environment description |
| `project_id` | Int64 | Relational project ID |
| `created_at` | Timestamp | Environment creation time |
| `updated_at` | Timestamp | Last record update time |

**Required filter:** `project_uuid`

### `coolify.servers`

Registered server nodes managed by Coolify.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal database ID of the server |
| `uuid` | Utf8 | Server UUID |
| `name` | Utf8 | Server name |
| `description` | Utf8 | Server description |
| `ip` | Utf8 | IP address or hostname used for SSH connections |
| `user` | Utf8 | SSH username |
| `port` | Int64 | SSH connection port |
| `proxy_type` | Utf8 | Embedded reverse proxy engine type (traefik, caddy, etc.) |
| `unreachable_count`| Int64 | Consecutively failed health check counts |

### `coolify.applications`

Deployed microservices, websites, and codebases.

| Column | Type | Description |
| --- | --- | --- |
| `tag` | Utf8 | Optional tag identifier filter |
| `id` | Int64 | Internal application ID |
| `uuid` | Utf8 | Application unique UUID |
| `name` | Utf8 | Application name |
| `description` | Utf8 | Application text details |
| `status` | Utf8 | Current container execution status |
| `fqdn` | Utf8 | Configured domain routing points |
| `git_repository` | Utf8 | Source control origin target |
| `git_branch` | Utf8 | Monitored branch target |
| `git_commit_sha` | Utf8 | Active running deployment commit hash |
| `build_pack` | Utf8 | Deployment builder framework (nixpacks, dockerfile, etc.) |
| `environment_id` | Int64 | Parent environment ID linkage |
| `destination_id` | Int64 | Destination infrastructure server ID |
| `health_check_enabled`| Boolean | State of endpoint automated monitoring checks |
| `created_at` | Timestamp | Application registration date |
| `updated_at` | Timestamp | Last configuration change date |

### `coolify.deployments`

Active or historic application build queue tracking.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal deployment row ID |
| `deployment_uuid` | Utf8 | Deployment worker execution UUID |
| `application_id` | Utf8 | Target resource identity string |
| `application_name` | Utf8 | Target resource name |
| `server_id` | Int64 | Destination build runner node ID |
| `server_name` | Utf8 | Destination build runner name |
| `status` | Utf8 | Active step state (in-progress, finished, failed) |
| `commit` | Utf8 | Processing commit hash identifier |
| `commit_message` | Utf8 | Associated deployment code commit logs |
| `deployment_url` | Utf8 | Direct internal panel path link to review build standard outputs |
| `force_rebuild` | Boolean | True if image builds ran without active layer caches |
| `is_webhook` | Boolean | True if automated git webhooks initiated execution |
| `is_api` | Boolean | True if manual execution triggers calls via endpoint loops |
| `rollback` | Boolean | True if rollback triggers invoked state actions |
| `git_type` | Utf8 | Code hosting hub provider origin details |
| `created_at` | Timestamp | Start timestamps for delivery operations |
| `updated_at` | Timestamp | End or final state synchronization check timestamps |

---

## Example queries

### Project Topology Discovery
```sql
SELECT uuid, name, description 
FROM coolify.projects 
ORDER BY name;
```

```sql
SELECT name, description, created_at 
FROM coolify.environments 
WHERE project_uuid = '0189b2c4-e5fd-7264-ba36-8cf9b3d2efaa';
```

### Server Cluster Auditing
```sql
SELECT name, ip, user, port, proxy_type, unreachable_count 
FROM coolify.servers 
WHERE unreachable_count > 0;
```

### Application Routing and Source Controls
```sql
SELECT name, status, fqdn, git_repository, git_branch 
FROM coolify.applications 
WHERE status = 'running' 
LIMIT 50;
```

### Pipeline Deployment Queue Review
```sql
SELECT application_name, server_name, status, commit_message, deployment_url 
FROM coolify.deployments 
WHERE status = 'in-progress' 
ORDER BY created_at DESC;
```

---

## Validation

Run the following format and syntax pipeline validation commands prior to generating a GitHub pull request:

```bash
# YAML and file style compliance check
make lint-sources

# Structural schema and type mapping verification
coral source lint sources/community/coolify/manifest.yaml
```

Execute a live target connection test locally:

```bash
export COOLIFY_BASE_URL=https://coolify.example.com
export COOLIFY_API_TOKEN=your_token_here

coral source add --file sources/community/coolify/manifest.yaml
coral source test coolify
```
