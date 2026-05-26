# Coolify (Community)

**Version:** 0.1.0
**Backend:** HTTP (Coolify REST API v1)
**Tables:** 5
**Base URL:** `{{input.COOLIFY_BASE_URL}}/api/v1`

Query Coolify projects, environments, servers, applications, and active deployments through Coral SQL using the [Coolify REST API](https://coolify.io/docs/api-reference).

Use this source for:

* deployment auditing
* application inventory
* server fleet monitoring
* Git deployment visibility
* environment topology inspection

Coral exposes read-only `GET` tables. Deployment execution, provisioning, updates, and destructive operations are out of scope for v1.

---

## Authentication & Token Scoping

Coolify API tokens are team-scoped.

Visibility returned through Coral depends entirely on the permissions assigned to the API token.

### Token setup

1. Open the Coolify dashboard.
2. Navigate to:
   `Keys & Tokens → API tokens`
3. Create a token with read access.

### Minimum permissions

The token should have:

* read/view access for projects
* read/view access for applications
* read/view access for servers
* read/view access for deployments

### Sensitive data visibility

Some environment variables or sensitive deployment metadata may remain hidden unless the token has elevated permissions.

---

## Install

Community sources are not bundled with the Coral binary.

```bash
coral source add --file sources/community/coolify/manifest.yaml
```

You can also copy `manifest.yaml` locally and reference it using:

```bash
coral source add --file ./manifest.yaml
```

---

## Inputs

| Input               | Kind     | Required | Description                                                   |
| ------------------- | -------- | -------- | ------------------------------------------------------------- |
| `COOLIFY_BASE_URL`  | variable | yes      | Root Coolify URL without trailing slash and without `/api/v1` |
| `COOLIFY_API_TOKEN` | secret   | yes      | API token generated from Coolify                              |

---

## Tables Overview

| Table          | Endpoint                                           | Required Filter |
| -------------- | -------------------------------------------------- | --------------- |
| `projects`     | `GET /api/v1/projects`                             | —               |
| `environments` | `GET /api/v1/projects/{project_uuid}/environments` | `project_uuid`  |
| `servers`      | `GET /api/v1/servers`                              | —               |
| `applications` | `GET /api/v1/applications`                         | —               |
| `deployments`  | `GET /api/v1/deployments`                          | —               |

---

## Filters and API Mapping

Only declared filters are pushed down directly to the Coolify API.

| SQL Filter     | API Parameter         | Tables         |
| -------------- | --------------------- | -------------- |
| `project_uuid` | path `{project_uuid}` | `environments` |
| `tag`          | query `tag`           | `applications` |

---

## Table Reference

### `coolify.projects`

Top-level Coolify projects.

| Column        | Type  | Description         |
| ------------- | ----- | ------------------- |
| `id`          | Int64 | Internal project ID |
| `uuid`        | Utf8  | Project UUID        |
| `name`        | Utf8  | Project name        |
| `description` | Utf8  | Project description |

---

### `coolify.environments`

Environments belonging to a project.

| Column         | Type      | Description             |
| -------------- | --------- | ----------------------- |
| `project_uuid` | Utf8      | Parent project UUID     |
| `id`           | Int64     | Environment ID          |
| `name`         | Utf8      | Environment name        |
| `description`  | Utf8      | Environment description |
| `project_id`   | Int64     | Internal project ID     |
| `created_at`   | Timestamp | Creation timestamp      |
| `updated_at`   | Timestamp | Last update timestamp   |

**Required filter:** `project_uuid`

---

### `coolify.servers`

Registered Coolify deployment servers.

| Column              | Type  | Description                      |
| ------------------- | ----- | -------------------------------- |
| `id`                | Int64 | Internal server ID               |
| `uuid`              | Utf8  | Server UUID                      |
| `name`              | Utf8  | Server name                      |
| `description`       | Utf8  | Server description               |
| `ip`                | Utf8  | Server IP or hostname            |
| `user`              | Utf8  | SSH username                     |
| `port`              | Int64 | SSH port                         |
| `proxy_type`        | Utf8  | Reverse proxy engine             |
| `unreachable_count` | Int64 | Consecutive failed health checks |

---

### `coolify.applications`

Applications deployed through Coolify.

| Column                 | Type      | Description                  |
| ---------------------- | --------- | ---------------------------- |
| `tag`                  | Utf8      | Optional application tag     |
| `id`                   | Int64     | Internal application ID      |
| `uuid`                 | Utf8      | Application UUID             |
| `name`                 | Utf8      | Application name             |
| `status`               | Utf8      | Runtime status               |
| `fqdn`                 | Utf8      | Application domains          |
| `git_repository`       | Utf8      | Source repository            |
| `git_branch`           | Utf8      | Git branch                   |
| `git_commit_sha`       | Utf8      | Active deployment commit SHA |
| `build_pack`           | Utf8      | Deployment build pack        |
| `environment_id`       | Int64     | Parent environment ID        |
| `destination_id`       | Int64     | Destination server ID        |
| `health_check_enabled` | Boolean   | Health check enabled         |
| `created_at`           | Timestamp | Creation timestamp           |
| `updated_at`           | Timestamp | Last update timestamp        |

---

### `coolify.deployments`

Currently running deployment queue entries.

> Historical deployments are not exposed through this endpoint.

| Column             | Type      | Description       |
| ------------------ | --------- | ----------------- |
| `deployment_uuid`  | Utf8      | Deployment UUID   |
| `application_name` | Utf8      | Application name  |
| `server_name`      | Utf8      | Target server     |
| `status`           | Utf8      | Deployment status |
| `commit`           | Utf8      | Commit SHA        |
| `commit_message`   | Utf8      | Commit message    |
| `deployment_url`   | Utf8      | Deployment UI URL |
| `created_at`       | Timestamp | Start timestamp   |

---

## Example Queries

### Project inventory

```sql
SELECT uuid, name
FROM coolify.projects
ORDER BY name;
```

### Environment inventory

```sql
SELECT name, created_at
FROM coolify.environments
WHERE project_uuid = 'project-uuid';
```

### Server audit

```sql
SELECT name, ip, proxy_type, unreachable_count
FROM coolify.servers
WHERE unreachable_count > 0;
```

### Running applications

```sql
SELECT name, status, fqdn, git_repository
FROM coolify.applications
WHERE status = 'running'
LIMIT 50;
```

### Active deployments

```sql
SELECT application_name, status, commit_message
FROM coolify.deployments
ORDER BY created_at DESC;
```

---

## Validation

Run before opening a PR:

```bash
make lint-sources

coral source lint sources/community/coolify/manifest.yaml
```

---

## Smoke Test

```bash
export COOLIFY_BASE_URL=https://coolify.example.com
export COOLIFY_API_TOKEN=<token>

coral source add --file sources/community/coolify/manifest.yaml

coral source test coolify
```

Example output from a sanitized local test run:

```text
$ coral source test coolify

  ✓ coolify connected successfully

    coolify (5 tables)
    ├─ applications
    ├─ deployments
    ├─ environments
    ├─ projects
    └─ servers

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT uuid, name FROM coolify.projects LIMIT 1
      1 row
```

Representative query output:

```text
$ coral sql "SELECT uuid, name FROM coolify.projects LIMIT 5"

+----------------+--------------+
| uuid           | name         |
+----------------+--------------+
| proj-smoke-001 | Demo Project |
+----------------+--------------+

$ coral sql "SELECT name, ip, proxy_type FROM coolify.servers LIMIT 5"

+--------------+-------------+-------------+
| name         | ip          | proxy_type  |
+--------------+-------------+-------------+
| prod-server  | 10.0.0.12   | traefik     |
| staging-node | 10.0.0.18   | caddy       |
+--------------+-------------+-------------+

$ coral sql "SELECT application_name, status, commit FROM coolify.deployments LIMIT 5"

+------------------+-------------+--------------+
| application_name | status      | commit       |
+------------------+-------------+--------------+
| api-service      | in_progress | abc123def456 |
+------------------+-------------+--------------+
```

---

## Limitations

* Read-only source
* No deployment execution support
* No provisioning or delete operations
* No historical deployment API modeling
* Token permissions affect visible rows
* Large instances should use SQL `LIMIT`
