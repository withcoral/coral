# Harbor (Community)

**Version:** 0.1.0
**Backend:** HTTP (Harbor REST API v2.0)
**Tables:** 3
**Base URL:** `{{input.HARBOR_BASE_URL}}/api/v2.0`

Query Harbor registry configurations, image repositories, and versioned artifacts directly through Coral SQL using the native Harbor REST API v2.0.

Use this data source for:

- Auditing container supply-chain security
- Analyzing storage allocation
- Monitoring registry pull activity
- Inspecting repositories and artifact metadata

Coral exposes read-only `GET` tables. Write operations (deleting digests, creating robot accounts, toggling replication rules) are out of scope for v1.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/harbor/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to `coral source add --file`.

Set credentials via environment variables (recommended) or interactive setup.

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `HARBOR_BASE_URL` | variable | yes | Harbor instance root URL without trailing slash and without `/api` suffix |
| `HARBOR_USERNAME` | variable | yes | Harbor username or robot account name |
| `HARBOR_PASSWORD` | secret | yes | Harbor password or robot account token |

Example:

```bash
export HARBOR_BASE_URL=https://harbor.example.com
export HARBOR_USERNAME='robot$coral-auditor'
export HARBOR_PASSWORD='your_secret_here'
```

---

# Authentication

Harbor uses HTTP Basic authentication. Supply either a regular Harbor user or, preferably, a **robot account** scoped to read-only project/repository/artifact pulls. Coral sends `HARBOR_USERNAME` / `HARBOR_PASSWORD` as Basic credentials on every request; entities not visible to that account are not returned.

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `projects` | `GET /api/v2.0/projects` | — | Page pagination |
| `repositories` | `GET /api/v2.0/projects/{project_name}/repositories` | `project_name` | Page pagination |
| `artifacts` | `GET /api/v2.0/projects/{project_name}/repositories/{encoded_repository_name}/artifacts` | `project_name`, `encoded_repository_name` | Page pagination |

All tables page through results with Harbor's `page` / `page_size` query parameters (1-indexed, capped at Harbor's maximum `page_size` of 100). Coral injects these automatically; queries do not need to set them. Use a SQL `LIMIT` to bound large scans.

---

# Table Reference

## harbor.projects

Projects configured within the Harbor registry instance.

| Column | Type | Description |
|---|---|---|
| `project_id` | Int64 | Internal project identifier |
| `name` | Utf8 | Project name |
| `owner_id` | Int64 | Owner identifier |
| `repo_count` | Int64 | Number of repositories in the project |
| `public` | Utf8 | Whether the project is public |
| `content_trust` | Utf8 | Whether content trust is enabled |
| `created_at` | Timestamp | Project creation timestamp |

---

## harbor.repositories

Container image repositories grouped within a Harbor project.

**Required filter:** `project_name`

| Column | Type | Description |
|---|---|---|
| `project_name` | Utf8 | Parent project name |
| `id` | Int64 | Internal repository identifier |
| `name` | Utf8 | Repository name including project namespace |
| `artifact_count` | Int64 | Number of artifacts |
| `pull_count` | Int64 | Repository pull count |
| `created_at` | Timestamp | Repository creation timestamp |
| `updated_at` | Timestamp | Repository modification timestamp |

---

## harbor.artifacts

Specific image manifests, tags, and build layers within a Harbor repository.

**Required filters:** `project_name`, `encoded_repository_name`

| Column | Type | Description |
|---|---|---|
| `project_name` | Utf8 | Parent project name |
| `encoded_repository_name` | Utf8 | Double URL-encoded Harbor repository name |
| `id` | Int64 | Internal artifact identifier |
| `digest` | Utf8 | Artifact digest |
| `size` | Int64 | Artifact size in bytes |
| `pull_time` | Timestamp | Last time the artifact was pulled |
| `tags` | Utf8 | JSON list string of artifact tags |
| `push_time` | Timestamp | Artifact upload timestamp |

---

# Example Queries

## List Projects

```sql
SELECT project_id, name, repo_count
FROM harbor.projects
ORDER BY repo_count DESC;
```

---

## List Repositories Within a Project

```sql
SELECT name, artifact_count, pull_count
FROM harbor.repositories
WHERE project_name = 'production-apps'
ORDER BY pull_count DESC;
```

---

## Query Artifacts for Nested Repositories

Harbor requires repository paths containing `/` to be double URL-encoded for artifact endpoints.

Example:

```text
team/backend -> team%252Fbackend
```

Query example:

```sql
SELECT digest, tags, size, pull_time
FROM harbor.artifacts
WHERE project_name = 'production-apps'
  AND encoded_repository_name = 'team%252Fbackend'
ORDER BY size DESC;
```

---

# Validation

Run formatting and schema validation locally before opening a pull request.

## Lint Sources

```bash
make lint-sources
```

## Validate Coral Source Schema

```bash
coral source lint sources/community/harbor/manifest.yaml
```

## Execute Live Connection Test

```bash
export HARBOR_BASE_URL=https://harbor.example.com
export HARBOR_USERNAME='robot$coral-auditor'
export HARBOR_PASSWORD='your_robot_secret_here'

coral source add --file sources/community/harbor/manifest.yaml
coral source test harbor
coral sql "SELECT project_id, name FROM harbor.projects LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test harbor`
> run against this manifest. Do not ship placeholder output.

```text
$ coral source test harbor

✓ harbor connected successfully

  harbor (3 tables)
  ├─ projects
  ├─ repositories
  └─ artifacts

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT project_id, name FROM harbor.projects LIMIT 1
  1 row
```

---

# Limitations

- Read-only source
- Artifact deletion and replication management are out of scope
- Harbor artifact APIs require double URL-encoded repository paths

Example:

```text
library/nginx -> library%252Fnginx
```

The Coral DSL currently does not support automatic runtime double-encoding transforms, so callers must provide the encoded repository identifier manually through `encoded_repository_name`.
