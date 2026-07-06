# Harbor (Community)

**Version:** 0.1.0
**Backend:** HTTP (Harbor REST API v2.0)
**Tables:** 3
**Base URL:** `{{input.HARBOR_BASE_URL}}/api/v2.0`

Query Harbor registry configurations, image repositories, and versioned artifacts directly through Coral SQL using the native Harbor REST API v2.0.

This integration provides read-only access to Harbor's REST API v2.0 for auditing container supply-chain security, analyzing storage allocation, monitoring registry pull activity, and inspecting repositories and artifact metadata.

Coral exposes read-only `GET` tables. Write operations (deleting digests, creating robot accounts, toggling replication rules) are out of scope for v1.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export HARBOR_BASE_URL=https://harbor.example.com
export HARBOR_USERNAME='robot$coral-auditor'
export HARBOR_PASSWORD='your_secret_here'
coral source add --file sources/community/harbor/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Harbor uses HTTP Basic authentication. Coral sends `HARBOR_USERNAME` / `HARBOR_PASSWORD` as Basic credentials on every request.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `HARBOR_BASE_URL` | variable | yes | Harbor instance root URL without trailing slash and without `/api` suffix |
| `HARBOR_USERNAME` | variable | yes | Harbor username or robot account name |
| `HARBOR_PASSWORD` | secret | yes | Harbor password or robot account token |

Supply either a regular Harbor user or, preferably, a **robot account** scoped to read-only project/repository/artifact pulls. Prefer a robot account for CI/CD pipelines and shared tooling that shouldn't depend on a single person's login; create one under **Administration → Robot Accounts** (or a project-scoped robot under the project's **Robot Accounts** tab) with pull/read permissions only.

Returned data is restricted by the permissions of the supplied account. Projects, repositories, and artifacts not visible to that account are not returned.

Official docs:

- [Harbor API v2.0 Reference (Swagger)](https://harbor.example.com/devcenter-api-2.0)
- [Harbor — Robot Accounts](https://goharbor.io/docs/latest/working-with-projects/project-configuration/create-robot-accounts/)

## Tables

| Table | API Endpoint | Required filters | Pagination |
| --- | --- | --- | --- |
| `harbor.projects` | `GET /api/v2.0/projects` | — | Page pagination |
| `harbor.repositories` | `GET /api/v2.0/projects/{project_name}/repositories` | `project_name` | Page pagination |
| `harbor.artifacts` | `GET /api/v2.0/projects/{project_name}/repositories/{encoded_repository_name}/artifacts` | `project_name`, `encoded_repository_name` | Page pagination |

All tables page through results with Harbor's `page` / `page_size` query parameters (1-indexed, capped at Harbor's maximum `page_size` of 100). Coral injects these automatically; queries do not need to set them. Use a SQL `LIMIT` to bound large scans.

### `harbor.projects`

Projects configured within the Harbor registry instance.

| Column | Type | Description |
| --- | --- | --- |
| `project_id` | Int64 | Internal project identifier |
| `name` | Utf8 | Project name |
| `owner_id` | Int64 | Owner identifier |
| `repo_count` | Int64 | Number of repositories in the project |
| `public` | Utf8 | Whether the project is public |
| `content_trust` | Utf8 | Whether content trust is enabled |
| `created_at` | Timestamp | Project creation timestamp |

### `harbor.repositories`

Container image repositories grouped within a Harbor project.

**Required filter:** `project_name`

| Column | Type | Description |
| --- | --- | --- |
| `project_name` | Utf8 | Parent project name |
| `id` | Int64 | Internal repository identifier |
| `name` | Utf8 | Repository name including project namespace |
| `artifact_count` | Int64 | Number of artifacts |
| `pull_count` | Int64 | Repository pull count |
| `created_at` | Timestamp | Repository creation timestamp |
| `updated_at` | Timestamp | Repository modification timestamp |

### `harbor.artifacts`

Specific image manifests, tags, and build layers within a Harbor repository.

**Required filters:** `project_name`, `encoded_repository_name`

| Column | Type | Description |
| --- | --- | --- |
| `project_name` | Utf8 | Parent project name |
| `encoded_repository_name` | Utf8 | Double URL-encoded Harbor repository name |
| `id` | Int64 | Internal artifact identifier |
| `digest` | Utf8 | Artifact digest |
| `size` | Int64 | Artifact size in bytes |
| `pull_time` | Timestamp | Last time the artifact was pulled |
| `tags` | Utf8 | JSON list string of artifact tags |
| `push_time` | Timestamp | Artifact upload timestamp |

#### Encoded repository names

Harbor requires repository paths containing `/` to be **double URL-encoded** for artifact endpoints:

```text
team/backend -> team%252Fbackend
library/nginx -> library%252Fnginx
```

The Coral DSL currently does not support automatic runtime double-encoding transforms, so callers must provide the encoded repository identifier manually through `encoded_repository_name`.

## Example queries

### List projects

```sql
SELECT
  project_id,
  name,
  repo_count
FROM harbor.projects
ORDER BY repo_count DESC;
```

### List repositories within a project

```sql
SELECT
  name,
  artifact_count,
  pull_count
FROM harbor.repositories
WHERE project_name = 'production-apps'
ORDER BY pull_count DESC;
```

### Query artifacts for nested repositories

```sql
SELECT
  digest,
  tags,
  size,
  pull_time
FROM harbor.artifacts
WHERE project_name = 'production-apps'
  AND encoded_repository_name = 'team%252Fbackend'
ORDER BY size DESC;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/harbor/manifest.yaml
Coral manifest schema validation: passed for sources/community/harbor/manifest.yaml
make lint-sources: passed
Live API tests: passed with a Harbor robot account
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/harbor/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export HARBOR_BASE_URL=https://harbor.example.com
export HARBOR_USERNAME='robot$coral-auditor'
export HARBOR_PASSWORD='your_robot_secret_here'
coral source add --file sources/community/harbor/manifest.yaml
coral source test harbor
```

Validate table access with representative SQL:

```bash
coral sql "SELECT project_id, name FROM harbor.projects LIMIT 5"
coral sql "SELECT name, artifact_count, pull_count FROM harbor.repositories WHERE project_name = 'production-apps' LIMIT 5"
coral sql "SELECT digest, tags, size FROM harbor.artifacts WHERE project_name = 'production-apps' AND encoded_repository_name = 'team%252Fbackend' LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'harbor'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'harbor' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
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

Representative query:

```sql
SELECT project_id, name, repo_count, public
FROM harbor.projects
ORDER BY repo_count DESC
LIMIT 3;
```

Example output:

```text
project_id | name             | repo_count | public
2          | production-apps  | 42         | false
5          | platform-shared  | 18         | false
1          | library          | 7          | true
```

## Limitations

- Read-only source.
- Artifact deletion and replication management are out of scope.
- Query results are limited by the permissions of the supplied account.
- `repositories` requires a `project_name` filter, and `artifacts` requires both `project_name` and `encoded_repository_name`.
- Harbor artifact APIs require double URL-encoded repository paths (for example `library/nginx -> library%252Fnginx`). Coral does not perform this encoding automatically, so callers must pass the encoded identifier through `encoded_repository_name`.
