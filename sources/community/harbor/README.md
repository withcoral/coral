# Harbor (Community)

**Version:** 0.1.0
**Backend:** HTTP (Harbor REST API v2.0)
**Tables:** 3
**Base URL:** `{{input.HARBOR_BASE_URL}}/api/v2.0`

Query Harbor projects, image repositories, and versioned artifacts through Coral SQL. Read-only access for container supply-chain auditing, storage analysis, and registry pull-activity reporting.

Coral exposes read-only `GET` tables. Write operations (deleting digests, creating robot accounts, toggling replication rules) are out of scope for v1.

## Install

```bash
export HARBOR_BASE_URL=https://harbor.example.com
export HARBOR_USERNAME='robot$production-apps+coral-auditor'
export HARBOR_PASSWORD='your_secret_here'
coral source add --file sources/community/harbor/manifest.yaml
```

## Authentication

Harbor uses HTTP Basic authentication. Coral sends `HARBOR_USERNAME` / `HARBOR_PASSWORD` as Basic credentials on every request.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `HARBOR_BASE_URL` | variable | yes | Harbor root URL, no trailing slash and without `/api` (e.g. `https://harbor.example.com`) |
| `HARBOR_USERNAME` | variable | yes | Harbor username or robot account name |
| `HARBOR_PASSWORD` | secret | yes | Harbor password or robot account secret |

Use a regular Harbor user or, preferably, a **robot account** — robots suit CI/CD and shared tooling that shouldn't depend on one person's login.

### Required permissions

These tables call Harbor's **list** endpoints, so the account needs the matching **list** permissions:

| Table | Harbor permission |
| --- | --- |
| `harbor.projects` | `List Project` |
| `harbor.repositories` | `List Repository` |
| `harbor.artifacts` | `List Artifact` |

`Pull Repository` is a **different** permission and does **not** grant access to these list endpoints — a pull-only robot will fail. Grant the three list permissions above (no push, delete, or admin permissions are needed).

### Robot account names

Harbor prefixes robot names (default prefix `robot$`, configurable by your administrator):

| Robot type | Format | Example |
| --- | --- | --- |
| Project robot | `<prefix><project_name>+<account_name>` | `robot$production-apps+coral-auditor` |
| System robot | `<prefix><account_name>` | `robot$coral-auditor` |

Create a project robot under the project's **Robot Accounts** tab, or a system robot under **Administration → Robot Accounts**. A project robot only sees its own project, so use a system robot (with the list permissions applied across projects) if you want `harbor.projects` to span the whole registry. Harbor shows the secret once — copy it immediately.

Returned data is restricted by the permissions of the supplied account.

Docs: [Robot account permission references](https://goharbor.io/docs/latest/administration/robot-accounts/#permission-references) · [Create project robot accounts](https://goharbor.io/docs/latest/working-with-projects/project-configuration/create-robot-accounts/) · [Harbor OpenAPI spec](https://github.com/goharbor/harbor/blob/main/api/v2.0/swagger.yaml). Your own instance also serves an API explorer at `<your-harbor>/devcenter-api-2.0`.

## Tables

| Table | Endpoint | Required filters | Pagination |
| --- | --- | --- | --- |
| `harbor.projects` | `GET /projects` | — | Page |
| `harbor.repositories` | `GET /projects/{project_name}/repositories` | `project_name` | Page |
| `harbor.artifacts` | `GET /projects/{project_name}/repositories/{encoded_repository_name}/artifacts` | `project_name`, `encoded_repository_name` | Page |

All tables page with Harbor's `page` / `page_size` parameters (1-indexed, capped at Harbor's max of 100). Coral injects these automatically. Use a SQL `LIMIT` to bound large scans.

### `harbor.projects`

| Column | Type | Description |
| --- | --- | --- |
| `project_id` | Int64 | Internal project identifier |
| `name` | Utf8 | Project name |
| `owner_id` | Int64 | Owner identifier |
| `repo_count` | Int64 | Number of repositories in the project |
| `public` | Utf8 | Whether the project is public (`"true"` / `"false"`) |
| `content_trust` | Utf8 | Whether content trust is enabled |
| `created_at` | Timestamp | Project creation timestamp |

### `harbor.repositories`

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

**Required filters:** `project_name`, `encoded_repository_name`

| Column | Type | Description |
| --- | --- | --- |
| `project_name` | Utf8 | Parent project name |
| `encoded_repository_name` | Utf8 | Double URL-encoded repository name (virtual) |
| `id` | Int64 | Internal artifact identifier |
| `digest` | Utf8 | Artifact digest |
| `size` | Int64 | Artifact size in bytes |
| `pull_time` | Timestamp | Last time the artifact was pulled |
| `tags` | Json | Array of tag objects as returned by Harbor — inspect with JSON functions |
| `push_time` | Timestamp | Artifact upload timestamp |

#### Encoded repository names

Harbor requires repository paths containing `/` to be **double URL-encoded** on artifact endpoints:

```text
team/backend  -> team%252Fbackend
library/nginx -> library%252Fnginx
```

Coral does not apply this encoding automatically, so pass the encoded identifier through `encoded_repository_name`.

## Example queries

List projects:

```sql
SELECT project_id, name, repo_count
FROM harbor.projects
ORDER BY repo_count DESC;
```

Repositories within a project:

```sql
SELECT name, artifact_count, pull_count
FROM harbor.repositories
WHERE project_name = 'production-apps'
ORDER BY pull_count DESC;
```

Artifacts in a nested repository:

```sql
SELECT digest, tags, size, pull_time
FROM harbor.artifacts
WHERE project_name = 'production-apps'
  AND encoded_repository_name = 'team%252Fbackend'
ORDER BY size DESC;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/harbor/manifest.yaml
coral source test harbor
```

Live output:

```text
<PASTE: coral source test harbor>
```

`harbor.repositories`:

```text
<PASTE: coral sql "SELECT name, artifact_count, pull_count FROM harbor.repositories WHERE project_name = '<your-project>' LIMIT 3">
```

`harbor.artifacts`:

```text
<PASTE: coral sql "SELECT digest, tags, size FROM harbor.artifacts WHERE project_name = '<your-project>' AND encoded_repository_name = '<encoded-repo>' LIMIT 3">
```

## Limitations

- Read-only source.
- Artifact deletion and replication management are out of scope.
- Results are limited by the permissions of the supplied account; the list permissions above are required.
- `repositories` requires `project_name`; `artifacts` requires both `project_name` and `encoded_repository_name`.
- Harbor artifact APIs require double URL-encoded repository paths (e.g. `library/nginx -> library%252Fnginx`). Coral does not encode automatically.
