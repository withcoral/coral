# Jenkins Connector (Community)

**Version:** 0.1.1
**Backend:** HTTP (Jenkins REST JSON API)
**Tables:** 4
**Default base URL:** `http://127.0.0.1:8081` (override with `JENKINS_BASE_URL`)

Query Jenkins jobs, last-build results, and Git revision metadata from Jenkins
(self-hosted). Designed for CI triage and build auditing without custom Jenkins
API scripts.

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/jenkins/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Reference the linked GitHub issue in your PR so maintainers can connect the
contribution to the prior discussion.

## Authentication and setup

### Local development (recommended for contributors)

Jenkins uses **HTTP Basic** for the JSON API, not a bare Bearer token. Export the
full `Authorization` header before adding the source:

```bash
export JENKINS_BASE_URL=http://127.0.0.1:8081
export JENKINS_AUTHORIZATION="Basic $(echo -n 'admin:YOUR_API_TOKEN' | base64 -w0)"
coral source add --file sources/community/jenkins/manifest.yaml
```

Create an API token under **User** → **Configure** → **API Token**. Use a user
that can read jobs and builds.

On macOS, build the header with `base64 | tr -d '\n'` instead of `base64 -w0`.

### Remote or shared Jenkins

Point `JENKINS_BASE_URL` at your controller root URL with no trailing slash (for
example `https://jenkins.example.com`). Ensure the Jenkins user has permission to
read jobs and builds for the tables you query.

### Multiple controllers

Register one Coral source per Jenkins instance (for example `jenkins_ci`,
`jenkins_prod`), each with its own `JENKINS_BASE_URL` and `JENKINS_AUTHORIZATION`.

## Table categories

### Job inventory

| Table | Description |
|---|---|
| `jobs` | Top-level jobs (`name`, `url`, `color`) |

### Per-job metadata

| Table | Description |
|---|---|
| `builds` | Last build number, result, URL, and `commit_hash` when present |
| `job_last_revision` | Git `SHA1` rows from job `actions` |
| `job_git_sha_by_action_index` | Git SHA via indexed `actions` path |

## Filters and pagination

Per-job tables require a `job_name` filter. The `jobs` table has no required
filter.

Example:

```sql
SELECT job_name, build_number, result, commit_hash
FROM jenkins.builds
WHERE job_name = 'my-pipeline';
```

Responses use Jenkins `tree` query parameters for bounded JSON payloads. Tables
use `pagination.mode: none` because each request returns a small, scoped result.
Prefer `LIMIT` when listing jobs on large controllers.

## Example relationships

| From | To | Join hint |
|---|---|---|
| `jenkins.jobs.name` | `jenkins.builds.job_name` | Job to last build |
| `jenkins.builds.commit_hash` | `k8s.pods.annotation_commit_hash` | When CI injects the same commit into cluster metadata |

## Example queries

### List jobs

```sql
SELECT name, color, url
FROM jenkins.jobs
LIMIT 50;
```

### Last build for a job

```sql
SELECT job_name, build_number, result, commit_hash, build_url
FROM jenkins.builds
WHERE job_name = 'my-pipeline';
```

### Unsuccessful last build

```sql
SELECT job_name, build_number, result
FROM jenkins.builds
WHERE job_name = 'api-deploy'
  AND result != 'SUCCESS';
```

### Git SHA from job actions

```sql
SELECT sha1, plugin_class
FROM jenkins.job_last_revision
WHERE job_name = 'my-pipeline';
```

## Validation

```bash
# YAML style (requires: cargo install ryl --locked)
make lint-sources

# Manifest structure and smoke queries (requires Coral CLI)
coral source lint sources/community/jenkins/manifest.yaml
export JENKINS_BASE_URL=http://127.0.0.1:8081
export JENKINS_AUTHORIZATION="Basic $(echo -n 'admin:YOUR_API_TOKEN' | base64 -w0)"
coral source add --file sources/community/jenkins/manifest.yaml
coral source test jenkins
```

## Limitations

- Read-only v1 (no build triggers).
- v1 focuses on last build per job, not full build history.
- `JENKINS_AUTHORIZATION` must be the full `Basic ...` header; a bare token returns `403`.
- `commit_hash` and Git SHA columns depend on job type and installed plugins.
- Community sources are maintained separately from bundled core sources.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/jenkins): add jenkins community source`.
