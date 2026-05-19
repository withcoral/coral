# Travis CI Source

Query CI/CD data from [Travis CI](https://www.travis-ci.com) — repositories,
builds, jobs, branches, stages, cron jobs, environment variables, settings,
caches, build requests, organizations, and broadcasts.

## Setup

### 1. Get your API token

Generate a Travis CI API token from your
[Travis CI profile settings](https://app.travis-ci.com/account/preferences)
or via the CLI:

```bash
travis login --pro
travis token --pro
```

### 2. Configure environment variables

```bash
export TRAVIS_CI_API_TOKEN="your-travis-api-token"
export TRAVIS_CI_OWNER="your-github-username-or-org"
```

### 3. Add the source

```bash
coral source add --file sources/community/travis_ci/manifest.yaml
```

## Authentication

| Input | Kind | Description |
|---|---|---|
| `TRAVIS_CI_API_TOKEN` | Secret | Travis CI API token |
| `TRAVIS_CI_OWNER` | Variable | GitHub owner login (user or org) |

Auth uses two headers per Travis CI API v3 requirements:
- `Authorization: token <TOKEN>`
- `Travis-API-Version: 3`

## Tables

### repositories

Lists repositories the configured owner has access to.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique Travis CI repository ID |
| `name` | Utf8 | Repository name |
| `slug` | Utf8 | Repository slug (owner/name) |
| `active` | Boolean | Whether active on Travis CI |
| `private` | Boolean | Whether the repository is private |
| `github_id` | Int64 | GitHub repository ID |
| `description` | Utf8 | Repository description from GitHub |
| `starred` | Boolean | Whether starred by current user |
| `managed_by_installation` | Boolean | Managed by GitHub App |
| `active_on_org` | Boolean | Active on travis-ci.org |
| `server_type` | Utf8 | Server type (git or svn) |

**Pagination:** Offset (max 100)

---

### builds

Lists builds for a repository.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `id` | Int64 | Unique build ID |
| `number` | Utf8 | Incremental build number |
| `state` | Utf8 | Build state (passed, failed, errored, canceled) |
| `duration` | Int64 | Build duration in seconds |
| `event_type` | Utf8 | Trigger event (push, pull_request, cron, api) |
| `previous_state` | Utf8 | State of the previous build |
| `started_at` | Utf8 | Build start timestamp |
| `finished_at` | Utf8 | Build finish timestamp |
| `created_by_login` | Utf8 | Login of the user who triggered the build |
| `branch_name` | Utf8 | Branch name |
| `commit_sha` | Utf8 | Commit SHA |
| `commit_message` | Utf8 | Commit message |
| `commit_author_name` | Utf8 | Commit author name |

**Required filter:** `repo_slug` (URL-encoded, e.g. `owner%2Frepo`)
**Pagination:** Offset (max 100)

---

### jobs

Lists jobs belonging to a specific build.

| Column | Type | Description |
|---|---|---|
| `build_id` | Utf8 | Build ID (virtual filter) |
| `id` | Int64 | Unique job ID |
| `number` | Utf8 | Job number (e.g. 1.1, 1.2) |
| `state` | Utf8 | Job state |
| `started_at` | Utf8 | Job start timestamp |
| `finished_at` | Utf8 | Job finish timestamp |
| `queue` | Utf8 | Queue this job ran on |
| `allow_failure` | Boolean | Whether this job is allowed to fail |

**Required filter:** `build_id`

---

### stages

Lists stages belonging to a build.

| Column | Type | Description |
|---|---|---|
| `build_id` | Utf8 | Build ID (virtual filter) |
| `id` | Int64 | Unique stage ID |
| `number` | Int64 | Incremental stage number |
| `name` | Utf8 | Stage name |
| `state` | Utf8 | Stage state |
| `started_at` | Utf8 | Stage start timestamp |
| `finished_at` | Utf8 | Stage finish timestamp |

**Required filter:** `build_id`

---

### branches

Lists branches for a repository with last build info.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `name` | Utf8 | Branch name |
| `default_branch` | Boolean | Whether this is the default branch |
| `exists_on_github` | Boolean | Whether the branch exists on GitHub |
| `last_build_id` | Int64 | ID of the last build |
| `last_build_state` | Utf8 | State of the last build |
| `last_build_number` | Utf8 | Number of the last build |
| `last_build_duration` | Int64 | Duration of the last build in seconds |

**Required filter:** `repo_slug`
**Pagination:** Offset (max 100)

---

### crons

Lists cron jobs for a repository.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `id` | Int64 | Unique cron ID |
| `interval` | Utf8 | Cron interval (daily, weekly, monthly) |
| `dont_run_if_recent_build_exists` | Boolean | Skip if recent build exists |
| `active` | Boolean | Whether the cron is active |
| `last_run` | Utf8 | Last run timestamp |
| `next_run` | Utf8 | Next scheduled run timestamp |
| `created_at` | Utf8 | Creation timestamp |
| `branch_name` | Utf8 | Branch name for this cron |

**Required filter:** `repo_slug`
**Pagination:** Offset (max 100)

---

### env_vars

Lists environment variables for a repository.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `id` | Utf8 | Unique variable ID |
| `name` | Utf8 | Variable name |
| `value` | Utf8 | Variable value (null if not public) |
| `public` | Boolean | Whether publicly visible |
| `branch` | Utf8 | Branch restriction |

**Required filter:** `repo_slug`

---

### settings

Lists settings for a repository.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `name` | Utf8 | Setting name |
| `value` | Json | Setting value (boolean or integer) |

**Required filter:** `repo_slug`

---

### caches

Lists build caches for a repository.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `slug` | Utf8 | Cache slug/name |
| `size` | Int64 | Cache size in bytes |
| `branch` | Utf8 | Branch this cache belongs to |
| `last_modified` | Utf8 | Last modified timestamp |

**Required filter:** `repo_slug`

---

### requests

Lists build requests for a repository.

| Column | Type | Description |
|---|---|---|
| `repo_slug` | Utf8 | Repository slug (virtual filter) |
| `id` | Int64 | Unique request ID |
| `state` | Utf8 | Request state |
| `result` | Utf8 | Request result (approved, rejected) |
| `message` | Utf8 | Request message |
| `event_type` | Utf8 | Event that triggered the request |
| `branch_name` | Utf8 | Branch name |
| `created_at` | Utf8 | Creation timestamp |

**Required filter:** `repo_slug`
**Pagination:** Offset (max 100)

---

### organizations

Lists organizations the current user is a member of.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique organization ID |
| `login` | Utf8 | Organization login |
| `name` | Utf8 | Organization name |
| `github_id` | Int64 | GitHub organization ID |
| `avatar_url` | Utf8 | Avatar URL |

**Pagination:** Offset (max 100)

---

### broadcasts

System broadcasts and notifications for the current user.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique broadcast ID |
| `message` | Utf8 | Broadcast message text |
| `category` | Utf8 | Category (announcement, warning) |
| `active` | Boolean | Whether the broadcast is still active |
| `created_at` | Utf8 | Creation timestamp |

---

### user

Current authenticated user profile (single-row table).

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Unique user ID |
| `login` | Utf8 | GitHub login |
| `name` | Utf8 | User display name |
| `github_id` | Int64 | GitHub user ID |
| `avatar_url` | Utf8 | Avatar URL |
| `is_syncing` | Boolean | Whether currently syncing |
| `synced_at` | Utf8 | Last sync timestamp |

## Example Queries

```sql
-- List all active repositories
SELECT id, name, slug, active
FROM travis_ci.repositories
WHERE active = true;

-- Recent builds for a repo
SELECT number, state, duration, event_type, branch_name, started_at
FROM travis_ci.builds
WHERE repo_slug = 'owner%2Frepo'
LIMIT 20;

-- Failed builds
SELECT number, state, branch_name, commit_message, finished_at
FROM travis_ci.builds
WHERE repo_slug = 'owner%2Frepo'
  AND state = 'failed';

-- Jobs for a specific build
SELECT id, number, state, queue, allow_failure, started_at
FROM travis_ci.jobs
WHERE build_id = '123456789';

-- Branch health overview
SELECT name, default_branch, last_build_state, last_build_duration
FROM travis_ci.branches
WHERE repo_slug = 'owner%2Frepo';

-- Audit environment variables
SELECT name, public, branch
FROM travis_ci.env_vars
WHERE repo_slug = 'owner%2Frepo';

-- Review repository settings
SELECT name, value
FROM travis_ci.settings
WHERE repo_slug = 'owner%2Frepo';

-- Check cron schedules
SELECT "interval", active, next_run, last_run, branch_name
FROM travis_ci.crons
WHERE repo_slug = 'owner%2Frepo';

-- List organizations
SELECT id, login, name
FROM travis_ci.organizations;

-- Current user info
SELECT id, login, name, github_id
FROM travis_ci.user;
```

## Pagination

| Table | Mode | Default | Max |
|---|---|---|---|
| `repositories` | offset | 100 | 100 |
| `builds` | offset | 25 | 100 |
| `jobs` | none | — | — |
| `stages` | none | — | — |
| `branches` | offset | 25 | 100 |
| `crons` | offset | 25 | 100 |
| `env_vars` | none | — | — |
| `settings` | none | — | — |
| `caches` | none | — | — |
| `requests` | offset | 25 | 100 |
| `organizations` | offset | 25 | 100 |
| `broadcasts` | none | — | — |
| `user` | none | — | — |

## Notes

- **Repo slug encoding**: When using `repo_slug` as a filter, the `/` in
  `owner/repo` must be URL-encoded as `%2F` (e.g. `owner%2Frepo`).
- **Read-only**: This source is read-only; no create, update, or delete
  operations.
- **travis-ci.org vs travis-ci.com**: The base URL defaults to
  `api.travis-ci.com`. Legacy open-source repos on `travis-ci.org` would
  need the base URL changed to `https://api.travis-ci.org`.
- **Private repos**: The API token must have sufficient permissions to
  access private repositories.
- **Rate limits**: Travis CI API has rate limits. The source does not
  configure custom rate-limit headers.
