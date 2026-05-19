# Codecov

Query repositories, commits, pull requests, branches, flags,
components, and commit uploads from Codecov.

## Setup

### Get Your API Token

1. Log in to [Codecov](https://app.codecov.io)
2. Navigate to **Settings > Access**
3. Generate an API access token
4. Copy the token

### Add the Source

```bash
export CODECOV_API_TOKEN="your_api_token"
export CODECOV_SERVICE="github"        # or gitlab, bitbucket
export CODECOV_OWNER="your_org_name"   # GitHub/GitLab username or org
coral source add --file sources/community/codecov/manifest.yaml
```

## Tables

### `repos`

Lists all repositories for the authenticated owner. Returns
repository name, language, activation status, and aggregate
coverage totals.

**Useful for:**

- Repository coverage inventory
- Identifying repos with low or missing coverage
- Auditing active vs inactive repositories

**Example:**

```sql
SELECT name, language, active, coverage, lines, hits, misses
FROM codecov.repos
LIMIT 20;
```

### `commits`

Lists commits with coverage data for a specific repository.
Returns commit SHA, message, branch, CI status, and per-commit
coverage totals.

**Requires:** `repo` filter

**Useful for:**

- Tracking coverage trends across commits
- Identifying commits that dropped coverage
- Correlating CI failures with coverage changes

**Example:**

```sql
SELECT commitid, message, branch, coverage, ci_passed, timestamp
FROM codecov.commits
WHERE repo = 'my-repo'
LIMIT 20;
```

### `pulls`

Lists pull requests with coverage data for a specific repository.
Returns pull number, title, state, CI status, patch coverage, and
base/head coverage totals.

**Requires:** `repo` filter

**Useful for:**

- Reviewing PR coverage impact
- Finding PRs that introduced coverage regressions
- Filtering by state (open/merged/closed)

**Example:**

```sql
SELECT pullid, title, state, coverage, base_coverage, patch_coverage
FROM codecov.pulls
WHERE repo = 'my-repo' AND state = 'open';
```

### `branches`

Lists branches for a specific repository with their last update time.

**Requires:** `repo` filter

**Example:**

```sql
SELECT name, updatestamp
FROM codecov.branches
WHERE repo = 'my-repo';
```

### `flags`

Lists coverage flags for a specific repository. Flags segment
coverage by test suite, component, or custom grouping.

**Requires:** `repo` filter

**Useful for:**

- Comparing coverage across test suites (unit, integration, e2e)
- Identifying flags with declining coverage
- Auditing flag configurations

**Example:**

```sql
SELECT flag_name, coverage
FROM codecov.flags
WHERE repo = 'my-repo';
```

### `components`

Lists coverage components for a specific repository. Components
are named groups of file paths defined in `codecov.yml`.

**Requires:** `repo` filter

**Useful for:**

- Tracking coverage for logical areas (frontend, backend, API)
- Identifying components with low coverage

**Example:**

```sql
SELECT component_id, name, coverage
FROM codecov.components
WHERE repo = 'my-repo';
```

### `commit_uploads`

Lists coverage report uploads for a specific commit. Useful for
debugging CI upload issues.

**Requires:** `repo` and `commitid` filters

**Example:**

```sql
SELECT upload_type, state, provider, build_url, flags, created_at
FROM codecov.commit_uploads
WHERE repo = 'my-repo' AND commitid = 'abc123def456';
```

## Authentication

The source uses Bearer token authentication. Generate a token at
https://app.codecov.io/account (Settings > Access). The token
must have read access to the target organization.

## Inputs

| Input | Kind | Default | Description |
|---|---|---|---|
| `CODECOV_API_TOKEN` | secret | — | API access token |
| `CODECOV_SERVICE` | variable | `github` | Git hosting provider (github, gitlab, bitbucket, etc.) |
| `CODECOV_OWNER` | variable | — | Owner username or org name |

## Pagination

All paginated tables use page-number pagination (`page` + `page_size`
query parameters). Coral will automatically paginate through all
pages to return complete results.

- Default page size: 20
- Maximum page size: 100

The `components` table is not paginated (returns all components
in a single response).

## Example Queries

### Find repos with coverage below 50%

```sql
SELECT name, language, coverage, lines, misses
FROM codecov.repos
WHERE coverage < 50
ORDER BY coverage ASC;
```

### Track coverage over recent commits

```sql
SELECT commitid, message, coverage, hits, misses, timestamp
FROM codecov.commits
WHERE repo = 'my-repo'
LIMIT 50;
```

### Compare coverage across test flags

```sql
SELECT flag_name, coverage
FROM codecov.flags
WHERE repo = 'my-repo'
ORDER BY coverage ASC;
```

### Find merged PRs that dropped coverage

```sql
SELECT pullid, title, coverage, base_coverage, patch_coverage, updatestamp
FROM codecov.pulls
WHERE repo = 'my-repo' AND state = 'merged'
LIMIT 20;
```

### Audit CI uploads for a commit

```sql
SELECT provider, upload_type, state, build_url, flags
FROM codecov.commit_uploads
WHERE repo = 'my-repo' AND commitid = 'abc123def456';
```

### Search repositories by name

```sql
SELECT name, coverage, active
FROM codecov.repos
WHERE search = 'api';
```

## Notes

- The source is read-only — no create, update, or delete operations
- All queries are scoped to a single `CODECOV_OWNER` organization
- Coverage values are percentages from 0 to 100 (Float64)
- The `totals` object from the API is flattened into top-level
  columns (coverage, files, lines, hits, misses, etc.)
- Timestamps are ISO 8601 strings
- The `commits`, `pulls`, `branches`, `flags`, `components`, and
  `commit_uploads` tables require a `repo` filter
- `commit_uploads` additionally requires a `commitid` filter
- The `flags` table exposes the flag name and aggregate coverage
- The `components` table is not paginated and returns all components
- `CODECOV_SERVICE` supports: `github`, `github_enterprise`, `gitlab`,
  `gitlab_enterprise`, `bitbucket`, `bitbucket_server`
