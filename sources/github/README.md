# GitHub Connector

**Version:** 2.0.0
**Source:** Curated GitHub schema
**Backend:** HTTP
**Tables:** 76
**Base URL:** `https://api.github.com` (override with `GITHUB_API_BASE`)

This source is a curated GitHub surface for Coral. It is not a 1:1 mirror of
the full REST API.

The default schema keeps the parts of GitHub that are useful for day-to-day
work and agent workflows:

- repositories, issues, pull requests, releases
- Actions and workflow runs
- checks and commit status
- security findings and advisories
- search
- org projects, gists, and codespaces

It intentionally drops Git internals, marketplace, billing, enterprise/admin
sprawl, and other low-signal REST plumbing.

Nested response structure is preserved with `__` columns, but the largest
embedded objects on core tables are trimmed down to compact summaries. Redundant
object-root columns are removed, and raw endpoint plumbing like `_links` is
stripped out.

## Authentication

Requires a `GITHUB_TOKEN` environment variable or a saved credential via:

```bash
coral source add github
```

For GitHub Enterprise, set `GITHUB_API_BASE` to `https://<host>/api/v3`.

## Main table groups

### Repositories

- `repo`
- `my_repos`
- `org_repos`
- `repo_branches`
- `repo_tags`
- `repo_labels`
- `repo_topics`
- `repo_contributors`
- `repo_languages`
- `repo_readme`
- `repo_license`

### Issues and pull requests

- `issues`
- `issue_comments`
- `issue_events`
- `issue_timeline`
- `pull_requests`
- `pull_request_files`
- `pull_request_reviews`
- `pull_request_review_comments`
- `pull_request_requested_reviewers`

### Releases

- `releases`
- `release_assets`
- `latest_release`
- `release_by_tag`

### Actions and checks

- `workflows`
- `workflow_runs`
- `workflow_runs_by_workflow`
- `workflow_run_jobs`
- `workflow_run_attempts`
- `workflow_run_artifacts`
- `workflow_run_timing`
- `workflow_artifacts`
- `workflow_timing`
- `workflow_requested_approvals`
- `workflow_pending_deployments`
- `commit_check_runs`
- `check_run_annotations`
- `check_run_details`
- `check_suite_details`
- `commit_check_suites`
- `commit_statuses`
- `combined_status`

### Security

- `global_advisories`
- `repository_security_advisories`
- `repo_code_scanning_alerts`
- `repo_dependabot_alerts`
- `repo_secret_scanning_alerts`
- `org_code_scanning_alerts`
- `org_secret_scanning_alerts`
- `private_vulnerability_reporting`
- `code_security_configuration`
- `default_setup`
- `scan_history`
- `sarifs`
- `variant_analyses`
- `sbom`
- `alerts`
- `analyses`
- `autofix`

### Search

- `repository_search`
- `issue_search`
- `user_search`
- `topic_search`
- `label_search`
- `commit_search`
- `code_search`

### Other GitHub domains

- `orgs`
- `org_members`
- `org_public_members`
- `projects`
- `project_fields`
- `project_items`
- `gist`
- `gists`
- `gist_comments`
- `codespaces`

## Query examples

```bash
# Repository details
coral sql \
  "SELECT full_name, owner, repo, owner__login, default_branch, visibility \
   FROM github.repo \
   WHERE owner = 'withcoral' AND repo = 'coral'"

# Repositories in an org
coral sql \
  "SELECT name, stargazers_count FROM github.org_repos \
   WHERE org = 'withcoral' ORDER BY stargazers_count DESC LIMIT 10"

# Recent issues for a repository
coral sql \
  "SELECT number, title, state, user__login, owner, repo \
   FROM github.issues \
   WHERE owner = 'withcoral' AND repo = 'coral' \
   ORDER BY updated_at DESC LIMIT 10"

# Open pull requests
coral sql \
  "SELECT number, title, state, user__login FROM github.pull_requests \
   WHERE owner = 'withcoral' AND repo = 'coral' AND state = 'open'"

# Latest workflow runs
coral sql \
  "SELECT workflow_id, display_title, status, conclusion, created_at \
   FROM github.workflow_runs \
   WHERE owner = 'withcoral' AND repo = 'coral' \
   ORDER BY created_at DESC LIMIT 10"

# Check runs for a commit SHA or branch ref
coral sql \
  "SELECT id, name, status, conclusion, ref, app__name, details_url \
   FROM github.commit_check_runs \
   WHERE owner = 'withcoral' AND repo = 'coral' AND ref = 'main' \
   ORDER BY started_at DESC LIMIT 10"

# Repository code scanning alerts
coral sql \
  "SELECT rule__description, state, severity, created_at \
   FROM github.repo_code_scanning_alerts \
   WHERE owner = 'withcoral' AND repo = 'coral' \
   ORDER BY created_at DESC LIMIT 10"

# Search repositories
coral sql \
  "SELECT full_name, stargazers_count FROM github.repository_search \
   WHERE q = 'language:rust stars:>1000' LIMIT 10"
```

## Discovery

Use Coral's catalog tables to inspect the installed GitHub schema:

```bash
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'github' ORDER BY 1"
coral sql "SELECT table_name, column_name FROM coral.columns WHERE schema_name = 'github' ORDER BY 1, 2"
```

## Not included

The bundled GitHub source no longer includes the full REST API surface.

If you need a niche endpoint that is not present in the curated schema, the
recommended path is to add a custom source spec for that API slice instead of
growing the default GitHub bundle back into a REST dump.
