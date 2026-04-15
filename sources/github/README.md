# GitHub Connector

**Version:** 1.1.5
**Source:** OpenAPI-generated from GitHub's v3 REST API spec
**Backend:** HTTP
**Tables:** 369
**Base URL:** `https://api.github.com` (override with `GITHUB_API_BASE` env var)

## Authentication

Requires a `GITHUB_TOKEN` environment variable or saved credential via `coral source add github`.

```bash
coral source add github
```

To rotate or update your token, run the same command again.

### Token types

| Token type | Coverage | Notes |
|---|---|---|
| Fine-grained PAT (repo-scoped) | ~145 tables | Best for personal repos. Must explicitly grant org access. |
| Classic PAT (broad scopes) | ~250+ tables | `repo`, `admin:org`, `user`, `gist`, `read:packages` recommended |
| GitHub App installation token | ~350 tables | Covers app-specific endpoints (401 tables below) |

### Rate limiting

GitHub allows 5,000 core API requests/hour per authenticated user.
Each Coral query maps to one or more API calls.
Search endpoints have a separate 30 requests/minute limit.
The connector treats a `403` response as rate-limited when GitHub's
`X-RateLimit-Remaining` header is `0` or a `Retry-After` header is present,
which avoids retrying ordinary permission failures.

## Table categories

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| No filter | 57 | `SELECT * FROM github.user_repos` |
| `owner` + `repo` | 147 | `WHERE owner = 'org' AND repo = 'name'` |
| `org` | 108 | `WHERE org = 'myorg'` |
| `username` | 19 | `WHERE username = 'user'` |
| `enterprise` | 12 | `WHERE enterprise = 'slug'` |
| Search (`q`) | 7 | `WHERE q = 'search terms'` |
| Compound (IDs) | 21 | `WHERE owner = '...' AND repo = '...' AND run_id = 123` |

### By access level

| Access level | Tables | What you need |
|---|---|---|
| Public / basic token | ~60 | Any valid `GITHUB_TOKEN` |
| Repo read | ~40 | Token with repo read access |
| Repo admin / owner | ~45 | Push access or repo owner (traffic, hooks, secrets, collaborators) |
| Org member | ~50 | Token scoped to the organization |
| Org admin | ~40 | Organization admin permissions |
| Enterprise | ~12 | Enterprise Cloud account |
| GitHub App JWT | ~19 | GitHub App installation, not a PAT |

#### No filter required (26 tables)

| Table | Description |
|---|---|
| `user` | Authenticated user profile |
| `user_repos` | Repositories for the authenticated user |
| `user_starred` | Starred repositories |
| `user_subscriptions` | Watched repositories |
| `user_orgs` | User's organizations |
| `user_issues` | Issues assigned to authenticated user |
| `user_codespaces` | User's codespaces |
| `user_social_accounts` | Social accounts |
| `user_teams` | Teams the user belongs to |
| `user_membership_orgs` | Organization memberships |
| `meta` | GitHub API meta information |
| `meta_get_all_versions` | API versions |
| `rate_limit` | Current rate limit status |
| `licenses` | Available open source licenses |
| `codes_of_conduct` | Available codes of conduct |
| `emojis` | Available emojis |
| `feeds` | Atom feeds |
| `templates` | Gitignore templates |
| `public_emails` | User's public email addresses |
| `marketplace_purchases` | User's marketplace purchases |
| `repository_invitations` | Pending repository invitations |
| `gists` | User's gists |
| `gist_starred` | Starred gists |
| `issues` | Issues across all repos (when no filter) |
| `classrooms` | GitHub Classroom |
| `rows`, `stubbed` | Marketplace stubs |

#### Repository tables — owner + repo (65 tables)

Core data:

| Table | Sample count | Notes |
|---|---|---|
| `repos_get` | 1 | Full repository details |
| `commits` | 22 | Commit history |
| `repo_branches` | 5 | Branch listing |
| `repo_tags` | 1 | Tag listing |
| `repo_labels` | 11 | Issue/PR labels |
| `repo_contributors` | 2 | Contributors list |
| `assignees` | 16 | Valid assignees |
| `languages` | 1 | Language breakdown |
| `license` | 1 | Repository license |
| `readme` | 1 | README content |
| `contents` | 1 | File contents (requires `path` filter) |
| `stargazers` | 8 | Users who starred |
| `subscribers` | - | Users watching |
| `repo_forks` | - | Fork listing |
| `milestones` | - | Milestone listing |
| `releases` | - | Release listing |
| `pulls` | 1 | Pull requests |
| `comments` | - | Commit comments |
| `activity` | 146 | Repository activity |
| `profile` | 1 | Community profile |
| `sbom` | 1 | Software Bill of Materials |

Issues and PRs:

| Table | Sample count | Notes |
|---|---|---|
| `repo_issue_comments` | 41 | All issue comments |
| `repo_issue_events` | 162 | All issue events |
| `repo_pull_comments` | 58 | All PR review comments |
| `issues_list_comments` | 2 | Comments on specific issue (requires `issue_number`) |
| `issues_list_events` | 21 | Events on specific issue (requires `issue_number`) |
| `timeline` | 66 | Full issue timeline (requires `issue_number`) |
| `files` | 61 | Changed files in PR (requires `pull_number`) |
| `reviews` | 33 | PR reviews (requires `pull_number`) |
| `requested_reviewers` | 1 | Requested reviewers (requires `pull_number`) |
| `pulls_list_review_comments` | 33 | Review comments (requires `pull_number`) |
| `repo_pull_review_comments` | 1 | Comments on specific review (requires `pull_number` + `review_id`) |
| `blocked_by` | - | Sub-issues blocked by (requires `issue_number`) |
| `blocking` | - | Issues this blocks (requires `issue_number`) |
| `sub_issues` | - | Sub-issues (requires `issue_number`) |
| `reactions` | - | Comment reactions (requires `comment_id`) |

CI/CD and Actions:

| Table | Sample count | Notes |
|---|---|---|
| `workflows` | 4 | Workflow definitions |
| `repo_action_runs` | - | All workflow runs |
| `repo_action_workflow_runs` | 149 | Runs for specific workflow (requires `workflow_id`) |
| `repo_action_workflow_timing` | 1 | Workflow timing (requires `workflow_id`) |
| `jobs` | 1 | Jobs in a run (requires `run_id`) |
| `repo_action_run_artifacts` | - | Run artifacts (requires `run_id`) |
| `repo_action_run_timing` | 1 | Run timing (requires `run_id`) |
| `attempts` | 1 | Run attempts (requires `run_id` + `attempt_number`) |
| `approvals` | - | Run approvals (requires `run_id`) |
| `pending_deployments` | - | Pending deployments (requires `run_id`) |
| `repo_action_artifacts` | 80 | All artifacts |
| `repo_action_cache_usage` | 1 | Cache usage |
| `repo_action_permissions` | 1 | Actions permissions |
| `repo_action_secrets` | - | Actions secrets |
| `repo_action_variables` | - | Actions variables |
| `repo_action_oidc_customization_sub` | 1 | OIDC customization |

Checks and status:

| Table | Sample count | Notes |
|---|---|---|
| `check_runs` | 4 | Check runs (requires `ref`) |
| `repo_commit_check_suites` | 10 | Check suites (requires `ref`) |
| `repo_commit_statuses` | - | Commit statuses (requires `ref`) |
| `status` | - | Combined status (requires `ref`) |
| `annotations` | 1 | Check run annotations (requires `check_run_id`) |
| `repo_check_runs` | - | Check run details (requires `check_run_id`) |
| `repo_check_suites` | 1 | Suite details (requires `check_suite_id`) |

Git objects:

| Table | Sample count | Notes |
|---|---|---|
| `matching_refs` | 1 | Git refs (requires `ref`) |
| `ref` | 1 | Specific ref (requires `ref`) |
| `repo_git_commits` | 1 | Git commit object (requires `commit_sha`) |
| `branches_where_head` | 1 | Branches with commit at HEAD (requires `commit_sha`) |
| `trees` | 16 | Git tree contents (requires `tree_sha`) |

Admin (requires owner/push access):

| Table | Sample count | Notes |
|---|---|---|
| `collaborators` | 1 | Repository collaborators |
| `clones` | 14 | Clone traffic |
| `views` | 14 | Page view traffic |
| `paths` | 4 | Popular content paths |
| `referrers` | 2 | Top referral sources |
| `default_setup` | 1 | Code scanning setup |
| `repo_hooks` | - | Webhooks |
| `repo_keys` | - | Deploy keys |
| `repo_invitations` | - | Pending invitations |
| `permission` | 1 | User permission on repo (requires `username`) |
| `repo_codespace_machines` | 2 | Available codespace machines |
| `automated_security_fixes` | 1 | Dependabot auto-fix status |
| `repo_immutable_releases` | 1 | Immutable releases setting |
| `private_vulnerability_reporting` | 1 | Vulnerability reporting status |
| `devcontainers` | - | Dev container configurations |

Statistics:

| Table | Sample count | Notes |
|---|---|---|
| `punch_card` | 168 | Commit frequency by hour/day |
| `code_frequency` | 1 | Weekly additions/deletions |
| `commit_activity` | 1 | Weekly commit counts |
| `participation` | 1 | Owner vs non-owner commits |
| `repo_stat_contributors` | 1 | Contributor statistics |

#### Organization tables (4 tables confirmed)

| Table | Notes |
|---|---|
| `orgs` | Organization details |
| `org_repos` | Organization repositories |
| `members` | Org members (empty with limited PAT) |
| `public_members` | Public org members |

Remaining ~104 org tables require the PAT to be scoped to the organization with admin permissions.

#### Username tables (12 tables confirmed)

| Table | Sample count |
|---|---|
| `activity_list_repos_starred_by_user` | 20 |
| `activity_list_repos_watched_by_user` | 22 |
| `user_event_public` | 1 |
| `users_list_followers_for_user` | 10 |
| `users_list_following_for_user` | 2 |
| `users_list_public_keys_for_user` | 3 |
| `users_list_gpg_keys_for_user` | - |
| `users_list_social_accounts_for_user` | - |
| `users_list_ssh_signing_keys_for_user` | - |
| `orgs_list_for_user` | - |

#### Search tables (6 tables confirmed)

| Table | Example query |
|---|---|
| `search_repositories` | `WHERE q = 'language:rust stars:>100'` |
| `search_users` | `WHERE q = 'username'` |
| `search_topics` | `WHERE q = 'machine-learning'` |
| `search_labels` | `WHERE q = 'bug' AND repository_id = 12345` |
| `code` | `WHERE q = 'className repo:owner/repo'` |
| `apps` | `WHERE app_slug = 'github-actions'` |

Note: `search_commits` requires actual search text.
Qualifier-only queries such as `q = 'repo:owner/repo'` return 422.
`search_issues` requires `is:issue` or `is:pull-request` in the query.

#### Requires GitHub App JWT (19 tables)

These tables serve GitHub App management endpoints and cannot be accessed with personal access tokens:

`app`, `app_hook_config`, `app_hook_deliveries`, `app_installations`,
`installation_requests`, `marketplace_listing_plans`,
`marketplace_listing_stubbed_plans`

#### Enterprise-only (12 tables)

Require a GitHub Enterprise Cloud account:

`configurations`, `defaults`, `enterprise_1_day`,
`enterprise_code_security_configuration_repositories`,
`enterprise_copilot_metric_report_enterprise_28_day_latest`,
`enterprise_team_memberships`, `enterprise_teams`, `latest`,
`repo` (enterprise), `retention_limit`, `storage_limit`, `users_1_day`

#### Large dataset tables

These tables return very large paginated datasets such as all public gists
and all GitHub users, so they are capped with `max_pages` to avoid runaway
queries. Use `LIMIT` or filters to get practical results:

| Table | Default cap | Tip |
|---|---|---|
| `gist_public` | 10 pages (~1000 gists) | Filter by `since` if supported |
| `organizations` | 10 pages (~1000 orgs) | Use `search_repositories` with `q = 'org:name'` instead |
| `users` | 10 pages (~1000 users) | Use `search_users` with `q = 'keyword'` instead |

Tables that return single JSON objects (not arrays) use `max_pages: 1` to prevent infinite pagination loops:

| Table | Why |
|---|---|
| `repo_topics` | Returns `{"names": [...]}` — one object, not a list |
| `repo_compare` | Returns a single comparison object |

## Cascading queries

Many tables require IDs from parent tables. Use this discovery order:

```text
user_repos / org_repos / search_repositories
  → owner, repo
    → commits → commit_sha → repo_git_commits, branches_where_head
    → commits → commit__tree__sha → trees
    → workflows → workflow_id → repo_action_workflow_runs
    → repo_action_runs → run_id → jobs, attempts, repo_action_run_timing
    → pulls → pull_number → files, reviews, requested_reviewers
    → reviews → review_id → repo_pull_review_comments
    → repo_issue_comments → comment_id → reactions
    → check_runs (ref) → check_run_id → annotations, repo_check_runs
    → repo_commit_check_suites (ref) → check_suite_id → repo_check_suites
```

## Quick start

```bash
# Setup
coral source add github
coral server stop && coral server start

# Discover tables
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'github'"

# Find required filters
coral sql \
  "SELECT table_name, column_name FROM coral.columns \
   WHERE schema_name = 'github' AND is_required_filter = true \
   ORDER BY table_name"

# Query examples
coral sql \
  "SELECT name, stargazers_count FROM github.user_repos \
   ORDER BY stargazers_count DESC LIMIT 10"
coral sql \
  "SELECT sha, commit__message FROM github.commits \
   WHERE owner = 'octocat' AND repo = 'Hello-World' LIMIT 5"
coral sql \
  "SELECT title, state, user__login FROM github.pulls \
   WHERE owner = 'myorg' AND repo = 'myrepo'"
coral sql \
  "SELECT full_name, stargazers_count FROM github.search_repositories \
   WHERE q = 'language:rust stars:>1000' LIMIT 10"
```
