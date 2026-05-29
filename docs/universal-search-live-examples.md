# Universal Search Live Examples

Generated: 2026-06-04T11:16:41Z

Binary: `/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral`

Version: `coral 0.4.1+fe6ffe50`

Configured sources:

| Source | Version | Origin | Secret storage |
| --- | --- | --- | --- |
| crag | 1.0.0 | imported | none |
| datadog | 2.1.0 | bundled | file (plaintext) |
| github | 1.1.6 | bundled | file (plaintext) |
| linear | 2.2.0 | bundled | file (plaintext) |
| notion | 0.1.0 | bundled | file (plaintext) |

## Example 1: Universal Search Degradation

USER:

```text
universal search results recently degraded heavily, find potential culprits
```

Agent intent:

Find likely recent changes, tickets, and search surfaces related to Universal
Search behavior, especially catalog metadata, observed values, provider status,
and GitHub/Linear records that could explain degraded results.

Generated keyword probes:

| Probe | Why this probe |
| --- | --- |
| `universal search degraded` | Preserve the user's feature name and failure symptom. |
| `search observed values` | Look for observed-value search surfaces and recently observed result data. |
| `catalog metadata search` | Look for catalog metadata search surfaces. |
| `search provider status` | Look for provider state/status surfaces that could explain empty or degraded output. |

### Search Test: `universal search degraded`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'universal search degraded'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | results_found | Catalog metadata returned 106 candidate search hints |
| observed_values | results_found | Observed values returned 31 candidate search hints |

Truncation: truncated, returned 50 of 137.

Observed-value hit positions in returned results: none in the returned top 50
results. The provider reported 31 observed-value candidates before final
ranking/truncation.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_metadata | column_hint | notion | search | raw | table_column | Full raw Notion search result |
| 2 | catalog_metadata | column_hint | notion | search | query | table_column | Optional title search query |
| 3 | catalog_metadata | catalog_item | notion | notion.search | notion.search | table | Pages and data sources shared with the integration |
| 4 | catalog_metadata | column_hint | notion | search_objects | raw | table_function_result_column | Full raw Notion search result |
| 5 | catalog_metadata | column_hint | notion | search_data_source_templates | raw | table_function_result_column | Full raw data source templates search result |
| 6 | catalog_metadata | column_hint | github | search_code | q | table_function_argument | Table function argument |
| 7 | catalog_metadata | column_hint | github | search_users | q | table_function_argument | Table function argument |
| 8 | catalog_metadata | column_hint | github | search_topics | q | table_function_argument | Table function argument |
| 9 | catalog_metadata | column_hint | github | search_issues | q | table_function_argument | Table function argument |
| 10 | catalog_metadata | column_hint | github | search_labels | q | table_function_argument | Table function argument |

### Search Test: `search observed values`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'search observed values'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | results_found | Catalog metadata returned 93 candidate search hints |
| observed_values | results_found | Observed values returned 31 candidate search hints |

Truncation: truncated, returned 50 of 124.

Observed-value hit positions in returned results: none in the returned top 50
results. The provider reported 31 observed-value candidates before final
ranking/truncation.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_metadata | column_hint | notion | search | raw | table_column | Full raw Notion search result |
| 2 | catalog_metadata | column_hint | notion | search | query | table_column | Optional title search query |
| 3 | catalog_metadata | catalog_item | notion | notion.search | notion.search | table | Pages and data sources shared with the integration |
| 4 | catalog_metadata | column_hint | notion | search_objects | raw | table_function_result_column | Full raw Notion search result |
| 5 | catalog_metadata | column_hint | github | org_property_values | repository_query | table_column | Finds repositories in the organization with a query containing search keywords and qualifiers |
| 6 | catalog_metadata | column_hint | github | org_property_values | properties | table_column | List of custom property names and associated values |
| 7 | catalog_metadata | column_hint | github | issue_field_values | repo | table_filter | Required table filter |
| 8 | catalog_metadata | column_hint | github | issue_field_values | owner | table_filter | Required table filter |
| 9 | catalog_metadata | column_hint | notion | search_data_source_templates | raw | table_function_result_column | Full raw data source templates search result |
| 10 | catalog_metadata | column_hint | github | repo_property_values | repo | table_filter | Required table filter |

### Search Test: `search provider status`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'search provider status'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | results_found | Catalog metadata returned 157 candidate search hints |
| observed_values | results_found | Observed values returned 31 candidate search hints |

Truncation: truncated, returned 50 of 188.

Observed-value hit positions in returned results: 29, 33, 36, 38, 41, 47, 49.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_metadata | column_hint | notion | search | raw | table_column | Full raw Notion search result |
| 2 | catalog_metadata | column_hint | notion | search | query | table_column | Optional title search query |
| 3 | catalog_metadata | catalog_item | notion | notion.search | notion.search | table | Pages and data sources shared with the integration |
| 4 | catalog_metadata | catalog_item | github | github.status | github.status | table | Get the combined status for a specific reference |
| 5 | catalog_metadata | column_hint | datadog | search_monitors | status | table_function_result_column | Monitor status |
| 6 | catalog_metadata | column_hint | notion | search_objects | raw | table_function_result_column | Full raw Notion search result |
| 7 | catalog_metadata | column_hint | github | repo_deployment_statuses | status_id | table_column |  |
| 8 | catalog_metadata | column_hint | notion | search_data_source_templates | raw | table_function_result_column | Full raw data source templates search result |
| 9 | catalog_metadata | catalog_item | notion | notion.search_objects | notion.search_objects | table_function | Provider-ranked search results for pages and data sources shared with the integration |
| 10 | catalog_metadata | native_search_path | notion | notion.search_objects | notion.search_objects | native search path | Provider-ranked search results for pages and data sources shared with the integration |

### Live Follow-Up: GitHub PRs

Command:

```sql
SELECT title, html_url, state, number, user_login, repository_url, score
FROM github.search_issues(q => 'repo:withcoral/coral universal search is:pull-request')
LIMIT 10;
```

Top 10 returned rows:

| Rank | PR | State | Title | URL | User | Score |
| ---: | ---: | --- | --- | --- | --- | ---: |
| 1 | 1017 | open | feat(search): add universal search contracts | https://github.com/withcoral/coral/pull/1017 | mystic123 | 1.0 |
| 2 | 1020 | open | feat(mcp): expose universal search tool | https://github.com/withcoral/coral/pull/1020 | mystic123 | 1.0 |
| 3 | 1180 | open | feat(search): fan out search providers | https://github.com/withcoral/coral/pull/1180 | mystic123 | 1.0 |
| 4 | 1021 | open | feat(cli): add coral search | https://github.com/withcoral/coral/pull/1021 | mystic123 | 1.0 |
| 5 | 1019 | open | feat(search): index observed values | https://github.com/withcoral/coral/pull/1019 | mystic123 | 1.0 |
| 6 | 1018 | open | feat(search): add tantivy catalog backend | https://github.com/withcoral/coral/pull/1018 | mystic123 | 1.0 |
| 7 | 1014 | closed | feat(search): add universal search | https://github.com/withcoral/coral/pull/1014 | mystic123 | 1.0 |
| 8 | 792 | closed | feat(search): add metadata universal search | https://github.com/withcoral/coral/pull/792 | mystic123 | 1.0 |
| 9 | 793 | closed | feat(mcp): expose universal search | https://github.com/withcoral/coral/pull/793 | mystic123 | 1.0 |
| 10 | 869 | closed | feat(search): use sqlite catalog retrieval | https://github.com/withcoral/coral/pull/869 | mystic123 | 1.0 |

### Live Follow-Up: Linear Issues

Command:

```sql
SELECT identifier, title, url, state_name, project_name, updated_at
FROM linear.issues
WHERE title ILIKE '%Universal Search%'
   OR description ILIKE '%Universal Search%'
ORDER BY updated_at DESC
LIMIT 10;
```

Returned rows: 9.

| Rank | Issue | State | Title | URL | Project | Updated |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | BENCH-457 | Triage | Universal Search v0: add SearchService and CatalogMetadataProvider | https://linear.app/withcoral/issue/BENCH-457/universal-search-v0-add-searchservice-and-catalogmetadataprovider | Coral Intelligence: Unknown Data Contents | 2026-05-28T09:03:31.478Z |
| 2 | BENCH-458 | Triage | Expose Universal Search through MCP search(query) | https://linear.app/withcoral/issue/BENCH-458/expose-universal-search-through-mcp-searchquery | Coral Intelligence: Unknown Data Contents | 2026-05-28T09:03:31.452Z |
| 3 | BENCH-460 | Triage | Add Universal Search evaluation hooks and benchmark reporting | https://linear.app/withcoral/issue/BENCH-460/add-universal-search-evaluation-hooks-and-benchmark-reporting | Coral Intelligence: Unknown Data Contents | 2026-05-28T09:03:31.295Z |
| 4 | BENCH-461 | Triage | Add SQLite-backed catalog and observed-value retrieval to Universal Search | https://linear.app/withcoral/issue/BENCH-461/add-sqlite-backed-catalog-and-observed-value-retrieval-to-universal | Coral Intelligence: Unknown Data Contents | 2026-05-27T07:36:29.267Z |
| 5 | BENCH-462 | Triage | v-next: add result ranking and explanations to Universal Search | https://linear.app/withcoral/issue/BENCH-462/v-next-add-result-ranking-and-explanations-to-universal-search | Coral Intelligence: Unknown Data Contents | 2026-05-25T12:09:22.998Z |
| 6 | BENCH-459 | Triage | Add CLI parity for coral search | https://linear.app/withcoral/issue/BENCH-459/add-cli-parity-for-coral-search | Coral Intelligence: Unknown Data Contents | 2026-05-25T12:09:11.878Z |
| 7 | BENCH-435 | Done | Sketch out universal search implementation | https://linear.app/withcoral/issue/BENCH-435/sketch-out-universal-search-implementation | Coral Intelligence: Unknown Data Contents | 2026-05-25T10:56:56.846Z |
| 8 | BENCH-436 | Todo | Design behavioural benchmarks with stored Universal Search context | https://linear.app/withcoral/issue/BENCH-436/design-behavioural-benchmarks-with-stored-universal-search-context | Coral Intelligence: Unknown Data Contents | 2026-05-21T10:48:18.782Z |
| 9 | BENCH-434 | Done | Fielding RFCs | https://linear.app/withcoral/issue/BENCH-434/fielding-rfcs | Coral Intelligence: Unknown Data Contents | 2026-05-21T10:48:14.418Z |

## Example 2: Anton Cache Feature Merge Status

USER:

```text
is the feature about caching Anton has been working on already merged to main
```

Agent intent:

Resolve the vague human reference into repository search terms, identify the
likely pull request, then inspect its live PR state, merge flag, target branch,
author, and update time.

Generated keyword probes:

| Probe | Why this probe |
| --- | --- |
| `Anton caching` | Preserve the user/person clue and feature noun. |
| `cache merged main` | Convert the question into merge-status terms. |
| `GitHub pull request cache` | Route to GitHub PR and cache-related surfaces. |
| `linear caching` | Check whether Linear has related cache/caching tasks. |

### Search Test: `Anton caching`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'Anton caching'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | empty | Catalog metadata returned no search hints |
| observed_values | results_found | Observed values returned 35 candidate search hints |

Truncation: not truncated, returned 35 of max 50.

Observed-value hit positions in returned results: 1-35.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | observed_values | observed_value | github | pulls | antonmry | table | Observed `user` value |
| 2 | observed_values | observed_value | github | pulls | anton/feature2 | table | Observed `head` value |
| 3 | observed_values | observed_value | github | pulls | anton/feature2 | table | Observed `head` value |
| 4 | observed_values | observed_value | github | pulls | anton/feature2 | table | Observed `head__label` value |
| 5 | observed_values | observed_value | github | pulls | withcoral:anton/feature2 | table | Observed `head` value |
| 6 | observed_values | observed_value | github | pulls | https://github.com/antonmry | table | Observed `user` value |
| 7 | observed_values | observed_value | github | pulls | https://api.github.com/users/antonmry | table | Observed `user` value |
| 8 | observed_values | observed_value | github | pulls | https://api.github.com/users/antonmry/repos | table | Observed `user` value |
| 9 | observed_values | observed_value | github | pulls | https://api.github.com/users/antonmry/orgs | table | Observed `user` value |
| 10 | observed_values | observed_value | github | pulls | https://api.github.com/users/antonmry/followers | table | Observed `user` value |

### Search Test: `cache merged main`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'cache merged main'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | results_found | Catalog metadata returned 141 candidate search hints |
| observed_values | results_found | Observed values returned 31 candidate search hints |

Truncation: truncated, returned 50 of 172.

Observed-value hit positions in returned results: 12, 19, 24, 29, 32, 35, 36,
42.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_metadata | column_hint | github | caches | sort | table_column | The property to sort cache results by |
| 2 | catalog_metadata | column_hint | github | caches | id | table_column |  |
| 3 | catalog_metadata | catalog_item | github | github.caches | github.caches | table | List GitHub Actions caches for a repository |
| 4 | catalog_metadata | column_hint | github | repo_action_cache_usage | active_caches_count | table_column | Number of active caches in the repository |
| 5 | catalog_metadata | column_hint | github | org_action_cache_usage | total_active_caches_count | table_column | Count of active caches across all repositories |
| 6 | catalog_metadata | column_hint | github | repo_action_cache_usage | active_caches_size_in_bytes | table_column | Sum of size in bytes of active cache items |
| 7 | catalog_metadata | column_hint | github | org_action_cache_usage | total_active_caches_size_in_bytes | table_column | Total size in bytes of active cache items |
| 8 | catalog_metadata | catalog_item | github | github.org_action_cache_usage | github.org_action_cache_usage | table | Get GitHub Actions cache usage for an organization |
| 9 | catalog_metadata | catalog_item | github | github.repo_action_cache_usage | github.repo_action_cache_usage | table | Get GitHub Actions cache usage for a repository |
| 10 | catalog_metadata | column_hint | github | pulls | merged | table_column | Pull request merged flag |

### Search Test: `GitHub pull request cache`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'GitHub pull request cache'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | results_found | Catalog metadata returned 129 candidate search hints |
| observed_values | results_found | Observed values returned 14 candidate search hints |

Truncation: truncated, returned 50 of 143.

Observed-value hit positions in returned results: none in the returned top 50
results. The provider reported 14 observed-value candidates before final
ranking/truncation.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_metadata | column_hint | github | pulls | base__repo__has_pull_requests | table_column | Whether pull requests are enabled |
| 2 | catalog_metadata | column_hint | github | pulls | head__repo__has_pull_requests | table_column | Whether pull requests are enabled |
| 3 | catalog_metadata | column_hint | github | repo_pull_comments | pull_request_url | table_column | URL for the pull request that the review comment belongs to |
| 4 | catalog_metadata | catalog_item | github | github.caches | github.caches | table | List GitHub Actions caches for a repository |
| 5 | catalog_metadata | column_hint | github | repo_pull_comments | pull_request_review_id | table_column | Pull request review ID |
| 6 | catalog_metadata | column_hint | github | required_pull_request_reviews | bypass_pull_request_allowances | table_column | Users, teams, or apps allowed to bypass pull request requirements |
| 7 | catalog_metadata | column_hint | github | required_pull_request_reviews | bypass_pull_request_allowances__apps | table_column | Apps allowed to bypass pull request requirements |
| 8 | catalog_metadata | column_hint | github | repo_pull_review_comments | pull_request_url | table_column |  |
| 9 | catalog_metadata | column_hint | github | repo_pull_review_comments | _links__pull_request | table_column | Hypermedia link |
| 10 | catalog_metadata | column_hint | github | repo_action_cache_usage | active_caches_count | table_column | Number of active caches in the repository |

### Search Test: `linear caching`

Command:

```bash
/Users/pakapica/src/withcoral/.cargo-target/coral/debug/coral search --json 'linear caching'
```

Status:

| Provider | State | Note |
| --- | --- | --- |
| catalog_metadata | results_found | Catalog metadata returned 42 candidate search hints |
| observed_values | results_found | Observed values returned 21 candidate search hints |

Truncation: truncated, returned 50 of 63.

Observed-value hit positions in returned results: 41, 42, 43, 44, 45, 46, 47,
48, 49, 50.

Top 10 returned results:

| Rank | Provider | Type | Schema | Surface | Name/value | Kind/role | Detail |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_metadata | column_hint | linear | teams | id | table_column | Team ID |
| 2 | catalog_metadata | column_hint | linear | users | id | table_column | User ID |
| 3 | catalog_metadata | column_hint | linear | cycles | id | table_column | Cycle ID |
| 4 | catalog_metadata | column_hint | linear | issues | id | table_column | Issue ID |
| 5 | catalog_metadata | column_hint | linear | teams | key | table_column | Team key |
| 6 | catalog_metadata | column_hint | linear | issues | url | table_column | Issue URL |
| 7 | catalog_metadata | column_hint | linear | users | name | table_column | User full name |
| 8 | catalog_metadata | column_hint | linear | cycles | name | table_column | Cycle name |
| 9 | catalog_metadata | column_hint | linear | projects | id | table_column | Project ID |
| 10 | catalog_metadata | column_hint | linear | projects | name | table_column | Project name |

### Live Follow-Up: GitHub PR Search

Command:

```sql
SELECT title, html_url, state, number, user_login, repository_url, score
FROM github.search_issues(q => 'repo:withcoral/coral cache caching is:pull-request')
LIMIT 10;
```

Top 10 returned rows:

| Rank | PR | State | Title | URL | User | Score |
| ---: | ---: | --- | --- | --- | --- | ---: |
| 1 | 288 | open | feat(http): add TTL-based in-memory response cache for HTTP tables | https://github.com/withcoral/coral/pull/288 | antonmry | 1.0 |
| 2 | 672 | closed | feat: Query Result Caching with Source/Query Fingerprints | https://github.com/withcoral/coral/pull/672 | Aditya8369 | 1.0 |
| 3 | 778 | open | feat(sources/community/pypi): add PyPI package registry source | https://github.com/withcoral/coral/pull/778 | git-sakshii | 1.0 |
| 4 | 567 | closed | ci: update zizmor action and fix workflow audit findings | https://github.com/withcoral/coral/pull/567 | simonwhitaker | 1.0 |
| 5 | 936 | closed | feat(http): add in-flight request deduplication for identical fetches | https://github.com/withcoral/coral/pull/936 | kris70lesgo | 1.0 |
| 6 | 1105 | closed | fix(mcp): reduce catalog discovery memory | https://github.com/withcoral/coral/pull/1105 | sauldhernandez | 1.0 |
| 7 | 945 | closed | perf(engine): reuse default HTTP source client | https://github.com/withcoral/coral/pull/945 | Bradley-Butcher | 1.0 |
| 8 | 928 | open | feat(sources/community/rootly): add rootly community source | https://github.com/withcoral/coral/pull/928 | nancysangani | 1.0 |
| 9 | 163 | closed | docs: rework README | https://github.com/withcoral/coral/pull/163 | AlbertQM | 1.0 |
| 10 | 828 | closed | feat(sources/npm): add npm package stats source | https://github.com/withcoral/coral/pull/828 | himanshu748 | 1.0 |

### Live Follow-Up: Anton-Specific GitHub PR Search

Command:

```sql
SELECT title, html_url, state, number, user_login, repository_url, score
FROM github.search_issues(q => 'repo:withcoral/coral cache caching is:pull-request author:antonmry')
LIMIT 10;
```

Returned rows: 1.

| Rank | PR | State | Title | URL | User | Score |
| ---: | ---: | --- | --- | --- | --- | ---: |
| 1 | 288 | open | feat(http): add TTL-based in-memory response cache for HTTP tables | https://github.com/withcoral/coral/pull/288 | antonmry | 1.0 |

### Live Follow-Up: PR 288 Detail

Command:

```sql
SELECT pull_number, state, merged, merged_at, title, html_url, user__login,
       base__ref, head__ref, mergeable_state, updated_at
FROM github.pulls
WHERE owner = 'withcoral'
  AND repo = 'coral'
  AND pull_number = 288
LIMIT 1;
```

Returned rows: 1.

| PR | State | Merged | Merged at | Title | URL | User | Base | Head | Mergeable state | Updated |
| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 288 | open | false |  | feat(http): add TTL-based in-memory response cache for HTTP tables | https://github.com/withcoral/coral/pull/288 | antonmry | main | anton/feature2 | blocked | 2026-06-04T08:48:52Z |

### Live Follow-Up: Linear Cache/Caching Rows

Command:

```sql
SELECT identifier, title, url, state_name, project_name, updated_at
FROM linear.issues
WHERE title ILIKE '%cache%'
   OR description ILIKE '%cache%'
   OR title ILIKE '%caching%'
   OR description ILIKE '%caching%'
ORDER BY updated_at DESC
LIMIT 10;
```

Top 10 returned rows:

| Rank | Issue | State | Title | URL | Project | Updated |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | BENCH-481 | Backlog | Position recipes as an alternative to source-declared prepared statements | https://linear.app/withcoral/issue/BENCH-481/position-recipes-as-an-alternative-to-source-declared-prepared | Coral Intelligence: Automatic Optimisation | 2026-06-03T15:37:30.822Z |
| 2 | BENCH-477 | Done | Surface dropped agent telemetry in MLflow traces (init config / reasoning / stop_reason / prompt) | https://linear.app/withcoral/issue/BENCH-477/surface-dropped-agent-telemetry-in-mlflow-traces-init-config-reasoning | General Improvements (Benchmarking) | 2026-06-03T12:38:50.082Z |
| 3 | BENCH-474 | Done | test.yaml CI still pins coral to the stale `pawel` feature branch (mirror drift from BENCH-469) | https://linear.app/withcoral/issue/BENCH-474/testyaml-ci-still-pins-coral-to-the-stale-pawel-feature-branch-mirror | General Improvements (Benchmarking) | 2026-05-29T11:52:00.460Z |
| 4 | BENCH-472 | Done | Nightly should always test the latest coral main (auto-track, not a frozen pin) | https://linear.app/withcoral/issue/BENCH-472/nightly-should-always-test-the-latest-coral-main-auto-track-not-a | General Improvements (Benchmarking) | 2026-05-29T11:41:28.329Z |
| 5 | BENCH-335 | Done | Polish bench coral-build -> bench deploy-branch chaining: stale tag lookup + repo-root artifacts blocking preflight | https://linear.app/withcoral/issue/BENCH-335/polish-bench-coral-build-bench-deploy-branch-chaining-stale-tag-lookup | General Improvements (Benchmarking) | 2026-05-21T10:52:41.334Z |
| 6 | UI-487 | Backlog | Codex plugin loads without Coral MCP tools | https://linear.app/withcoral/issue/UI-487/codex-plugin-loads-without-coral-mcp-tools | Coral Feedback | 2026-05-11T08:10:27.042Z |
| 7 | UI-488 | Backlog | MCP plugin ignores Coral brand icon | https://linear.app/withcoral/issue/UI-488/mcp-plugin-ignores-coral-brand-icon | Coral Feedback | 2026-05-09T09:14:36.853Z |
| 8 | BENCH-212 | Done | Justify Vakra dataset for iteration in the next several months | https://linear.app/withcoral/issue/BENCH-212/justify-vakra-dataset-for-iteration-in-the-next-several-months | Value Proposition & Validation (Benchmarking) | 2026-05-08T13:01:15.040Z |
| 9 | BENCH-283 | Done | Auto-isolate CORAL_CONFIG_DIR per bench run invocation | https://linear.app/withcoral/issue/BENCH-283/auto-isolate-coral-config-dir-per-bench-run-invocation | Local Dev Workflow (Benchmarking) | 2026-05-04T09:27:06.713Z |
| 10 | BENCH-294 | Done | CRAG MCP bridge fails to launch inside safehouse sandbox locally (`bench run --suite crag` reports `MCP server(s) failed to connect: crag`) | https://linear.app/withcoral/issue/BENCH-294/crag-mcp-bridge-fails-to-launch-inside-safehouse-sandbox-locally-bench | Local Dev Workflow (Benchmarking) | 2026-05-04T09:18:38.073Z |
