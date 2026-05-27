# OpenSSF Scorecard Source for Coral

Adds `scorecard.checks` and `scorecard.project` as queryable SQL tables, powered by the [OpenSSF Scorecard API](https://api.securityscorecards.dev). Returns security health checks for public GitHub repositories tracked by OpenSSF Scorecard — no authentication required.

## Install

```bash
coral source add --file sources/community/scorecard/manifest.yaml
```

## What is OpenSSF Scorecard?

The [Open Source Security Foundation (OpenSSF) Scorecard](https://github.com/ossf/scorecard) automatically evaluates open-source projects against security best practices. It checks things like:

- Are PRs reviewed before merging? (`Code-Review`)
- Are GitHub Actions tokens scoped to least-privilege? (`Token-Permissions`)
- Are dependencies pinned by hash? (`Pinned-Dependencies`)
- Is there a security policy? (`Security-Policy`)
- Is SAST (static analysis) running on all commits? (`SAST`)
- Are releases signed? (`Signed-Releases`)

Each check produces a score from **0** (lowest) to **10** (highest). Score **-1** means the check is not applicable (e.g. no releases to sign). Scores are a heuristic signal — checks also carry separate risk levels in the upstream Scorecard project.

> **Note:** The Scorecard API serves precomputed results for repositories already indexed by OpenSSF. Querying an unindexed repository returns zero rows (the underlying API 404 is handled gracefully). See the [Scorecard REST API docs](https://github.com/ossf/scorecard#scorecard-rest-api) for details on coverage.

## Tables

| Table | Required filters | Purpose |
|---|---|---|
| `scorecard.checks` | `owner`, `repo` | Per-check scores (0–10), reasons, actionable details, and doc links |
| `scorecard.project` | `owner`, `repo` | Aggregate score, scored timestamp, commit SHA, tool version |

Both tables return empty rows (not an error) when the repository is not yet indexed by OpenSSF Scorecard.

## Usage

```sql
-- All security checks for a repo, lowest scores first
SELECT check_name, score, reason
FROM scorecard.checks
WHERE owner = 'expressjs'
  AND repo  = 'express'
ORDER BY CASE WHEN score = -1 THEN 999 ELSE score END ASC
```

```sql
-- Low-scoring checks Scorecard flags for review (score < 5, excluding N/A)
SELECT check_name, score, reason
FROM scorecard.checks
WHERE owner = 'django'
  AND repo  = 'django'
  AND score >= 0 AND score < 5
ORDER BY score ASC
```

```sql
-- Score distribution summary
SELECT
  COUNT(*) AS total_checks,
  SUM(CASE WHEN score >= 8 THEN 1 ELSE 0 END) AS high_scoring,
  SUM(CASE WHEN score >= 0 AND score < 5 THEN 1 ELSE 0 END) AS low_scoring
FROM scorecard.checks
WHERE owner = 'expressjs'
  AND repo  = 'express'
```

```sql
-- Include documentation links for remediation
SELECT check_name, score, reason, documentation_url
FROM scorecard.checks
WHERE owner = 'django'
  AND repo  = 'django'
  AND score < 8
ORDER BY score ASC
```

```sql
-- Check data freshness and which commit was scored
SELECT date, aggregate_score, repo_commit, scorecard_version
FROM scorecard.project
WHERE owner = 'expressjs'
  AND repo  = 'express'
```

## Score reference

Scores are a heuristic, opinionated signal from the OpenSSF Scorecard tool. They reflect the tool's weighting of each check, not an absolute measure of security. See the [Scorecard scoring docs](https://github.com/ossf/scorecard/blob/main/README.md#aggregate-score) for methodology.

| Score | Meaning |
|---|---|
| 10 | Highest — check fully satisfied |
| 8–9 | High |
| 5–7 | Mid-range |
| 1–4 | Low — area Scorecard flags for review |
| 0 | Lowest — check not satisfied |
| -1 | N/A — check not applicable for this repo |

## Rate limits

The OpenSSF Scorecard API is free and publicly accessible. No API key is required. Scores are precomputed weekly — data is cached and requests are lightweight.

## DSL features used

| Pattern | Where used |
|---|---|
| `{{filter.owner}}` path template | `owner` filter injected into URL path: `/projects/github.com/{owner}/{repo}` |
| `{{filter.repo}}` path template | `repo` filter injected into URL path |
| `rows_path: [checks]` | `checks` table — navigates to the nested `checks` array |
| `rows_path: []` | `project` table — root object treated as single row |
| Nested path (`documentation.url`) | `documentation_url` column — nested field access |
| Nested path (`repo.commit`, `scorecard.version`) | `project` table metadata columns |
| `from_filter` | `owner` and `repo` echo columns in both tables |
| `pagination: mode: none` | Single API call returns all data (no pagination needed) |

## Limitations

- Only covers public GitHub repositories **already indexed by OpenSSF Scorecard**. Querying an unindexed repository returns zero rows — the underlying API 404 is converted to an empty result by `allow_404_empty: true`.
- Scores are updated **weekly** — they reflect the state of the repo at last scan time.
- `Branch-Protection` may return -1 due to GitHub token limitations in the Scorecard service.
- The set of checks varies by repository — not all 18 checks apply to every project.

## Validation

```
YAML parse:                     passed for sources/community/scorecard/manifest.yaml
Coral manifest schema:          passed (dsl_version: 3, backend: http, 2 tables)
test_queries:                   passed — SELECT check_name, score, reason FROM scorecard.checks WHERE owner = 'expressjs' AND repo = 'express' LIMIT 5
Live API test (checks):         passed — 18 checks, <1s response time
Live API test (project):        passed — 1 row: date, aggregate_score, repo_commit, scorecard_version
Path template ({{filter.*}}):   passed — /projects/github.com/expressjs/express resolved correctly
rows_path: [checks]:            passed — nested array correctly iterated as rows
rows_path: []:                  passed — root object returned as single project row
```

## Why this source is unique

Most security tools focus on **vulnerabilities in dependencies** (CVEs, Dependabot). This source surfaces **security practices in the repo itself**:

- Are PRs reviewed? (`Code-Review`)
- Are CI tokens least-privilege? (`Token-Permissions`)
- Are dependencies reproducible? (`Pinned-Dependencies`)

The `project` table adds a data freshness dimension — users can see exactly when the score was produced and which commit was evaluated, which is important when interpreting precomputed weekly results.
