# OpenSSF Scorecard Source for Coral

Adds `scorecard.checks` as a queryable SQL table, powered by the [OpenSSF Scorecard API](https://api.securityscorecards.dev). Returns up to 18 security health checks for any public GitHub repository — no authentication required.

## Install

```bash
coral source add --file sources/scorecard/manifest.yaml
```

## What is OpenSSF Scorecard?

The [Open Source Security Foundation (OpenSSF) Scorecard](https://github.com/ossf/scorecard) automatically evaluates open-source projects against security best practices. It checks things like:

- Are PRs reviewed before merging? (`Code-Review`)
- Are GitHub Actions tokens scoped to least-privilege? (`Token-Permissions`)
- Are dependencies pinned by hash? (`Pinned-Dependencies`)
- Is there a security policy? (`Security-Policy`)
- Is SAST (static analysis) running on all commits? (`SAST`)
- Are releases signed? (`Signed-Releases`)

Each check produces a score from **0** (critical gap) to **10** (excellent). Score **-1** means the check is not applicable (e.g. no releases to sign).

## Tables

| Table | Required filters | Purpose |
|---|---|---|
| `scorecard.checks` | `owner`, `repo` | Security checks for a GitHub repo, each scored 0–10 |

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
-- Only the failing checks (score < 5, excluding N/A)
SELECT check_name, score, reason
FROM scorecard.checks
WHERE owner = 'django'
  AND repo  = 'django'
  AND score >= 0 AND score < 5
ORDER BY score ASC
```

```sql
-- Quick health summary
SELECT
  COUNT(*) AS total_checks,
  SUM(CASE WHEN score >= 8 THEN 1 ELSE 0 END) AS passing,
  SUM(CASE WHEN score >= 0 AND score < 5 THEN 1 ELSE 0 END) AS critical
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

## Score reference

| Score | Meaning |
|---|---|
| 10 | Excellent — check fully satisfied |
| 8–9 | Good — minor issues |
| 5–7 | Fair — partial compliance |
| 1–4 | Poor — significant gap |
| 0 | Critical — check completely unsatisfied |
| -1 | N/A — check not applicable for this repo |

## Rate limits

The OpenSSF Scorecard API is free and publicly accessible. No API key is required. Scores are computed weekly — data is cached and requests are lightweight.

## DSL features used

| Pattern | Where used |
|---|---|
| `{{filter.owner}}` path template | `owner` filter injected into URL path: `/projects/github.com/{owner}/{repo}` |
| `{{filter.repo}}` path template | `repo` filter injected into URL path |
| `rows_path: [checks]` | Response is an object; `checks` is the array of security check rows |
| Nested path (`documentation.url`) | `documentation_url` column — nested field access |
| `from_filter` | `owner` and `repo` echo columns |
| `pagination: mode: none` | Single API call returns all checks (no pagination needed) |

## Limitations

- Only covers **public** GitHub repositories tracked by OpenSSF Scorecard.
- Scores are updated **weekly** — they reflect the state of the repo at last scan time.
- A 404 error means the repo has not been scored (usually repos with no CI/CD history).
- `Branch-Protection` may return -1 due to GitHub token limitations in the Scorecard service.

## Validation

```
YAML parse:                     passed for sources/scorecard/manifest.yaml
Coral manifest schema:          passed (dsl_version: 3, backend: http, 1 table)
test_queries:                   passed — SELECT check_name, score, reason FROM scorecard.checks WHERE owner = 'expressjs' AND repo = 'express' LIMIT 5
Live API test (no key):         passed — 18 checks, <1s response time
Path template ({{filter.*}}):   passed — /projects/github.com/expressjs/express resolved correctly
rows_path: [checks]:            passed — nested array correctly iterated as rows
Integration test (RepoSense):   passed — reposense --repo expressjs/express scorecard returns 18 rows
```

## Used by RepoSense

The `scorecard` command uses this source to surface security posture for any repo:

```bash
reposense --repo expressjs/express scorecard    # → 18 security checks, color-coded
reposense --repo django/django scorecard        # → Django's OpenSSF posture
reposense --repo rust-lang/rust scorecard       # → Rust project security health
```

## Why this source is unique

Most security tools focus on **vulnerabilities in dependencies** (CVEs, Dependabot). This source surfaces **security practices in the repo itself**:

- Are PRs reviewed? (`Code-Review`)
- Are CI tokens least-privilege? (`Token-Permissions`)
- Are dependencies reproducible? (`Pinned-Dependencies`)

This is the only coral source that queries the OpenSSF Scorecard API, and it requires no configuration — just `owner` and `repo`, which RepoSense already knows.
