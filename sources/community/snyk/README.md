# Snyk Source Spec

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `https://api.snyk.io/rest`

Query your entire Snyk security posture via SQL — organizations, scanned targets (repositories), individual projects (manifests, containers, IaC), and security issues (vulnerabilities, license violations, code quality). Turn your vulnerability data into a queryable intelligence layer to find unpatched critical CVEs, audit reachable vulnerabilities, and ensure compliance across your engineering organization.

This spec implements the modern Snyk REST API (v3) using the JSON:API standard.

## Authentication

Requires one input:

| Input | Kind | Description |
|---|---|---|
| `SNYK_API_TOKEN` | secret | Your Snyk API token (Personal or Service Account). Generate one in your [Snyk Account Settings](https://app.snyk.io/account). |

Authentication uses standard header auth: `Authorization: token {SNYK_API_TOKEN}`.

## Available Tables

| Table | Required Filter | Description |
|---|---|---|
| `orgs` | None | All Snyk organizations your token has access to. The `id` from this table is required to query the others. |
| `targets` | `org_id` | Scanned resources (e.g., GitHub repositories, container registries, CLI workspaces). |
| `projects` | `org_id` | Individual scanned artifacts (e.g., `package.json`, `Dockerfile`, Terraform files). |
| `issues` | `org_id` | Security vulnerabilities, license issues, and code quality problems. |

## Quick Start

```bash
# Step 1 — add the source spec to your workspace
coral source add --file sources/community/snyk/manifest.yaml --interactive
# You will be prompted for your SNYK_API_TOKEN
```

## Example Queries

### Organization Inventory

```sql
-- List all organizations you have access to
SELECT id, name, slug, created_at
FROM snyk.orgs;
```

> **Note:** For the following queries, replace `'your-org-id'` with the actual `id` obtained from the `orgs` table.

### Security Vulnerability Auditing

```sql
-- Find all Critical and High vulnerabilities
SELECT id, title, issue_type, package_name, package_version, cvss_score
FROM snyk.issues
WHERE org_id = 'your-org-id' 
  AND effective_severity_level IN ('critical', 'high');

-- Find patchable vulnerabilities (quick wins)
SELECT title, package_name, cvss_score, is_patchable, is_upgradable
FROM snyk.issues
WHERE org_id = 'your-org-id'
  AND (is_patchable = true OR is_upgradable = true)
  AND effective_severity_level = 'critical';

-- Identify REACHABLE vulnerabilities in your code
SELECT title, package_name, severity
FROM snyk.issues
WHERE org_id = 'your-org-id'
  AND is_reachable = true;

-- Find ignored vulnerabilities for compliance auditing
SELECT title, severity, package_name
FROM snyk.issues
WHERE org_id = 'your-org-id'
  AND is_ignored = true;
```

### Targets and Projects Analytics

```sql
-- List all GitHub repositories being scanned
SELECT display_name, remote_url, is_private
FROM snyk.targets
WHERE org_id = 'your-org-id'
  AND origin = 'github';

-- Breakdown of projects by ecosystem (npm, maven, docker, etc.)
SELECT project_type, COUNT(*) as project_count
FROM snyk.projects
WHERE org_id = 'your-org-id'
GROUP BY project_type
ORDER BY project_count DESC;

-- Find recently added projects
SELECT name, project_type, origin, created_at
FROM snyk.projects
WHERE org_id = 'your-org-id'
ORDER BY created_at DESC
LIMIT 10;
```

### Cross-Referencing

```sql
-- Match projects to their parent repositories (targets)
SELECT p.name AS project_name, p.project_type, t.display_name AS repo_name, t.remote_url
FROM snyk.projects p
JOIN snyk.targets t ON p.target_reference = t.id
WHERE p.org_id = 'your-org-id' AND t.org_id = 'your-org-id';
```

## Pagination

All list endpoints use cursor-based pagination per the JSON:API specification. The API returns a `links.next` field which is automatically followed by Coral. Default page size is 50; maximum is 100 (configurable via `limit`).

## API Versioning

This source spec hardcodes the Snyk REST API version query parameter to `?version=2024-10-15` to ensure stable and consistent JSON:API attribute extraction.
