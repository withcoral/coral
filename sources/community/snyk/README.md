# Snyk

Query Snyk organizations, targets, projects, issues, issue paths, and dependencies via the Snyk REST API.

## Setup

### Get Your API Token
1. Log into Snyk.
2. Go to **Account Settings > General**.
3. Generate a Personal Access Token (or use a Service Account token).

### Add the Source
```bash
coral source add snyk
```
When prompted, provide your token as `SNYK_API_TOKEN`.

## Tables

### `current_user`
Validates the credentials and retrieves the current authenticated user identity.

### `organizations`
Organizations available to the authenticated Snyk user. Use this to find the `org_id` required for other queries.

### `targets`
Targets are the foundational assets in Snyk (e.g., GitHub repos, Docker images, Kubernetes workloads).
**Requires:** `org_id`

### `projects`
Scan instances associated with targets.
**Requires:** `org_id`

### `project_issue_counts`
Analytics for project issue counts broken down by severity (critical, high, medium, low).
**Requires:** `org_id`

### `issues`
Vulnerabilities and license issues in Snyk projects.
**Requires:** `org_id`, `project_id`

### `issue_paths`
Dependency paths showing exactly how an issue was introduced (e.g., express -> lodash -> minimist -> vuln).
**Requires:** `org_id`, `project_id`, `issue_id`

### `dependencies`
Project dependency inventory (SBOM).
**Requires:** `org_id`, `project_id`

## Authentication
The source uses the Snyk REST API with token authentication:
```text
Authorization: Token <SNYK_API_TOKEN>
```
