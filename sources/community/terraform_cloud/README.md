# Terraform Cloud Coral Source

## What This Source Exposes

This source exposes [HCP Terraform](https://developer.hashicorp.com/terraform/cloud-docs)
or Terraform Enterprise metadata as SQL tables in Coral. It is designed for
infrastructure governance: projects, workspaces, runs, variable metadata, and
state version summaries.

It is read-only. The `variables` table intentionally omits variable values, and
the `state_versions` table exposes metadata and resource summaries instead of
raw state file contents.

## Authentication

Create a user or team token with read access to the organization and pass it as
`TERRAFORM_CLOUD_TOKEN`.

Required inputs:

| Input | Kind | Default | Description |
|---|---|---|---|
| `TERRAFORM_CLOUD_API_BASE` | variable | `https://app.terraform.io/api/v2` | HCP Terraform or Terraform Enterprise API base URL. |
| `TERRAFORM_CLOUD_ORGANIZATION` | variable | - | Terraform organization name. |
| `TERRAFORM_CLOUD_TOKEN` | secret | - | User or team token with read access. |

## Setup

For HCP Terraform:

```bash
TERRAFORM_CLOUD_ORGANIZATION=my-org \
TERRAFORM_CLOUD_TOKEN=your_token_here \
coral source add --file sources/community/terraform_cloud/manifest.yaml
```

For Terraform Enterprise:

```bash
TERRAFORM_CLOUD_API_BASE=https://terraform.example.com/api/v2 \
TERRAFORM_CLOUD_ORGANIZATION=my-org \
TERRAFORM_CLOUD_TOKEN=your_token_here \
coral source add --file sources/community/terraform_cloud/manifest.yaml
```

Or run interactively:

```bash
coral source add --interactive --file sources/community/terraform_cloud/manifest.yaml
```

## Tables

| Table | Purpose |
|---|---|
| `terraform_cloud.projects` | Projects in the configured organization. |
| `terraform_cloud.workspaces` | Workspaces in the configured organization. |
| `terraform_cloud.runs` | Runs for a specific workspace. Requires `workspace_id`. |
| `terraform_cloud.variables` | Workspace variable metadata. Requires `workspace_id`; values are not exposed. |
| `terraform_cloud.state_versions` | State version metadata for a workspace name. Requires `workspace_name`. |

## Example Queries

Workspace inventory:

```sql
SELECT id, name, project_id, execution_mode, terraform_version, locked
FROM terraform_cloud.workspaces
ORDER BY name;
```

Recent failed or errored runs for a workspace:

```sql
SELECT workspace_id, id, status, created_at, message
FROM terraform_cloud.runs
WHERE workspace_id = 'ws-abc123'
  AND status = 'errored'
ORDER BY created_at DESC
LIMIT 20;
```

Locked workspaces:

```sql
SELECT name, project_id, execution_mode, terraform_version
FROM terraform_cloud.workspaces
WHERE locked = true
ORDER BY name;
```

Audit sensitive variable coverage:

```sql
SELECT category, sensitive, COUNT(*) AS variable_count
FROM terraform_cloud.variables
WHERE workspace_id = 'ws-abc123'
GROUP BY category, sensitive
ORDER BY category, sensitive;
```

State versions with unprocessed resource metadata:

```sql
SELECT workspace_name, id, status, serial, resources_processed, created_at
FROM terraform_cloud.state_versions
WHERE workspace_name = 'production-network'
  AND resources_processed = false;
```

## Limitations

- HCP Terraform uses JSON:API response shapes. Most list tables read from the
  top-level `data` array.
- `terraform_cloud.runs` and `terraform_cloud.variables` require
  `workspace_id`.
- `terraform_cloud.state_versions` requires `workspace_name` because the HCP
  Terraform list state versions endpoint filters by organization name and
  workspace name.
- Variable values are deliberately excluded from this source spec.
- Raw state files are deliberately excluded from this source spec.

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/terraform_cloud/manifest.yaml
Coral manifest schema validation: passed for sources/community/terraform_cloud/manifest.yaml
git diff --check: passed
make lint-sources: passed
Live API tests: not run; require real Terraform Cloud credentials
```

Testing note: live API tests require user-provided credentials and are intentionally not part of default CI. The manifest includes `test_queries`, and no credentials or customer data are committed.
