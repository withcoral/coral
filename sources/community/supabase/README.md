# Supabase

Query Supabase **Management API** platform metadata — organizations, projects,
edge functions, secrets inventory, storage buckets, service health, database
backups, branches, and PostgREST configuration — through SQL.

> **Scope:** This source covers the
> [Supabase Management API](https://supabase.com/docs/reference/api/introduction)
> (`api.supabase.com`), which exposes platform and infrastructure metadata. It
> does **not** query your project's Postgres database (the PostgREST data-plane
> at `<ref>.supabase.co`). To query application data stored in your Supabase
> Postgres tables, connect directly with a Postgres-compatible source or client.

## Setup

### 1. Create a personal access token

Generate a personal access token (PAT) at
<https://supabase.com/dashboard/account/tokens>.

PATs carry the **same privileges as your user account**. Treat them like
passwords: do not commit them to version control and rotate them periodically.

For least-privilege access, Supabase supports fine-grained permissions on PATs.
The minimum permissions required by this source are:

| Permission scope                                                | Access level | Used by tables                                      |
|-----------------------------------------------------------------|--------------|-----------------------------------------------------|
| `organizations_read`                                            | Read         | `organizations`                                     |
| `members_read`                                                  | Read         | `organization_members`                              |
| `projects_read`                                                 | Read         | `projects`                                          |
| `edge_functions_read`                                           | Read         | `edge_functions`                                    |
| `edge_functions_secrets_read`                                   | Read         | `secrets`                                           |
| `storage_read`                                                  | Read         | `storage_buckets`                                   |
| `project_admin_read`                                            | Read         | `service_health`                                    |
| `backups_read`                                                  | Read         | `backups`                                           |
| `branching_production_read` or `branching_development_read`     | Read         | `branches`                                          |
| `data_api_config_read`                                          | Read         | `postgrest_config`                                  |

For third-party integrations, Supabase also supports OAuth2 with scoped tokens.
See [Build a Supabase Integration](https://supabase.com/docs/guides/integrations/build-a-supabase-integration)
for details.

### 2. Add the source

```bash
coral source add --file sources/community/supabase/manifest.yaml
```

When prompted, provide your PAT as `SUPABASE_ACCESS_TOKEN`.

## Inputs

| Name                    | Kind   | Required | Description                                       |
|-------------------------|--------|----------|---------------------------------------------------|
| `SUPABASE_ACCESS_TOKEN` | secret | yes      | Personal access token from the Supabase dashboard |

## Tables

| Table                  | Endpoint                                      | Filter Required                  | Notes                                              |
|------------------------|-----------------------------------------------|----------------------------------|----------------------------------------------------|
| `organizations`        | `GET /v1/organizations`                       | none                             | All orgs for the authenticated user                |
| `projects`             | `GET /v1/projects`                            | none                             | All projects across all orgs                       |
| `organization_members` | `GET /v1/organizations/{slug}/members`        | `slug` (required)                | Members and roles within an org                    |
| `edge_functions`       | `GET /v1/projects/{ref}/functions`            | `project_ref` (required)         | Deployed Edge Functions                            |
| `secrets`              | `GET /v1/projects/{ref}/secrets`              | `project_ref` (required)         | Secret names and timestamps only (no values)       |
| `storage_buckets`      | `GET /v1/projects/{ref}/storage/buckets`      | `project_ref` (required)         | Public and private storage buckets                 |
| `service_health`       | `GET /v1/projects/{ref}/health`               | `project_ref`, `service` (req.)  | Single-service health and version info             |
| `backups`              | `GET /v1/projects/{ref}/database/backups`     | `project_ref` (required)         | Logical and physical backup snapshots              |
| `branches`             | `GET /v1/projects/{ref}/branches`             | `project_ref` (required)         | Database branches (requires branching plan)        |
| `postgrest_config`     | `GET /v1/projects/{ref}/postgrest`            | `project_ref` (required)         | Data API (PostgREST) settings for the project      |

## Quick start

After adding the source, run the discovery tables first to verify your token
and find the identifiers needed for project-scoped queries:

```sql
-- 1. Verify credentials and list organizations
SELECT id, name, slug FROM supabase.organizations;

-- 2. List all projects with status and region
SELECT id, ref, name, region, status, created_at
FROM supabase.projects;
```

Then use `ref` and `slug` values to drill into project-scoped tables:

```sql
-- Members of an organization
SELECT user_name, email, role_name, mfa_enabled
FROM supabase.organization_members
WHERE slug = 'my-org-slug';

-- Edge functions for a project
SELECT id, name, slug, status, version, verify_jwt
FROM supabase.edge_functions
WHERE project_ref = 'abcdefghijklmnopqrst';

-- Secret inventory (Coral omits the upstream value field)
SELECT name, updated_at
FROM supabase.secrets
WHERE project_ref = 'abcdefghijklmnopqrst';

-- Storage buckets and their visibility
SELECT id, name, public, owner, created_at
FROM supabase.storage_buckets
WHERE project_ref = 'abcdefghijklmnopqrst';

-- Service health
SELECT name, healthy, status, info__version, error
FROM supabase.service_health
WHERE project_ref = 'abcdefghijklmnopqrst'
  AND service = 'auth';

-- Database backups
SELECT id, status, is_physical_backup, inserted_at
FROM supabase.backups
WHERE project_ref = 'abcdefghijklmnopqrst';

-- Database branches (requires branching to be enabled)
SELECT name, git_branch, is_default, persistent, status, created_at
FROM supabase.branches
WHERE project_ref = 'abcdefghijklmnopqrst';

-- PostgREST configuration (Data API settings, not application data)
SELECT db_schema, max_rows, db_pool
FROM supabase.postgrest_config
WHERE project_ref = 'abcdefghijklmnopqrst';
```

## Discovery flow

Most tables require a project ref or organization slug as a filter. Start with
the top-level tables to discover those identifiers:

```
organizations
  → slug
    → organization_members (slug)

projects
  → ref (project_ref)
    → edge_functions     (project_ref)
    → secrets            (project_ref)
    → storage_buckets    (project_ref)
    → service_health     (project_ref + service)
    → backups            (project_ref)
    → branches           (project_ref)
    → postgrest_config   (project_ref)
```

## Security notes

- **Secrets table is inventory-only in SQL.** Supabase's upstream Management
  API response for project secrets includes secret values. This Coral source
  intentionally omits the `value` field from SQL results and exposes only
  secret names and timestamps for inventory/audit workflows.
- **PostgREST config omits upstream secret material.** Supabase's upstream
  PostgREST config response includes secret-bearing fields such as
  `jwt_secret`. This Coral source intentionally omits `jwt_secret` and exposes
  only non-secret Data API settings such as schemas, row limits, and pool size.
- **Token scope.** PATs inherit your full account privileges. Use fine-grained
  permissions to restrict the token to the minimum scopes listed above.

## Rate limits

The Supabase Management API enforces 120 requests per minute per user per scope
(project or organization). Rate limits are tracked independently per scope, so
requests to different projects do not interfere with each other. Every response
includes `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers.

## Limitations

- **Read-only**: no create, update, or delete operations
- **Management API only**: this source queries platform metadata at
  `api.supabase.com`, not your project's Postgres database. To query
  application data stored in Supabase Postgres tables, use a
  Postgres-compatible source or client pointed at `<ref>.supabase.co`
- **No pagination**: most Management API list endpoints return all items in a
  single response; accounts with very large project/org counts may hit
  API-side limits
- **Branching**: the `branches` table requires a paid plan with branching
  enabled on the project
- **Service health**: requires specifying one service to check via the
  `service` filter (e.g. `auth`, `realtime`, `storage`, or `rest`). Query
  multiple services with separate SQL statements.
- **Inactive projects**: for paused/inactive projects, querying
  `service_health` returns an API 400 and `postgrest_config` returns an
  API 404; Coral surfaces these upstream errors rather than crashing
