# Supabase Source Spec

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 14
**Base URL:** `https://api.supabase.com/v1`

Query projects, organizations, edge functions, database backups, secrets, network restrictions, auth configs, postgrest configs, pgbouncer configs, custom hostnames, network bans, and ssl enforcement from Supabase — the open source Firebase alternative.

## Authentication

Requires a `SUPABASE_ACCESS_TOKEN`. Generate a Personal Access Token from:
**Supabase Dashboard → Account → Access Tokens → Generate New Token**

## Available Tables

| Table | Required Filter | Description |
|---|---|---|
| `organizations` | None | Lists all organizations the user belongs to. |
| `projects` | None | Lists all projects the user belongs to. Returns `ref` needed for other tables. |
| `edge_functions` | `project_ref` | Lists all Edge Functions for a project. |
| `database_backups` | `project_ref` | Lists all database backups (physical/logical). |
| `secrets` | `project_ref` | Lists all secrets and environment variables. |
| `network_restrictions` | `project_ref` | Lists IP allow-lists and CIDR blocks. |
| `api_keys` | `project_ref` | Lists anon and service_role API keys. |
| `branches` | `project_ref` | Lists all database branches. |
| `auth_configs` | `project_ref` | Extremely detailed Auth & SSO configuration (Google/GitHub/Email/MFA). |
| `postgrest_configs` | `project_ref` | PostgREST configuration (max rows, db schema, anon roles). |
| `pgbouncer_configs` | `project_ref` | PgBouncer configuration (pool modes, connection sizes). |
| `custom_hostnames` | `project_ref` | Custom hostname status and configuration. |
| `network_bans` | `project_ref` | Lists currently banned IPv4 addresses. |
| `ssl_enforcement` | `project_ref` | Current SSL enforcement status. |

## Quick start

```bash
# Step 1 — add the source spec to your workspace
coral source add --file sources/community/supabase/manifest.yaml --interactive
# You will be prompted to paste your SUPABASE_ACCESS_TOKEN
```

### Example Queries

```sql
-- Step 2 — List all your projects and get their `ref` strings
SELECT id, ref, name, region, status 
FROM supabase.projects;

-- Step 3 — Audit SSO and Auth Settings
SELECT external_google_enabled, external_github_enabled, mailer_secure_email_change_enabled 
FROM supabase.auth_configs 
WHERE project_ref = 'abcdefghijklm';

-- Step 4 — Check database backup status
SELECT id, is_physical_backup, status, inserted_at 
FROM supabase.database_backups 
WHERE project_ref = 'abcdefghijklm';

-- Step 5 — Audit Network Security (Bans and Restrictions)
SELECT entitled, status, db_allowed_cidrs 
FROM supabase.network_restrictions 
WHERE project_ref = 'abcdefghijklm';

SELECT banned_ipv4_addresses 
FROM supabase.network_bans 
WHERE project_ref = 'abcdefghijklm';

-- Step 6 — Audit PostgREST settings
SELECT max_rows, db_schema, db_anon_role 
FROM supabase.postgrest_configs 
WHERE project_ref = 'abcdefghijklm';
```
