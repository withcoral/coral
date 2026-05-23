# Supabase PostgREST Connector (Community)

**Version:** 0.1.1
**Backend:** HTTP (PostgREST data API)
**Tables:** 3
**Base URL:** `SUPABASE_REST_URL` (for example `https://<project-ref>.supabase.co/rest/v1`)

Query rows from a Supabase project through the **PostgREST data API**. This source
does **not** wrap the Supabase Management API (`api.supabase.com`); see open PR
[#444](https://github.com/withcoral/coral/pull/444) for platform metadata coverage.

Read-only v1 uses legacy anon/service_role JWT keys and **fixed table paths** (no
dynamic `/{{table}}` segments) so requests cannot escape `/rest/v1` into other
API surfaces.

## Install

```bash
coral source add --file sources/community/supabase/manifest.yaml
```

## Authentication and setup

From **Project Settings → API** in the Supabase dashboard:

```bash
export SUPABASE_REST_URL=https://YOUR_PROJECT_REF.supabase.co/rest/v1
export SUPABASE_API_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
coral source add --file sources/community/supabase/manifest.yaml
```

| Key | When to use |
| --- | --- |
| **anon (JWT)** | Respects Row Level Security; recommended |
| **service_role (JWT)** | Bypasses RLS; trusted environments only |

v1 expects legacy JWT-style API keys sent as `apikey` and `Authorization: Bearer`.
New Supabase publishable/secret keys are not supported in this manifest.

`coral source test` uses `rest_openapi` only, so it works even when you do not
have a `profiles` table.

## Tables

| Table | Description |
| --- | --- |
| `rest_openapi` | `GET /` OpenAPI document (connectivity check) |
| `profiles` | Example list table at `/profiles` (when exposed) |
| `profile_by_id` | Example lookup at `/profiles` with `profile_id` filter |

Add more tables by copying the `profiles` / `profile_by_id` pattern with a fixed
path for each PostgREST resource you need.

## Example queries

### Connectivity check

```sql
SELECT openapi FROM supabase.rest_openapi LIMIT 1;
```

### List profiles (when the table exists)

```sql
SELECT id, email, created_at
FROM supabase.profiles
LIMIT 50;
```

### Profile by id

```sql
SELECT profile_id, id, email, row
FROM supabase.profile_by_id
WHERE profile_id = '550e8400-e29b-41d4-a716-446655440000'
LIMIT 1;
```

### Join with HubSpot (requires HubSpot source)

```sql
SELECT p.email, c.firstname, c.lifecyclestage
FROM supabase.profiles p
JOIN hubspot.contacts c ON LOWER(p.email) = LOWER(c.email)
LIMIT 20;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/supabase/manifest.yaml
export SUPABASE_REST_URL=https://YOUR_PROJECT_REF.supabase.co/rest/v1
export SUPABASE_API_KEY=your-anon-or-service-role-jwt
coral source add --file sources/community/supabase/manifest.yaml
coral source test supabase
```

## Limitations

- PostgREST data API only; not organizations/projects/storage config (Management API).
- Fixed paths only; no dynamic table name in URLs.
- Example `profiles` tables require that resource in your project schema.
- Read-only v1 (`GET` only).

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md). Coordinate with [#444](https://github.com/withcoral/coral/pull/444) if adding overlapping Supabase coverage.
