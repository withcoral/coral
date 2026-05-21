# Supabase Connector (Community)

**Version:** 0.1.0
**Backend:** HTTP (PostgREST)
**Tables:** 2
**Base URL:** `SUPABASE_REST_URL` (for example `https://<project-ref>.supabase.co/rest/v1`)

Query rows from Supabase Postgres tables exposed through the PostgREST API. Read-only
v1 uses your project REST URL and API key. Join app data with HubSpot, GitHub, or
other Coral sources on `email`, `user_id`, or fields in the `row` JSON column.

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/supabase/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Reference the linked GitHub issue in your PR so maintainers can connect the
contribution to the prior discussion.

## Authentication and setup

### API URL and key

From the Supabase dashboard (**Project Settings → API**):

```bash
export SUPABASE_REST_URL=https://YOUR_PROJECT_REF.supabase.co/rest/v1
export SUPABASE_API_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
coral source add --file sources/community/supabase/manifest.yaml
```

| Key | When to use |
| --- | --- |
| **anon** | Respects Row Level Security; recommended for read-only app tables exposed to clients |
| **service_role** | Bypasses RLS; trusted environments only—never commit or expose publicly |

Tables must be exposed to the API (typically `public` schema with grants and RLS
policies that allow `SELECT` for your key).

### Multiple projects

Register one Coral source per Supabase project (for example `supabase_prod`,
`supabase_staging`), each with its own `SUPABASE_REST_URL` and `SUPABASE_API_KEY`.

## Tables

| Table | Description |
| --- | --- |
| `table_rows` | List rows from any PostgREST table (required `table` filter) |
| `row_by_id` | Single row by `id` (required `table` and `row_id` filters) |

### Filters

**`table_rows`**

- `table` (required) — PostgREST resource name, for example `profiles`, `orders`
- `select_columns` (optional) — PostgREST `select` list, for example `id,email,created_at`

**`row_by_id`**

- `table` (required)
- `row_id` (required) — primary key value (uuid or text)

## Example queries

### List profiles

```sql
SELECT table, id, email, created_at
FROM supabase.table_rows
WHERE table = 'profiles'
LIMIT 50;
```

### Single row

```sql
SELECT table, row_id, id, email, row
FROM supabase.row_by_id
WHERE table = 'profiles'
  AND row_id = '550e8400-e29b-41d4-a716-446655440000'
LIMIT 1;
```

### Narrow columns (PostgREST select)

```sql
SELECT table, id, email
FROM supabase.table_rows
WHERE table = 'profiles'
  AND select_columns = 'id,email,created_at'
LIMIT 20;
```

### Join with HubSpot (requires HubSpot source)

```sql
SELECT p.email, c.firstname, c.lifecyclestage
FROM supabase.table_rows p
JOIN hubspot.contacts c ON LOWER(p.email) = LOWER(c.email)
WHERE p.table = 'profiles'
LIMIT 20;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/supabase/manifest.yaml
export SUPABASE_REST_URL=https://YOUR_PROJECT_REF.supabase.co/rest/v1
export SUPABASE_API_KEY=your-anon-or-service-role-key
coral source add --file sources/community/supabase/manifest.yaml
coral source test supabase
```

Adjust the `table` name in `test_queries` in `manifest.yaml` if your project does
not expose a `profiles` table.

## Limitations

- Read-only v1 (`GET` only); no inserts, updates, or RPC
- Table names and columns depend on your Supabase schema; use `row` JSON for extra fields
- `auth.users` and other non-exposed schemas are out of scope unless exposed via PostgREST
- Service role keys bypass RLS—treat them like production secrets

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/supabase): add supabase community source`.
