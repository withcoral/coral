# Airtable

Query Airtable base schema and records through Coral SQL. This community source
is generic: it discovers table IDs from Airtable's Metadata API and queries
records from any table in the configured base.

## Setup

Create an Airtable personal access token at
<https://airtable.com/create/tokens>. Grant it access to the base you want to
query with these read scopes:

| Scope | Used by |
| --- | --- |
| `schema.bases:read` | `airtable.tables` schema discovery |
| `data.records:read` | `airtable.records` and `airtable.search_records` |

Find your Airtable base ID in the base URL or in Airtable's API documentation
for the base. Base IDs start with `app`.

```bash
export AIRTABLE_ACCESS_TOKEN="<your-airtable-token>"
export AIRTABLE_BASE_ID="<your-base-id>"
coral source add --file sources/community/airtable/manifest.yaml
```

`AIRTABLE_ACCESS_TOKEN` should be a personal access token or OAuth access token
sent as `Authorization: Bearer <token>`. If you want to create a test base or
seed validation records through the Airtable API, use a temporary token with
`schema.bases:write` and `data.records:write`, then rotate it after setup.

## Tables

| Table | Description |
| --- | --- |
| `tables` | Airtable table schema metadata from the configured base. |
| `records` | Records from any Airtable table in the configured base. |

Start with schema discovery:

```sql
SELECT id, name, fields
FROM airtable.tables
LIMIT 20;
```

Airtable recommends table IDs because table names can change. Use the `id`
from `airtable.tables` as `table_id_or_name` when querying records.

## Records

`airtable.records` requires `table_id_or_name` and supports optional
provider-side filters:

| Filter | Description |
| --- | --- |
| `table_id_or_name` | Required Airtable table ID, or URL-safe table name. |
| `view` | Airtable saved view name or view ID. |
| `filter_by_formula` | Airtable formula passed as `filterByFormula`. |
| `sort_field` | Airtable field name or field ID for provider-side sorting. |
| `sort_direction` | Airtable sort direction, `asc` or `desc`. |

Use provider-side sort parameters when order matters. Airtable does not
guarantee record order unless a view or sort parameter is supplied.

```sql
SELECT id, name, status, fields
FROM airtable.records
WHERE table_id_or_name = 'tblXXXXXXXXXXXXXX'
  AND sort_field = 'Name'
  AND sort_direction = 'asc'
LIMIT 20;
```

Filter with Airtable formulas:

```sql
SELECT id, name, status, fields
FROM airtable.records
WHERE table_id_or_name = 'tblXXXXXXXXXXXXXX'
  AND filter_by_formula = '{Status} = ''Active'''
LIMIT 20;
```

Use the `fields` JSON column for base-specific fields:

```sql
SELECT
  id,
  name,
  json_get_str(fields, 'Manager') AS manager,
  json_get_str(fields, 'Laptop Serial') AS laptop_serial
FROM airtable.records
WHERE table_id_or_name = 'tblXXXXXXXXXXXXXX'
LIMIT 20;
```

## Search function

`airtable.search_records` is a provider-native search surface over
`filterByFormula`:

```sql
SELECT id, name, status, fields
FROM airtable.search_records(
  table_id_or_name => 'tblXXXXXXXXXXXXXX',
  formula => 'AND({Status} = ''Active'', {Department} = ''IT'')',
  sort_field => 'Name',
  sort_direction => 'asc'
)
LIMIT 10;
```

## Notes

- Airtable list-record requests use a path of `{baseId}/{tableIdOrName}`.
  Prefer stable table IDs from `airtable.tables`. If you use a table name in
  `table_id_or_name`, provide a URL-safe value.
- Airtable returns list records in pages of up to 100 records. Coral follows
  the returned `offset` cursor until the SQL limit is satisfied or Airtable has
  no more records.
- Airtable omits empty field values from record payloads. Missing convenience
  columns such as `name` or `status` therefore appear as `NULL`.
- Airtable applies a rate limit of 5 requests per second per base.
- `filterByFormula` must be a valid Airtable formula for the target table. If a
  `view` is also provided, Airtable returns only records in that view that also
  satisfy the formula.

## Validation

```bash
coral source lint sources/community/airtable/manifest.yaml
coral source add --file sources/community/airtable/manifest.yaml
coral source test airtable
```

The default declared test reads one row from `airtable.tables`, so the token
must include `schema.bases:read`.

### Captured live validation

The following output was captured against a live Airtable base with 12 tables
and 125 seeded employee records.

#### Add source

```bash
coral source add --file sources/community/airtable/manifest.yaml
```

```text
Added source airtable (secrets: keychain)

  ✓ airtable connected successfully
  Secrets: keychain

    airtable (2 tables)
    ├─ records
    └─ tables
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, name FROM airtable.tables LIMIT 1
      1 row
```

#### Source test

```bash
coral source test airtable
```

```text
  ✓ airtable connected successfully
  Secrets: keychain

    airtable (2 tables)
    ├─ records
    └─ tables
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, name FROM airtable.tables LIMIT 1
      1 row
```

#### Representative queries

Discover table IDs:

```sql
SELECT id, name
FROM airtable.tables
ORDER BY name
LIMIT 5;
```

```text
+-------------------+-----------------+
| id                | name            |
+-------------------+-----------------+
| tblDIq8uqk3SnoSXl | Table 1         |
| tblccIfAq4MhnsdQE | assets          |
| tblwWFBzsTJmgozeX | clients         |
| tblH8zutTJLkpLlln | departments     |
| tblfkdLkwI48oTYlL | employees       |
+-------------------+-----------------+
```

Query records with provider-side sorting:

```sql
SELECT name, status
FROM airtable.records
WHERE table_id_or_name = 'tblfkdLkwI48oTYlL'
  AND sort_field = 'Name'
  AND sort_direction = 'asc'
LIMIT 5;
```

```text
+---------------+-------------+
| name          | status      |
+---------------+-------------+
| employees 001 | Active      |
| employees 002 | Active      |
| employees 003 | In Progress |
| employees 004 | Active      |
| employees 005 | Active      |
+---------------+-------------+
```

Search with `filterByFormula`:

```sql
SELECT name, status
FROM airtable.search_records(
  table_id_or_name => 'tblfkdLkwI48oTYlL',
  formula => '{Status} = ''Active''',
  sort_field => 'Name',
  sort_direction => 'asc'
)
LIMIT 5;
```

```text
+---------------+--------+
| name          | status |
+---------------+--------+
| employees 001 | Active |
| employees 002 | Active |
| employees 004 | Active |
| employees 005 | Active |
| employees 007 | Active |
+---------------+--------+
```

Verify cursor pagination across more than one Airtable page:

```sql
SELECT COUNT(*) AS employee_rows
FROM (
  SELECT id
  FROM airtable.records
  WHERE table_id_or_name = 'tblfkdLkwI48oTYlL'
  LIMIT 125
);
```

```text
+---------------+
| employee_rows |
+---------------+
| 125           |
+---------------+
```

The 125-row query verified that Coral followed Airtable's first-page `offset`
cursor after the initial 100-record page.
