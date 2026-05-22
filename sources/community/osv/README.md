# OSV Connector

This source queries the [OSV vulnerability database](https://osv.dev) over its
public HTTP API. No credentials are required.

## Start querying

Look up vulnerabilities for a package version:

```sql
SELECT id, summary, aliases, severity
FROM osv.query_by_version
WHERE package_name = 'jinja2'
  AND ecosystem = 'PyPI'
  AND version = '3.1.4'
LIMIT 20;
```

Fetch a single vulnerability document by identifier:

```sql
SELECT id, summary, published, modified, affected, references
FROM osv.vulns
WHERE id = 'GHSA-jfh8-c2jp-5v3q';
```

Find vulnerabilities introduced at a commit:

```sql
SELECT id, summary, affected
FROM osv.query_by_commit
WHERE commit = '6879efc2c1596d11a6a6ad296f80063b558d5e0f'
LIMIT 10;
```

## Tables

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| `package_name` + `ecosystem` + `version` | 1 | `WHERE package_name = 'lodash' AND ecosystem = 'npm' AND version = '4.17.15'` |
| `commit` | 1 | `WHERE commit = '<sha>'` |
| `id` | 1 | `WHERE id = 'CVE-2021-44228'` |

### query_by_version

Matches OSV vulnerability records for a given package name, ecosystem, and
version. Maps to `POST /v1/query` and paginates via `nextPageToken`. Optional
filters: none (purl is mutually exclusive with package_name and ecosystem and
is not exposed in this table).

### query_by_commit

Matches OSV vulnerability records for a given git commit hash. Maps to
`POST /v1/query`. Optionally narrow results with `package_name` and
`ecosystem`. `purl` is mutually exclusive with those optional fields.

### vulns

Fetches a single OSV vulnerability document by identifier. Accepts CVE, GHSA,
or OSV-format identifiers. Maps to `GET /v1/vulns/{id}`.

## Out of scope

`POST /v1/querybatch` and `POST /v1experimental/determineversion` require
constructing variable-length arrays of objects in the request body. The current
manifest DSL uses fixed-path body field mapping and cannot build those shapes
from SQL filter values.
