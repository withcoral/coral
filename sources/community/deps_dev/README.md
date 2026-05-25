# deps.dev Connector

This source queries package metadata, dependency graphs, and security advisories from the [deps.dev API](https://docs.deps.dev/api/v3/).
No credentials are required.

> **URL encoding**: Coral does not automatically URL-encode filter values
> interpolated into API paths. Package names containing special characters —
> such as scoped npm packages (`@types/react` → `%40types%2Freact`) or Maven
> coordinates with colons (`org.apache.logging.log4j:log4j-core` →
> `org.apache.logging.log4j%3Alog4j-core`) — must be percent-encoded in your
> `WHERE` clause filter values.

## Start querying

Inspect metadata for a package version:

```sql
SELECT version, published_at, licenses, advisory_keys, related_projects
FROM deps_dev.versions
WHERE system = 'NPM'
  AND package_name = 'minimist'
  AND version = '0.0.8'
LIMIT 1;
```

Fetch the dependency graph nodes for a package version:

```sql
SELECT dependency_system, dependency_name, dependency_version, relation
FROM deps_dev.dependencies
WHERE system = 'NPM'
  AND package_name = 'minimist'
  AND version = '0.0.8';
```

Fetch the declared dependency requirements payload:

```sql
SELECT npm, pypi, maven, cargo
FROM deps_dev.requirements
WHERE system = 'NPM'
  AND package_name = 'minimist'
  AND version = '0.0.8'
LIMIT 1;
```

Fetch full advisory details for an OSV ID or GHSA:

```sql
SELECT title, cvss3_score, aliases
FROM deps_dev.advisories
WHERE advisory_id = 'GHSA-vh95-rmgr-6w4m'
LIMIT 1;
```

Query a Go module version:

```sql
SELECT version, published_at, licenses
FROM deps_dev.versions
WHERE system = 'GO'
  AND package_name = 'golang.org%2Fx%2Ftext'
  AND version = 'v0.3.7'
LIMIT 1;
```

Fetch the edges of a dependency graph to reconstruct topology:

```sql
SELECT from_node, to_node, requirement
FROM deps_dev.dependency_edges
WHERE system = 'NPM'
  AND package_name = 'react'
  AND version = '18.2.0';
```

Fetch project health metrics and OpenSSF Scorecards:

```sql
SELECT open_issues_count, stars_count, license
FROM deps_dev.projects
WHERE project_id = 'github.com%2Fgolang%2Fgo'
LIMIT 1;
```

## Tables

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| `system` + `package_name` | 1 | `WHERE system = 'NPM' AND package_name = 'minimist'` |
| `system` + `package_name` + `version` | 4 | `WHERE system = 'NPM' AND package_name = 'minimist' AND version = '0.0.8'` |
| `advisory_id` | 1 | `WHERE advisory_id = 'GHSA-vh95-rmgr-6w4m'` |
| `project_id` | 1 | `WHERE project_id = 'github.com%2Fgolang%2Fgo'` |

### versions

Fetches metadata for one package version. Maps to
`GET /v3/systems/{system}/packages/{package_name}/versions/{version}`.

Useful columns include:
- `licenses`
- `advisory_keys`
- `links`
- `slsa_provenances`
- `attestations`
- `related_projects`
- `registries`

### packages

Fetches project-level details and a list of all available versions. Maps to
`GET /v3/systems/{system}/packages/{package_name}`.

The `versions` column is a JSON array where each element contains
`versionKey.system`, `versionKey.name`, `versionKey.version`, `publishedAt`,
`isDefault`, `isDeprecated`, and `deprecatedReason`.

### dependencies

Fetches the resolved dependency graph nodes for a specific package version. Maps to
`GET /v3/systems/{system}/packages/{package_name}/versions/{version}:dependencies`.

Includes `is_bundled` to indicate whether a node is bundled into the package version.

### dependency_edges

Fetches the resolved dependency graph edges for a specific package version. Used
alongside `dependencies` to fully reconstruct dependency topology. Maps to the same
endpoint `GET /v3/systems/{system}/packages/{package_name}/versions/{version}:dependencies`
but extracts the `edges` array.

### requirements

Fetches declared dependency requirements for a package version. Maps to
`GET /v3/systems/{system}/packages/{package_name}/versions/{version}:requirements`.

The requirements are split into ecosystem-specific columns: `npm`, `maven`,
`pypi`, `cargo`, `go`, `rubygems`, and `nuget`. Only the column matching the
queried system will contain data; the others will be null.

### advisories

Fetches detailed vulnerability/advisory metadata given an advisory key (e.g., OSV/GHSA ID). Maps to
`GET /v3/advisories/{advisory_id}`.

Includes `advisory_url` for quick navigation to the advisory detail page.

### projects

Fetches project-level health metrics like GitHub stars, forks, issues, and OpenSSF Scorecard data. Maps to
`GET /v3/projects/{project_id}`.

Requires URL-encoding for the `project_id` (e.g. `github.com%2Fgolang%2Fgo`).

## Supported systems

deps.dev supports the following package systems: `NPM`, `PYPI`, `MAVEN`, `GO`,
`CARGO`, `RUBYGEMS`, and `NUGET`. Use the system names exactly as expected by
deps.dev (uppercase).

### Package naming notes

- Maven uses `group:artifact` coordinates (for example `org.apache.logging.log4j:log4j-core`).
- PyPI names are normalized per PEP 503 (lowercase, normalize `-`, `_`, `.` to `-`).
- NuGet names are lowercased per NuGet API normalization rules.
- Scoped npm packages (e.g. `@types/react`) require URL-encoding: `%40types%2Freact`.

## Rate limiting

The deps.dev API is public and requires no authentication. The API documentation
does not specify rate limits, but as a public Google service, excessive request
volume may be throttled. Use `LIMIT` clauses and specific filters to keep
request counts reasonable.
