# deps.dev Connector

This source queries package metadata, dependency graphs, and security advisories from the [deps.dev API](https://docs.deps.dev/api/v3/).
No credentials are required.

> **URL encoding**: Coral does not automatically URL-encode filter values
> interpolated into API paths. Package names containing special characters —
> such as scoped npm packages (`@types/react` → `%40types%2Freact`) or Maven
> coordinates with colons (`org.apache.logging.log4j:log4j-core` →
> `org.apache.logging.log4j%3Alog4j-core`) — must be percent-encoded in your
> `WHERE` clause filter values.
>
> **Canonicalization**: deps.dev may canonicalize package, version, advisory,
> and project identifiers. The primary identifier columns return the canonical
> values from the API response; `requested_*` columns preserve submitted filters.

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

Fetch the full dependency graph payload when you need to reconstruct topology:

```sql
SELECT nodes, edges, error
FROM deps_dev.dependency_graph
WHERE system = 'NPM'
  AND package_name = 'react'
  AND version = '18.2.0'
LIMIT 1;
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

Fetch the edges of a dependency graph:

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

Find package versions built from a project:

```sql
SELECT system, package_name, version, relation_type
FROM deps_dev.project_package_versions
WHERE project_id = 'github.com%2Ffacebook%2Freact'
LIMIT 5;
```

## Tables

### By required filter

| Filter pattern | Tables | Example |
|---|---|---|
| `system` + `package_name` | `packages` | `WHERE system = 'NPM' AND package_name = 'minimist'` |
| `system` + `package_name` + `version` | `versions`, `dependency_graph`, `dependencies`, `requirements`, `dependency_edges` | `WHERE system = 'NPM' AND package_name = 'minimist' AND version = '0.0.8'` |
| `advisory_id` | `advisories` | `WHERE advisory_id = 'GHSA-vh95-rmgr-6w4m'` |
| `project_id` | `projects`, `project_package_versions` | `WHERE project_id = 'github.com%2Fgolang%2Fgo'` |

### versions

Fetches metadata for one package version. Maps to
`GET /v3/systems/{system}/packages/{package_name}/versions/{version}`.

Useful columns include:

- `requested_system`
- `requested_package_name`
- `requested_version`
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

Includes `requested_system` and `requested_package_name` to preserve the
submitted filters when deps.dev canonicalizes the package key.

The `versions` column is a JSON array where each element contains
`versionKey.system`, `versionKey.name`, `versionKey.version`, `publishedAt`,
`isDefault`, `isDeprecated`, and `deprecatedReason`.

### dependency_graph

Fetches the full resolved dependency graph for a specific package version. Maps to
`GET /v3/systems/{system}/packages/{package_name}/versions/{version}:dependencies`.

Returns the provider's `nodes`, `edges`, and graph-level `error` together so
`edges[].fromNode` and `edges[].toNode` can be interpreted as indexes into the
same `nodes` array. Resolved dependency graphs are currently available only for
`NPM`, `CARGO`, `MAVEN`, and `PYPI`.

### dependencies

Fetches the resolved dependency graph nodes for a specific package version. Maps to
`GET /v3/systems/{system}/packages/{package_name}/versions/{version}:dependencies`.

Includes `is_bundled` to indicate whether a node is bundled into the package version.
Use `dependency_graph` when you need to interpret edge indexes against the full
nodes array. Resolved dependency graphs are currently available only for `NPM`,
`CARGO`, `MAVEN`, and `PYPI`.

### dependency_edges

Fetches the resolved dependency graph edges for a specific package version. Maps
to the same endpoint `GET /v3/systems/{system}/packages/{package_name}/versions/{version}:dependencies`
but extracts the `edges` array. Edge indexes refer to positions in the provider's
nodes array; use `dependency_graph` to retrieve `nodes` and `edges` together for
topology reconstruction. Resolved dependency graphs are currently available only
for `NPM`, `CARGO`, `MAVEN`, and `PYPI`.

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
Includes `requested_advisory_id` when deps.dev canonicalizes the advisory key.

### projects

Fetches project-level health metrics like GitHub stars, forks, issues, and OpenSSF Scorecard data. Maps to
`GET /v3/projects/{project_id}`.

Requires URL-encoding for the `project_id` (e.g. `github.com%2Fgolang%2Fgo`).
Includes `requested_project_id` when deps.dev canonicalizes the project key.

### project_package_versions

Fetches package versions known to be built from a project. Maps to
`GET /v3/projects/{project_id}:packageversions`.

Use this table to start from a repository-like project ID and discover package
versions you can join into `versions`, `dependencies`, `requirements`, or
`dependency_graph`. deps.dev returns at most 1500 package versions, with
attestation-derived mappings served first.

## Supported systems

deps.dev supports the following package systems: `NPM`, `PYPI`, `MAVEN`, `GO`,
`CARGO`, `RUBYGEMS`, and `NUGET`. Use the system names exactly as expected by
deps.dev (uppercase).

Resolved dependency graph tables (`dependency_graph`, `dependencies`, and
`dependency_edges`) have narrower deps.dev coverage and are currently available
only for `NPM`, `CARGO`, `MAVEN`, and `PYPI`.

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
