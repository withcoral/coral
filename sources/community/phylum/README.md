# Phylum source

This source queries [Phylum](https://www.phylum.io/) software supply-chain
intelligence through Coral SQL. It is read-only and exposes groups, project
summaries, package versions, package risk details, and package author metadata.

## Authentication

Set `PHYLUM_API_KEY` to a Phylum API token:

```bash
export PHYLUM_API_KEY="ph0_..."
coral source add --file sources/community/phylum/manifest.yaml
```

You can generate a token in the Phylum web UI, or run `phylum auth token` if
you use the Phylum CLI. Coral sends the token as
`Authorization: Bearer <token>`.

The source defaults to `https://api.phylum.io/api/v0`. Override
`PHYLUM_API_BASE` only if Phylum documents a different API host for your
environment.

## Provider docs

- [Phylum OpenAPI document](https://api.phylum.io/api/v0/openapi.json)
- [Phylum API token docs](https://docs.phylum.io/knowledge_base/api-keys)

## Tables

| Table | Required filters | Description |
|---|---|---|
| `groups` | none | Groups visible to the authenticated user |
| `projects` | none | Project summaries, including repository URLs when configured |
| `package_versions` | `package_registry`, `package_name` | Known versions and total risk scores for one package |
| `packages` | `package_registry`, `package_name`, `package_version` | Package details, risk scores, issues, dependencies, and metadata |
| `package_authors` | `package_registry`, `package_name`, `package_version` | Maintainers and contributors for one package version |

Supported `package_registry` values are `npm`, `pypi`, `maven`, `rubygems`,
`nuget`, `cargo`, and `golang`.

## Example queries

List visible projects:

```sql
SELECT id, name, group_name, repository_url
FROM phylum.projects
LIMIT 20;
```

Find risky versions for a package:

```sql
SELECT version, total_risk_score, published_date
FROM phylum.package_versions
WHERE package_registry = 'npm'
  AND package_name = 'lodash'
ORDER BY published_date DESC
LIMIT 20;
```

Inspect one package version:

```sql
SELECT
  name,
  version,
  risk_total,
  risk_vulnerability,
  risk_malicious_code,
  risk_author,
  risk_engineering,
  risk_license,
  issues
FROM phylum.packages
WHERE package_registry = 'npm'
  AND package_name = 'lodash'
  AND package_version = '4.17.21'
LIMIT 1;
```

Get maintainers and contributors:

```sql
SELECT maintainers, contributors
FROM phylum.package_authors
WHERE package_registry = 'npm'
  AND package_name = 'lodash'
  AND package_version = '4.17.21'
LIMIT 1;
```

Query a scoped npm package by URL-encoding the scope separator in
`package_name`. For example, pass `%40babel%2Fcore` for `@babel/core`:

```sql
SELECT version, total_risk_score, published_date
FROM phylum.package_versions
WHERE package_registry = 'npm'
  AND package_name = '%40babel%2Fcore'
LIMIT 20;
```

## Notes

- Scoped npm package names are interpolated into the Phylum package URL path.
  Use URL-encoded values such as `%40scope%2Fname` rather than `@scope/name`.
- `issues`, `dependencies`, `versions`, `maintainers`, and `contributors` are
  JSON columns because the upstream payloads are nested collections.
- The Phylum threat feed uses a separate host (`https://threats.phylum.io`).
  It is not included in this v1 source so this spec can keep one API base URL.

## Out of scope

- Creating or updating Phylum projects
- Package submission or analysis job creation
- Policy or firewall mutation
- Webhook registration
- Phylum threat-feed ingestion
