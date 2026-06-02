# npm (npm)

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `https://registry.npmjs.org`

Query the [public npm registry REST API](https://github.com/npm/registry/blob/master/docs/REGISTRY-API.md)
to search for packages (`npm.search`) and retrieve full metadata for a single
package by name (`npm.packages`) — latest version, repository, publish
timestamps, maintainers, license, README and more.

```bash
coral source add --file sources/community/npm/manifest.yaml
```

## Configuration

This source does not require any authentication or input configuration. The npm registry search API is fully public.

## Tables

| Table          | Description                                            | Key filters                   |
| -------------- | ------------------------------------------------------ | ----------------------------- |
| `npm.search`   | Search npm packages by keyword, ranked by popularity   | `text` (**required**)         |
| `npm.packages` | Full metadata for one package, fetched by name         | `package_name` (**required**) |

The `packages` table returns one row per package via `GET /{package_name}`
(scoped packages like `@types/node` supported): canonical name, `latest_version`
(`dist-tags.latest`), `repository__url`, `time__created`/`time__modified` (real
`Timestamp` columns), maintainers, keywords, dist-tags, license, bugs URL, README
and a `raw` column with the full document. For download counts, use the separate
`npm_downloads` source.

## Example queries

```sql
-- Search for packages matching 'express'
SELECT name, version, description, weekly_downloads
FROM npm.search
WHERE text = 'express'
LIMIT 10;

-- Boost popular React-related packages in results
SELECT name, version, weekly_downloads, score_final
FROM npm.search
WHERE text = 'react'
  AND popularity_weight = 1.0
LIMIT 20;

-- Boost packages with high maintenance scores
SELECT name, description, score_maintenance
FROM npm.search
WHERE text = 'logger'
  AND maintenance_weight = 1.0
LIMIT 10;

-- Inspect author and license information
SELECT name, license, publisher_username, publisher_email
FROM npm.search
WHERE text = 'webpack'
LIMIT 10;

-- Full metadata for a single package by name
SELECT name, latest_version, time__modified, repository__url, license
FROM npm.packages
WHERE package_name = 'react';

-- Scoped package
SELECT name, latest_version
FROM npm.packages
WHERE package_name = '@types/node';
```

## Pagination

The `npm.search` table uses offset-based pagination (supported via `from` and `size` parameters).
Coral handles this automatically — just use `LIMIT` to control how many rows you want.
The default page size is 20, up to a maximum of 250.
Without an explicit `LIMIT`, results are capped at 100 rows (`fetch_limit_default`) to
avoid unbounded scans against the public registry.

## Notes

- **No authentication required.** The registry is completely public.
- **Search criteria.** The `text` filter searches package names, descriptions, and readmes. npm supports a number of [special search qualifiers](https://github.com/npm/registry/blob/main/docs/REGISTRY-API.md#get-v1search) inside the `text` value — the list below is a selection of common ones; refer to the API docs for the full set:
  - `author:<name>` — packages published by a specific author
  - `maintainer:<name>` — packages with a specific maintainer
  - `scope:<scope>` — scoped packages (e.g. `scope:@babel`)
  - `keywords:<kw>` — packages with a specific keyword
  - `not:unstable` — exclude pre-release versions
  - `not:insecure` / `is:insecure` — filter by security status
  - `is:unstable` — include only pre-release versions
  - `boost-exact:false` — disable exact-name boosting in ranking
- **Ranking weights.** `popularity_weight`, `quality_weight`, and `maintenance_weight` are floats between 0.0 and 1.0 that influence how npm ranks search results — they are not restrictive filters.
- **Rate limiting.** The npm registry is a shared public resource. Avoid aggressive polling loops, use `LIMIT` to fetch only what you need, and do not run broad queries without a `LIMIT` clause.
- **Dependents.** The `dependents` column is returned by the API as a string, not an integer.
- **Scores.** Quality, popularity, and maintenance scores are floats between 0 and 1.

## Validation

```bash
coral source lint sources/community/npm/manifest.yaml
coral source add --file sources/community/npm/manifest.yaml
coral source test npm
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'npm'"

coral sql "SELECT name, version, description FROM npm.search WHERE text = 'express' LIMIT 1"
# +---------+---------+-----------------------------------------------+
# | name    | version | description                                   |
# +---------+---------+-----------------------------------------------+
# | express | 5.2.1   | Fast, unopinionated, minimalist web framework |
# +---------+---------+-----------------------------------------------+

coral sql "SELECT name, version, description FROM npm.search WHERE text = 'react' LIMIT 1"
# +-------+---------+-------------------------------------------------------------+
# | name  | version | description                                                 |
# +-------+---------+-------------------------------------------------------------+
# | react | 19.2.6  | React is a JavaScript library for building user interfaces. |
# +-------+---------+-------------------------------------------------------------+
```
