# npm_downloads

Query the public **npm downloads API** (`api.npmjs.org`) for package download
counts over the trailing week and month. No authentication required.

> Package metadata (versions, repository, maintainers…) lives on a different
> host (`registry.npmjs.org`) and is provided by the separate `npm` source spec.

## Setup

```bash
coral source add --file sources/community/npm_downloads/manifest.yaml
```

No authentication required.

## Tables

### `downloads_last_month`
Total downloads in the trailing 30 days. One row per package; filter on
`package_name` (required).

### `downloads_last_week`
Total downloads in the trailing 7 days. One row per package; filter on
`package_name` (required). Useful for short-term trend detection.

Both tables expose the same columns:

| Column | Type | Notes |
|---|---|---|
| `package_name` | Utf8 | Virtual — echoes the `WHERE` filter |
| `package` | Utf8 | Canonical package name from the API |
| `downloads` | Int64 | Total downloads in the period |
| `start` | Utf8 | First day of the window (YYYY-MM-DD) |
| `end` | Utf8 | Last day of the window (YYYY-MM-DD) |

> **One package per query.** Both tables map to npm's *single-package* point
> endpoint and return one row. npm also exposes a comma-separated *bulk* point
> endpoint whose response is a differently-shaped object keyed by package name —
> that shape is **not** modeled here, so a comma-separated value like
> `package_name = 'npm,express'` returns a single row with `null`
> `package`/`downloads`/`start`/`end`. Query one package at a time.

## Example queries

```sql
-- Monthly popularity
SELECT downloads FROM npm_downloads.downloads_last_month
WHERE package_name = 'lodash';

-- Short-term trend: compare a week (×4) against the month
SELECT m.downloads AS month, w.downloads AS week
FROM npm_downloads.downloads_last_month m
JOIN npm_downloads.downloads_last_week  w ON 1 = 1
WHERE m.package_name = 'express' AND w.package_name = 'express';
```

## Configuration

| Input | Default | Description |
|---|---|---|
| `NPM_DOWNLOADS_BASE` | `https://api.npmjs.org` | Base URL for the npm download-counts API |

## Validation

Captured live against the public API with Coral 0.2.0:

```text
$ coral source add --file sources/community/npm_downloads/manifest.yaml
Added source npm_downloads

  ✓ npm_downloads connected successfully

    npm_downloads (2 tables)
    ├─ downloads_last_month
    └─ downloads_last_week
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT downloads FROM npm_downloads.downloads_last_month WHERE package_name = 'lodash' LIMIT 1
      1 row
    ✓ SELECT downloads FROM npm_downloads.downloads_last_week WHERE package_name = 'lodash' LIMIT 1
      1 row
```

```text
$ coral sql "SELECT package_name, package, downloads, start, \"end\"
             FROM npm_downloads.downloads_last_month WHERE package_name = 'express'"
+--------------+---------+-----------+------------+------------+
| package_name | package | downloads | start      | end        |
+--------------+---------+-----------+------------+------------+
| express      | express | 425540092 | 2026-05-02 | 2026-05-31 |
+--------------+---------+-----------+------------+------------+
```

## References

- npm download counts API: https://github.com/npm/registry/blob/main/docs/download-counts.md
