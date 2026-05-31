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

## References

- npm download counts API: https://github.com/npm/registry/blob/main/docs/download-counts.md
