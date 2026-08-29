# PyPI (pypi)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** `https://pypi.org`

Query Python package metadata, dependencies, author information, and
project URLs via the public [PyPI JSON API](https://docs.pypi.org/api/json/).
No authentication required.

```bash
coral source add --file sources/community/pypi/manifest.yaml
```

## Configuration

This source does not require any authentication or input configuration. The PyPI JSON API is fully public.

## Tables

| Table           | Description                                       | Key filters           |
| --------------- | ------------------------------------------------- | --------------------- |
| `pypi.packages` | Look up metadata for a specific Python package    | `name` (**required**) |

## Example queries

```sql
-- Fetch metadata for the 'requests' package
SELECT package_name, version, summary, requires_python
FROM pypi.packages
WHERE name = 'requests';

-- Inspect author and license information
SELECT package_name, author, author_email, license
FROM pypi.packages
WHERE name = 'django';

-- Get project URLs and classifiers
SELECT project_url, docs_url, home_page, project_urls, classifiers
FROM pypi.packages
WHERE name = 'flask';

-- Check if a package is yanked
SELECT package_name, version, yanked, yanked_reason
FROM pypi.packages
WHERE name = 'urllib3';
```

## Pagination

The PyPI JSON API for packages returns a single JSON object per package rather than a
list of records. Therefore, `pypi.packages` always returns exactly one row if the
package exists, or zero rows if the package is not found. The `LIMIT` clause is
not required.

## Notes

- **No authentication required.** The registry is completely public.
- **Lookup only.** PyPI does not currently expose a JSON search endpoint, so you must know the exact name of the package you want to look up (via the `name` filter).
- **Graceful missing packages.** If a package does not exist (404), the query gracefully returns zero rows rather than throwing an error.
- **API policy.** PyPI does not currently enforce rate limits, but responses may be cached. Please consume the API responsibly: set a descriptive `User-Agent`, avoid unnecessary high-frequency polling, and prefer the [Simple/Index API](https://docs.pypi.org/api/index-api/) for bulk package-listing or distribution-file operations. See the [PyPI API policies](https://docs.pypi.org/api/#api-policies) for the latest guidance.

## Validation

```bash
coral source lint sources/community/pypi/manifest.yaml
coral source add --file sources/community/pypi/manifest.yaml
coral source test pypi
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'pypi'"

coral sql "SELECT name, version, summary FROM pypi.packages WHERE name = 'requests' LIMIT 1"
# +----------+---------+-------------------------+
# | name     | version | summary                 |
# +----------+---------+-------------------------+
# | requests | 2.34.2  | Python HTTP for Humans. |
# +----------+---------+-------------------------+
```
