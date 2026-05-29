# GitBook source

Query GitBook organizations, spaces, collections, sites, members, teams,
pages, files, and content search through Coral SQL.

## Credentials

Create a GitBook API token with read access to the organizations and spaces
you want to query, then add the source:

```bash
export GITBOOK_TOKEN="..."
coral source add --file sources/community/gitbook/manifest.yaml
```

The source sends the token as `Authorization: Bearer <token>` to the GitBook
API at `https://api.gitbook.com/v1`.

## Start here

```sql
SELECT id, title, app_url
FROM gitbook.organizations
LIMIT 10;
```

Use an organization ID to inspect content containers:

```sql
SELECT id, title, visibility, updated_at
FROM gitbook.spaces
WHERE organization_id = 'org_...'
ORDER BY updated_at DESC
LIMIT 20;
```

Search uses GitBook's provider-ranked retrieval endpoints, exposed as Coral
search functions:

```sql
SELECT id, title, score, pages
FROM gitbook.search_org_content(
  organization_id => 'org_...',
  query => 'authentication'
)
LIMIT 10;
```

For one space:

```sql
SELECT id, title, path, score, url
FROM gitbook.search_space_content(
  space_id => 'space_...',
  query => 'webhook'
)
LIMIT 10;
```

## Useful joins

Find public docs spaces and the sites that publish them:

```sql
SELECT s.title AS space_title,
       s.visibility,
       site.title AS site_title,
       site.published_url
FROM gitbook.spaces s
JOIN gitbook.sites site
  ON site.organization_id = s.organization_id
WHERE s.organization_id = 'org_...'
  AND site.published = true;
```

Inspect pages for a selected space:

```sql
SELECT id, title, path, hidden, no_index
FROM gitbook.pages
WHERE space_id = 'space_...'
LIMIT 50;
```

Fetch a single page, optionally requesting Markdown output:

```sql
SELECT title, path, markdown
FROM gitbook.page
WHERE space_id = 'space_...'
  AND page_id = 'page_...'
  AND format = 'markdown';
```

## Notes

- The manifest is read-only and avoids GitBook mutation endpoints.
- Paginated list endpoints use GitBook's `next.page` cursor.
- Nested fields follow Coral's double-underscore convention, for example
  `dimensions__width`.
- Each table exposes a `raw` JSON column for fields GitBook may add over time.

## References

- GitBook API OpenAPI spec: <https://api.gitbook.com/openapi.yaml>
- GitBook API docs: <https://gitbook.com/docs/developers/gitbook-api>
