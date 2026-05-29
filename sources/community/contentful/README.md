# Contentful source

Query Contentful content types, entries, assets, locales, tags, and full-text
content search from Coral SQL.

This source targets Contentful's read-only Delivery API by default. Set
`CONTENTFUL_API_BASE=https://preview.contentful.com` when you want to use a
Preview API token instead.

## Credentials

```bash
export CONTENTFUL_SPACE_ID="..."
export CONTENTFUL_ENVIRONMENT_ID="master"
export CONTENTFUL_ACCESS_TOKEN="..."
coral source add --file sources/community/contentful/manifest.yaml
```

## Start here

```sql
SELECT id, name, display_field
FROM contentful.content_types
ORDER BY updated_at DESC
LIMIT 20;
```

List entries for one content type:

```sql
SELECT id, content_type_id, updated_at, fields
FROM contentful.entries
WHERE content_type = 'blogPost'
ORDER BY updated_at DESC
LIMIT 20;
```

Search entries with Contentful full-text search:

```sql
SELECT id, content_type_id, updated_at, fields
FROM contentful.search_entries(
  query => 'security',
  content_type => 'blogPost',
  locale => 'en-US'
)
LIMIT 20;
```

Query assets:

```sql
SELECT id, title, description, file
FROM contentful.assets
WHERE mimetype_group = 'image'
LIMIT 20;
```

## Notes

- Contentful spaces define custom fields, so entry and asset fields are exposed
  as JSON in the `fields` and `file` columns.
- List endpoints use `skip`/`limit` offset pagination. Contentful documents a
  maximum `limit` of 1000 for collection endpoints.
- Use `CONTENTFUL_API_BASE=https://cdn.contentful.com` for published content
  and `https://preview.contentful.com` for preview content.

## References

- Content Delivery API reference: <https://www.contentful.com/developers/docs/references/content-delivery-api/>
- Content Preview API reference: <https://www.contentful.com/developers/docs/references/content-preview-api/>
- Contentful search parameters: <https://www.contentful.com/developers/docs/references/content-delivery-api/#/reference/search-parameters>
