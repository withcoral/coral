# Notion source

This bundled source queries Notion's read APIs with an internal integration
token.

## Configure

Create a Notion internal integration, copy its internal integration token, and
share the pages or databases you want to query with that integration.

```sh
export NOTION_API_KEY="ntn_..."
coral source add notion
```

## Start querying

Discover shared pages and data sources:

```sql
SELECT id, object, url
FROM notion.search
LIMIT 20;
```

Inspect a data source schema:

```sql
SELECT name, id, type
FROM notion.data_source_properties
WHERE data_source_id = '...';
```

Query pages from a data source:

```sql
SELECT id, url, created_time, last_edited_time, properties
FROM notion.data_source_pages
WHERE data_source_id = '...'
LIMIT 100;
```
