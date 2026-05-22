Notion source for Coral
======================

Setup
-----

- Provide a Notion integration token as the `NOTION_API_KEY` input when creating the source. Example (shell):

  export NOTION_API_KEY="secret_..."

- Share the relevant pages and databases with your Notion integration (see Notion guides).

Quick example queries
---------------------

- Search for pages with "meeting notes":

  SELECT id, url FROM notion.search WHERE query = 'meeting notes' LIMIT 10

- List rows from a database (replace `<database_id>`):

  SELECT id, properties FROM notion.database_rows WHERE database_id = '<database_id>' LIMIT 20

- Get a single page by ID:

  SELECT id, properties FROM notion.pages WHERE page_id = '<page_id>'

Notes
-----

- The Notion API requires the integration or token to be granted access to the pages/databases you query. If a related database is not shared with the connection, relation properties may be omitted.
- The manifest uses `Notion-Version: 2026-03-11`. Update if a newer API version is required.
