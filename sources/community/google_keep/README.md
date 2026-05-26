# Google Keep community source

The `google_keep` community source exposes read-only Google Keep note and checklist data through Coral SQL.

> [!WARNING]
> **API Limitation: No Labels/Tags Support**
> The official Google Keep API v1 does **not** expose any endpoints or fields for retrieving, querying, or managing labels/tags of notes. Therefore, a `labels` table or column cannot be provided.

## Setup

Google Keep API is primarily designed for enterprise/Google Workspace domains and requires high-privilege Workspace scopes.

To use the Google Keep source:
1. Ensure your Google Workspace domain administrator has enabled the Google Keep API.
2. Obtain an OAuth 2.0 access token with the `https://www.googleapis.com/auth/keep.readonly` scope.
3. Install the source:

```sh
export GOOGLE_KEEP_ACCESS_TOKEN="<oauth-access-token>"
cargo run -p coral-cli -- source add --file sources/community/google_keep/manifest.yaml
```

Alternatively, you can run the interactive setup flow in the Coral UI by selecting **Connect Google Keep** and providing your Google Cloud OAuth Client ID and Secret.

## Tables

| Table | Purpose |
| --- | --- |
| `google_keep.notes` | Notes and checklists, optionally filtered by trashed status or modification timestamps. |

All tables are read-only. This source does not create, update, delete, pin, or archive Google Keep notes.

### Notes Schema Highlights
* **`body_text`**: Populated with the text content for standard text notes (from `body.text.text`).
* **`list_items_text`**: Populated with the top-level checklist items formatted as a newline-separated string (from `body.list.listItems[*].text.text`).
* **`list_items`**: A raw JSON array preserving the full nested structure of checklists (including `childListItems` for sub-items).

## Example queries

Discover all active (non-trashed) notes and checklists:

```sql
SELECT id, title, body_text, list_items_text
FROM google_keep.notes
WHERE filter = 'trashed = false'
LIMIT 20;
```

Search note titles or body text for a keyword:

```sql
SELECT title, body_text
FROM google_keep.notes
WHERE title LIKE '%Project%' OR body_text LIKE '%Project%';
```

Find all trashed notes:

```sql
SELECT id, title, trash_time
FROM google_keep.notes
WHERE filter = 'trashed = true';
```

Query raw checklist structures to inspect sub-items:

```sql
SELECT title, list_items
FROM google_keep.notes
WHERE list_items IS NOT NULL
LIMIT 5;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/google_keep/manifest.yaml
```

Install and test with a real or mock token:

```sh
export GOOGLE_KEEP_ACCESS_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/google_keep/manifest.yaml
cargo run -p coral-cli -- source test google_keep
```

Inspect the registered source metadata:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'google_keep'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'google_keep' ORDER BY table_name, ordinal_position"
```

## Notes

- The Google Keep API v1 lists active notes by default but includes trashed notes if specified by custom filtering.
- Pagination is handled using a cursor token through `pageToken` and page sizes configured using `pageSize` up to a maximum of 1000 notes per request.
- Attachments are exposed as a raw JSON array (`attachments`) on each note.
