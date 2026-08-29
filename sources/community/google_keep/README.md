# Google Keep community source

The `google_keep` community source exposes read-only Google Keep note and checklist data through Coral SQL.

> [!WARNING]
> **API Limitation: No Labels/Tags Support**
> The official Google Keep API v1 does **not** expose any endpoints or fields for retrieving, querying, or managing labels/tags of notes. Therefore, a `labels` table or column cannot be provided.

## Setup

> [!IMPORTANT]
> **Enterprise & Administrator Approval Requirements**
> Google Keep API is designed primarily for enterprise environments. Accessing it requires special authorization setup by a Google Workspace administrator:
> 
> 1. **Workspace Admin Approval**: Your Google Workspace administrator must enable the Google Keep API in the Google Cloud Console.
> 2. **OAuth Scope Approval**: If using the OAuth flow, the administrator must pre-approve the Keep OAuth scope (`https://www.googleapis.com/auth/keep.readonly`) for your Client ID.
> 3. **Service Accounts / Domain-Wide Delegation**: If accessing notes on behalf of other users in a Workspace domain, you must use a service account with Domain-Wide Delegation. Service-account or domain-wide-delegation access tokens must be generated outside of Coral and pasted via the **Paste access token** option.

To use the Google Keep source:
1. Ensure your Workspace admin has approved the Keep API scopes for your client/account.
2. Obtain an OAuth 2.0 access token with the `https://www.googleapis.com/auth/keep.readonly` scope.
3. Install the source:

```sh
export GOOGLE_KEEP_ACCESS_TOKEN="<oauth-access-token>"
coral source add --file sources/community/google_keep/manifest.yaml
```

Alternatively, run the interactive setup flow by selecting **Connect Google Keep** and providing your approved OAuth Client ID and Secret:

```sh
coral source add --interactive --file sources/community/google_keep/manifest.yaml
```

## Tables

| Table | Purpose |
| --- | --- |
| `google_keep.notes` | Notes and checklists, optionally filtered by trashed status or modification timestamps. By default, it fetches up to 100 notes. |

All tables are read-only. This source does not create, update, delete, pin, or archive Google Keep notes.

### Notes Schema Highlights
* **`body_text`**: Populated with the text content for standard text notes (from `body.text.text`).
* **`list_items_text`**: Populated with the top-level checklist items formatted as a newline-separated string (from `body.list.listItems[*].text.text`).
* **`list_items`**: A raw JSON array preserving the full nested structure of checklists (including `childListItems` for sub-items).

## Example queries

Discover all active (non-trashed) notes and checklists (server-side filtered):

```sql
SELECT id, title, body_text, list_items_text
FROM google_keep.notes
WHERE filter = 'trashed = false'
LIMIT 50;
```

Search note titles or body text for a keyword using Coral's local SQL engine:

> [!NOTE]
> The Google Keep API does not support keyword search filtering on the server. The `LIKE` operator is evaluated locally by Coral after fetching the records. To prevent fetching your entire note history, always pair local text search with a server-side `filter` (e.g. `trashed = false` or `update_time >= ...`) and a `LIMIT`.

```sql
SELECT title, body_text
FROM google_keep.notes
WHERE (title LIKE '%Project%' OR body_text LIKE '%Project%')
  AND filter = 'trashed = false'
LIMIT 50;
```

Find all trashed notes (server-side filtered):

```sql
SELECT id, title, trash_time
FROM google_keep.notes
WHERE filter = 'trashed = true'
LIMIT 50;
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
coral source lint sources/community/google_keep/manifest.yaml
```

Install and test with a real or mock token:

```sh
export GOOGLE_KEEP_ACCESS_TOKEN="<token>"
coral source add --file sources/community/google_keep/manifest.yaml
coral source test google_keep
```

Inspect the registered source metadata:

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'google_keep'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'google_keep' ORDER BY table_name, ordinal_position"
```

## Notes

- **Fetch Limits**: By default, the `notes` table is configured with a `fetch_limit_default: 100` to prevent excessive API load. Specify a higher SQL `LIMIT` intentionally to fetch more notes.
- The Google Keep API v1 lists active notes by default but includes trashed notes if specified by custom filtering.
- Pagination is handled using a cursor token through `pageToken`. The source is configured with a maximum page size cap of 1000 notes per request via `pageSize`.
- Attachments are exposed as a raw JSON array (`attachments`) on each note.

