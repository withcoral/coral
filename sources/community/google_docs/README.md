# Google Docs Connector

This source queries the [Google Docs API](https://developers.google.com/workspace/docs/api/reference/rest/v1)
to expose document metadata, structural body content, document tabs, named
ranges, and lists as queryable tables.

## Auth

Use Coral's interactive OAuth flow to connect Google Docs:

```bash
coral source add --interactive --file sources/community/google_docs/manifest.yaml
```

Choose **Connect Google Docs** when Coral asks for the
`GOOGLE_DOCS_ACCESS_TOKEN` credential. Provide a Google OAuth Desktop app
client ID and client secret from a Google Cloud project with the Google Docs
API enabled.

The OAuth flow requests the Docs read-only scope:

```text
https://www.googleapis.com/auth/documents.readonly
```

See Google's [Docs API scope guide](https://developers.google.com/workspace/docs/api/auth)
for the data that scope can read.

The OAuth authorization request also asks for offline access so Google can
return a refresh token when consent is granted.

To add the source with an existing access token instead:

```bash
export GOOGLE_DOCS_ACCESS_TOKEN="<access-token>"
coral source add --file sources/community/google_docs/manifest.yaml
```

## Start querying

Google Docs API reads are scoped to a document ID. Copy the ID from a Docs URL.

Inspect one document:

```sql
SELECT document_id, title, revision_id
FROM google_docs.documents
WHERE document_id = '<document-id>';
```

Read top-level structural body elements from the first tab:

```sql
SELECT start_index, end_index, list_id, paragraph_text, table
FROM google_docs.body_content
WHERE document_id = '<document-id>'
ORDER BY start_index
LIMIT 100;
```

Inspect tab-aware named ranges and list maps:

```sql
SELECT tab_id, title, nesting_level, named_ranges, lists, child_tabs
FROM google_docs.tabs
WHERE document_id = '<document-id>'
ORDER BY tab_index;
```

Read named ranges and list definitions:

```sql
SELECT name, named_ranges
FROM google_docs.named_ranges
WHERE document_id = '<document-id>';

SELECT list_id, list_properties
FROM google_docs.lists
WHERE document_id = '<document-id>';
```

Review a document with suggestions inline:

```sql
SELECT document_id, title, raw
FROM google_docs.documents
WHERE document_id = '<document-id>'
  AND suggestions_view_mode = 'SUGGESTIONS_INLINE';
```

## Tables

### documents

Document-level metadata and raw structure for one document. Requires
`document_id`. Optional filter: `suggestions_view_mode`. Legacy document-level
content fields are first-tab fields; use `tabs` for tab-aware named range and
list maps.

### body_content

Top-level structural body elements for one document's first tab. Requires
`document_id`. Optional filter: `suggestions_view_mode`. Exposes `list_id`
for joining bullet and numbered-list paragraphs to `google_docs.lists`.

### tabs

Top-level document tabs with tab-scoped named ranges and lists, plus one
nested `child_tabs` JSON level. Requires `document_id`. Optional filter:
`suggestions_view_mode`. Google models tabs as a recursive tree, and this
table does not flatten grandchildren or deeper descendants into separate rows.
Use `body_content` for first-tab structural body content.

### named_ranges

First-tab named range groups for one document. Requires `document_id`.

### lists

First-tab list definitions keyed by list ID. Requires `document_id`.

## Rate limits

Every table maps to `documents.get`. Google Docs publishes read quotas of
3,000 requests per minute per project and 300 requests per minute per user per
project. Repeated scans across many documents can receive `429 Too Many
Requests`; wait and retry later with backoff if that happens.

## Notes

- This source is read-only. It does not create, edit, or delete Docs content.
- Google Docs API reads require a document ID; this API does not provide a
  document listing endpoint.
- `google_docs.body_content`, `google_docs.named_ranges`, and
  `google_docs.lists` read the legacy document-level fields Google returns for
  the first tab. Use `google_docs.tabs` for tab-aware named ranges and list
  maps.
- Rich Docs structures such as paragraphs, tables, list properties, and child
  tabs stay in JSON columns so callers can inspect the provider payload without
  losing layout-specific fields.
- Google Docs access tokens expire. Coral stores OAuth refresh metadata when
  Google returns it, but automatic token refresh is not implemented yet.
