# Google Drive Connector

This source queries the [Google Drive API](https://developers.google.com/workspace/drive/api/reference/rest/v3)
to expose files, file metadata, shared drives, permissions, comments,
revisions, and account metadata as queryable tables.

## Auth

Use Coral's interactive OAuth flow to connect Google Drive:

```bash
coral source add --interactive --file sources/community/google_drive/manifest.yaml
```

Choose **Connect Google Drive** when Coral asks for the
`GOOGLE_DRIVE_ACCESS_TOKEN` credential. Provide a Google OAuth Desktop app
client ID and client secret from a Google Cloud project with the Google Drive
API enabled.

The OAuth flow requests the Drive read-only scope:

```text
https://www.googleapis.com/auth/drive.readonly
```

See Google's [Drive API scope guide](https://developers.google.com/workspace/drive/api/guides/api-specific-auth)
for the data that scope can read.

The OAuth authorization request also asks for offline access so Google can
return a refresh token when consent is granted.

To add the source with an existing access token instead:

```bash
export GOOGLE_DRIVE_ACCESS_TOKEN="<access-token>"
coral source add --file sources/community/google_drive/manifest.yaml
```

Verify the connection and declared smoke queries:

```bash
coral source test google_drive
```

## Start querying

Find recently modified files:

```sql
SELECT id, name, mime_type, modified_time, web_view_link
FROM google_drive.files
WHERE q = 'trashed = false'
  AND order_by = 'modifiedTime desc'
LIMIT 20;
```

Inspect the connected Drive account and capabilities:

```sql
SELECT user__email, storage_quota_usage_in_drive, can_create_drives
FROM google_drive.about;
```

Use Google Drive file query syntax to list folders:

```sql
SELECT id, name, parent_ids
FROM google_drive.files
WHERE q = 'trashed = false and mimeType = ''application/vnd.google-apps.folder'''
LIMIT 50;
```

Read metadata for one file ID directly:

```sql
SELECT id, name, mime_type, version, modified_time, shared
FROM google_drive.file
WHERE file_id = '<file-id>';
```

Discover shared drives:

```sql
SELECT id, name, created_time, hidden
FROM google_drive.shared_drives
ORDER BY name
LIMIT 50;
```

Scan files in one shared drive:

```sql
SELECT id, name, mime_type, modified_time
FROM google_drive.files
WHERE corpora = 'drive'
  AND drive_id = '<shared-drive-id>'
  AND q = 'trashed = false'
LIMIT 50;
```

Audit permissions for one file or shared drive:

```sql
SELECT type, role, email_address, display_name, domain
FROM google_drive.permissions
WHERE file_id = '<file-id>'
ORDER BY role, email_address
LIMIT 100;
```

Read comments for one file:

```sql
SELECT id, author__display_name, content, resolved, modified_time
FROM google_drive.comments
WHERE file_id = '<file-id>'
ORDER BY modified_time DESC
LIMIT 50;
```

Inspect revision history:

```sql
SELECT id, mime_type, modified_time, last_modifying_user__display_name
FROM google_drive.revisions
WHERE file_id = '<file-id>'
ORDER BY modified_time DESC
LIMIT 50;
```

Google may omit older revisions for files with large revision histories, so use
this as provider-visible revision metadata rather than a complete immutable
audit log.

## Tables

### about

Authenticated user, Drive quota, and system capability metadata. Maps to
`GET /about`. No filter required.

### files

Files and folders visible to the authenticated user. No filter required. Maps
to `GET /files`. Optional filters: `q`, `corpora`, `drive_id`, `spaces`, and
`order_by`. Paginates via `nextPageToken` with up to 100 items per page.
For broad scans such as `corpora = 'allDrives'`, Google can return
`incompleteSearch = true`, meaning some results may be missing. Narrow to
`corpora = 'user'` or `corpora = 'drive'` with `drive_id` when completeness
matters.

### file

Metadata for a specific file or folder. Requires `file_id`. Maps to
`GET /files/{file_id}`.

### shared_drives

Shared drives visible to the authenticated user. No filter required. Maps to
`GET /drives`. Optional filter: `q`.

### permissions

Permissions for a specific file, folder, or shared drive. Requires `file_id`.
Maps to `GET /files/{file_id}/permissions`.

### comments

Comments for a specific file. Requires `file_id`. Maps to
`GET /files/{file_id}/comments`. Optional filters: `include_deleted` and
`start_modified_time`.

### revisions

Revision history for a specific file. Requires `file_id`. Maps to
`GET /files/{file_id}/revisions`. Google may omit older revisions for files
with large revision histories.

## Notes

- This source is read-only. It does not upload, edit, share, trash, or delete
  Drive content.
- Google Drive access tokens expire. Coral stores OAuth refresh metadata when
  Google returns it, but automatic token refresh is not implemented yet.
- Drive returns trashed files from `google_drive.files` unless the `q` filter
  excludes them, for example `q = 'trashed = false'`.
- Broad file scans can be incomplete when Google returns `incompleteSearch =
  true`; narrow broad scans to `corpora = 'user'` or a specific shared drive
  with `corpora = 'drive'` and `drive_id`.
- Use file IDs from `google_drive.files` with `permissions`, `comments`, and
  `revisions`.
- Google may omit older revisions from `google_drive.revisions` for files with
  large revision histories.
- Google may reject `google_drive.permissions` for readable files whose sharing
  details the authenticated user cannot enumerate.
- The comments endpoint requires a `fields` partial-response projection, which
  the source configures in the manifest.
