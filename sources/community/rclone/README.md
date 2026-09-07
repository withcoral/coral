# Rclone

**Version:** 0.1.0
**Backend:** HTTP
**Functions:** 4

Query files, directories, and storage usage across 70+ cloud storage providers through rclone's RC API. Supports Google Drive, Dropbox, S3, OneDrive, MEGA, pCloud, Box, Backblaze B2, and more — all from a single source.

## Installation

1. Install and configure rclone with your cloud storage remotes:

```bash
brew install rclone    # or see https://rclone.org/install/
rclone config          # set up remotes (Google Drive, Dropbox, etc.)
```

2. Start the rclone RC API daemon:

```bash
rclone rcd --rc-no-auth
```

3. Install the Coral source:

```bash
coral source add --file sources/community/rclone/manifest.yaml
```

## Prerequisites

- [rclone](https://rclone.org/) installed with at least one remote configured
- rclone RC daemon running (`rclone rcd --rc-no-auth`)

## Quick Start

```sql
-- List files in a MEGA remote
SELECT path, name, size, mime_type, is_dir
FROM rclone.files(fs => 'mega:', path => '')
LIMIT 10;

-- List files in a Google Drive folder
SELECT path, name, size, mod_time
FROM rclone.files(fs => 'gdrive:', path => 'Documents')
LIMIT 10;

-- Check storage usage for a remote
SELECT total, used, free
FROM rclone.about(fs => 'mega:');

-- List only directories
SELECT path, name
FROM rclone.files(fs => 'onedrive:', path => '')
WHERE is_dir = true;

-- Find large files (> 10 MB)
SELECT path, name, size
FROM rclone.files(fs => 'gdrive:', path => '')
WHERE size > 10485760
ORDER BY size DESC;

-- Get info about a specific file
SELECT path, name, size, mime_type, mod_time
FROM rclone.stat(fs => 'mega:', path => 'document.pdf');

-- Get recursive size and file count
SELECT bytes, count
FROM rclone.size(fs => 'gdrive:Photos');

```

## Functions

### `rclone.files`

List files and directories in a cloud storage remote. Returns file metadata including name, size, MIME type, and modification time.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `fs` | Utf8 | (Required) Remote name with colon (e.g. `mega:`, `gdrive:`, `s3:bucket`) |
| `path` | Utf8 | (Required) Path within the remote (use `''` for root) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `path` | Utf8 | Full path relative to the remote root |
| `name` | Utf8 | File or directory name |
| `size` | Int64 | File size in bytes (-1 for directories) |
| `mime_type` | Utf8 | MIME type (e.g. application/pdf, inode/directory) |
| `mod_time` | Utf8 | Last modification time (ISO 8601 with timezone) |
| `is_dir` | Boolean | Whether the entry is a directory |
| `id` | Utf8 | Provider-specific file or directory ID |

---

### `rclone.about`

Get storage usage for a cloud storage remote. Returns total, used, and free space in bytes.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `fs` | Utf8 | (Required) Remote name with colon (e.g. `mega:`, `gdrive:`) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `total` | Int64 | Total storage space in bytes |
| `used` | Int64 | Storage space used in bytes |
| `free` | Int64 | Free storage space in bytes |
| `trashed` | Int64 | Space used by trashed files in bytes |
| `other` | Int64 | Space used by other (non-file) data in bytes |

---

### `rclone.stat`

Get metadata for a single file or directory. Returns the same fields as `files` for one specific path.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `fs` | Utf8 | (Required) Remote name with colon |
| `path` | Utf8 | (Required) Path to the file or directory |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `path` | Utf8 | Full path relative to the remote root |
| `name` | Utf8 | File or directory name |
| `size` | Int64 | File size in bytes (-1 for directories) |
| `mime_type` | Utf8 | MIME type of the file |
| `mod_time` | Utf8 | Last modification time (ISO 8601 with timezone) |
| `is_dir` | Boolean | Whether the entry is a directory |
| `id` | Utf8 | Provider-specific file or directory ID |

---

### `rclone.size`

Get the total size and file count for a remote or path, recursively. Pass the path in the `fs` argument (e.g. `fs => 'gdrive:Photos'`).

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `fs` | Utf8 | (Required) Remote with optional path (e.g. `mega:`, `gdrive:Photos`) |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `bytes` | Int64 | Total size of all files in bytes |
| `count` | Int64 | Total number of files |
| `sizeless` | Int64 | Number of files with unknown size (may not be present on all rclone versions) |

## Source scope

- Targets the rclone RC API (default `http://localhost:5572`). The base URL is configurable via `RCLONE_RC_URL`.
- No API key required — rclone handles cloud provider authentication through its own config.
- `files` lists files/directories, `about` returns storage usage, `stat` gets single file info, `size` gets recursive count/total.
- All rclone RC calls use `POST` with JSON body.
- No pagination — rclone returns the full directory listing per call.
- Supports any rclone remote: Google Drive, Dropbox, S3, OneDrive, MEGA, pCloud, Box, B2, SFTP, and 60+ more.
- No declared test queries — all functions require a user-specific remote name. Test manually with your configured remote after adding the source.

## Limitations

- Requires rclone installed and the RC daemon running (`rclone rcd --rc-no-auth`). The source cannot query cloud storage without a running daemon.
- The `files` function lists a single directory level — it does not recurse into subdirectories. Query subdirectories by changing the `path` argument.
- No built-in pagination — large directories return all entries in a single response. For directories with thousands of files, this may be slow.
- `mod_time` is returned as a Utf8 string with timezone (e.g. `2026-07-12T01:37:39+05:30`) — format varies by provider.
- `size` is `-1` for directories on most providers.
- Not all providers support `about` (storage usage). Some may return null or error.
- This source is designed for loopback-only access with `--rc-no-auth`. The rclone RC API is equivalent to shell access — do not expose it on a public network without authentication. For authenticated setups (`--rc-user`/`--rc-pass`), configure auth outside of this source.
- The `publiclink` endpoint is intentionally excluded because it creates public links (a write operation).

## Provider docs

- Rclone installation: https://rclone.org/install/
- Rclone RC API: https://rclone.org/rc/
- Supported providers: https://rclone.org/overview/
- operations/list: https://rclone.org/rc/#operations-list
- operations/about: https://rclone.org/rc/#operations-about

## Live validation output

Validated against a live rclone RC daemon with a configured MEGA remote.

```bash
$ coral source lint sources/community/rclone/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/rclone/manifest.yaml
Added source rclone

  ✓ rclone connected successfully
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json
FROM coral.table_functions
WHERE schema_name = 'rclone';
```

```text
+---------------+-------+-----------------------------------------------------------------------------------------+
| function_name | kind  | arguments_json                                                                          |
+---------------+-------+-----------------------------------------------------------------------------------------+
| about         | table | [{"name":"fs","required":true,"values":[]}]                                             |
| files         | table | [{"name":"fs","required":true,"values":[]},{"name":"path","required":true,"values":[]}] |
| size          | table | [{"name":"fs","required":true,"values":[]}]                                             |
| stat          | table | [{"name":"fs","required":true,"values":[]},{"name":"path","required":true,"values":[]}] |
+---------------+-------+-----------------------------------------------------------------------------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'rclone';
```

```text
+---------------+----------+----------+--------+
| key           | kind     | required | is_set |
+---------------+----------+----------+--------+
| RCLONE_RC_URL | variable | false    | true   |
+---------------+----------+----------+--------+
```

**Live files proof (MEGA):**

```sql
SELECT path, name, size, mime_type, is_dir
FROM rclone.files(fs => 'mega:', path => '')
LIMIT 5;
```

```text
+-----------------+-----------------+-------+---------------------------+--------+
| path            | name            | size  | mime_type                 | is_dir |
+-----------------+-----------------+-------+---------------------------+--------+
| .DS_Store       | .DS_Store       | 10244 | application/octet-stream  | false  |
| ._.DS_Store     | ._.DS_Store     | 0     | application/octet-stream  | false  |
| untitled folder | untitled folder | -1    | inode/directory           | true   |
+-----------------+-----------------+-------+---------------------------+--------+
```

**Live about proof (MEGA):**

```sql
SELECT total, used, free
FROM rclone.about(fs => 'mega:');
```

```text
+-------------+-------+-------------+
| total       | used  | free        |
+-------------+-------+-------------+
| 21474836480 | 43054 | 21474793426 |
+-------------+-------+-------------+
```

**Live stat proof (MEGA):**

```sql
SELECT path, name, size, mime_type, is_dir
FROM rclone.stat(fs => 'mega:', path => '.DS_Store');
```

```text
+-----------+-----------+-------+--------------------------+--------+
| path      | name      | size  | mime_type                | is_dir |
+-----------+-----------+-------+--------------------------+--------+
| .DS_Store | .DS_Store | 10244 | application/octet-stream | false  |
+-----------+-----------+-------+--------------------------+--------+
```

**Live size proof (MEGA):**

```sql
SELECT bytes, count, sizeless
FROM rclone.size(fs => 'mega:');
```

```text
+-------+-------+----------+
| bytes | count | sizeless |
+-------+-------+----------+
| 10244 | 2     | 0        |
+-------+-------+----------+
```
