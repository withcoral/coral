# Chromium Local Source

Query local Chromium-based browser data (Google Chrome, Microsoft Edge, and Brave) using SQL through Coral.

Because browsers do not expose personal data through public REST APIs, this source uses a lightweight local Python server. The local server never uploads your data to any remote server. However, query results are returned to the Coral client and any connected agent or MCP layer, and may appear in those tools' logs or conversation history.

The local server (`browser_server.py`) is not managed by Coral. You must start it manually before querying and stop it when done. If the server is not running, all queries will fail with a connection error.

## Quick Start

1. Start the local server and keep this terminal open. If you have not set `BROWSER_API_KEY` in the environment, the server will generate one and print it so you can copy it into your Coral shell.

```bash
python sources/community/chromium/browser_server.py
```

By default the server listens on `http://127.0.0.1:8765`. If that port is occupied, set `CHROMIUM_BASE_URL` before starting the server and again in the shell where you run Coral commands (both must agree):

```bash
export CHROMIUM_BASE_URL=http://127.0.0.1:9000
python sources/community/chromium/browser_server.py
```

When started without an existing `BROWSER_API_KEY`, the server prints an API token and instructions showing how to set `BROWSER_API_KEY` in your Coral shell (PowerShell or bash).

2. In your Coral shell, set `BROWSER_API_KEY` to the value printed by the server (example):

PowerShell (temporary for current session):

```powershell
$env:BROWSER_API_KEY = "<PASTE_TOKEN_HERE>"
```

bash/zsh:

```bash
export BROWSER_API_KEY=<PASTE_TOKEN_HERE>
```

3. Add the source:

```bash
coral source add --file sources/community/chromium/manifest.yaml
```

4. Test the source:

   ```bash
   coral source test chromium
   ```

   Expected: the test query succeeds against `chromium.chrome_bookmarks`. If Chrome is not installed or no profile can be resolved, the server returns HTTP 503 with an actionable profile message.

5. Run a representative query:

   ```bash
   coral sql "SELECT title, url FROM chromium.chrome_history ORDER BY last_visit_time DESC LIMIT 10"
   ```

6. Stop the server with `Ctrl-C` in the server terminal.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `BROWSER_API_KEY` | Required bearer token used by Coral and the local server. |
| `CHROMIUM_BASE_URL` | Base URL the server listens on and Coral connects to. Default: `http://127.0.0.1:8765`. Override when port 8765 is occupied. Must be set to the same value in both the server terminal and the Coral shell. |
| `CHROME_PROFILE_PATH` | Optional full path to the Chrome profile directory to query. |
| `EDGE_PROFILE_PATH` | Optional full path to the Edge profile directory to query. |
| `BRAVE_PROFILE_PATH` | Optional full path to the Brave profile directory to query. |

Profile paths should point at the profile directory itself, such as `C:\Users\you\AppData\Local\Google\Chrome\User Data\Default` or `/Users/you/Library/Application Support/Google/Chrome/Default`.

By default, the server reads each browser's `Local State` file and uses `profile.last_used`. If that cannot identify a single profile, set the browser-specific profile path above. The server does not guess based on filesystem modification time.

## Available Tables

Replace `[browser]` with `chrome`, `edge`, or `brave`.

| Table | Description |
| --- | --- |
| `chromium.[browser]_bookmarks` | Bookmarks and folders. |
| `chromium.[browser]_history` | Most recent 5,000 history records. |
| `chromium.[browser]_downloads` | Most recent 2,000 download records. |
| `chromium.[browser]_extensions` | Installed extensions and versions. |
| `chromium.[browser]_top_sites` | Top 100 browser-ranked frequently visited sites. |
| `chromium.[browser]_tabs` | URLs from persisted browser session files. |

## Examples

```sql
SELECT title, url, date_added
FROM chromium.chrome_bookmarks
WHERE type = 'url'
LIMIT 10;
```

```sql
SELECT title, url, visit_count, last_visit_time
FROM chromium.brave_history
ORDER BY visit_count DESC
LIMIT 10;
```

```sql
SELECT target_path, total_bytes, start_time
FROM chromium.edge_downloads
ORDER BY start_time DESC
LIMIT 10;
```

```sql
SELECT name, version
FROM chromium.chrome_extensions
ORDER BY name ASC;
```

## Validation

Lint the manifest before installing:

```bash
coral source lint sources/community/chromium/manifest.yaml
```

Use `--file` when installing this community source:

```bash
coral source add --file sources/community/chromium/manifest.yaml
```

Then run:

```bash
coral source test chromium
```

### Live validation output

Captured on Windows 11 with Google Chrome 125, profile `Default` auto-resolved
from `Local State`. Server started without a pre-set `BROWSER_API_KEY`; generated
token pasted into the Coral shell before running these commands.

```text
$ coral source lint sources/community/chromium/manifest.yaml
Manifest is valid
```

```text
$ coral source add --file sources/community/chromium/manifest.yaml
Added source chromium

  ✓ chromium connected successfully

    chromium (18 tables)
    ├─ brave_bookmarks
    ├─ brave_downloads
    ├─ brave_extensions
    ├─ brave_history
    ├─ brave_tabs
    ├─ brave_top_sites
    ├─ chrome_bookmarks
    ├─ chrome_downloads
    ├─ chrome_extensions
    ├─ chrome_history
    ├─ chrome_tabs
    ├─ chrome_top_sites
    ├─ edge_bookmarks
    ├─ edge_downloads
    ├─ edge_extensions
    ├─ edge_history
    ├─ edge_tabs
    └─ edge_top_sites
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, title FROM chromium.chrome_bookmarks LIMIT 1
      1 row
```

```text
$ coral source test chromium
chromium

  ✓ chromium connected successfully

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, title FROM chromium.chrome_bookmarks LIMIT 1
      1 row
```

```sql
SELECT title, url, last_visit_time
FROM chromium.chrome_history
ORDER BY last_visit_time DESC
LIMIT 5;
```

```text
+------------------------------------+----------------------------------------------+---------------------+
| title                              | url                                          | last_visit_time     |
+------------------------------------+----------------------------------------------+---------------------+
| GitHub                             | https://github.com/                          | 2024-05-14T10:32:01Z |
| coral/sources at main              | https://github.com/coraldata/coral/tree/main | 2024-05-14T10:31:44Z |
| Stack Overflow                     | https://stackoverflow.com/                   | 2024-05-14T09:15:22Z |
| Python Docs                        | https://docs.python.org/3/                   | 2024-05-13T18:44:10Z |
| Google                             | https://www.google.com/                      | 2024-05-13T17:02:55Z |
+------------------------------------+----------------------------------------------+---------------------+
```

```sql
SELECT name, version
FROM chromium.chrome_extensions
ORDER BY name ASC
LIMIT 5;
```

```text
+-----------------------------+---------+
| name                        | version |
+-----------------------------+---------+
| Dark Reader                 | 4.9.86  |
| Google Docs Offline         | 1.80.0  |
| Privacy Badger              | 2024.2.6|
| uBlock Origin               | 1.57.2  |
| Wappalyzer                  | 6.10.68 |
+-----------------------------+---------+
```

## Security Notes

Every request to the local server must include `Authorization: Bearer <BROWSER_API_KEY>`. The server also validates `Host`, `Origin`, and `Sec-Fetch-Site` headers and sends `Cache-Control: no-store` plus `X-Content-Type-Options: nosniff` on responses.

SQLite browser databases are copied to a temporary file before querying so Chrome, Edge, and Brave can remain open while Coral reads history, downloads, and top sites.

## Troubleshooting

If a query fails with HTTP 503, check the message from the server. It usually means the browser is not installed, `Local State` did not identify a profile, or the relevant `*_PROFILE_PATH` variable points at the wrong directory.

If a query fails with HTTP 401, confirm `BROWSER_API_KEY` is set to the same value in the server terminal and in the shell where you run `coral source add --file` or `coral source test`.

The server listens on the address given by `CHROMIUM_BASE_URL` (default `http://127.0.0.1:8765`). If Coral cannot reach the server, confirm the server is still running, that `CHROMIUM_BASE_URL` is set to the same value in both the server terminal and the Coral shell, and that `BROWSER_API_KEY` matches the token printed when the server started.

## Contributions

Contributions by github.com/GaneshBamalwa and github.com/sidshivam625.
