# Chromium Local Source

Query local Chromium-based browser data (Google Chrome, Microsoft Edge, and Brave) using SQL through Coral.

Because browsers do not expose personal data through public REST APIs, this source uses a lightweight local Python server. The local server never uploads your data to any remote server. However, query results are returned to the Coral client and any connected agent or MCP layer, and may appear in those tools' logs or conversation history.

The local server (`browser_server.py`) is not managed by Coral. You must start it manually before querying and stop it when done. If the server is not running, all queries will fail with a connection error.

## Quick Start

1. Start the local server and keep this terminal open. If you have not set `CHROMIUM_API_KEY` in the environment, the server will generate one and print it so you can copy it into your Coral shell.

```bash
python sources/community/chromium/browser_server.py
```

By default the server listens on `http://127.0.0.1:8765`. If that port is occupied, set `CHROMIUM_BASE_URL` to the same value in **both** the server terminal and the Coral shell before running any commands:

```bash
# server terminal
export CHROMIUM_BASE_URL=http://127.0.0.1:9000
python sources/community/chromium/browser_server.py
```

```bash
# Coral shell (must match the server)
export CHROMIUM_BASE_URL=http://127.0.0.1:9000
```

When started without an existing `CHROMIUM_API_KEY`, the server prints an API token and instructions showing how to set `CHROMIUM_API_KEY` in your Coral shell (PowerShell or bash).

2. In your Coral shell, set `CHROMIUM_API_KEY` to the value printed by the server (example):

PowerShell (temporary for current session):

```powershell
$env:CHROMIUM_API_KEY = "<PASTE_TOKEN_HERE>"
```

bash/zsh:

```bash
export CHROMIUM_API_KEY=<PASTE_TOKEN_HERE>
```

3. Add the source:

```bash
coral source add --file sources/community/chromium/manifest.yaml
```

4. Test the source:

   ```bash
   coral source test chromium
   ```

   The test query hits `chromium.health`, which resolves the first installed browser among Chrome, Edge, and Brave and reads its real bookmark data. It passes as long as **any one** of the three browsers is installed with a resolvable profile — no specific browser is required — so Chrome-only, Edge-only, and Brave-only setups all succeed.

5. Run a representative query. First check which browser resolved, then query that browser's tables:

   ```bash
   coral sql "SELECT browser FROM chromium.health LIMIT 1"
   ```

   Use the returned prefix (`chrome`, `edge`, or `brave`) in place of `[browser]`:

   ```bash
   coral sql "SELECT title, url FROM chromium.[browser]_history ORDER BY last_visit_time DESC LIMIT 10"
   ```

6. Stop the server with `Ctrl-C` in the server terminal.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `CHROMIUM_API_KEY` | Required bearer token used by Coral and the local server. |
| `CHROMIUM_BASE_URL` | Base URL the server listens on and Coral connects to. Default: `http://127.0.0.1:8765`. Override when port 8765 is occupied. Must be set to the same value in both the server terminal and the Coral shell. |
| `CHROME_PROFILE_PATH` | Optional full path to the Chrome profile directory to query. |
| `EDGE_PROFILE_PATH` | Optional full path to the Edge profile directory to query. |
| `BRAVE_PROFILE_PATH` | Optional full path to the Brave profile directory to query. |

Profile paths should point at the profile directory itself, such as `C:\Users\you\AppData\Local\Google\Chrome\User Data\Default` or `/Users/you/Library/Application Support/Google/Chrome/Default`.

By default, the server reads each browser's `Local State` file and uses `profile.last_used`. If that cannot identify a single profile, set the browser-specific profile path above. The server does not guess based on filesystem modification time.

## Available Tables

| Table | Description |
| --- | --- |
| `chromium.health` | First-available browser check. Resolves the first installed browser (Chrome, Edge, or Brave) and returns its `browser`, `display_name`, `profile_path`, and `bookmark_count`. Used as the test query — not browser-specific. |
| `chromium.[browser]_bookmarks` | Bookmarks and folders. Replace `[browser]` with `chrome`, `edge`, or `brave`. |
| `chromium.[browser]_history` | Most recent 5,000 history records. |
| `chromium.[browser]_downloads` | Most recent 2,000 download records. |
| `chromium.[browser]_extensions` | Installed extensions and versions. Localized extension names are resolved via `_locales`. |
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

The output below was **captured from a real run** on Windows 11 with Chrome,
Edge, and Brave installed (each with a resolvable `Default` profile) and the
local server running on `http://127.0.0.1:8765`. Counts reflect the test
machine; no URLs, titles, or profile paths are included.

```text
$ coral source lint sources/community/chromium/manifest.yaml
Manifest is valid
```

```text
$ coral source test chromium

  ✓ chromium connected successfully
  Secrets: keychain

    chromium (19 tables)
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
    ├─ edge_top_sites
    └─ health
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT browser, bookmark_count FROM chromium.health LIMIT 1
      1 row
```

Representative `coral sql` runs against the live profiles:

```text
$ coral sql "SELECT browser, display_name, bookmark_count FROM chromium.health LIMIT 1"
+---------+---------------+----------------+
| browser | display_name  | bookmark_count |
+---------+---------------+----------------+
| chrome  | Google Chrome | 3              |
+---------+---------------+----------------+
```

```text
$ coral sql "SELECT count(*) AS chrome_history_rows FROM chromium.chrome_history"
+---------------------+
| chrome_history_rows |
+---------------------+
| 116                 |
+---------------------+
```

## Security Notes

The server enforces loopback-only binding: `CHROMIUM_BASE_URL` must resolve to `127.0.0.1`, `localhost`, or `::1` (IPv6 loopback). Any other host is rejected at startup with a clear error, so the server cannot be exposed on a LAN or external interface. IPv6 loopback is handled by a dedicated server class with `address_family = AF_INET6`.

Every request must include `Authorization: Bearer <CHROMIUM_API_KEY>`. The server also validates `Host`, `Origin`, and `Sec-Fetch-Site` headers and sends `Cache-Control: no-store` plus `X-Content-Type-Options: nosniff` on all responses.

SQLite browser databases (history, downloads, top sites) are opened **read-only** and snapshotted into a temporary database via SQLite's online backup API before querying. Your live browser data is never written to, so Chrome, Edge, and Brave can stay open while Coral reads. A read or query failure on a database that exists is returned as an HTTP 503 error rather than as empty results, so a corrupt or unreadable database is never silently presented as "no data".

## Troubleshooting

If a query fails with HTTP 503, check the message from the server. It usually means the browser is not installed, `Local State` did not identify a profile, or the relevant `*_PROFILE_PATH` variable points at the wrong directory.

If a query fails with HTTP 401, confirm `CHROMIUM_API_KEY` is set to the same value in the server terminal and in the shell where you run `coral source add --file` or `coral source test`.

The server listens on the address given by `CHROMIUM_BASE_URL` (default `http://127.0.0.1:8765`). If Coral cannot reach the server, confirm the server is still running, that `CHROMIUM_BASE_URL` is set to the same value in both the server terminal and the Coral shell, and that `CHROMIUM_API_KEY` matches the token printed when the server started.

## Contributions

Contributions by github.com/GaneshBamalwa and github.com/sidshivam625.
