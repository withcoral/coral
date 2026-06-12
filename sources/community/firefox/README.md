# Firefox Local Source

Query your local Mozilla Firefox browser data using SQL through Coral.

Because browsers do not expose personal data via public REST APIs, this source
uses a lightweight, zero-dependency local Python server. The server reads your
Firefox SQLite databases, serves them to Coral over `localhost`, and **requires
a shared bearer token** so that only the Coral client that knows the token can
read the data.

> **⚠️ Local data exposure risk**
> The server makes your Firefox bookmarks, browsing history, and installed
> extensions readable to any process on your machine that holds the bearer
> token. Query results are returned to the local Coral client, MCP server, or
> agent that asks for them, and may appear in transcripts or logs managed by
> those tools. Start the server only when you intend to query Coral, and stop it
> afterwards with **Ctrl+C**. Never expose the configured port beyond
> `127.0.0.1` or `localhost`.

The Python server itself does not upload Firefox data. Treat anything returned
to Coral as data shared with the local Coral workflow that requested it.

---

## Features

* **Shared bearer token:** Every request must carry `Authorization: Bearer <token>`.
  The token is generated at startup (or read from `FIREFOX_API_KEY`) and
  must be exported before running `coral source add`.
* **Host, origin, and fetch-metadata checks:** Requests with an unexpected
  `Host`, `Origin`, or `Referer` header, or a cross-site `Sec-Fetch-Site`
  value, are rejected, blocking DNS-rebinding and browser-side request
  smuggling.
* **Security headers:** All responses include `Cache-Control: no-store` and
  `X-Content-Type-Options: nosniff`.
* **Accurate profile resolution:** Reads Firefox's `profiles.ini` metadata to
  use a single unambiguous install default when one exists, falls back to the
  legacy `[Profile...] Default=1` entry, and refuses to guess when multiple
  Firefox installations advertise different defaults.
* **Explicit profile override:** Set `FIREFOX_PROFILE_PATH` to pin the server
  to one specific profile directory.
* **Safe SQLite extraction:** Uses SQLite's backup API to create a consistent
  temporary snapshot before querying `places.sqlite`.
* **Zero dependencies:** Only standard Python libraries – no `pip install`
  required.
* **Configurable loopback port:** Runs on port `8766` by default. Set
  `FIREFOX_PORT` and matching `FIREFOX_BASE_URL` if that port is occupied.

---

## Extracted Data

| Table | Description |
|-------|-------------|
| `bookmarks` | Saved URLs and folder structures from `moz_bookmarks` |
| `history` | Recent browsing history sample from `moz_places`, newest first, capped at 5,000 server-side rows |
| `history_slice(...)` | Parameterized history function for deliberate server-side slices by limit, time range, URL substring, or title substring |
| `extensions` | Installed extensions and versions from `extensions.json` |
| `top_sites` | Top 100 sites ranked by Mozilla's `frecency` algorithm |

*Note: Session-restore (tabs) parsing is intentionally omitted to preserve the
zero-dependency requirement, as Mozilla uses a proprietary `jsonlz4` format.*

---

## Setup – First-Success Walkthrough

### Step 1 – (Optional) Identify your profile directory

If you have multiple Firefox profiles and want to pin the server to a specific
one, find the profile directory that contains `places.sqlite`:

If you have multiple Firefox installations, set `FIREFOX_PROFILE_PATH` to the
exact profile directory instead of relying on automatic discovery. The server
now requires that override when `profiles.ini` exposes more than one distinct
install-scoped default.

```
# macOS
ls ~/Library/Application\ Support/Firefox/Profiles/

# Linux
ls ~/.mozilla/firefox/

# Windows (PowerShell)
ls $env:APPDATA\Mozilla\Firefox\Profiles\
```

Note the full path to the profile you want (e.g.
`~/.mozilla/firefox/abc123.default-release`).

### Step 2 – Start the local browser server

Keep the full `sources/community/firefox/` directory available locally. Coral
installs the manifest, but it does not package, copy, or supervise
`browser_server.py`; you must start this server yourself before each query
session and keep it running while Coral queries.

Open a **dedicated terminal** and run from the `sources/community/firefox`
directory:

```bash
# Requires Python 3.8 or later.

# Pin to a specific profile (recommended if you have more than one):
export FIREFOX_PROFILE_PATH="~/.mozilla/firefox/abc123.default-release"
python3 browser_server.py
```

On Windows PowerShell:

```powershell
$env:FIREFOX_PROFILE_PATH = "$env:APPDATA\Mozilla\Firefox\Profiles\abc123.default-release"
py -3 browser_server.py
```

If Firefox keeps `places.sqlite` locked on Windows, close Firefox before
querying Coral.

If port `8766` is already in use, choose another loopback port and keep the
server and manifest configuration in sync:

```bash
# macOS / Linux
export FIREFOX_PORT=8767
export FIREFOX_BASE_URL=http://127.0.0.1:8767
python3 browser_server.py
```

```powershell
$env:FIREFOX_PORT = "8767"
$env:FIREFOX_BASE_URL = "http://127.0.0.1:8767"
py -3 browser_server.py
```

On first run the server prints a generated bearer token:

```
============================================================
No FIREFOX_API_KEY set. Generated a new token:
  <your-generated-64-character-token>

Export this value before running `coral source add firefox`:
  export FIREFOX_API_KEY=<your-generated-64-character-token>   # macOS/Linux
  $env:FIREFOX_API_KEY="<your-generated-64-character-token>"  # PowerShell
============================================================
Starting Firefox local server on http://127.0.0.1:8766
Resolved profile via install default metadata: <path-to-your-firefox-profile>
```

**Keep this terminal open.** The server must stay running while you query Coral.

### Step 3 – Export the bearer token

In a **second terminal**, export the token printed in Step 2 so that
`coral source add` can forward it in the manifest. If you changed the port,
export the same `FIREFOX_BASE_URL` here too:

```bash
# macOS / Linux
export FIREFOX_API_KEY=a3f8c1d2e4b5...
# Only needed when not using the default port:
export FIREFOX_BASE_URL=http://127.0.0.1:8767

# Windows PowerShell
$env:FIREFOX_API_KEY="a3f8c1d2e4b5..."
# Only needed when not using the default port:
$env:FIREFOX_BASE_URL="http://127.0.0.1:8767"
```

> **Tip:** To avoid retyping the token on every server restart, set
> `FIREFOX_API_KEY` to a fixed value in your shell profile (e.g.
> `~/.zshrc`) **and** export it before starting the server:
> ```bash
> export FIREFOX_API_KEY="my-fixed-secret-value"
> python3 browser_server.py
> ```

### Step 4 – Add the source to Coral

```bash
# From the sources/community/firefox/ directory:
coral source add --file ./manifest.yaml

# Or, from the repository root:
coral source add --file ./sources/community/firefox/manifest.yaml
```

This command registers the manifest and stores the input values. It does not
install or start `browser_server.py`; leave the Step 2 terminal running for
tests and queries.

### Step 5 – Verify the source

```bash
coral source test firefox
```

Expected output (abridged):

```
  ✓ firefox connected successfully
  Secrets: keychain

    firefox (4 tables)
    ├─ bookmarks
    ├─ extensions
    ├─ history
    └─ top_sites
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, title FROM firefox.bookmarks LIMIT 1
      1 row
```

If the command reports an error, check that:
- The server is still running (Step 2 terminal).
- `FIREFOX_API_KEY` is exported in **this** terminal and matches the
  value printed by the server (Step 3).
- If you changed ports, `FIREFOX_BASE_URL` was exported before running
  `coral source add` and matches the running server.
- The profile path contains `places.sqlite`.
- On Windows, close Firefox if the server reports that `places.sqlite` is
  locked or unavailable.

If Firefox profile metadata cannot be resolved, is ambiguous, or `places.sqlite`
is missing, `coral source test firefox` fails with a clear error instead of
returning an empty success.

### Step 6 – Run a representative query

```sql
-- 10 most recently visited URLs from the bounded recent-history table
SELECT title, url, last_visit_date
FROM firefox.history
ORDER BY last_visit_date DESC
LIMIT 10;
```

```sql
-- A deliberate server-side slice by timestamp and URL substring
SELECT title, url, visit_count, last_visit_date
FROM firefox.history_slice(
  limit => 1000,
  after => '2026-01-01T00:00:00Z',
  url => 'mozilla'
)
ORDER BY last_visit_date DESC;
```

```sql
-- All bookmarks added in the last 30 days
SELECT title, url, date_added
FROM firefox.bookmarks
WHERE date_added >= NOW() - INTERVAL '30 days'
ORDER BY date_added DESC;
```

### Example Queries and Outputs

Here is evidence of the source successfully querying and truncating real local data:

**1. Latest Bookmarks (Truncated)**
To see your most recently added bookmarks first, sort by `date_added` in descending (`DESC`) order:

```powershell
coral sql "SELECT SUBSTR(title, 1, 40) AS title, type, date_added FROM firefox.bookmarks ORDER BY date_added DESC LIMIT 3;"
```
```
+-----------------+--------+--------------------------+
| title           | type   | date_added               |
+-----------------+--------+--------------------------+
| Sony Blog       | url    | 2026-05-31T12:49:02.811Z |
| Sony            | folder | 2026-05-31T12:49:02.811Z |
| SonyStyle Store | url    | 2026-05-31T12:49:02.811Z |
+-----------------+--------+--------------------------+
```

**2. Absolute Top Sites**
Top sites are ranked by Mozilla's frecency algorithm. To see the most visited sites first, sort by `url_rank` in ascending (`ASC`) order (where rank 1 is highest):

```powershell
coral sql "SELECT url_rank, SUBSTR(title, 1, 30) AS title, SUBSTR(url, 1, 40) AS url FROM firefox.top_sites ORDER BY url_rank ASC LIMIT 3;"
```
```
+----------+--------------------------------+------------------------------------------+
| url_rank | title                          | url                                      |
+----------+--------------------------------+------------------------------------------+
| 1        | Mozilla accounts               | https://accounts.firefox.com/settings    |
| 2        | Inbox (12) - example.user      | https://gmail.com/                       |
| 3        | Privacy Badger – Get this Exte | https://addons.mozilla.org/en-US/firefox |
+----------+--------------------------------+------------------------------------------+
```

**3. Installed Extensions (Alphabetical)**
Since extensions don't have timestamps, you can sort them alphabetically by `name` using `ASC`:

```powershell
coral sql "SELECT SUBSTR(name, 1, 40) AS name, version FROM firefox.extensions ORDER BY name ASC LIMIT 3;"
```
```
+----------------------------------+---------+
| name                             | version |
+----------------------------------+---------+
| Add-ons Search Detection         | 3.0.0   |
| Data Leak Blocker                | 144.0.0 |
| Firefox Multi-Account Containers | 8.3.7   |
+----------------------------------+---------+
```

**4. Server-Side History Search**
Use the `history_slice` function to push filters (like searching for 'mozilla' in the URL) directly to the local Python server:

```powershell
coral sql "SELECT SUBSTR(title, 1, 30) AS title, visit_count FROM firefox.history_slice(url => 'mozilla', limit => 3);"
```
```
+-----------------------------+-------------+
| title                       | visit_count |
+-----------------------------+-------------+
| Set up Firefox sync | Mozil | 1           |
| Set up Firefox sync | Mozil | 1           |
| Set up Firefox sync | Mozil | 1           |
+-----------------------------+-------------+
```

**5. SQL Aggregations**
You can run standard SQL aggregations, such as counting how many bookmark folders you have versus actual saved URLs:

```powershell
coral sql "SELECT type, COUNT(*) AS total_count FROM firefox.bookmarks GROUP BY type LIMIT 3;"
```
```
+--------+-------------+
| type   | total_count |
+--------+-------------+
| folder | 12          |
| url    | 60          |
+--------+-------------+
```

### Step 7 – Stop the server when done

Return to the first terminal and press **Ctrl+C**:

```
^C
Shutting down server.
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `FIREFOX_API_KEY` | Bearer token for the local HTTP server. If unset, a random token is generated at startup and printed to stdout. |
| `FIREFOX_PORT` | Loopback port for `browser_server.py`. Defaults to `8766`. If changed, set `FIREFOX_BASE_URL` to the same port before `coral source add`. |
| `FIREFOX_BASE_URL` | Base URL for both the server's Host/Origin checks and the Coral manifest input, e.g. `http://127.0.0.1:8767`. Must include an explicit port. Defaults to `http://127.0.0.1:8766`. |
| `FIREFOX_PROFILE_PATH` | Full path to a specific Firefox profile directory (must contain `places.sqlite`). Overrides automatic profile detection. |
| `FIREFOX_BASE_PATH` | Full path to the Firefox configuration directory that contains `profiles.ini`. Overrides the platform default used by the profile scanner. |
| `FIREFOX_PROFILES_PATH` | Backward-compatible alias for `FIREFOX_BASE_PATH`. If you point it at a `Profiles` directory whose parent contains `profiles.ini`, the server automatically uses that parent directory. |

---

# Contributions by github.com/GaneshBamalwa and github.com/Vishy-MK
