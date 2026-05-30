# Chromium Local Source

Query your local Chromium-based browser data (**Google Chrome**, **Microsoft Edge**, and **Brave**) using SQL through Coral.

Because browsers do not expose personal data through public REST APIs, this source uses a lightweight, zero-dependency local Python server. The server automatically locates active browser profiles, safely reads SQLite databases and JSON files, and serves the data to Coral through localhost.

> All processing happens entirely on your machine. No data ever leaves your computer.

---

# Features

| Feature | Description |
|----------|-------------|
| Multi-Browser Support | Query Chrome, Edge, and Brave seamlessly. |
| Deep Profile Scanning | Automatically detects the most recently used profile (Default, Profile 1, Profile 2, etc.). |
| Zero Dependencies | Uses only Python standard libraries. No additional packages required. |
| Graceful Fallbacks | If a browser is not installed, queries return an empty table instead of failing. |

---

# Extracted Data

This source extracts the following data across all supported browsers.

| Data Type | Description |
|------------|-------------|
| Bookmarks | Saved bookmarks, URLs, and folder structures |
| History | Browsing history, titles, visit counts, and timestamps |
| Downloads | Download history, file paths, and file sizes |
| Extensions | Installed browser extensions and versions |
| Top Sites | Browser-ranked frequently visited websites |
| Tabs | Currently open tabs from active browser sessions |

---

# Setup

## 1. Start the Local Browser Server

Run the included Python script and keep it running in the background while using Coral.

```bash
python sources/community/chromium/browser_server.py
```

The server runs locally at:

```text
http://127.0.0.1:8765
```

---

## 2. Add the Source to Coral

Register the source using the Coral CLI.

```bash
coral source add --file sources/community/chromium/manifest.yaml
```

No API keys or interactive prompts are required.

---

# Available Tables

Replace `[browser]` with:

- `chrome`
- `edge`
- `brave`

| Table | Description |
|---------|-------------|
| `chromium.[browser]_bookmarks` | Saved bookmarks, folders, and timestamps |
| `chromium.[browser]_history` | Browsing history with visit counts |
| `chromium.[browser]_downloads` | Download history and local file paths |
| `chromium.[browser]_extensions` | Installed extensions and versions |
| `chromium.[browser]_top_sites` | Most frequently visited websites |
| `chromium.[browser]_tabs` | Currently open browser tabs |

---

# Supported Browser Prefixes

| Browser | Prefix |
|----------|---------|
| Google Chrome | `chrome_` |
| Microsoft Edge | `edge_` |
| Brave Browser | `brave_` |

---

# Core Commands

## Lint the Source Manifest

Validates `manifest.yaml` for syntax and schema errors before installation.

```bash
./coral.exe source lint ./sources/community/chromium/manifest.yaml
```

## Add the Source to Coral

Registers the source and all its tables in your local Coral environment.

```bash
./coral.exe source add --file ./sources/community/chromium/manifest.yaml
```

## Remove the Source

Removes the source from Coral if you need to cleanly reset or delete it.

```bash
./coral.exe source remove chromium
```

---

# Example Queries

## Chrome Bookmarks

```sql
SELECT
    title,
    url,
    date_added
FROM chromium.chrome_bookmarks
WHERE type = 'url';
```

## Brave History

```sql
SELECT
    title,
    url,
    visit_count
FROM chromium.brave_history
ORDER BY visit_count DESC
LIMIT 10;
```

---

# Browser Query Commands

Run these commands directly from the Coral CLI to query browser data.

---

## Google Chrome

### View Bookmarks

```bash
./coral.exe sql "SELECT title, url, date_added FROM chromium.chrome_bookmarks LIMIT 5"
```

### View History (Sorted by Visits)

```bash
./coral.exe sql "SELECT title, url, visit_count FROM chromium.chrome_history ORDER BY visit_count DESC LIMIT 5"
```

### View Downloads

```bash
./coral.exe sql "SELECT target_path, received_bytes FROM chromium.chrome_downloads ORDER BY start_time DESC LIMIT 5"
```

### View Installed Extensions

```bash
./coral.exe sql "SELECT name, version FROM chromium.chrome_extensions"
```

### View Top Sites

```bash
./coral.exe sql "SELECT title, url_rank FROM chromium.chrome_top_sites LIMIT 5"
```

### View Active Open Tabs

```bash
./coral.exe sql "SELECT url FROM chromium.chrome_tabs"
```

---

## Microsoft Edge

### View Bookmarks

```bash
./coral.exe sql "SELECT title, url, date_added FROM chromium.edge_bookmarks LIMIT 5"
```

### View History (Sorted by Visits)

```bash
./coral.exe sql "SELECT title, url, visit_count FROM chromium.edge_history ORDER BY visit_count DESC LIMIT 5"
```

### View Downloads

```bash
./coral.exe sql "SELECT target_path, received_bytes FROM chromium.edge_downloads ORDER BY start_time DESC LIMIT 5"
```

### View Installed Extensions

```bash
./coral.exe sql "SELECT name, version FROM chromium.edge_extensions"
```

### View Top Sites

```bash
./coral.exe sql "SELECT title, url_rank FROM chromium.edge_top_sites LIMIT 5"
```

### View Active Open Tabs

```bash
./coral.exe sql "SELECT url FROM chromium.edge_tabs"
```

---

## Brave Browser

### View Bookmarks

```bash
./coral.exe sql "SELECT title, url, date_added FROM chromium.brave_bookmarks LIMIT 5"
```

### View History (Sorted by Visits)

```bash
./coral.exe sql "SELECT title, url, visit_count FROM chromium.brave_history ORDER BY visit_count DESC LIMIT 5"
```

### View Downloads

```bash
./coral.exe sql "SELECT target_path, received_bytes FROM chromium.brave_downloads ORDER BY start_time DESC LIMIT 5"
```

### View Installed Extensions

```bash
./coral.exe sql "SELECT name, version FROM chromium.brave_extensions"
```

### View Top Sites

```bash
./coral.exe sql "SELECT title, url_rank FROM chromium.brave_top_sites LIMIT 5"
```

### View Active Open Tabs

```bash
./coral.exe sql "SELECT url FROM chromium.brave_tabs"
```

---

# Advanced Analytics

## Largest Files Downloaded via Edge

```sql
SELECT
    target_path,
    total_bytes,
    start_time
FROM chromium.edge_downloads
ORDER BY total_bytes DESC
LIMIT 5;
```

## Installed Chrome Extensions

```sql
SELECT
    name,
    version
FROM chromium.chrome_extensions
ORDER BY name ASC;
```

## Currently Open Brave Tabs

```sql
SELECT
    url
FROM chromium.brave_tabs;
```

---

# Troubleshooting

## Query Returns 0 Rows

If a query returns an empty table:

- The browser may not be installed.
- The browser profile may not have been detected.
- The requested data file may be empty.

Check the terminal running `browser_server.py` for real-time logs showing which paths are being scanned.

## Schema Validation Errors

Ensure you are using the latest version of the Coral CLI.

```bash
coral update
```

or upgrade Coral using the installation method appropriate for your environment.

---

# Security & Privacy

| Aspect | Details |
|----------|----------|
| Data Processing | Entirely local |
| Network Access | Localhost only (`127.0.0.1`) |
| External APIs | None |
| Data Transmission | No browser data leaves your machine |
| Dependencies | Python standard library only |

This source is designed with a privacy-first approach, ensuring all browser data remains under your control.