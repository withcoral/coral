# Google Sheets

**Version:** 0.1.0
**Backend:** File (JSONL)
**Tables:** 2

Query Google Sheets data from local JSONL files. Extract spreadsheet rows with proper column headers and sheet metadata through SQL.

## Installation

1. Set your API key in the environment and run the converter script to fetch spreadsheet data:

```bash
export GOOGLE_SHEETS_API_KEY=YOUR_KEY
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --spreadsheet-id YOUR_SPREADSHEET_ID
```

2. Install the source:

```bash
coral source add --file sources/community/google_sheets/manifest.yaml
```

## Prerequisites

- Python 3.8+ (no external dependencies — uses only stdlib)
- Google Sheets API key from [Google Cloud Console](https://console.cloud.google.com)
- Spreadsheet must be shared as **"Anyone with the link"** (public read access)

**Getting and restricting an API key:**

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create or select a project
3. Enable the **Google Sheets API**
4. Go to **Credentials** > **Create Credentials** > **API Key**
5. Copy the key, then **restrict it to the Google Sheets API only**:
   - Open the key's edit page
   - Under **API restrictions**, choose **Restrict key**
   - Select only **Google Sheets API**
   - Save

Restricting the key limits blast radius if the key ever leaks.

## Providing the API key

Prefer the most secure option that fits your environment. The script checks
options in this order and uses the first one that is set:

| Priority | Option | Recommended for |
| --- | --- | --- |
| 1 | `--api-key-file <path>` | CI, shared runners, scheduled jobs |
| 2 | `$GOOGLE_SHEETS_API_KEY` env var | Local shells, scripts |
| 3 | `--api-key YOUR_KEY` flag | One-off invocations (visible in shell history) |

The key is always sent as the `X-Goog-Api-Key` request header, never as a
URL query parameter, per [Google's API key best practices](https://docs.cloud.google.com/docs/authentication/api-keys-best-practices#avoid_using_query_parameters_to_provide_your_api_key_to_google_apis).

Examples:

```bash
# CI / scheduled job
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --api-key-file ~/.keys/sheets.key \
  --spreadsheet-id YOUR_SPREADSHEET_ID

# Local shell
export GOOGLE_SHEETS_API_KEY=YOUR_KEY
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --spreadsheet-id YOUR_SPREADSHEET_ID

# One-off
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --api-key YOUR_KEY --spreadsheet-id YOUR_SPREADSHEET_ID
```

## Quick Start

```sql
-- List all rows with full data
SELECT _sheet_name, _row_number, data
FROM google_sheets.rows
LIMIT 10;

-- Extract specific fields from the data column
SELECT
  json_as_text(data, 'app_name') AS app,
  json_as_text(data, 'category') AS category,
  json_as_text(data, 'pricing') AS pricing
FROM google_sheets.rows
LIMIT 10;

-- Filter by sheet name
SELECT _row_number, data
FROM google_sheets.rows
WHERE _sheet_name = 'App_Master'
LIMIT 10;

-- View sheet metadata
SELECT _spreadsheet_title, sheet_name, row_count, column_count
FROM google_sheets.sheets;
```

## Converter Usage

```bash
# Fetch all sheets from a spreadsheet
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --spreadsheet-id SHEET_ID

# Fetch a specific sheet tab only
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --spreadsheet-id SHEET_ID --sheet "App_Master"

# Custom output directory
python3 sources/community/google_sheets/scripts/sheets-to-jsonl.py \
  --spreadsheet-id SHEET_ID --output /path/to/output
```

Default output directory: `~/.coral/google_sheets/`

**Note:** The `--output` option writes to a custom path, but the manifest reads from `~/.coral/google_sheets/`. Update the manifest `source.location` if using a custom path.

## Tables

### `rows`

Data rows from Google Sheets with column headers as keys in a JSON `data` column. Each row includes spreadsheet ID, sheet name, and row number for multi-sheet queries.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `_spreadsheet_id` | Utf8 | Google Spreadsheet ID |
| `_sheet_name` | Utf8 | Sheet tab name within the spreadsheet |
| `_row_number` | Int64 | Row number within the sheet (1-indexed, excluding header) |
| `data` | Json | Row data as a JSON object with column headers as keys |

Use Coral's JSON functions to extract specific fields:
```sql
SELECT json_as_text(data, 'column_name') FROM google_sheets.rows
```

---

### `sheets`

Metadata for each sheet tab in the spreadsheet.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `_spreadsheet_id` | Utf8 | Google Spreadsheet ID |
| `_spreadsheet_title` | Utf8 | Title of the spreadsheet |
| `sheet_name` | Utf8 | Name of the sheet tab |
| `sheet_id` | Int64 | Numeric ID of the sheet tab |
| `sheet_type` | Utf8 | Sheet type (GRID, OBJECT, etc.) |
| `row_count` | Int64 | Allocated grid rows (includes empty rows) |
| `column_count` | Int64 | Allocated grid columns (includes empty columns) |

## Source scope

- File-backed source reading from `~/.coral/google_sheets/rows.jsonl` and `~/.coral/google_sheets/sheets.jsonl`.
- No API key stored in Coral — the converter script uses the key at run time only.
- The converter uses Python stdlib only (`urllib`). No external dependencies.
- Data is static — re-run the converter script to refresh.
- The first row of each sheet is used as column headers for the `data` JSON object. If a data row is wider than the header row, missing headers are generated as `col_N` to avoid silently dropping cells (Google Sheets API omits trailing empty cells, so this matters for any sheet where the header is shorter than the data).
- Empty cells are represented as `null` in the JSON.
- 2 declared test queries (`rows` + `sheets`) require no filters.

## Limitations

- The spreadsheet must be publicly shared ("Anyone with the link" > Viewer). Private sheets require a service account, which is not supported in this version.
- Spreadsheet columns are stored inside a `data` JSON column — use `json_as_text(data, 'column_name')` to extract specific fields.
- The converter fetches all rows from the API in a single request. Very large sheets (100K+ rows) may be slow or hit API limits.
- Only GRID-type sheets are fetched. Charts, embedded objects, and other sheet types are skipped.
- Formulas are evaluated — the converter receives computed values, not formula text.
- The Google Sheets API has a read quota of 300 requests per minute per project and 60 requests per minute per user.

## Provider docs

- **[Google Sheets API reference](https://developers.google.com/sheets/api/reference/rest)** — REST methods, request/response schemas, and data types. The converter fetches sheet metadata and row content through the `spreadsheets.get` and `spreadsheets.values.get` endpoints.
- **[API credentials console](https://console.cloud.google.com/apis/credentials)** — Create and manage the API key required to authenticate Sheets API requests. Restrict the key to the Sheets API for security.
- **[Enable the Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com)** — Activate the Google Sheets API for your Google Cloud project so API keys can access spreadsheet data.
- **[A1 notation syntax](https://developers.google.com/workspace/sheets/api/guides/concepts#a1_notation)** — How spreadsheet ranges are specified (e.g., `Sheet1!A1:Z`). The source uses A1 notation from the `sheets.jsonl` fixture to identify which sheet tabs to read.
- **[API key security best practices](https://cloud.google.com/docs/authentication/api-keys-best-practices)** — Google's guidance on securing API keys, including preferring the `x-goog-api-key` HTTP header over query-parameter transmission.

## Validation output

Run `coral source add --file sources/community/google_sheets/manifest.yaml`
after generating a JSONL fixture with the converter script to verify
the source against your own spreadsheet. The output below was produced
from a synthetic demo fixture (`~/.coral/google_sheets/rows.jsonl` +
`~/.coral/google_sheets/sheets.jsonl`) that mimics the converter's output
shape.

### Regression tests

The converter's A1 range escaping and header normalization are covered by
fixture-based regression tests (synthetic demo data under `fixtures/`).
From the Coral repo root:

```bash
python3 sources/community/google_sheets/tests/validate-fixtures.py sources/community/google_sheets/fixtures
```

```text
OK a1_sheet_ref: plain, spaces, slashes, apostrophes
OK normalize_headers: numeric and boolean headers stringify
OK normalize_headers: widest-row preservation
OK normalize_headers: duplicate and literal/generated collisions
OK normalize_headers: empty cells and whitespace
OK fixtures: 5 rows, 1 sheet(s)
All google_sheets converter checks passed
```

### `coral source lint`

```bash
$ coral source lint sources/community/google_sheets/manifest.yaml
Manifest is valid
```

### `coral source add`

```bash
$ coral source add --file sources/community/google_sheets/manifest.yaml
Added source google_sheets (secrets: none)
Validating source...

  ✓ google_sheets connected successfully
  Secrets: none

    google_sheets (2 tables)
    ├─ rows
    └─ sheets
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT _spreadsheet_id, _sheet_name, _row_number, data FROM google_sheets.rows LIMIT 3
      3 rows

    ✓ SELECT _spreadsheet_title, sheet_name, row_count FROM google_sheets.sheets LIMIT 3
      1 row
```

### `coral source test`

```bash
$ coral source test google_sheets

  ✓ google_sheets connected successfully
  Secrets: none

    google_sheets (2 tables)
    ├─ rows
    └─ sheets
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT _spreadsheet_id, _sheet_name, _row_number, data FROM google_sheets.rows LIMIT 3
      3 rows

    ✓ SELECT _spreadsheet_title, sheet_name, row_count FROM google_sheets.sheets LIMIT 3
      1 row
```

### `coral source info`

```bash
$ coral source info google_sheets
google_sheets
  Status:      installed
  Origin:      imported
  Secrets:     file (plaintext)
  Version:     0.1.0
  Description: Query Google Sheets data from local JSONL files. Extract spreadsheet rows with proper column headers and sheet metadata through SQL.
```

### Row and sheet counts

```sql
SELECT COUNT(*) AS row_count, COUNT(DISTINCT _sheet_name) AS sheet_count
FROM google_sheets.rows;
```

```text
+-----------+-------------+
| row_count | sheet_count |
+-----------+-------------+
| 5         | 1           |
+-----------+-------------+
```

```sql
SELECT _spreadsheet_title, sheet_name, sheet_type, row_count, column_count
FROM google_sheets.sheets;
```

```text
+--------------------+------------+------------+-----------+--------------+
| _spreadsheet_title | sheet_name | sheet_type | row_count | column_count |
+--------------------+------------+------------+-----------+--------------+
| Demo Apps Catalog  | App_Master | GRID       | 5         | 21           |
+--------------------+------------+------------+-----------+--------------+
```

### Sample rows

```sql
SELECT _row_number,
       json_as_text(data, 'app_name') AS app,
       json_as_text(data, 'category') AS category,
       json_as_text(data, 'pricing')  AS pricing
FROM google_sheets.rows
ORDER BY _row_number;
```

```text
+-------------+------------+------------+----------+
| _row_number | app        | category   | pricing  |
+-------------+------------+------------+----------+
| 1           | Calendly   | scheduling | freemium |
| 2           | Acuity     | scheduling | paid     |
| 3           | HubSpot    | crm        | freemium |
| 4           | Salesforce | crm        | paid     |
| 5           | Zendesk    | support    | paid     |
+-------------+------------+------------+----------+
```

### Group queries

```sql
SELECT json_as_text(data, 'category') AS category, COUNT(*) AS apps
FROM google_sheets.rows GROUP BY category ORDER BY apps DESC;
```

```text
+------------+------+
| category   | apps |
+------------+------+
| crm        | 2    |
| scheduling | 2    |
| support    | 1    |
+------------+------+
```

### Filter query

```sql
SELECT json_as_text(data, 'app_name') AS app,
       json_as_text(data, 'category') AS category
FROM google_sheets.rows
WHERE json_as_text(data, 'pricing') = 'paid';
```

```text
+------------+------------+
| app        | category   |
+------------+------------+
| Acuity     | scheduling |
| Salesforce | crm        |
| Zendesk    | support    |
+------------+------------+
```

### Catalog introspection

```sql
SELECT schema_name, table_name, description
FROM coral.tables
WHERE schema_name = 'google_sheets'
ORDER BY table_name;
```

```text
+---------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| schema_name   | table_name | description                                                                                                                                                                               |
+---------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| google_sheets | rows       | Data rows from Google Sheets with column headers as field names. Each row includes the spreadsheet ID and sheet name for multi-sheet queries. Run the converter script first to populate. |
| google_sheets | sheets     | Metadata for each sheet tab in the spreadsheet, including name, type, and allocated grid dimensions.                                                                                      |
+---------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

```sql
SELECT table_name, column_name, data_type, is_virtual, description
FROM coral.columns
WHERE schema_name = 'google_sheets'
ORDER BY table_name, ordinal_position;
```

```text
+------------+--------------------+-----------+------------+------------------------------------------------------------+
| table_name | column_name        | data_type | is_virtual | description                                                |
+------------+--------------------+-----------+------------+------------------------------------------------------------+
| rows       | _spreadsheet_id    | Utf8      | false      | Google Spreadsheet ID.                                     |
| rows       | _sheet_name        | Utf8      | false      | Sheet tab name within the spreadsheet.                     |
| rows       | _row_number        | Int64     | false      | Row number within the sheet (1-indexed, excluding header). |
| rows       | data               | Json      | false      | Row data as a JSON object with column headers as keys.     |
| sheets     | _spreadsheet_id    | Utf8      | false      | Google Spreadsheet ID.                                     |
| sheets     | _spreadsheet_title | Utf8      | false      | Title of the spreadsheet.                                  |
| sheets     | sheet_name         | Utf8      | false      | Name of the sheet tab.                                     |
| sheets     | sheet_id           | Int64     | false      | Numeric ID of the sheet tab.                               |
| sheets     | sheet_type         | Utf8      | false      | Sheet type (GRID, OBJECT, etc.).                           |
| sheets     | row_count          | Int64     | false      | Allocated grid rows (includes empty rows).                 |
| sheets     | column_count       | Int64     | false      | Allocated grid columns (includes empty columns).           |
+------------+--------------------+-----------+------------+------------------------------------------------------------+
```