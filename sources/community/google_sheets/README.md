# Google Sheets (Community)

**Version:** 0.1.0
**Backend:** HTTP (Google Sheets API v4)
**Tables:** 3 · **Functions:** 1
**Base URL:** `https://sheets.googleapis.com`

Query Google Sheets spreadsheet metadata, sheet structure, named ranges, and
cell values via the Sheets API v4. Authenticates with Google OAuth
(authorization_code + PKCE). Join with GitHub, Linear, or Notion sources to
correlate spreadsheet data with engineering activity.

## Install

```bash
coral source add --file sources/community/google_sheets/manifest.yaml
```

## Authentication and setup

Requires a Google Sheets OAuth client ID and client secret with the
`https://www.googleapis.com/auth/spreadsheets.readonly` scope.

1. In [Google Cloud Console](https://console.cloud.google.com), create an
   OAuth 2.0 client ID (Desktop app type) under **APIs & Services -> Credentials**.
2. Enable the **Google Sheets API** under **APIs & Services -> Library**.
3. Run the interactive setup:

```bash
coral source add --interactive --file sources/community/google_sheets/manifest.yaml
```

Choose **Connect with Google**, then paste your `GOOGLE_SHEETS_OAUTH_CLIENT_ID` and
`GOOGLE_SHEETS_OAUTH_CLIENT_SECRET` when prompted. Coral completes the PKCE OAuth
flow locally, stores the token in the system keyring as
`GOOGLE_SHEETS_ACCESS_TOKEN`, and automatically refreshes it. The authorization
request asks Google for offline access and a refresh token.

> **Reusing a Google OAuth client:** these inputs use source-specific keys
> (`GOOGLE_SHEETS_OAUTH_*`), matching the per-source convention the Gmail source
> uses (`GMAIL_OAUTH_*`). If you already created a Google Cloud OAuth client for
> another Google source, you do not need a new one: enable the Google Sheets API
> on that same Cloud project and enter the existing client ID and secret under the
> `GOOGLE_SHEETS_OAUTH_*` keys.

To paste an access token directly instead:

```bash
export GOOGLE_SHEETS_ACCESS_TOKEN=ya29.a0...
coral source add --file sources/community/google_sheets/manifest.yaml
```

Manually pasted access tokens must be replaced when they expire.

## Tables and functions

### Metadata tables

| Table | Required filters | Description |
| --- | --- | --- |
| `spreadsheet_info` | `spreadsheet_id` | Title, locale, time zone, and recalc interval |
| `sheets` | `spreadsheet_id` | All sheet tabs with ID, index, type, and grid size |
| `named_ranges` | `spreadsheet_id` | Named ranges with sheet location and row/column bounds |

### Table functions

| Function | Required args | Description |
| --- | --- | --- |
| `get_values(spreadsheet_id, range)` | both | Cell values for an A1-notation range |

The `spreadsheet_id` is the value between `/d/` and `/edit` in the spreadsheet
URL: `https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit`.
`get_values` accepts a sheet-qualified A1 range such as `Sheet1!A1:D10`, an
unqualified range such as `A1:D10`, or a named range. It always requests rows as
the major dimension, so each result row contains one JSON array in `row_data`.

## Validation

The test queries use Google's publicly readable sample spreadsheet
(`1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms`, from the
[Sheets API quick start](https://developers.google.com/workspace/sheets/api/quickstart/go))
so any configured token with the `spreadsheets.readonly` scope can run them.

```bash
# Add interactively (output sanitized — token redacted)
coral source add --interactive --file sources/community/google_sheets/manifest.yaml
# coral source test google_sheets produces the same output
```

```text
  ✓ google_sheets connected successfully
  Secrets: keyring

    google_sheets (3 tables, 1 function)
    ├─ named_ranges
    ├─ sheets
    ├─ spreadsheet_info
    └─ get_values (function)

    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT spreadsheet_id, title, locale, time_zone
        FROM google_sheets.spreadsheet_info
        WHERE spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms'
      1 row

    ✓ SELECT spreadsheet_id, range, row_data
        FROM google_sheets.get_values('1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms', 'Class Data!A1:E2')
        LIMIT 2
      2 rows
```

```bash
# spreadsheet_info (output sanitized)
coral sql "SELECT spreadsheet_id, title, locale, time_zone, auto_recalc FROM google_sheets.spreadsheet_info WHERE spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms'"
```

```text
+----------------------------------------------+--------------------+--------+------------------+-------------+
| spreadsheet_id                               | title              | locale | time_zone        | auto_recalc |
+----------------------------------------------+--------------------+--------+------------------+-------------+
| 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms | Student Sample Data | en_US  | America/New_York | ON_CHANGE   |
+----------------------------------------------+--------------------+--------+------------------+-------------+
1 row
```

```bash
# sheets — list all tabs (output sanitized)
coral sql "SELECT sheet_id, title, index, sheet_type, row_count, column_count FROM google_sheets.sheets WHERE spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms' ORDER BY index"
```

```text
+----------+------------+-------+------------+-----------+--------------+
| sheet_id | title      | index | sheet_type | row_count | column_count |
+----------+------------+-------+------------+-----------+--------------+
| 0        | Class Data | 0     | GRID       | 1000      | 26           |
+----------+------------+-------+------------+-----------+--------------+
1 row
```

```bash
# named_ranges (output sanitized — this spreadsheet has no named ranges)
coral sql "SELECT named_range_id, name, sheet_id, start_row_index, end_row_index FROM google_sheets.named_ranges WHERE spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms'"
```

```text
+----------------+------+----------+-----------------+---------------+
| named_range_id | name | sheet_id | start_row_index | end_row_index |
+----------------+------+----------+-----------------+---------------+
(0 rows — this spreadsheet has no named ranges)
```

```bash
# get_values — header row and first data row (output sanitized)
coral sql "SELECT spreadsheet_id, range, json_get_str(row_data, 0) AS col_a, json_get_str(row_data, 1) AS col_b, json_get_str(row_data, 2) AS col_c FROM google_sheets.get_values('1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms', 'Class Data!A1:E2') LIMIT 2"
```

```text
+----------------------------------------------+-------------------+--------------+--------+-------------+
| spreadsheet_id                               | range             | col_a        | col_b  | col_c       |
+----------------------------------------------+-------------------+--------------+--------+-------------+
| 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms | Class Data!A1:E2  | Student Name | Gender | Class Level |
| 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms | Class Data!A1:E2  | Alexandra    | Female | 4. Senior   |
+----------------------------------------------+-------------------+--------------+--------+-------------+
2 rows
```

## Example queries

### Spreadsheet metadata

```sql
SELECT spreadsheet_id, title, locale, time_zone
FROM google_sheets.spreadsheet_info
WHERE spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms';
```

### List all sheet tabs

```sql
SELECT sheet_id, title, index, sheet_type, row_count, column_count
FROM google_sheets.sheets
WHERE spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms'
ORDER BY index;
```

### Visible tabs only

```sql
SELECT sheet_id, title, index
FROM google_sheets.sheets
WHERE spreadsheet_id = 'YOUR_SPREADSHEET_ID'
  AND (hidden = false OR hidden IS NULL)
ORDER BY index;
```

### Cell values for a range

```sql
SELECT spreadsheet_id, range, row_data
FROM google_sheets.get_values('1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms', 'Class Data!A1:E10')
LIMIT 10;
```

Extract individual cells by column index:

```sql
SELECT
  json_get_str(row_data, 0) AS student_name,
  json_get_str(row_data, 1) AS gender,
  json_get_str(row_data, 2) AS class_level
FROM google_sheets.get_values('1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms', 'Class Data!A2:E30')
LIMIT 20;
```

### Named ranges

```sql
SELECT name, sheet_id, start_row_index, end_row_index, start_column_index, end_column_index
FROM google_sheets.named_ranges
WHERE spreadsheet_id = 'YOUR_SPREADSHEET_ID';
```

### Join sheet structure with named range bounds

```sql
SELECT nr.name, s.title AS sheet_title, nr.start_row_index, nr.end_row_index
FROM google_sheets.named_ranges nr
JOIN google_sheets.sheets s
  ON nr.sheet_id = s.sheet_id AND nr.spreadsheet_id = s.spreadsheet_id
WHERE nr.spreadsheet_id = 'YOUR_SPREADSHEET_ID';
```

## Cross-source examples

### Correlate spreadsheet roster with Linear issue assignments

```sql
SELECT
  json_get_str(rv.row_data, 0) AS engineer_name,
  json_get_str(rv.row_data, 1) AS team,
  COUNT(li.id) AS open_issues
FROM google_sheets.get_values('SPREADSHEET_ID', 'Roster!A2:B50') rv
JOIN linear.issues li ON LOWER(json_get_str(rv.row_data, 0)) = LOWER(li.assignee_name)
WHERE li.state_type != 'completed'
GROUP BY 1, 2
ORDER BY open_issues DESC
LIMIT 20;
```

### Match a spreadsheet task list against open GitHub pull requests

`github.pulls` requires constant `owner` and `repo` filters, so scope it to a
single repo and join on the PR title against a spreadsheet column:

```sql
SELECT
  json_get_str(rv.row_data, 0) AS task_name,
  pr.title AS matching_pr,
  pr.state
FROM google_sheets.get_values('SPREADSHEET_ID', 'Tasks!A2:A30') rv
JOIN github.pulls pr
  ON LOWER(pr.title) LIKE '%' || LOWER(json_get_str(rv.row_data, 0)) || '%'
WHERE pr.owner = 'your-org'
  AND pr.repo = 'your-repo'
  AND pr.state = 'open'
ORDER BY task_name
LIMIT 20;
```

## Limitations

- **Read-only** — this source exposes read-only Sheets API endpoints only.
- **Cell values as JSON arrays** — `get_values` returns each row as a JSON
  array of strings; individual cells require `json_get_str(row_data, N)`.
- **No formula expressions** — `valueRenderOption: FORMATTED_VALUE` returns
  displayed strings, not underlying formulas.
- **No batch ranges** — `get_values` fetches one range per call.
- **Returned range and major dimension** — the Sheets API returns `range` and
  `majorDimension` as response-envelope fields outside the per-row `values`
  array. These cannot be surfaced as additional per-row columns by the Coral DSL.
  The `range` and `spreadsheet_id` columns on `get_values` echo the function
  arguments (the requested range), not the API-returned range. `majorDimension`
  is always `ROWS` since the request hardcodes that value.
- **Quota** — the Sheets API enforces per-project and per-user request quotas.
  See [Sheets API usage limits](https://developers.google.com/workspace/sheets/api/limits).
- Community sources are maintained separately from bundled core sources.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the linked issue
first, sign the CLA if this is your first contribution, run `make lint-sources`,
and open a focused PR titled
`feat(sources/community/google_sheets): add google sheets community source`.
