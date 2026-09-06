#!/usr/bin/env python3
"""Fetch Google Sheets data and write JSONL for the Coral google_sheets source.

Uses only Python stdlib (urllib). No external dependencies.

Usage:
    python3 sheets-to-jsonl.py --spreadsheet-id SHEET_ID
    python3 sheets-to-jsonl.py --api-key YOUR_KEY --spreadsheet-id SHEET_ID
    python3 sheets-to-jsonl.py --api-key-file ~/.keys/sheets.key --spreadsheet-id SHEET_ID
    GOOGLE_SHEETS_API_KEY=... python3 sheets-to-jsonl.py --spreadsheet-id SHEET_ID

Credential precedence (first non-empty wins):
    --api-key-file <path>   read key from a file (recommended for CI)
    $GOOGLE_SHEETS_API_KEY  environment variable (recommended for local use)
    --api-key <key>         inline flag (least preferred; visible in shell history)

The key is sent as the `X-Goog-Api-Key` request header, never as a URL
query parameter, per
https://docs.cloud.google.com/docs/authentication/api-keys-best-practices#avoid_using_query_parameters_to_provide_your_api_key_to_google_apis
Restrict the key to the Google Sheets API only in the Cloud Console
(APIs & Services > Credentials > Application restrictions > API restrictions).

Output:
    rows.jsonl   — one row per data row with column headers as keys
    sheets.jsonl — one row per sheet tab with metadata
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

DEFAULT_OUTPUT = os.path.expanduser("~/.coral/google_sheets")
API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"


def fetch_json(url, headers=None):
    try:
        req = Request(url, headers=headers or {})
        resp = urlopen(req, timeout=30)
        return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            err = json.loads(body)
            msg = err.get("error", {}).get("message", body)
        except (json.JSONDecodeError, KeyError):
            msg = body
        print(f"  ✗ API error ({exc.code}): {msg}", file=sys.stderr)
        return None
    except URLError as exc:
        print(f"  ✗ Connection error: {exc.reason}", file=sys.stderr)
        return None


def a1_sheet_ref(sheet_name):
    """Wrap a sheet title in single quotes per Google A1 notation rules.

    A1 notation requires sheet names containing spaces, special characters,
    or starting with a digit to be wrapped in single quotes. Internal single
    quotes are escaped by doubling. The resulting reference is returned
    unencoded so callers can URL-encode the whole segment as one path part.

    https://developers.google.com/workspace/sheets/api/guides/concepts#a1_notation
    """
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


def build_headers(api_key):
    return {"X-Goog-Api-Key": api_key}


def fetch_metadata(spreadsheet_id, api_key):
    url = (
        f"{API_BASE}/{quote(spreadsheet_id)}"
        f"?fields=spreadsheetId,properties.title,sheets.properties"
    )
    return fetch_json(url, headers=build_headers(api_key))


def fetch_values(spreadsheet_id, sheet_name, api_key):
    ref = a1_sheet_ref(sheet_name)
    url = (
        f"{API_BASE}/{quote(spreadsheet_id)}"
        f"/values/{quote(ref, safe='')}"
    )
    return fetch_json(url, headers=build_headers(api_key))


def scalar_to_key(value):
    """Convert a cell value to a stable header key.

    Google Sheets cell values are strings, numbers, or booleans; empty cells
    arrive as None. Strings are stripped, numbers and booleans are stringified
    (e.g. a `2024` year header becomes the `"2024"` JSON key) so the keys match
    what users see in the sheet, and empty values fall back to a placeholder.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value.strip()
    return str(value)


def normalize_headers(values):
    """Build a collision-safe header list sized to the widest returned row.

    Google omits trailing empty cells from returned rows, so a header row
    shorter than the data rows silently loses those cells. We size the header
    list to the widest returned row, generate stable placeholder names
    (`col_N`) for any position the header row did not cover, and disambiguate
    any duplicates (literal or generated) with a numeric suffix.
    """
    max_width = max((len(r) for r in values), default=0)
    used = set()
    normalized = []
    for i in range(max_width):
        if i < len(values[0]):
            base = scalar_to_key(values[0][i])
            key = base if base else f"col_{i}"
        else:
            key = f"col_{i}"
        if key in used:
            suffix = 1
            while f"{key}_{suffix}" in used:
                suffix += 1
            key = f"{key}_{suffix}"
        used.add(key)
        normalized.append(key)
    return normalized


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Google Sheets data to JSONL for Coral"
    )
    parser.add_argument(
        "--api-key", default=None,
        help="Google Sheets API key (least preferred; visible in shell history).",
    )
    parser.add_argument(
        "--api-key-file", default=None,
        help="Path to a file containing the API key (recommended for CI).",
    )
    parser.add_argument(
        "--spreadsheet-id", required=True,
        help="Google Spreadsheet ID from the URL.",
    )
    parser.add_argument(
        "--sheet", default=None,
        help="Specific sheet tab name (default: all sheets).",
    )
    parser.add_argument(
        "--output", "-o", default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    api_key = None
    if args.api_key_file:
        key_path = Path(args.api_key_file)
        if not key_path.is_file():
            print(
                f"  ✗ --api-key-file not found: {args.api_key_file}",
                file=sys.stderr,
            )
            sys.exit(1)
        api_key = key_path.read_text().strip()
    if not api_key:
        api_key = os.environ.get("GOOGLE_SHEETS_API_KEY", "").strip()
    if not api_key:
        api_key = (args.api_key or "").strip()
    if not api_key:
        print(
            "  ✗ No API key provided. Use --api-key-file, $GOOGLE_SHEETS_API_KEY,"
            " or --api-key.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"  Fetching metadata for {args.spreadsheet_id}...")
    meta = fetch_metadata(args.spreadsheet_id, api_key)
    if meta is None:
        sys.exit(1)

    title = meta.get("properties", {}).get("title", "untitled")
    all_sheets = meta.get("sheets", [])
    print(f"  Spreadsheet: {title} ({len(all_sheets)} sheets)")

    if args.sheet:
        target_sheets = [
            s for s in all_sheets
            if s["properties"]["title"] == args.sheet
        ]
        if not target_sheets:
            names = [s["properties"]["title"] for s in all_sheets]
            print(
                f"  ✗ Sheet '{args.sheet}' not found. Available: {names}",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        target_sheets = all_sheets

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = output_dir / "rows.jsonl"
    sheets_path = output_dir / "sheets.jsonl"

    tmp_rows = tempfile.NamedTemporaryFile(
        mode="w", dir=output_dir, suffix=".jsonl", delete=False,
    )
    tmp_sheets = tempfile.NamedTemporaryFile(
        mode="w", dir=output_dir, suffix=".jsonl", delete=False,
    )

    total_rows = 0
    fail_count = 0

    try:
        for sheet_info in target_sheets:
            props = sheet_info["properties"]
            sheet_name = props["title"]
            sheet_type = props.get("sheetType", "GRID")
            grid = props.get("gridProperties", {})

            sheet_meta = {
                "_spreadsheet_id": args.spreadsheet_id,
                "_spreadsheet_title": title,
                "sheet_name": sheet_name,
                "sheet_id": props.get("sheetId"),
                "sheet_type": sheet_type,
                "row_count": grid.get("rowCount"),
                "column_count": grid.get("columnCount"),
            }
            tmp_sheets.write(json.dumps(sheet_meta) + "\n")

            if sheet_type != "GRID":
                print(f"  → {sheet_name} (skipping, type={sheet_type})")
                continue

            print(f"  → {sheet_name}")
            data = fetch_values(args.spreadsheet_id, sheet_name, api_key)
            if data is None:
                fail_count += 1
                continue

            values = data.get("values", [])
            if len(values) < 2:
                print(f"    (empty or header-only, skipping)")
                continue

            normalized_headers = normalize_headers(values)
            if len(normalized_headers) != len(values[0]):
                generated = len(normalized_headers) - len(values[0])
                print(
                    f"    (header row had {len(values[0])} columns;"
                    f" widened to {len(normalized_headers)} with {generated}"
                    f" generated name(s))"
                )

            for row_idx, row_values in enumerate(values[1:], start=1):
                row_data = {}
                for i, key in enumerate(normalized_headers):
                    row_data[key] = (
                        row_values[i] if i < len(row_values) else None
                    )
                row = {
                    "_spreadsheet_id": args.spreadsheet_id,
                    "_sheet_name": sheet_name,
                    "_row_number": row_idx,
                    "data": row_data,
                }
                tmp_rows.write(json.dumps(row) + "\n")
                total_rows += 1

        tmp_rows.close()
        tmp_sheets.close()

        if fail_count > 0:
            os.unlink(tmp_rows.name)
            os.unlink(tmp_sheets.name)
            print(f"\n  ✗ {fail_count} sheets failed — existing files preserved",
                  file=sys.stderr)
            sys.exit(1)

        shutil.move(tmp_rows.name, rows_path)
        shutil.move(tmp_sheets.name, sheets_path)

    except BaseException:
        tmp_rows.close()
        tmp_sheets.close()
        for f in [tmp_rows.name, tmp_sheets.name]:
            if os.path.exists(f):
                os.unlink(f)
        raise

    print(f"\n  ✓ {total_rows} rows → {rows_path}")
    print(f"  ✓ {len(target_sheets)} sheets → {sheets_path}")


if __name__ == "__main__":
    main()