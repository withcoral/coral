#!/usr/bin/env python3
"""Regression tests for the google_sheets converter script.

Covers the A1 tab-name escaping and header-normalization logic that needed
correctness fixes during review, plus fixture sanity checks, per the source
contribution testing expectations in CONTRIBUTING.md.
"""

import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE_DIR = HERE.parent
SCRIPT_PATH = SOURCE_DIR / "scripts" / "sheets-to-jsonl.py"


def load_script():
    spec = importlib.util.spec_from_file_location("sheets_to_jsonl", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def test_a1_sheet_ref(mod) -> None:
    require(mod.a1_sheet_ref("Sheet1") == "'Sheet1'", "plain name should be quoted")
    require(mod.a1_sheet_ref("App Master") == "'App Master'", "space in name must be quoted")
    require(mod.a1_sheet_ref("Q1/2026") == "'Q1/2026'", "slash in name must be quoted")
    require(
        mod.a1_sheet_ref("O'Brien") == "'O''Brien'",
        "apostrophe must be escaped by doubling",
    )
    require(
        mod.a1_sheet_ref("Sheet 'A' / 1") == "'Sheet ''A'' / 1'",
        "mixed quote/slash/space name must round-trip",
    )
    print("OK a1_sheet_ref: plain, spaces, slashes, apostrophes")


def test_normalize_headers_scalars(mod) -> None:
    headers = mod.normalize_headers([[2024, False, True, "b"], ["x", "y", "z", "w"]])
    require(
        headers == ["2024", "false", "true", "b"],
        f"scalar headers should stringify, got {headers}",
    )
    headers = mod.normalize_headers([[2024, False], ["a", "b"]])
    require(
        headers == ["2024", "false"],
        f"year/boolean headers must become JSON keys, got {headers}",
    )
    print("OK normalize_headers: numeric and boolean headers stringify")


def test_normalize_headers_widest_row(mod) -> None:
    headers = mod.normalize_headers([["a", "b"], ["c", "d", "e", "f"]])
    require(
        headers == ["a", "b", "col_2", "col_3"],
        f"widest-row positions should get col_N placeholders, got {headers}",
    )
    headers = mod.normalize_headers([["a"], ["x", "y", "z"]])
    require(
        headers == ["a", "col_1", "col_2"],
        f"short header + wide data should preserve cells, got {headers}",
    )
    print("OK normalize_headers: widest-row preservation")


def test_normalize_headers_collisions(mod) -> None:
    headers = mod.normalize_headers([["a", "a", "a", "col_2"], ["1", "2", "3", "4"]])
    require(
        headers == ["a", "a_1", "a_2", "col_2"],
        f"duplicate literal headers should be suffixed, got {headers}",
    )
    headers = mod.normalize_headers([["col_1"], ["x", "y"]])
    require(
        headers == ["col_1", "col_1_1"],
        f"literal/generated collision should be disambiguated, got {headers}",
    )
    print("OK normalize_headers: duplicate and literal/generated collisions")


def test_normalize_headers_empty_cells(mod) -> None:
    headers = mod.normalize_headers([[None, "a"], ["1", "2"]])
    require(
        headers == ["col_0", "a"],
        f"empty header cells should fall back to col_N, got {headers}",
    )
    headers = mod.normalize_headers([["  padded  ", "b"], ["x", "y"]])
    require(
        headers == ["padded", "b"],
        f"string headers should be stripped, got {headers}",
    )
    print("OK normalize_headers: empty cells and whitespace")


def validate_fixture_files() -> None:
    fixture_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "fixtures"
    rows_path = fixture_dir / "rows.jsonl"
    sheets_path = fixture_dir / "sheets.jsonl"
    if not rows_path.exists() or not sheets_path.exists():
        raise FileNotFoundError(f"fixture files missing in {fixture_dir}")
    rows = [json.loads(line) for line in rows_path.read_text().splitlines() if line.strip()]
    sheets = [json.loads(line) for line in sheets_path.read_text().splitlines() if line.strip()]
    require(len(rows) > 0, "rows.jsonl has no rows")
    require(len(sheets) > 0, "sheets.jsonl has no rows")
    for row in rows:
        require("_spreadsheet_id" in row and "_sheet_name" in row and "_row_number" in row,
                "rows row missing required key")
        require(isinstance(row.get("data"), dict), "rows data must be a JSON object")
    for sheet in sheets:
        require("_spreadsheet_title" in sheet and "sheet_name" in sheet,
                "sheets row missing required key")
        require("row_count" in sheet and "column_count" in sheet,
                "sheets row missing grid dimensions")
    require(len(rows) == sheets[0]["row_count"],
            "rows count must match sheets.grid row_count")
    print(f"OK fixtures: {len(rows)} rows, {len(sheets)} sheet(s)")


def main() -> None:
    mod = load_script()
    test_a1_sheet_ref(mod)
    test_normalize_headers_scalars(mod)
    test_normalize_headers_widest_row(mod)
    test_normalize_headers_collisions(mod)
    test_normalize_headers_empty_cells(mod)
    validate_fixture_files()
    print("All google_sheets converter checks passed")


if __name__ == "__main__":
    main()
