//! End-to-end coverage for structured `DataFusion` error enrichment.
//!
//! Verifies that `coral-engine` promotes `DataFusionError::SchemaError` and
//! `DataFusionError::Plan` table-not-found variants into
//! `CoreError::QueryFailure` with case-aware hints.

use std::path::Path;

use coral_engine::{CoralQuery, CoreError, StatusCode, StructuredQueryError};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{TestRuntime, build_source, dir_url, write_jsonl_file};

fn manifest(name: &str, table: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": table,
            "description": "Structured-error fixture",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "playerID", "type": "Utf8" }
            ]
        }]
    })
}

fn structured(error: CoreError) -> StructuredQueryError {
    match error {
        CoreError::QueryFailure(sqe) => *sqe,
        other => panic!("expected CoreError::QueryFailure, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_table_in_installed_schema_suggests_case_preserved_quoted_name() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "rows.jsonl",
        &[json!({"id": 1, "playerID": "ov8"})],
    );
    let source = build_source(manifest("hockey", "Master", temp.path()));

    // DataFusion lowercases the unquoted `Master` to `master`, which won't
    // match our case-preserving `Master` table in the catalog.
    let error = CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM hockey.Master")
        .await
        .expect_err("unknown table should fail");

    let sqe = structured(error);
    assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
    assert_eq!(sqe.status(), StatusCode::NotFound);
    let hint = sqe.hint().expect("hint should be present");
    assert!(
        hint.contains("hockey.\"Master\""),
        "hint should suggest case-preserving quoted form, got: {hint}"
    );
}

#[tokio::test]
async fn unknown_table_missing_schema_points_at_coral_tables_catalog() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "rows.jsonl",
        &[json!({"id": 1, "playerID": "ov8"})],
    );
    let source = build_source(manifest("hockey", "games", temp.path()));

    let error = CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM nba.games")
        .await
        .expect_err("unknown schema should fail");

    let sqe = structured(error);
    assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
    let hint = sqe.hint().expect("hint should be present");
    // The engine layer is transport-neutral: the hint must not assume a
    // particular surface (CLI / MCP / API). SQL catalog queries work
    // uniformly across all of them.
    assert!(hint.contains("coral.tables"), "got: {hint}");
    assert!(
        !hint.contains("coral source"),
        "engine hint must not embed a CLI command, got: {hint}"
    );
}

#[tokio::test]
async fn unknown_table_similar_name_levenshtein_suggests_closest() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "rows.jsonl",
        &[json!({"id": 1, "playerID": "ov8"})],
    );
    let source = build_source(manifest("hockey", "games", temp.path()));

    let error = CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM hockey.gamers")
        .await
        .expect_err("unknown table should fail");

    let sqe = structured(error);
    let hint = sqe.hint().expect("hint should be present");
    assert!(
        hint.contains("hockey.games"),
        "hint should suggest closest table, got: {hint}"
    );
}

#[tokio::test]
async fn unknown_column_on_aliased_join_suggests_case_preserved_quoted_name() {
    // Real-world shape: an agent discovers `playerID` in `coral.columns`
    // and writes a self-join `ON g.playerID = m.playerID`. DataFusion
    // lowercases both unquoted identifiers to `g.playerid` / `m.playerid`,
    // which don't match the case-preserving `playerID` column in the
    // schema. Our hint must point at `g."playerID"` (case-preserving
    // quoted form). DataFusion's own error text suggests `"g.playerid"`
    // (lowercased, wrong) — our negative assertion below guards against
    // accidentally regressing to that shape.
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "rows.jsonl",
        &[json!({"id": 1, "playerID": "ov8"})],
    );
    let source = build_source(manifest("hockey", "master", temp.path()));

    let error = CoralQuery::execute_sql(
        &[source],
        &TestRuntime,
        "SELECT g.id FROM hockey.master AS g \
         JOIN hockey.master AS m ON g.playerID = m.playerID",
    )
    .await
    .expect_err("unknown field should fail");

    let sqe = structured(error);
    assert_eq!(sqe.reason(), "UNKNOWN_COLUMN");
    assert_eq!(sqe.status(), StatusCode::InvalidArgument);
    let hint = sqe.hint().expect("hint should be present");
    assert!(
        hint.contains("g.\"playerID\"") || hint.contains("m.\"playerID\""),
        "hint should suggest case-preserving quoted alias.column, got: {hint}"
    );
    assert!(
        !hint.contains("\"playerid\""),
        "hint must not suggest the lowercased quoted form, got: {hint}"
    );
}

#[tokio::test]
async fn unknown_column_levenshtein_suggests_closest_field() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "rows.jsonl",
        &[json!({"id": 1, "playerID": "ov8"})],
    );
    let source = build_source(manifest("hockey", "master", temp.path()));

    // `id2` doesn't exist and isn't a case-twin of anything; the closest
    // candidate by Levenshtein is `id`.
    let error = CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT id2 FROM hockey.master")
        .await
        .expect_err("unknown field should fail");

    let sqe = structured(error);
    assert_eq!(sqe.reason(), "UNKNOWN_COLUMN");
    let hint = sqe.hint().expect("hint should be present");
    assert!(
        hint.contains("id"),
        "expected did-you-mean hint, got: {hint}"
    );
}
