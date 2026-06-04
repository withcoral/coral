//! End-to-end coverage for structured `DataFusion` error enrichment.
//!
//! Verifies that `coral-engine` promotes `DataFusionError::SchemaError` and
//! `DataFusionError::Plan` table-not-found variants into
//! `CoreError::QueryFailure` with case-aware hints.

use std::path::Path;

use coral_engine::{CoralQuery, CoreError, StatusCode, StructuredQueryError};
use serde_json::{Value, json};

use crate::harness::{build_source, dir_url, test_runtime, write_jsonl_file};

fn manifest(name: &str, table: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": table,
            "description": "Structured-error fixture",
            "format": "jsonl",
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

async fn structured_fixture_error(
    schema: &str,
    table: &str,
    sql: &str,
    expectation: &str,
) -> StructuredQueryError {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "rows.jsonl",
        &[json!({"id": 1, "playerID": "ov8"})],
    );
    let source = build_source(manifest(schema, table, temp.path()));
    let error = CoralQuery::execute_sql(&[source], test_runtime(), sql)
        .await
        .expect_err(expectation);
    structured(error)
}

fn hint(sqe: &StructuredQueryError) -> &str {
    sqe.hint().expect("hint should be present")
}

#[tokio::test]
async fn table_not_found_hints_cover_case_catalog_and_similarity() {
    for (case, schema, table, sql, expected_hint, forbidden_hint) in [
        (
            "case-preserved quoted name",
            "hockey",
            "Master",
            "SELECT * FROM hockey.Master",
            "hockey.\"Master\"",
            None,
        ),
        (
            "missing schema catalog query",
            "hockey",
            "games",
            "SELECT * FROM nba.games",
            "coral.tables",
            Some("coral source"),
        ),
        (
            "closest table name",
            "hockey",
            "games",
            "SELECT * FROM hockey.gamers",
            "hockey.games",
            None,
        ),
    ] {
        let sqe = structured_fixture_error(schema, table, sql, case).await;
        assert_eq!(sqe.reason(), "TABLE_NOT_FOUND", "{case}");
        assert_eq!(sqe.status(), StatusCode::NotFound, "{case}");
        let hint = hint(&sqe);
        assert!(
            hint.contains(expected_hint),
            "{case}: expected hint to contain {expected_hint:?}, got: {hint}"
        );
        if let Some(forbidden_hint) = forbidden_hint {
            assert!(
                !hint.contains(forbidden_hint),
                "{case}: hint must not contain {forbidden_hint:?}, got: {hint}"
            );
        }
    }
}

#[tokio::test]
async fn unqualified_table_suggests_schema_qualified_name() {
    // A bare `FROM account` query against a catalog that registers
    // `stripe.accounts` must hint at the schema-qualified name end-to-end
    // — the engine layer translates DataFusion's synthetic
    // `datafusion.public.account` into an unqualified table-not-found and
    // the hint builder picks the closest cross-schema match.
    let sqe = structured_fixture_error(
        "stripe",
        "accounts",
        "SELECT * FROM account",
        "unqualified unknown table should fail",
    )
    .await;
    assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
    assert_eq!(sqe.status(), StatusCode::NotFound);
    assert_eq!(sqe.metadata().get("schema"), None);
    assert_eq!(
        sqe.metadata().get("table").map(String::as_str),
        Some("account")
    );
    let hint = hint(&sqe);
    assert!(
        hint.contains("stripe.accounts"),
        "hint should suggest the schema-qualified name, got: {hint}"
    );
}

#[tokio::test]
async fn quoted_dotted_missing_table_stays_one_identifier() {
    let sqe = structured_fixture_error(
        "hockey",
        "player.stats",
        "SELECT * FROM \"player.stat\"",
        "unknown quoted dotted table should fail",
    )
    .await;
    assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
    assert_eq!(sqe.metadata().get("schema"), None);
    assert_eq!(
        sqe.metadata().get("table").map(String::as_str),
        Some("player.stat")
    );
    let hint = hint(&sqe);
    assert!(
        hint.contains("hockey.\"player.stats\""),
        "hint should preserve the dotted table as a quoted identifier, got: {hint}"
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
    let sqe = structured_fixture_error(
        "hockey",
        "master",
        "SELECT g.id FROM hockey.master AS g \
         JOIN hockey.master AS m ON g.playerID = m.playerID",
        "unknown field should fail",
    )
    .await;
    assert_eq!(sqe.reason(), "UNKNOWN_COLUMN");
    assert_eq!(sqe.status(), StatusCode::InvalidArgument);
    let hint = hint(&sqe);
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
    // `id2` doesn't exist and isn't a case-twin of anything; the closest
    // candidate by Levenshtein is `id`.
    let sqe = structured_fixture_error(
        "hockey",
        "master",
        "SELECT id2 FROM hockey.master",
        "unknown field should fail",
    )
    .await;
    assert_eq!(sqe.reason(), "UNKNOWN_COLUMN");
    let hint = hint(&sqe);
    assert!(
        hint.contains("id"),
        "expected did-you-mean hint, got: {hint}"
    );
}
