#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::path::Path;

use coral_engine::{CoralQuery, CoreError, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    assert_row_count, assert_table_not_found, build_source, dir_url, execution_to_rows,
    test_runtime, users_rows, write_jsonl_file,
};

fn jsonl_manifest(name: &str, dir: &Path, glob: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "source": {
                "location": dir_url(dir),
                "glob": glob
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" },
                { "name": "email", "type": "Utf8" }
            ]
        }]
    })
}

#[tokio::test]
async fn select_all_from_jsonl_source() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_users", temp.path(), "**/*.jsonl"));

    let execution = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, name, email FROM jsonl_users.users ORDER BY id",
    )
    .await
    .expect("query should succeed");

    assert_row_count(&execution, 3);
    assert_eq!(execution_to_rows(&execution), users_rows());
}

#[tokio::test]
async fn quoted_fully_qualified_table_reference_reports_sql_reference_hint() {
    let temp = TempDir::new().expect("temp dir");
    let source = github_pulls_source(temp.path());

    let error =
        CoralQuery::execute_sql(&[source], test_runtime(), "SELECT * FROM \"github.pulls\"")
            .await
            .expect_err("whole-reference quoted table should fail");

    assert_quoted_fully_qualified_table_reference_hint(error);
}

#[tokio::test]
async fn explain_sql_quoted_fully_qualified_table_reference_reports_sql_reference_hint() {
    let temp = TempDir::new().expect("temp dir");
    let source = github_pulls_source(temp.path());

    let error =
        CoralQuery::explain_sql(&[source], test_runtime(), "SELECT * FROM \"github.pulls\"")
            .await
            .expect_err("whole-reference quoted table should fail during explanation");

    assert_quoted_fully_qualified_table_reference_hint(error);
}

fn assert_quoted_fully_qualified_table_reference_hint(error: CoreError) {
    assert_eq!(error.status_code(), StatusCode::NotFound);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
            assert_eq!(sqe.metadata().get("schema"), None);
            assert_eq!(
                sqe.metadata().get("table").map(String::as_str),
                Some("github.pulls")
            );
            let hint = sqe.hint().expect("hint should be present");
            assert!(
                hint.contains("`\"github.pulls\"` is one quoted identifier"),
                "hint should explain the quoted-qualified mistake, got: {hint}"
            );
            assert!(
                hint.contains("`github.pulls`"),
                "hint should suggest the list_tables sql_reference form, got: {hint}"
            );
            assert!(
                hint.contains("`\"github\".\"pulls\"`"),
                "hint should show per-identifier quoting as valid SQL, got: {hint}"
            );
        }
        other => panic!("expected CoreError::QueryFailure, got {other:?}"),
    }
}

#[tokio::test]
async fn explain_sql_returns_logical_and_physical_plans() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_plan", temp.path(), "**/*.jsonl"));

    let plan = CoralQuery::explain_sql(
        &[source],
        test_runtime(),
        "SELECT id, name FROM jsonl_plan.users WHERE id > 1 ORDER BY name",
    )
    .await
    .expect("query should explain");

    assert!(plan.unoptimized_logical_plan().contains("jsonl_plan.users"));
    assert!(plan.optimized_logical_plan().contains("jsonl_plan.users"));
    assert!(plan.physical_plan().contains("Exec"));
}

fn github_pulls_source(dir: &Path) -> coral_engine::QuerySource {
    write_jsonl_file(
        dir,
        "pulls.jsonl",
        &[json!({"id": 1, "title": "Fix table hint"})],
    );
    build_source(json!({
        "name": "github",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "pulls",
            "description": "Pull requests fixture",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "title", "type": "Utf8" }
            ]
        }]
    }))
}

#[tokio::test]
async fn select_with_column_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest(
        "jsonl_projection",
        temp.path(),
        "**/*.jsonl",
    ));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT name FROM jsonl_projection.users ORDER BY name DESC",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"name": "Linus"}),
            json!({"name": "Grace"}),
            json!({"name": "Ada"})
        ]
    );
}

#[tokio::test]
async fn select_with_where_filter() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_filter", temp.path(), "**/*.jsonl"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, name FROM jsonl_filter.users WHERE id = 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 2, "name": "Grace"})]);
}

#[tokio::test]
async fn select_with_order_by_and_limit() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_order", temp.path(), "**/*.jsonl"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT name FROM jsonl_order.users ORDER BY name DESC LIMIT 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({"name": "Linus"}), json!({"name": "Grace"})]
    );
}

#[tokio::test]
async fn select_count_aggregation() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_count", temp.path(), "**/*.jsonl"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT COUNT(*) AS n FROM jsonl_count.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn glob_matches_multiple_files() {
    let temp = TempDir::new().expect("temp dir");
    let rows = users_rows();
    write_jsonl_file(temp.path(), "nested/one.jsonl", &rows[..2]);
    write_jsonl_file(temp.path(), "nested/deeper/two.jsonl", &rows[2..]);
    let source = build_source(jsonl_manifest("jsonl_glob", temp.path(), "**/*.jsonl"));

    let execution = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, name, email FROM jsonl_glob.users ORDER BY id",
    )
    .await
    .expect("query should succeed");

    assert_eq!(execution_to_rows(&execution), rows);
}

#[tokio::test]
async fn missing_file_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(jsonl_manifest("jsonl_missing", &missing_dir, "**/*.jsonl"));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT * FROM jsonl_missing.users",
    )
    .await
    .expect_err("missing jsonl source should fail");

    assert_table_not_found(error, "jsonl_missing", "users");
}
