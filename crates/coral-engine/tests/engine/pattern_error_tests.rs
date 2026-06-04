//! End-to-end coverage for clear errors when LIKE wildcards are used in
//! regex-style operators.

use std::path::Path;

use coral_engine::{CoralQuery, CoreError, QueryExecution, QuerySource};
use serde_json::{Value, json};

use crate::harness::{
    assert_row_count, build_source, execution_to_rows, test_runtime, write_jsonl_file,
};

fn manifest(dir: &Path) -> Value {
    json!({
        "name": "linear",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "projects",
            "description": "Pattern validator fixture",
            "format": "jsonl",
            "source": {
                "location": format!("file://{}/", dir.display()),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" }
            ]
        }]
    })
}

fn invalid_input_detail(error: CoreError) -> String {
    match error {
        CoreError::InvalidInput(detail) => detail,
        other => panic!("expected CoreError::InvalidInput, got {other:?}"),
    }
}

fn project_names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| {
            row.get("name")
                .and_then(Value::as_str)
                .expect("row should contain project name")
        })
        .collect()
}

fn write_projects_fixture(dir: &Path) -> QuerySource {
    write_jsonl_file(
        dir,
        "projects.jsonl",
        &[
            json!({"id": 1, "name": "Slack Planning"}),
            json!({"id": 2, "name": "Weekly Sync"}),
            json!({"id": 3, "name": "Backlog Triage"}),
        ],
    );
    build_source(manifest(dir))
}

async fn execute_projects_query(sql: &str) -> Result<QueryExecution, CoreError> {
    let temp = tempfile::tempdir().expect("temp dir");
    let source = write_projects_fixture(temp.path());
    CoralQuery::execute_sql(&[source], test_runtime(), sql).await
}

async fn assert_project_names_query(sql: &str, expected: &[&str]) {
    let execution = execute_projects_query(sql)
        .await
        .expect("project query should succeed");

    assert_row_count(&execution, expected.len());
    let rows = execution_to_rows(&execution);
    assert_eq!(project_names(&rows), expected);
}

#[tokio::test]
async fn similar_to_with_like_wildcard_returns_clear_error() {
    let error = execute_projects_query(
        "SELECT id, name FROM linear.projects WHERE name SIMILAR TO '(Slack|Weekly)%'",
    )
    .await
    .expect_err("SIMILAR TO wildcard mismatch should fail");

    let detail = invalid_input_detail(error);
    assert!(detail.contains("SIMILAR TO pattern '(Slack|Weekly)%'"));
    assert!(detail.contains("Use `.*` instead of `%`"));
}

#[tokio::test]
async fn regex_match_with_percent_is_valid() {
    // % is a literal character in regex — no error, just zero matches
    assert_project_names_query(
        "SELECT id, name FROM linear.projects WHERE name ~ '(Slack|Weekly)%'",
        &[],
    )
    .await;
}

#[tokio::test]
async fn similar_to_with_regex_syntax_succeeds() {
    assert_project_names_query(
        "SELECT id, name FROM linear.projects WHERE name SIMILAR TO '(Slack|Weekly).*' ORDER BY id",
        &["Slack Planning", "Weekly Sync"],
    )
    .await;
}

#[tokio::test]
async fn regex_match_with_valid_regex_succeeds() {
    assert_project_names_query(
        "SELECT id, name FROM linear.projects WHERE name ~ '^(Slack|Weekly)' ORDER BY id",
        &["Slack Planning", "Weekly Sync"],
    )
    .await;
}

#[tokio::test]
async fn like_with_wildcards_still_works() {
    assert_project_names_query(
        "SELECT id, name FROM linear.projects WHERE name LIKE 'Slack%' OR name LIKE 'Weekly%' ORDER BY id",
        &["Slack Planning", "Weekly Sync"],
    )
    .await;
}
