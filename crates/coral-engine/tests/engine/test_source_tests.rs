#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::path::Path;

use coral_engine::CoralQuery;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, test_runtime, users_rows, write_jsonl_file};

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
async fn test_source_lists_registered_tables() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest(
        "jsonl_test_source",
        temp.path(),
        "**/*.jsonl",
    ));

    let tables = CoralQuery::test_source(&source, test_runtime())
        .await
        .expect("test_source should succeed");

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].schema_name, "jsonl_test_source");
    assert_eq!(tables[0].table_name, "users");
}

#[tokio::test]
async fn test_source_missing_directory_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(jsonl_manifest(
        "jsonl_test_missing",
        &missing_dir,
        "**/*.jsonl",
    ));

    let error = CoralQuery::test_source(&source, test_runtime())
        .await
        .expect_err("test_source should fail for missing directories");

    assert_eq!(
        error.status_code(),
        coral_engine::StatusCode::FailedPrecondition
    );
    assert!(
        error.to_string().contains(&format!(
            "jsonl_test_missing.users source.location '{}' is not a directory",
            dir_url(&missing_dir)
        )),
        "expected missing-directory detail in error, got: {error}"
    );
}

#[tokio::test]
async fn validate_source_fails_when_source_never_registers() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(jsonl_manifest(
        "jsonl_test_missing",
        &missing_dir,
        "**/*.jsonl",
    ));
    let queries = vec!["SELECT * FROM jsonl_test_missing.users".to_string()];

    let error = CoralQuery::validate_source(&source, test_runtime(), &queries)
        .await
        .expect_err("validate_source should fail when the source never registers");

    assert_eq!(
        error.status_code(),
        coral_engine::StatusCode::FailedPrecondition
    );
    assert!(
        error.to_string().contains("is not a directory"),
        "expected skipped-registration detail in error, got: {error}"
    );
}

#[tokio::test]
async fn validate_source_reports_passing_and_failing_queries() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest(
        "jsonl_test_source",
        temp.path(),
        "**/*.jsonl",
    ));
    let queries = vec![
        "SELECT * FROM jsonl_test_source.users".to_string(),
        "SELECT * FROM jsonl_test_source.missing".to_string(),
    ];

    let report = CoralQuery::validate_source(&source, test_runtime(), &queries)
        .await
        .expect("validate_source should succeed");

    assert_eq!(report.tables.len(), 1);
    assert_eq!(report.query_tests.len(), 2);
    assert!(report.query_tests[0].passed());
    assert_eq!(report.query_tests[0].row_count(), Some(3));
    assert!(!report.query_tests[1].passed());
    assert!(
        report.query_tests[1]
            .error_message()
            .expect("failed query should carry an error")
            .contains("not found")
            || report.query_tests[1]
                .error_message()
                .expect("failed query should carry an error")
                .contains("invalid input")
    );
    assert!(matches!(
        report.query_tests[0].result(),
        Ok(success) if success.row_count() == 3
    ));
}

#[tokio::test]
async fn validate_source_maps_non_read_only_queries_to_stable_error() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest(
        "jsonl_test_source",
        temp.path(),
        "**/*.jsonl",
    ));
    let queries = vec!["SET datafusion.execution.batch_size = 1".to_string()];

    let report = CoralQuery::validate_source(&source, test_runtime(), &queries)
        .await
        .expect("validate_source should succeed");

    assert_eq!(
        report.query_tests[0].error_message(),
        Some("test query must be read-only SQL")
    );
}
