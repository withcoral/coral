use std::path::Path;

use coral_engine::CoralQuery;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    TestRuntime, assert_invalid_input, build_source, dir_url, users_rows, write_jsonl_file,
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
async fn test_source_lists_registered_tables() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest(
        "jsonl_test_source",
        temp.path(),
        "**/*.jsonl",
    ));

    let tables = CoralQuery::test_source(&source, &TestRuntime)
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

    let error = CoralQuery::test_source(&source, &TestRuntime)
        .await
        .expect_err("test_source should fail for missing directories");

    assert_invalid_input(
        error,
        &format!(
            "jsonl_test_missing.users source.location '{}' is not a directory",
            dir_url(&missing_dir)
        ),
    );
}
