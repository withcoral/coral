use coral_engine::{CoralQuery, CoreError};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, test_runtime};

fn jsonl_bindable_manifest(name: &str, dir: &TempDir) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "filters": [{ "name": "id", "bindable": true }],
            "source": {
                "location": dir_url(dir.path()),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Utf8" }
            ]
        }]
    })
}

fn parquet_bindable_manifest(name: &str, dir: &TempDir) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "parquet",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "filters": [{ "name": "id", "bindable": true }],
            "source": {
                "location": dir_url(dir.path()),
                "glob": "**/*.parquet"
            },
            "columns": []
        }]
    })
}

fn http_bindable_manifest(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "users",
            "description": "Users",
            "filters": [{ "name": "id", "bindable": true }],
            "request": {
                "path": "/users"
            },
            "columns": [
                { "name": "id", "type": "Utf8" }
            ]
        }]
    })
}

#[tokio::test]
async fn bindable_filter_on_file_backends_fails_registration() {
    for (manifest, backend) in [
        {
            let temp = TempDir::new().expect("temp dir");
            (jsonl_bindable_manifest("jsonl_bindable", &temp), "jsonl")
        },
        {
            let temp = TempDir::new().expect("temp dir");
            (
                parquet_bindable_manifest("parquet_bindable", &temp),
                "parquet",
            )
        },
    ] {
        let source = build_source(manifest);
        let error = CoralQuery::validate_source(&source, test_runtime(), &[])
            .await
            .expect_err("bindable file backend should fail registration");

        match error {
            CoreError::FailedPrecondition(detail) => {
                assert!(
                    detail.contains(&format!(
                        "bindable filters are not supported by the current engine for backend '{backend}' in V1"
                    )),
                    "unexpected error detail: {detail}"
                );
            }
            other => panic!("expected FailedPrecondition, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn bindable_filter_on_http_backend_registers() {
    let source = build_source(http_bindable_manifest(
        "http_bindable",
        "https://example.invalid",
    ));

    let report = CoralQuery::validate_source(&source, test_runtime(), &[])
        .await
        .expect("http bindable source should register");

    assert_eq!(report.tables.len(), 1);
    let table = report.tables.first().expect("one registered table");
    assert_eq!(table.schema_name, "http_bindable");
}
