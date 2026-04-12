//! Integration tests for live-eval row injection.

use std::collections::BTreeMap;

use arrow::array::Array;
use async_trait as _;
use coral_engine::{
    CoralQuery, CoreError, QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource,
    SourceDecorator, StatusCode,
};
use coral_evals::NeedleInjectionConfig;
use coral_spec::parse_source_manifest_value;
use datafusion as _;
use serde as _;
use serde_json::json;
use serde_yaml as _;
use thiserror as _;

#[derive(Clone)]
struct TestRuntime {
    needle_injection_config: Option<NeedleInjectionConfig>,
}

impl QueryRuntimeProvider for TestRuntime {
    fn runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext::default()
    }

    fn source_decorators(&self) -> Vec<Box<dyn SourceDecorator>> {
        self.needle_injection_config
            .iter()
            .map(NeedleInjectionConfig::source_decorator)
            .collect()
    }
}

fn build_source(location: &str) -> QuerySource {
    let manifest = parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_jsonl",
        "version": "0.1.0",
        "backend": "jsonl",
        "tables": [{
            "name": "events",
            "description": "test events",
            "source": {
                "location": location,
                "glob": "**/*.jsonl",
                "partitions": [],
            },
            "columns": [
                {"name": "id", "type": "Utf8", "nullable": false},
                {"name": "text", "type": "Utf8"},
                {"name": "score", "type": "Int64", "nullable": false},
            ],
        }]
    }))
    .expect("manifest should parse");
    QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new())
}

fn write_jsonl_fixture(dir: &tempfile::TempDir) {
    std::fs::write(
        dir.path().join("events.jsonl"),
        r#"{"id":"live-1","text":"baseline row","score":10}
{"id":"live-2","text":"high priority live row","score":75}
"#,
    )
    .expect("write jsonl fixture");
}

fn execution_ids(execution: &QueryExecution) -> Vec<String> {
    let mut ids = Vec::new();
    for batch in execution.batches() {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string ids");
        for index in 0..column.len() {
            ids.push(column.value(index).to_string());
        }
    }
    ids
}

fn assert_invalid_input(error: CoreError, expected_substring: &str) {
    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    match error {
        CoreError::InvalidInput(detail) => assert!(
            detail.contains(expected_substring),
            "expected '{expected_substring}' in '{detail}'"
        ),
        other => panic!("expected invalid input, got {other:?}"),
    }
}

fn assert_failed_precondition(error: CoreError, expected_substring: &str) {
    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match error {
        CoreError::FailedPrecondition(detail) => assert!(
            detail.contains(expected_substring),
            "expected '{expected_substring}' in '{detail}'"
        ),
        other => panic!("expected failed precondition, got {other:?}"),
    }
}

#[tokio::test]
async fn injected_rows_union_with_live_data_and_respect_where_filters() {
    let fixture_dir = tempfile::tempdir().expect("tempdir");
    write_jsonl_fixture(&fixture_dir);

    let needles_path = fixture_dir.path().join("needles.yaml");
    std::fs::write(
        &needles_path,
        r#"
- schema: test_jsonl
  table: events
  data:
    id: "needle-1"
    text: "matching needle row"
    score: 99
- schema: test_jsonl
  table: events
  data:
    id: "needle-2"
    text: "filtered needle row"
    score: 1
"#,
    )
    .expect("write needles fixture");

    let source = build_source(&format!("file://{}/", fixture_dir.path().display()));
    let runtime = TestRuntime {
        needle_injection_config: Some(NeedleInjectionConfig::new(&needles_path)),
    };

    let all_ids = execution_ids(
        &CoralQuery::execute_sql(
            std::slice::from_ref(&source),
            &runtime,
            "SELECT id FROM test_jsonl.events ORDER BY id",
        )
        .await
        .expect("execute unfiltered query"),
    );
    assert_eq!(all_ids, vec!["live-1", "live-2", "needle-1", "needle-2"]);

    let filtered_ids = execution_ids(
        &CoralQuery::execute_sql(
            &[source],
            &runtime,
            "SELECT id FROM test_jsonl.events WHERE score > 50 ORDER BY id",
        )
        .await
        .expect("execute filtered query"),
    );
    assert_eq!(filtered_ids, vec!["live-2", "needle-1"]);
}

#[tokio::test]
async fn malformed_yaml_fails_runtime_build() {
    let fixture_dir = tempfile::tempdir().expect("tempdir");
    write_jsonl_fixture(&fixture_dir);

    let needles_path = fixture_dir.path().join("needles.yaml");
    std::fs::write(&needles_path, "not: valid: yaml: [").expect("write malformed yaml");

    let source = build_source(&format!("file://{}/", fixture_dir.path().display()));
    let runtime = TestRuntime {
        needle_injection_config: Some(NeedleInjectionConfig::new(&needles_path)),
    };

    let error = CoralQuery::list_tables(&[source], &runtime, None)
        .await
        .expect_err("malformed yaml should fail runtime build");
    assert_invalid_input(error, "failed to parse needles YAML");
}

#[tokio::test]
async fn missing_file_fails_precondition() {
    let source = build_source("file:///path/that/does/not/exist/");
    let runtime = TestRuntime {
        needle_injection_config: Some(NeedleInjectionConfig::new("/tmp/no-such-needles-file.yaml")),
    };

    let error = CoralQuery::list_tables(&[source], &runtime, None)
        .await
        .expect_err("missing file should fail runtime build");
    assert_failed_precondition(error, "/tmp/no-such-needles-file.yaml");
}

#[tokio::test]
async fn invalid_row_for_non_nullable_column_fails_runtime_build() {
    let fixture_dir = tempfile::tempdir().expect("tempdir");
    write_jsonl_fixture(&fixture_dir);

    let needles_path = fixture_dir.path().join("needles.yaml");
    std::fs::write(
        &needles_path,
        r#"
- schema: test_jsonl
  table: events
  data:
    id: "needle-1"
    text: "missing required score"
"#,
    )
    .expect("write needles yaml");

    let source = build_source(&format!("file://{}/", fixture_dir.path().display()));
    let runtime = TestRuntime {
        needle_injection_config: Some(NeedleInjectionConfig::new(&needles_path)),
    };

    let error = CoralQuery::list_tables(&[source], &runtime, None)
        .await
        .expect_err("invalid row should fail");
    assert_invalid_input(error, "failed to convert needle data to Arrow");
}

#[tokio::test]
async fn unused_targets_fail_runtime_build() {
    let fixture_dir = tempfile::tempdir().expect("tempdir");
    write_jsonl_fixture(&fixture_dir);

    let needles_path = fixture_dir.path().join("needles.yaml");
    std::fs::write(
        &needles_path,
        r#"
- schema: test_jsonl
  table: missing_table
  data:
    id: "needle-1"
    text: "orphan needle row"
    score: 99
"#,
    )
    .expect("write needles yaml");

    let source = build_source(&format!("file://{}/", fixture_dir.path().display()));
    let runtime = TestRuntime {
        needle_injection_config: Some(NeedleInjectionConfig::new(&needles_path)),
    };

    let error = CoralQuery::list_tables(&[source], &runtime, None)
        .await
        .expect_err("unused target should fail");
    assert_invalid_input(error, "test_jsonl.missing_table");
}

#[tokio::test]
async fn targeted_source_registration_failure_still_fails_runtime_build() {
    let fixture_dir = tempfile::tempdir().expect("tempdir");
    let needles_path = fixture_dir.path().join("needles.yaml");
    std::fs::write(
        &needles_path,
        r#"
- schema: test_jsonl
  table: events
  data:
    id: "needle-1"
    text: "blocked by source registration failure"
    score: 99
"#,
    )
    .expect("write needles yaml");

    let source = build_source("file:///path/that/does/not/exist/");
    let runtime = TestRuntime {
        needle_injection_config: Some(NeedleInjectionConfig::new(&needles_path)),
    };

    let error = CoralQuery::list_tables(&[source], &runtime, None)
        .await
        .expect_err("targeted source failure should fail runtime build");
    assert_invalid_input(error, "test_jsonl.events");
}
