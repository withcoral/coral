use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, EngineExtensions, QueryExecutionProvenance, QueryResultObserver,
    QueryResultObserverError, QueryRuntimeConfig, QueryRuntimeContext, StatusCode,
    UdfRuntimeImplementation, UdfRuntimeSignature, UdfRuntimeSqlDefinition,
};
use coral_spec::ManifestDataType;
use serde_json::{Value, json};

use crate::harness::{build_source, dir_url, test_runtime, write_jsonl_file};

#[derive(Debug, Default)]
struct RowCountObserver {
    calls: AtomicUsize,
}

impl RowCountObserver {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl QueryResultObserver for RowCountObserver {
    fn name(&self) -> &'static str {
        "row_count"
    }

    fn observe_result(
        &self,
        _sql: &str,
        _schema: &Schema,
        _batches: &[RecordBatch],
        _provenance: &QueryExecutionProvenance,
    ) -> Result<(), QueryResultObserverError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn events_manifest(name: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "Event rows",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" }
            ]
        }]
    })
}

fn search_function_manifest(name: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "functions": [{
            "name": "search_issues",
            "description": "Search issues",
            "args": [
                {
                    "name": "q",
                    "required": true,
                    "bind": { "arg": "q" }
                },
                {
                    "name": "min_score",
                    "type": "Float64",
                    "bind": { "arg": "min_score" }
                },
                {
                    "name": "max_items",
                    "type": "Int64",
                    "bind": { "arg": "max_items" }
                },
                {
                    "name": "payload",
                    "type": "Json",
                    "bind": { "arg": "payload" }
                },
                {
                    "name": "since",
                    "type": "Timestamp",
                    "bind": { "arg": "since" }
                }
            ],
            "request": {
                "method": "GET",
                "path": "/api/search/issues",
                "query": [
                    { "name": "q", "from": "arg", "key": "q" },
                    { "name": "min_score", "from": "arg", "key": "min_score" },
                    { "name": "max_items", "from": "arg", "key": "max_items" },
                    { "name": "payload", "from": "arg", "key": "payload" },
                    { "name": "since", "from": "arg", "key": "since" }
                ]
            },
            "response": {
                "rows_path": ["items"]
            },
            "columns": [
                { "name": "title", "type": "Utf8" },
                { "name": "score", "type": "Float64" }
            ]
        }]
    })
}

fn udf(name: &str, query: impl Into<String>) -> UdfRuntimeSqlDefinition {
    UdfRuntimeSqlDefinition {
        name: name.to_string(),
        implementation: UdfRuntimeImplementation::CoralSql {
            query: query.into(),
        },
    }
}

fn min_id_udf(source_name: &str) -> UdfRuntimeSqlDefinition {
    udf(
        "min_id_events",
        format!("select id from {source_name}.events where id >= $min_id order by id"),
    )
}

fn review_queue_udf(source_name: &str) -> UdfRuntimeSqlDefinition {
    udf(
        "review_queue",
        format!(
            "select title, score from {source_name}.search_issues(q => $query, min_score => $min_score, payload => $payload, since => $since)"
        ),
    )
}

fn argument_types(signature: &UdfRuntimeSignature) -> Vec<(&str, ManifestDataType)> {
    signature
        .arguments
        .iter()
        .map(|argument| (argument.name.as_str(), argument.data_type))
        .collect()
}

fn column_types(signature: &UdfRuntimeSignature) -> Vec<(&str, &DataType)> {
    signature
        .result_columns
        .iter()
        .map(|column| (column.name.as_str(), &column.data_type))
        .collect()
}

fn runtime_with_observer(observer: Arc<dyn QueryResultObserver>) -> QueryRuntimeConfig {
    let mut extensions = EngineExtensions::default();
    extensions.query_result_observers.push(observer);
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions)
}

#[tokio::test]
async fn infer_udf_signature_uses_source_function_schema_with_parameters() {
    let source = build_source(search_function_manifest("signature_param_search"));

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        review_queue_udf("signature_param_search"),
    )
    .await
    .expect("udf signature inference should use source function schema without binding params");

    assert_eq!(
        argument_types(&signature),
        [
            ("min_score", ManifestDataType::Float64),
            ("payload", ManifestDataType::Json),
            ("query", ManifestDataType::Utf8),
            ("since", ManifestDataType::Timestamp),
        ]
    );

    let columns = column_types(&signature);
    assert!(matches!(
        columns.as_slice(),
        [
            ("title", DataType::Utf8 | DataType::Utf8View),
            ("score", DataType::Float64)
        ]
    ));
}

#[tokio::test]
async fn infer_udf_signature_uses_explicit_cast_for_argument_type() {
    let signature = CoralQuery::infer_udf_signature(
        &[],
        test_runtime(),
        udf("cast_label", "select cast($label as VARCHAR) as label"),
    )
    .await
    .expect("udf schema inference should use cast type");

    assert_eq!(
        argument_types(&signature),
        [("label", ManifestDataType::Utf8)]
    );
    let columns = column_types(&signature);
    let [("label", column_type)] = columns.as_slice() else {
        panic!("expected label result column");
    };
    assert!(matches!(column_type, DataType::Utf8 | DataType::Utf8View));
}

#[tokio::test]
async fn infer_udf_signature_accepts_timestamp_source_argument_and_cast() {
    let source = build_source(search_function_manifest("mixed_timestamp_search"));

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        udf(
            "mixed_timestamp",
            "select cast($since as TIMESTAMP) as since \
             from mixed_timestamp_search.search_issues(q => 'open', since => $since)",
        ),
    )
    .await
    .expect("timestamp source argument and timestamp cast should agree");

    assert_eq!(
        argument_types(&signature),
        [("since", ManifestDataType::Timestamp)]
    );
}

#[tokio::test]
async fn infer_udf_signature_uses_column_comparison_for_argument_type() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "events.jsonl",
        &[json!({"id": 1}), json!({"id": 2})],
    );
    let source = build_source(events_manifest("schema_udf_events", temp.path()));
    let observer = Arc::new(RowCountObserver::default());

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        runtime_with_observer(observer.clone()),
        min_id_udf("schema_udf_events"),
    )
    .await
    .expect("udf signature inference should plan without collection");

    assert_eq!(
        argument_types(&signature),
        [("min_id", ManifestDataType::Int64)]
    );
    assert!(matches!(
        column_types(&signature).as_slice(),
        [("id", DataType::Int64)]
    ));
    assert_eq!(observer.calls(), 0);
}

#[tokio::test]
async fn infer_udf_signature_rejects_ambiguous_argument_type() {
    let error = CoralQuery::infer_udf_signature(
        &[],
        test_runtime(),
        udf("ambiguous_value", "select $value as value"),
    )
    .await
    .expect_err("ambiguous udf argument should fail");

    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("SQL parameter '$value' has no inferred type"),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("CAST($value AS VARCHAR)"),
        "unexpected error: {error}"
    );
    assert!(
        !error.to_string().contains("Error during planning"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn infer_udf_signature_rejects_conflicting_argument_types() {
    let source = build_source(search_function_manifest("conflicting_param_search"));
    let error = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        udf(
            "conflicting_param",
            "select title from conflicting_param_search.search_issues(q => $value, min_score => $value)",
        ),
    )
    .await
    .expect_err("conflicting udf argument types should fail");

    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("conflicting types for parameter"),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("use explicit casts"),
        "unexpected error: {error}"
    );
}
