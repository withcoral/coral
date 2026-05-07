use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, CoreError, EngineExtensions, QueryResultObserver, QueryResultObserverError,
    QueryRuntimeConfig, QueryRuntimeContext, StatusCode,
};
use serde_json::{Value, json};

use crate::harness::{build_source, dir_url, execution_to_rows, users_rows, write_jsonl_file};

#[derive(Debug, Clone, PartialEq)]
struct ObservedQuery {
    sql: String,
    column_names: Vec<String>,
    row_count: usize,
    rows: Vec<Value>,
}

#[derive(Debug, Default)]
struct RecordingObserver {
    calls: Mutex<Vec<ObservedQuery>>,
}

impl RecordingObserver {
    fn calls(&self) -> Vec<ObservedQuery> {
        self.calls
            .lock()
            .expect("observer calls lock should not be poisoned")
            .clone()
    }
}

impl QueryResultObserver for RecordingObserver {
    fn name(&self) -> &'static str {
        "recording"
    }

    fn observe_result(
        &self,
        sql: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> Result<(), QueryResultObserverError> {
        self.calls
            .lock()
            .expect("observer calls lock should not be poisoned")
            .push(ObservedQuery {
                sql: sql.to_string(),
                column_names: schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
                row_count: batches.iter().map(RecordBatch::num_rows).sum(),
                rows: batches_to_rows(batches),
            });
        Ok(())
    }
}

#[derive(Debug)]
struct FailingObserver;

impl QueryResultObserver for FailingObserver {
    fn name(&self) -> &'static str {
        "failing"
    }

    fn observe_result(
        &self,
        _sql: &str,
        _schema: &Schema,
        _batches: &[RecordBatch],
    ) -> Result<(), QueryResultObserverError> {
        Err(QueryResultObserverError::failed_precondition(
            "expected benchmark state is missing",
        ))
    }
}

#[tokio::test]
async fn observer_called_after_successful_query_and_sees_final_batches() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("observer_success", temp.path()));
    let observer = Arc::new(RecordingObserver::default());
    let runtime = runtime_with_observer(observer.clone());
    let sql = "SELECT id, name FROM observer_success.users WHERE id >= 2 ORDER BY id";

    let execution = CoralQuery::execute_sql(&[source], runtime, sql)
        .await
        .expect("query should succeed");

    assert_eq!(execution.row_count(), 2);
    let calls = observer.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0],
        ObservedQuery {
            sql: sql.to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            row_count: 2,
            rows: vec![
                json!({"id": 2, "name": "Grace"}),
                json!({"id": 3, "name": "Linus"}),
            ],
        }
    );
}

#[tokio::test]
async fn observer_errors_fail_query_with_structured_core_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("observer_error", temp.path()));
    let runtime = runtime_with_observer(Arc::new(FailingObserver));

    let error = CoralQuery::execute_sql(
        &[source],
        runtime,
        "SELECT id FROM observer_error.users ORDER BY id",
    )
    .await
    .expect_err("observer failure should fail the query");

    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match error {
        CoreError::FailedPrecondition(detail) => {
            assert_eq!(
                detail,
                "query result observer 'failing': expected benchmark state is missing"
            );
        }
        other => panic!("expected CoreError::FailedPrecondition, got {other:?}"),
    }
}

#[tokio::test]
async fn no_observer_keeps_existing_query_behavior_unchanged() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("observer_none", temp.path()));

    let execution = CoralQuery::execute_sql(
        &[source],
        QueryRuntimeConfig::default(),
        "SELECT id, name FROM observer_none.users WHERE id < 3 ORDER BY id",
    )
    .await
    .expect("query should succeed without observers");

    assert_eq!(execution.row_count(), 2);
    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({"id": 1, "name": "Ada"}),
            json!({"id": 2, "name": "Grace"}),
        ]
    );
}

#[tokio::test]
async fn observer_sees_filtered_projected_result_not_raw_source_rows() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("observer_final", temp.path()));
    let observer = Arc::new(RecordingObserver::default());
    let sql = "SELECT name FROM observer_final.users WHERE id = 2";

    let execution =
        CoralQuery::execute_sql(&[source], runtime_with_observer(observer.clone()), sql)
            .await
            .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"name": "Grace"})]
    );
    let calls = observer.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0],
        ObservedQuery {
            sql: sql.to_string(),
            column_names: vec!["name".to_string()],
            row_count: 1,
            rows: vec![json!({"name": "Grace"})],
        }
    );
}

fn runtime_with_observer(observer: Arc<dyn QueryResultObserver>) -> QueryRuntimeConfig {
    let mut extensions = EngineExtensions::default();
    extensions.query_result_observers.push(observer);
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions)
}

fn jsonl_manifest(name: &str, dir: &Path) -> Value {
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
                "glob": "**/*.jsonl",
            },
            "columns": [
                {"name": "id", "type": "Int64"},
                {"name": "name", "type": "Utf8"},
                {"name": "email", "type": "Utf8"},
            ],
        }],
    })
}

fn batches_to_rows(batches: &[RecordBatch]) -> Vec<Value> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
        for batch in batches {
            writer.write(batch).expect("batch should encode to json");
        }
        writer.finish().expect("json writer should finish");
    }
    serde_json::from_slice(&bytes).expect("json rows should decode")
}
