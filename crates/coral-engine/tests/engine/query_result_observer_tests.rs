use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, CoreError, EngineExtensions, QueryResultObserver, QueryResultObserverError,
    QueryRuntimeConfig, QueryRuntimeContext, StatusCode,
};
use serde_json::{Value, json};

use crate::harness::{batches_to_rows, execution_to_rows, users_jsonl_source};

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
            .map_err(|_err| {
                QueryResultObserverError::failed_precondition(
                    "observer calls lock should not be poisoned",
                )
            })?
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
    let (_temp, source) = users_jsonl_source("observer_success");
    let observer = Arc::new(RecordingObserver::default());
    let runtime = runtime_with_observer(observer.clone());
    let sql = "SELECT id, name FROM observer_success.users WHERE id >= 2 ORDER BY id";

    let execution = CoralQuery::execute_sql(&[source], runtime, sql)
        .await
        .expect("query should succeed");

    assert_eq!(execution.row_count(), 2);
    let calls = observer.calls();
    assert_eq!(calls.len(), 1);
    let call = calls.first().expect("observer call");
    assert_eq!(
        call,
        &ObservedQuery {
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
    let (_temp, source) = users_jsonl_source("observer_error");
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
    let (_temp, source) = users_jsonl_source("observer_none");

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
    let (_temp, source) = users_jsonl_source("observer_final");
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
    let call = calls.first().expect("observer call");
    assert_eq!(
        call,
        &ObservedQuery {
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
