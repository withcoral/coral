use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, EngineExtensions, QueryParameterValue, QueryResultObserver,
    QueryResultObserverError, QueryRuntimeConfig, QueryRuntimeContext,
    SavedFunctionRuntimeArgument, SavedFunctionRuntimeArgumentType, SavedFunctionRuntimeDefinition,
    SavedFunctionRuntimeImplementation, StatusCode,
};
use serde_json::{Value, json};

use crate::harness::{build_source, dir_url, test_runtime, write_jsonl_file};

#[derive(Debug, Default)]
struct RowCountObserver {
    row_counts: Mutex<Vec<usize>>,
}

impl RowCountObserver {
    fn row_counts(&self) -> Vec<usize> {
        self.row_counts
            .lock()
            .expect("observer row count lock should not be poisoned")
            .clone()
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
        batches: &[RecordBatch],
    ) -> Result<(), QueryResultObserverError> {
        self.row_counts
            .lock()
            .map_err(|_err| {
                QueryResultObserverError::failed_precondition(
                    "observer row count lock should not be poisoned",
                )
            })?
            .push(batches.iter().map(RecordBatch::num_rows).sum());
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

fn min_id_saved_function(source_name: &str) -> SavedFunctionRuntimeDefinition {
    SavedFunctionRuntimeDefinition {
        name: "min_id_events".to_string(),
        arguments: vec![SavedFunctionRuntimeArgument {
            name: "min_id".to_string(),
            data_type: SavedFunctionRuntimeArgumentType::Integer,
            required: true,
        }],
        implementation: SavedFunctionRuntimeImplementation::CoralSql {
            query: format!("select id from {source_name}.events where id >= $min_id order by id"),
        },
    }
}

fn events_saved_function(source_name: &str) -> SavedFunctionRuntimeDefinition {
    SavedFunctionRuntimeDefinition {
        name: "events".to_string(),
        arguments: Vec::new(),
        implementation: SavedFunctionRuntimeImplementation::CoralSql {
            query: format!("select id from {source_name}.events order by id"),
        },
    }
}

fn optional_label_saved_function() -> SavedFunctionRuntimeDefinition {
    SavedFunctionRuntimeDefinition {
        name: "optional_label".to_string(),
        arguments: vec![SavedFunctionRuntimeArgument {
            name: "label".to_string(),
            data_type: SavedFunctionRuntimeArgumentType::String,
            required: false,
        }],
        implementation: SavedFunctionRuntimeImplementation::CoralSql {
            query: "select $label as label".to_string(),
        },
    }
}

fn runtime_with_observer(observer: Arc<dyn QueryResultObserver>) -> QueryRuntimeConfig {
    let mut extensions = EngineExtensions::default();
    extensions.query_result_observers.push(observer);
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions)
}

#[tokio::test]
async fn validate_saved_function_preserves_optional_null_argument_type() {
    let schema = CoralQuery::validate_saved_function(
        &[],
        test_runtime(),
        optional_label_saved_function(),
        BTreeMap::new(),
    )
    .await
    .expect("saved_function schema should infer typed null argument");

    let field = schema.fields().first().expect("label field");
    assert_eq!(field.name(), "label");
    assert_eq!(field.data_type(), &arrow::datatypes::DataType::Utf8);
}

#[tokio::test]
async fn validate_saved_function_returns_schema_from_explicit_validation_args() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "events.jsonl",
        &[json!({"id": 1}), json!({"id": 2})],
    );
    let source = build_source(events_manifest("schema_saved_function_events", temp.path()));

    let schema = CoralQuery::validate_saved_function(
        &[source],
        test_runtime(),
        min_id_saved_function("schema_saved_function_events"),
        BTreeMap::from([("min_id".to_string(), QueryParameterValue::Integer(Some(1)))]),
    )
    .await
    .expect("saved_function schema should infer");

    let field = schema.fields().first().expect("id field");
    assert_eq!(field.name(), "id");
    assert_eq!(field.data_type(), &arrow::datatypes::DataType::Int64);
}

#[tokio::test]
async fn validate_saved_function_does_not_collect_rows() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "events.jsonl",
        &[json!({"id": 1}), json!({"id": 2})],
    );
    let source = build_source(events_manifest("validation_limit_events", temp.path()));
    let observer = Arc::new(RowCountObserver::default());

    let schema = CoralQuery::validate_saved_function(
        &[source],
        runtime_with_observer(observer.clone()),
        events_saved_function("validation_limit_events"),
        BTreeMap::new(),
    )
    .await
    .expect("saved_function validation should infer schema without collection");

    assert_eq!(schema.fields().len(), 1);
    assert_eq!(observer.row_counts(), Vec::<usize>::new());
}

#[tokio::test]
async fn validate_saved_function_rejects_missing_validation_args() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &[json!({"id": 1})]);
    let source = build_source(events_manifest(
        "missing_arg_saved_function_events",
        temp.path(),
    ));

    let error = CoralQuery::validate_saved_function(
        &[source],
        test_runtime(),
        min_id_saved_function("missing_arg_saved_function_events"),
        BTreeMap::new(),
    )
    .await
    .expect_err("missing validation args should fail");

    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("saved_function 'min_id_events' is missing required argument 'min_id'"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn validate_saved_function_rejects_invalid_argument_values() {
    let cases = [
        (
            "unknown supplied arg",
            min_id_saved_function("invalid_arg_saved_function_events"),
            BTreeMap::from([(
                "not_declared".to_string(),
                QueryParameterValue::Integer(Some(1)),
            )]),
            "saved_function 'min_id_events' received unknown argument 'not_declared'",
        ),
        (
            "required null arg",
            min_id_saved_function("invalid_arg_saved_function_events"),
            BTreeMap::from([("min_id".to_string(), QueryParameterValue::Integer(None))]),
            "saved_function 'min_id_events' argument 'min_id' is required and cannot be null",
        ),
        (
            "wrong arg type",
            min_id_saved_function("invalid_arg_saved_function_events"),
            BTreeMap::from([(
                "min_id".to_string(),
                QueryParameterValue::String(Some("1".to_string())),
            )]),
            "saved_function 'min_id_events' argument 'min_id' expected integer, got string",
        ),
    ];

    for (name, saved_function, arguments, expected_message) in cases {
        let Err(error) =
            CoralQuery::validate_saved_function(&[], test_runtime(), saved_function, arguments)
                .await
        else {
            panic!("{name} should fail");
        };

        assert_eq!(error.status_code(), StatusCode::InvalidArgument, "{name}");
        assert!(
            error.to_string().contains(expected_message),
            "{name}: unexpected error: {error}"
        );
    }
}

#[tokio::test]
async fn validate_saved_function_rejects_duplicate_runtime_argument_names() {
    let mut saved_function = optional_label_saved_function();
    saved_function.arguments.push(SavedFunctionRuntimeArgument {
        name: "label".to_string(),
        data_type: SavedFunctionRuntimeArgumentType::String,
        required: false,
    });

    let error =
        CoralQuery::validate_saved_function(&[], test_runtime(), saved_function, BTreeMap::new())
            .await
            .expect_err("duplicate saved_function runtime arguments should fail");

    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    assert!(
        error.to_string().contains(
            "saved_function 'optional_label' argument 'label' is declared more than once"
        ),
        "unexpected error: {error}"
    );
}
