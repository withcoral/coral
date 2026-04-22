//! Backend-agnostic coverage for the JSON UDFs registered on the engine's
//! `SessionContext` (`json_get*`, `json_contains`, `->`, `->>`) and the
//! `Json` manifest type. Uses JSONL as a lightweight vehicle; the same
//! functions work against any backend that lands JSON in a `Utf8` column.

use std::path::Path;

use coral_engine::CoralQuery;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{TestRuntime, build_source, dir_url, execution_to_rows, write_jsonl_file};

fn events_manifest(name: &str, dir: &Path, column_type: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "events",
            "description": "events with JSON-valued properties",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "properties", "type": column_type }
            ]
        }]
    })
}

fn events_fixture() -> Vec<Value> {
    vec![
        json!({"id": 1, "properties": {"$browser": "Firefox", "count": 7}}),
        json!({"id": 2, "properties": {"$browser": "Chrome", "count": 3}}),
    ]
}

#[tokio::test]
async fn json_get_str_extracts_from_json_typed_column() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest("json_typed", temp.path(), "Json"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, json_get_str(properties, '$browser') AS browser \
             FROM json_typed.events ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "browser": "Firefox"}),
            json!({"id": 2, "browser": "Chrome"}),
        ]
    );
}

#[tokio::test]
async fn json_functions_also_work_on_utf8_columns() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest("json_utf8", temp.path(), "Utf8"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, json_get_str(properties, '$browser') AS browser \
             FROM json_utf8.events ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "browser": "Firefox"}),
            json!({"id": 2, "browser": "Chrome"}),
        ]
    );
}

#[tokio::test]
async fn json_get_int_filters_typed_values() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest("json_filter", temp.path(), "Json"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id FROM json_filter.events \
             WHERE json_get_int(properties, 'count') > 5",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 1})]);
}

#[tokio::test]
async fn arrow_operator_extracts_text() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest("json_arrow", temp.path(), "Json"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, properties->>'$browser' AS browser \
             FROM json_arrow.events ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "browser": "Firefox"}),
            json!({"id": 2, "browser": "Chrome"}),
        ]
    );
}

#[tokio::test]
async fn arrow_operator_with_cast_extracts_typed_value() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest("json_cast", temp.path(), "Json"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id FROM json_cast.events \
             WHERE (properties->'count')::bigint > 5",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 1})]);
}
