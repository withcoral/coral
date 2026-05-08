//! Backend-agnostic coverage for the JSON UDFs registered on the engine's
//! `SessionContext` (`json_get*`, `json_contains`, `json_length`,
//! `json_as_text`) and the `Json` manifest type. Uses JSONL as a
//! lightweight vehicle; the same functions work against any backend that
//! lands JSON in a `Utf8` column.

use std::path::Path;

use coral_engine::{CoralQuery, CoreError};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

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
        json!({
            "id": 1,
            "properties": {
                "$browser": "Firefox",
                "count": 7,
                "score": 4.5,
                "active": true,
                "tags": ["alpha", "beta"],
                "geo": {"country": "US"}
            }
        }),
        json!({
            "id": 2,
            "properties": {
                "$browser": "Chrome",
                "count": 3,
                "score": 1.0,
                "active": false,
                "tags": ["gamma"],
                "geo": {"country": "DE"}
            }
        }),
    ]
}

async fn query(name: &str, sql: &str) -> Vec<Value> {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest(name, temp.path(), "Json"));
    execution_to_rows(
        &CoralQuery::execute_sql(&[source], test_runtime(), sql)
            .await
            .expect("query should succeed"),
    )
}

#[tokio::test]
async fn json_get_str_extracts_from_json_typed_column() {
    let rows = query(
        "json_typed",
        "SELECT id, json_get_str(properties, '$browser') AS browser \
         FROM json_typed.events ORDER BY id",
    )
    .await;

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
            test_runtime(),
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
async fn json_string_scalars_round_trip_as_json_text() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "events.jsonl",
        &[json!({"id": 1, "properties": "hello"})],
    );
    let source = build_source(events_manifest("json_scalar", temp.path(), "Json"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, json_as_text(properties) AS value FROM json_scalar.events",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 1, "value": "hello"})]);
}

#[tokio::test]
async fn json_get_int_filters_typed_values() {
    let rows = query(
        "json_filter",
        "SELECT id FROM json_filter.events \
         WHERE json_get_int(properties, 'count') > 5",
    )
    .await;

    assert_eq!(rows, vec![json!({"id": 1})]);
}

#[tokio::test]
async fn json_get_float_extracts_typed_value() {
    let rows = query(
        "json_float",
        "SELECT id, json_get_float(properties, 'score') AS score \
         FROM json_float.events ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "score": 4.5}),
            json!({"id": 2, "score": 1.0}),
        ]
    );
}

#[tokio::test]
async fn json_get_bool_extracts_typed_value() {
    let rows = query(
        "json_bool",
        "SELECT id FROM json_bool.events \
         WHERE json_get_bool(properties, 'active')",
    )
    .await;

    assert_eq!(rows, vec![json!({"id": 1})]);
}

#[tokio::test]
async fn json_get_json_returns_nested_object_text() {
    let rows = query(
        "json_nested",
        "SELECT json_get_str(json_get_json(properties, 'geo'), 'country') AS country \
         FROM json_nested.events WHERE id = 1",
    )
    .await;

    assert_eq!(rows, vec![json!({"country": "US"})]);
}

#[tokio::test]
async fn json_get_array_returns_array_payload() {
    let rows = query(
        "json_array",
        "SELECT id, cardinality(json_get_array(properties, 'tags')) AS tag_count \
         FROM json_array.events ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "tag_count": 2}),
            json!({"id": 2, "tag_count": 1}),
        ]
    );
}

#[tokio::test]
async fn json_get_returns_union_value_via_cast() {
    let rows = query(
        "json_union",
        "SELECT id FROM json_union.events \
         WHERE json_get(properties, 'count')::bigint > 5",
    )
    .await;

    assert_eq!(rows, vec![json!({"id": 1})]);
}

#[tokio::test]
async fn json_contains_checks_path_existence() {
    let rows = query(
        "json_contains",
        "SELECT id FROM json_contains.events \
         WHERE json_contains(properties, '$browser') \
           AND NOT json_contains(properties, 'missing')",
    )
    .await;

    assert_eq!(rows, vec![json!({"id": 1}), json!({"id": 2})]);
}

#[tokio::test]
async fn json_length_returns_array_length() {
    let rows = query(
        "json_length",
        "SELECT id, json_length(properties, 'tags') AS len \
         FROM json_length.events ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![json!({"id": 1, "len": 2}), json!({"id": 2, "len": 1}),]
    );
}

#[tokio::test]
async fn json_as_text_renders_value_as_string() {
    let rows = query(
        "json_as_text",
        "SELECT id, json_as_text(properties, 'count') AS count_text \
         FROM json_as_text.events ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "count_text": "7"}),
            json!({"id": 2, "count_text": "3"}),
        ]
    );
}

#[tokio::test]
async fn json_operators_are_rejected() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &events_fixture());
    let source = build_source(events_manifest("json_ops", temp.path(), "Json"));

    for sql in [
        "SELECT id, properties->>'$browser' AS browser FROM json_ops.events",
        "SELECT id FROM json_ops.events WHERE (properties->'count')::bigint > 5",
        "SELECT id FROM json_ops.events WHERE properties ? '$browser'",
    ] {
        let error = CoralQuery::execute_sql(std::slice::from_ref(&source), test_runtime(), sql)
            .await
            .expect_err("query should reject JSON operators");

        assert!(
            matches!(
                error,
                CoreError::InvalidInput(_) | CoreError::Unimplemented(_) | CoreError::Internal(_)
            ),
            "expected a planning failure for `{sql}`, got {error:?}"
        );
    }
}
