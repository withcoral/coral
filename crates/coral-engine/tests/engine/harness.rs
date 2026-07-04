use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{CoreError, QueryExecution, QueryRuntimeConfig, QuerySource, StatusCode};
use coral_spec::parse_source_manifest_value;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

pub(crate) fn test_runtime() -> QueryRuntimeConfig {
    QueryRuntimeConfig::default()
}

pub(crate) fn build_source(value: Value) -> QuerySource {
    build_source_with_inputs(value, BTreeMap::new(), BTreeMap::new())
}

pub(crate) fn build_source_with_secrets(
    value: Value,
    secrets: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> QuerySource {
    build_source_with_inputs(value, BTreeMap::new(), string_map(secrets))
}

pub(crate) fn build_source_with_inputs(
    value: Value,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
) -> QuerySource {
    let manifest = parse_source_manifest_value(value).expect("manifest should parse");
    QuerySource::new(manifest, variables, secrets)
}

pub(crate) fn execution_to_rows(execution: &QueryExecution) -> Vec<Value> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
        for batch in execution.batches() {
            writer.write(batch).expect("batch should encode to json");
        }
        writer.finish().expect("json writer should finish");
    }
    serde_json::from_slice(&bytes).expect("json rows should decode")
}

pub(crate) fn assert_row_count(execution: &QueryExecution, expected: usize) {
    assert_eq!(execution.row_count(), expected);
    assert_eq!(execution_to_rows(execution).len(), expected);
}

#[expect(
    dead_code,
    reason = "retained for other engine tests that may assert invalid input"
)]
pub(crate) fn assert_invalid_input(error: CoreError, expected_detail: &str) {
    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    match error {
        CoreError::InvalidInput(detail) => assert_eq!(detail, expected_detail),
        other => panic!("expected CoreError::InvalidInput, got {other:?}"),
    }
}

pub(crate) fn assert_table_not_found(
    error: CoreError,
    expected_schema: &str,
    expected_table: &str,
) {
    assert_eq!(error.status_code(), StatusCode::NotFound);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
            assert_eq!(
                sqe.metadata().get("schema").map(String::as_str),
                Some(expected_schema)
            );
            assert_eq!(
                sqe.metadata().get("table").map(String::as_str),
                Some(expected_table)
            );
        }
        other => panic!("expected CoreError::QueryFailure, got {other:?}"),
    }
}

pub(crate) fn write_jsonl_file(dir: &Path, filename: &str, rows: &[Value]) {
    let path = dir.join(filename);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("jsonl parent directory should exist");
    }
    let mut data = String::new();
    for row in rows {
        data.push_str(&serde_json::to_string(row).expect("json row should serialize for fixture"));
        data.push('\n');
    }
    fs::write(path, data).expect("jsonl fixture should write");
}

pub(crate) fn write_parquet_file(dir: &Path, filename: &str, batch: &RecordBatch) {
    let path = dir.join(filename);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parquet parent directory should exist");
    }
    let file = fs::File::create(path).expect("parquet fixture should open");
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer should start");
    writer.write(batch).expect("parquet batch should write");
    writer.close().expect("parquet writer should close");
}

pub(crate) fn dir_url(path: &Path) -> String {
    format!("file://{}/", path.display())
}

/// Richer SHARED virtual-graph fixture ("fixture B"): duplicated numeric values (meaningful
/// DISTINCT aggregates), a numeric spread (stddev), and null-bearing `city` (null ordering).
/// People-only (single-label Person) — relationships are `#[serde(default)]` so a node-only graph
/// is valid; a KNOWS extension is a documented follow-up for relationship-level cypher aggregates.
pub(crate) fn write_rich_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "rich_people.jsonl",
        &[
            json!({"id": 1, "name": "Ada", "age": 20, "city": "London", "score": 8.0,  "weight": 7.0,  "joined": "2020-06-01T09:00:00Z", "birthday": "1990-05-20"}),
            json!({"id": 2, "name": "Bea", "age": 20, "city": "London", "score": 8.0,  "weight": 9.0,  "joined": "2021-03-15T09:00:00Z", "birthday": "1985-12-01"}),
            json!({"id": 3, "name": "Cee", "age": 20, "city": null,     "score": 8.0,  "weight": 10.0, "joined": "2019-11-30T09:00:00Z", "birthday": "1995-08-15"}),
            json!({"id": 4, "name": "Dot", "age": 30, "city": "Paris",  "score": 12.0, "weight": 10.0, "joined": "2022-01-10T09:00:00Z", "birthday": "1988-03-30"}),
            json!({"id": 5, "name": "Eve", "age": 40, "city": null,     "score": 12.0, "weight": 11.0, "joined": "2018-07-22T09:00:00Z", "birthday": "1992-10-10"}),
            json!({"id": 6, "name": "Fay", "age": 50, "city": "Paris",  "score": 12.0, "weight": 13.0, "joined": "2023-09-05T09:00:00Z", "birthday": "1979-01-25"}),
        ],
    );
}

pub(crate) fn rich_manifest(dir: &Path) -> Value {
    json!({
        "name": "rich",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "people",
                "description": "Richer shared virtual-graph people fixture (duplicates, spread, nulls)",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "rich_people.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "name", "type": "Utf8" },
                    { "name": "age", "type": "Int64" },
                    { "name": "city", "type": "Utf8" },
                    { "name": "score", "type": "Float64" },
                    { "name": "weight", "type": "Float64" },
                    { "name": "joined", "type": "Timestamp" },
                    { "name": "birthday", "type": "Date" }
                ]
            }
        ]
    })
}

pub(crate) const RICH_GRAPH: &str = r"
version: 1
name: rich-shared-fixture
description: Richer shared virtual-graph fixture — duplicated values, numeric spread, null-bearing props
nodes:
  - label: Person
    table: { schema: rich, name: people }
    key: id
    properties:
      name: name
      age: age
      city: city
      score: score
      weight: weight
      joined: joined
      birthday: birthday
";

pub(crate) fn users_rows() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "Ada", "email": "ada@example.com"}),
        json!({"id": 2, "name": "Grace", "email": "grace@example.com"}),
        json!({"id": 3, "name": "Linus", "email": "linus@example.com"}),
    ]
}

pub(crate) fn users_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["Ada", "Grace", "Linus"])),
            Arc::new(StringArray::from(vec![
                "ada@example.com",
                "grace@example.com",
                "linus@example.com",
            ])),
        ],
    )
    .expect("user batch should build")
}

fn string_map(
    items: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> BTreeMap<String, String> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
