use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, CoreError, QueryExecution, QueryRuntimeConfig, QuerySource, StatusCode,
};
use coral_spec::parse_source_manifest_value;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use tempfile::TempDir;

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
    batches_to_rows(execution.batches())
}

pub(crate) async fn source_rows(source: &QuerySource, sql: &str) -> Vec<Value> {
    source_rows_with_runtime(source, test_runtime(), sql).await
}

pub(crate) async fn assert_source_rows(source: &QuerySource, sql: &str, expected: Vec<Value>) {
    assert_query_rows(std::slice::from_ref(source), sql, expected).await;
}

pub(crate) async fn source_rows_with_runtime(
    source: &QuerySource,
    runtime: QueryRuntimeConfig,
    sql: &str,
) -> Vec<Value> {
    query_rows_with_runtime(std::slice::from_ref(source), runtime, sql).await
}

pub(crate) async fn query_rows(sources: &[QuerySource], sql: &str) -> Vec<Value> {
    query_rows_with_runtime(sources, test_runtime(), sql).await
}

pub(crate) async fn assert_query_rows(sources: &[QuerySource], sql: &str, expected: Vec<Value>) {
    assert_eq!(query_rows(sources, sql).await, expected);
}

pub(crate) async fn query_rows_with_runtime(
    sources: &[QuerySource],
    runtime: QueryRuntimeConfig,
    sql: &str,
) -> Vec<Value> {
    execution_to_rows(
        &CoralQuery::execute_sql(sources, runtime, sql)
            .await
            .expect("query should succeed"),
    )
}

pub(crate) async fn source_error(source: &QuerySource, sql: &str) -> CoreError {
    query_error(std::slice::from_ref(source), sql).await
}

pub(crate) async fn query_error(sources: &[QuerySource], sql: &str) -> CoreError {
    CoralQuery::execute_sql(sources, test_runtime(), sql)
        .await
        .expect_err("query should fail")
}

pub(crate) async fn assert_users_query_matrix(
    source_prefix: &str,
    source_factory: fn(&str) -> (TempDir, QuerySource),
) {
    for (case_name, sql_template, expected) in [
        (
            "users",
            "SELECT id, name, email FROM {schema}.users ORDER BY id",
            users_rows(),
        ),
        (
            "projection",
            "SELECT name FROM {schema}.users ORDER BY name DESC",
            vec![
                json!({"name": "Linus"}),
                json!({"name": "Grace"}),
                json!({"name": "Ada"}),
            ],
        ),
        (
            "filter",
            "SELECT id, name FROM {schema}.users WHERE id = 2",
            vec![json!({"id": 2, "name": "Grace"})],
        ),
        (
            "order",
            "SELECT name FROM {schema}.users ORDER BY name DESC LIMIT 2",
            vec![json!({"name": "Linus"}), json!({"name": "Grace"})],
        ),
        (
            "count",
            "SELECT COUNT(*) AS n FROM {schema}.users",
            vec![json!({"n": 3})],
        ),
    ] {
        let schema = format!("{source_prefix}_{case_name}");
        let (_temp, source) = source_factory(&schema);
        let sql = sql_template.replace("{schema}", &schema);
        let rows = source_rows(&source, &sql).await;
        assert_eq!(
            rows, expected,
            "{source_prefix} query matrix case {case_name}"
        );
    }
}

pub(crate) fn batches_to_rows(batches: &[RecordBatch]) -> Vec<Value> {
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

pub(crate) fn assert_row_count(execution: &QueryExecution, expected: usize) {
    assert_eq!(execution.row_count(), expected);
    assert_eq!(execution_to_rows(execution).len(), expected);
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

pub(crate) fn users_rows() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "Ada", "email": "ada@example.com"}),
        json!({"id": 2, "name": "Grace", "email": "grace@example.com"}),
        json!({"id": 3, "name": "Linus", "email": "linus@example.com"}),
    ]
}

pub(crate) fn users_jsonl_manifest(name: &str, dir: &Path, glob: &str) -> Value {
    users_file_manifest(
        name,
        dir,
        "jsonl",
        glob,
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "name", "type": "Utf8" }),
            json!({ "name": "email", "type": "Utf8" }),
        ],
    )
}

pub(crate) fn users_parquet_manifest(name: &str, dir: &Path) -> Value {
    users_file_manifest(name, dir, "parquet", "**/*.parquet", &[])
}

pub(crate) fn file_table_manifest(
    name: &str,
    table: &str,
    description: &str,
    format: &str,
    source: &Value,
    columns: &[Value],
) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": table,
            "description": description,
            "format": format,
            "source": source,
            "columns": columns
        }]
    })
}

fn users_file_manifest(
    name: &str,
    dir: &Path,
    format: &str,
    glob: &str,
    columns: &[Value],
) -> Value {
    file_table_manifest(
        name,
        "users",
        "Users fixture",
        format,
        &json!({
            "location": dir_url(dir),
            "glob": glob
        }),
        columns,
    )
}

pub(crate) fn users_jsonl_source(name: &str) -> (TempDir, QuerySource) {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(users_jsonl_manifest(name, temp.path(), "**/*.jsonl"));
    (temp, source)
}

pub(crate) fn users_parquet_source(name: &str) -> (TempDir, QuerySource) {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(users_parquet_manifest(name, temp.path()));
    (temp, source)
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
