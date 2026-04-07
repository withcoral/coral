use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource};
use coral_spec::parse_source_manifest_yaml;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

pub(crate) struct TestRuntime;

impl QueryRuntimeProvider for TestRuntime {
    fn runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext::default()
    }
}

pub(crate) fn build_source(yaml: &str) -> QuerySource {
    build_source_with_inputs(yaml, BTreeMap::new(), BTreeMap::new())
}

pub(crate) fn build_source_with_secrets(
    yaml: &str,
    secrets: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> QuerySource {
    build_source_with_inputs(yaml, BTreeMap::new(), string_map(secrets))
}

pub(crate) fn build_source_with_inputs(
    yaml: &str,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
) -> QuerySource {
    let manifest = parse_source_manifest_yaml(yaml).expect("manifest should parse");
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
