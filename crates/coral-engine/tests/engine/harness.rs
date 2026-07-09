use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, CoreError, QueryExecution, QueryRuntimeConfig, QuerySource, StatisticsObservation,
    StatusCode,
};
use coral_spec::parse_source_manifest_value;
use opentelemetry::Value as OtelValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

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

pub(crate) async fn execute_sql_with_trace_observations(
    sources: &[QuerySource],
    sql: &str,
) -> Result<(QueryExecution, Vec<StatisticsObservation>), CoreError> {
    let source_vec = sources.to_vec();
    let sql = sql.to_string();
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("coral-engine-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

    let result = {
        let _guard = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!("coral.query");
        let result = CoralQuery::execute_sql(&source_vec, test_runtime(), &sql)
            .instrument(span.clone())
            .await;
        drop(span);
        result
    };

    provider.force_flush().expect("trace provider should flush");
    let spans = exporter
        .get_finished_spans()
        .expect("finished spans should be readable");
    let observations = statistics_observations_from_spans(&spans);
    result.map(|execution| (execution, observations))
}

fn statistics_observations_from_spans(spans: &[SpanData]) -> Vec<StatisticsObservation> {
    spans
        .iter()
        .flat_map(|span| {
            span.events
                .events
                .iter()
                .flat_map(|event| event.attributes.iter())
        })
        .filter(|attribute| attribute.key.as_str() == "coral.statistics.observation")
        .filter_map(|attribute| match &attribute.value {
            OtelValue::String(value) => serde_json::from_str(value.as_str()).ok(),
            _ => None,
        })
        .collect()
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
