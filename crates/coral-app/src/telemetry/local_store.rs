//! Parquet-backed span export for local trace capture.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array as _, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use opentelemetry::trace::{SpanId, SpanKind, Status};
use opentelemetry::{Array as OtelArray, KeyValue, Value as OtelValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};

const PARQUET_BATCH_SIZE: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalTraceStoreError {
    #[error("failed to create local trace store directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to create local trace store file {path}: {source}")]
    CreateFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to publish local trace store file {from} to {to}: {source}")]
    PublishFile {
        from: PathBuf,
        to: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to encode local trace store file {path}: {source}")]
    Arrow { path: PathBuf, source: ArrowError },
    #[error("failed to write local trace store file {path}: {source}")]
    Parquet { path: PathBuf, source: ParquetError },
}

#[derive(Debug, Clone)]
pub(crate) struct ParquetSpanExporter {
    dir: PathBuf,
    resource_json: Arc<Mutex<String>>,
    shutdown_called: Arc<AtomicBool>,
    file_counter: Arc<AtomicU64>,
}

impl ParquetSpanExporter {
    pub(crate) fn new(dir: PathBuf) -> Result<Self, LocalTraceStoreError> {
        fs::create_dir_all(&dir).map_err(|source| LocalTraceStoreError::CreateDir {
            path: dir.clone(),
            source,
        })?;

        Ok(Self {
            dir,
            resource_json: Arc::new(Mutex::new("{}".to_string())),
            shutdown_called: Arc::new(AtomicBool::new(false)),
            file_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    fn resource_json(&self) -> String {
        self.resource_json
            .lock()
            .map_or_else(|_| "{}".to_string(), |resource_json| resource_json.clone())
    }
}

impl SpanExporter for ParquetSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        if self.shutdown_called.load(Ordering::Relaxed) {
            return Err(OTelSdkError::AlreadyShutdown);
        }

        write_batch(&self.dir, &self.resource_json(), &self.file_counter, &batch)
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))
    }

    fn shutdown_with_timeout(&mut self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown_called.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        if let Ok(mut resource_json) = self.resource_json.lock() {
            *resource_json = resource_json_from_resource(resource);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TraceStore {
    dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraceStoreError {
    #[error("trace '{0}' not found")]
    NotFound(String),
    #[error("failed to read local trace store directory {path}: {source}")]
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to open local trace store file {path}: {source}")]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to decode local trace store file {path}: {source}")]
    Arrow { path: PathBuf, source: ArrowError },
    #[error("failed to read local trace store file {path}: {source}")]
    Parquet { path: PathBuf, source: ParquetError },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoredTraceStatus {
    Unspecified,
    Ok,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceSummaryRecord {
    pub(crate) trace_id: String,
    pub(crate) root_span_id: String,
    pub(crate) name: String,
    pub(crate) query: String,
    pub(crate) status: StoredTraceStatus,
    pub(crate) start_time_unix_nanos: i64,
    pub(crate) end_time_unix_nanos: i64,
    pub(crate) duration_nanos: i64,
    pub(crate) span_count: u32,
    pub(crate) row_count: u64,
    pub(crate) row_count_recorded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceSpanRecord {
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) parent_span_id: Option<String>,
    pub(crate) parent_span_is_remote: bool,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) status: StoredTraceStatus,
    pub(crate) status_message: Option<String>,
    pub(crate) start_time_unix_nanos: i64,
    pub(crate) end_time_unix_nanos: i64,
    pub(crate) duration_nanos: i64,
    pub(crate) attributes_json: String,
    pub(crate) events_json: String,
    pub(crate) links_json: String,
    pub(crate) resource_json: String,
    pub(crate) scope_name: String,
    pub(crate) scope_version: Option<String>,
    pub(crate) scope_schema_url: Option<String>,
    pub(crate) scope_attributes_json: String,
    pub(crate) trace_flags: i32,
    pub(crate) trace_state: String,
    pub(crate) is_remote: bool,
    pub(crate) dropped_attributes_count: u32,
    pub(crate) dropped_events_count: u32,
    pub(crate) dropped_links_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceDetailRecord {
    pub(crate) summary: TraceSummaryRecord,
    pub(crate) spans: Vec<TraceSpanRecord>,
}

struct TraceAggregate {
    trace_id: String,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    span_count: u32,
    error_count: u32,
}

impl TraceStore {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub(crate) fn list_traces(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        let mut traces: HashMap<String, Vec<TraceSpanRecord>> = HashMap::new();
        for span in self.read_spans()? {
            traces.entry(span.trace_id.clone()).or_default().push(span);
        }

        let mut summaries = traces
            .into_iter()
            .map(|(trace_id, spans)| summary_from_spans(&trace_id, &spans))
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            right
                .end_time_unix_nanos
                .cmp(&left.end_time_unix_nanos)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });

        Ok(summaries.into_iter().skip(offset).take(limit).collect())
    }

    pub(crate) fn get_trace(&self, trace_id: &str) -> Result<TraceDetailRecord, TraceStoreError> {
        let mut spans = self
            .read_spans()?
            .into_iter()
            .filter(|span| span.trace_id == trace_id)
            .collect::<Vec<_>>();

        if spans.is_empty() {
            return Err(TraceStoreError::NotFound(trace_id.to_string()));
        }

        spans.sort_by(|left, right| {
            left.start_time_unix_nanos
                .cmp(&right.start_time_unix_nanos)
                .then_with(|| left.span_id.cmp(&right.span_id))
        });

        let summary = summary_from_spans(trace_id, &spans);
        Ok(TraceDetailRecord { summary, spans })
    }

    fn read_spans(&self) -> Result<Vec<TraceSpanRecord>, TraceStoreError> {
        let mut spans_by_id = HashMap::new();
        for path in self.parquet_files()? {
            for span in read_spans_file(&path)? {
                spans_by_id.insert((span.trace_id.clone(), span.span_id.clone()), span);
            }
        }
        Ok(spans_by_id.into_values().collect())
    }

    fn parquet_files(&self) -> Result<Vec<PathBuf>, TraceStoreError> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let entries = fs::read_dir(&self.dir).map_err(|source| TraceStoreError::ReadDir {
            path: self.dir.clone(),
            source,
        })?;
        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| TraceStoreError::ReadDir {
                path: self.dir.clone(),
                source,
            })?;
            let path = entry.path();
            if path
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|extension| extension == "parquet")
            {
                files.push(path);
            }
        }
        files.sort();
        Ok(files)
    }
}

fn read_spans_file(path: &Path) -> Result<Vec<TraceSpanRecord>, TraceStoreError> {
    let file = File::open(path).map_err(|source| TraceStoreError::OpenFile {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|source| TraceStoreError::Parquet {
            path: path.to_path_buf(),
            source,
        })?
        .with_batch_size(PARQUET_BATCH_SIZE)
        .build()
        .map_err(|source| TraceStoreError::Parquet {
            path: path.to_path_buf(),
            source,
        })?;
    let mut spans = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|source| TraceStoreError::Arrow {
            path: path.to_path_buf(),
            source,
        })?;
        spans.extend(
            records_from_batch(&batch).map_err(|source| TraceStoreError::Arrow {
                path: path.to_path_buf(),
                source,
            })?,
        );
    }
    Ok(spans)
}

fn summary_from_spans(trace_id: &str, spans: &[TraceSpanRecord]) -> TraceSummaryRecord {
    let start_time_unix_nanos = spans
        .iter()
        .map(|span| span.start_time_unix_nanos)
        .min()
        .unwrap_or_default();
    let end_time_unix_nanos = spans
        .iter()
        .map(|span| span.end_time_unix_nanos)
        .max()
        .unwrap_or(start_time_unix_nanos);
    let error_count = spans
        .iter()
        .filter(|span| span.status == StoredTraceStatus::Error)
        .count();
    let aggregate = TraceAggregate {
        trace_id: trace_id.to_string(),
        start_time_unix_nanos,
        end_time_unix_nanos,
        span_count: usize_to_u32(spans.len()),
        error_count: usize_to_u32(error_count),
    };
    let primary = spans.iter().min_by_key(|span| {
        let priority = if span.name == "coral.query" {
            0
        } else if span.parent_span_id.is_none() {
            1
        } else {
            2
        };
        (priority, span.start_time_unix_nanos, span.span_id.as_str())
    });
    summary_from_aggregate(&aggregate, primary)
}

fn summary_from_aggregate(
    aggregate: &TraceAggregate,
    primary: Option<&TraceSpanRecord>,
) -> TraceSummaryRecord {
    let fallback_status = if aggregate.error_count > 0 {
        StoredTraceStatus::Error
    } else {
        StoredTraceStatus::Unspecified
    };
    let duration_nanos = aggregate
        .end_time_unix_nanos
        .saturating_sub(aggregate.start_time_unix_nanos);

    primary.map_or_else(
        || TraceSummaryRecord {
            trace_id: aggregate.trace_id.clone(),
            root_span_id: String::new(),
            name: "trace".to_string(),
            query: String::new(),
            status: fallback_status,
            start_time_unix_nanos: aggregate.start_time_unix_nanos,
            end_time_unix_nanos: aggregate.end_time_unix_nanos,
            duration_nanos,
            span_count: aggregate.span_count,
            row_count: 0,
            row_count_recorded: false,
        },
        |primary| {
            let attributes = parse_attributes(&primary.attributes_json);
            let status = status_from_attributes(attributes.as_ref()).unwrap_or_else(|| {
                if primary.status == StoredTraceStatus::Unspecified {
                    fallback_status
                } else {
                    primary.status
                }
            });
            let row_count = attributes
                .as_ref()
                .and_then(|attrs| attr_u64(attrs, "row_count"));

            TraceSummaryRecord {
                trace_id: aggregate.trace_id.clone(),
                root_span_id: primary.span_id.clone(),
                name: primary.name.clone(),
                query: attributes
                    .as_ref()
                    .and_then(|attrs| attr_string(attrs, "sql"))
                    .unwrap_or_default(),
                status,
                start_time_unix_nanos: aggregate.start_time_unix_nanos,
                end_time_unix_nanos: aggregate.end_time_unix_nanos,
                duration_nanos,
                span_count: aggregate.span_count,
                row_count: row_count.unwrap_or_default(),
                row_count_recorded: row_count.is_some(),
            }
        },
    )
}

fn write_batch(
    dir: &Path,
    resource_json: &str,
    file_counter: &AtomicU64,
    batch: &[SpanData],
) -> Result<(), LocalTraceStoreError> {
    if batch.is_empty() {
        return Ok(());
    }

    fs::create_dir_all(dir).map_err(|source| LocalTraceStoreError::CreateDir {
        path: dir.to_path_buf(),
        source,
    })?;
    let schema = trace_store_schema();
    let record_batch = span_batch(schema.clone(), resource_json, batch).map_err(|source| {
        LocalTraceStoreError::Arrow {
            path: dir.to_path_buf(),
            source,
        }
    })?;
    let (temp_path, final_path) = next_batch_paths(dir, file_counter);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|source| LocalTraceStoreError::CreateFile {
            path: temp_path.clone(),
            source,
        })?;
    let mut writer = ArrowWriter::try_new(file, schema, None).map_err(|source| {
        LocalTraceStoreError::Parquet {
            path: temp_path.clone(),
            source,
        }
    })?;
    writer
        .write(&record_batch)
        .map_err(|source| LocalTraceStoreError::Parquet {
            path: temp_path.clone(),
            source,
        })?;
    writer
        .close()
        .map_err(|source| LocalTraceStoreError::Parquet {
            path: temp_path.clone(),
            source,
        })?;
    fs::rename(&temp_path, &final_path).map_err(|source| LocalTraceStoreError::PublishFile {
        from: temp_path,
        to: final_path,
        source,
    })
}

fn next_batch_paths(dir: &Path, file_counter: &AtomicU64) -> (PathBuf, PathBuf) {
    let sequence = file_counter.fetch_add(1, Ordering::Relaxed);
    let unix_nanos = unix_nanos(SystemTime::now());
    let filename = format!(
        "spans-{unix_nanos:020}-{}-{sequence:016}.parquet",
        process::id()
    );
    let final_path = dir.join(filename);
    let temp_path = final_path.with_extension("parquet.tmp");
    (temp_path, final_path)
}

fn span_batch(
    schema: SchemaRef,
    resource_json: &str,
    batch: &[SpanData],
) -> Result<RecordBatch, ArrowError> {
    let records = batch
        .iter()
        .map(|span| span_record(resource_json, span))
        .collect::<Vec<_>>();
    records_to_batch(schema, &records)
}

#[allow(
    clippy::too_many_lines,
    reason = "The Parquet record schema is intentionally explicit and column-oriented"
)]
fn records_to_batch(
    schema: SchemaRef,
    records: &[TraceSpanRecord],
) -> Result<RecordBatch, ArrowError> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.trace_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.span_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.parent_span_id.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            records
                .iter()
                .map(|record| record.parent_span_is_remote)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.kind.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| status_code(record.status))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.status_message.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.start_time_unix_nanos)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.end_time_unix_nanos)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.duration_nanos)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.attributes_json.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.events_json.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.links_json.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.resource_json.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.scope_name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.scope_version.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.scope_schema_url.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.scope_attributes_json.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            records
                .iter()
                .map(|record| record.trace_flags)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.trace_state.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            records
                .iter()
                .map(|record| record.is_remote)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            records
                .iter()
                .map(|record| record.dropped_attributes_count)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            records
                .iter()
                .map(|record| record.dropped_events_count)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            records
                .iter()
                .map(|record| record.dropped_links_count)
                .collect::<Vec<_>>(),
        )),
    ];

    RecordBatch::try_new(schema, columns)
}

fn span_record(resource_json: &str, span: &SpanData) -> TraceSpanRecord {
    let span_context = &span.span_context;
    let parent_span_id =
        (span.parent_span_id != SpanId::INVALID).then(|| span.parent_span_id.to_string());
    let (status, status_message) = status_parts(&span.status);

    TraceSpanRecord {
        trace_id: span_context.trace_id().to_string(),
        span_id: span_context.span_id().to_string(),
        parent_span_id,
        parent_span_is_remote: span.parent_span_is_remote,
        name: span.name.to_string(),
        kind: span_kind(&span.span_kind).to_string(),
        status,
        status_message,
        start_time_unix_nanos: unix_nanos(span.start_time),
        end_time_unix_nanos: unix_nanos(span.end_time),
        duration_nanos: duration_nanos(span.start_time, span.end_time),
        attributes_json: key_values_json(span.attributes.iter()).to_string(),
        events_json: events_json(span).to_string(),
        links_json: links_json(span).to_string(),
        resource_json: resource_json.to_string(),
        scope_name: span.instrumentation_scope.name().to_string(),
        scope_version: span
            .instrumentation_scope
            .version()
            .map(ToString::to_string),
        scope_schema_url: span
            .instrumentation_scope
            .schema_url()
            .map(ToString::to_string),
        scope_attributes_json: key_values_json(span.instrumentation_scope.attributes()).to_string(),
        trace_flags: i32::from(span_context.trace_flags().to_u8()),
        trace_state: span_context.trace_state().header(),
        is_remote: span_context.is_remote(),
        dropped_attributes_count: span.dropped_attributes_count,
        dropped_events_count: span.events.dropped_count,
        dropped_links_count: span.links.dropped_count,
    }
}

fn records_from_batch(batch: &RecordBatch) -> Result<Vec<TraceSpanRecord>, ArrowError> {
    let trace_id = string_column(batch, "trace_id")?;
    let span_id = string_column(batch, "span_id")?;
    let parent_span_id = string_column(batch, "parent_span_id")?;
    let parent_span_is_remote = bool_column(batch, "parent_span_is_remote")?;
    let name = string_column(batch, "name")?;
    let kind = string_column(batch, "kind")?;
    let status_code = string_column(batch, "status_code")?;
    let status_message = string_column(batch, "status_message")?;
    let start_time_unix_nanos = i64_column(batch, "start_time_unix_nanos")?;
    let end_time_unix_nanos = i64_column(batch, "end_time_unix_nanos")?;
    let duration_nanos = i64_column(batch, "duration_nanos")?;
    let attributes_json = string_column(batch, "attributes_json")?;
    let events_json = string_column(batch, "events_json")?;
    let links_json = string_column(batch, "links_json")?;
    let resource_json = string_column(batch, "resource_json")?;
    let scope_name = string_column(batch, "scope_name")?;
    let scope_version = string_column(batch, "scope_version")?;
    let scope_schema_url = string_column(batch, "scope_schema_url")?;
    let scope_attributes_json = string_column(batch, "scope_attributes_json")?;
    let trace_flags = i32_column(batch, "trace_flags")?;
    let trace_state = string_column(batch, "trace_state")?;
    let is_remote = bool_column(batch, "is_remote")?;
    let dropped_attributes_count = u32_column(batch, "dropped_attributes_count")?;
    let dropped_events_count = u32_column(batch, "dropped_events_count")?;
    let dropped_links_count = u32_column(batch, "dropped_links_count")?;

    let mut records = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        records.push(TraceSpanRecord {
            trace_id: required_string(trace_id, row, "trace_id")?,
            span_id: required_string(span_id, row, "span_id")?,
            parent_span_id: optional_string(parent_span_id, row),
            parent_span_is_remote: parent_span_is_remote.value(row),
            name: required_string(name, row, "name")?,
            kind: required_string(kind, row, "kind")?,
            status: stored_status(&required_string(status_code, row, "status_code")?),
            status_message: optional_string(status_message, row),
            start_time_unix_nanos: start_time_unix_nanos.value(row),
            end_time_unix_nanos: end_time_unix_nanos.value(row),
            duration_nanos: duration_nanos.value(row),
            attributes_json: required_string(attributes_json, row, "attributes_json")?,
            events_json: required_string(events_json, row, "events_json")?,
            links_json: required_string(links_json, row, "links_json")?,
            resource_json: required_string(resource_json, row, "resource_json")?,
            scope_name: required_string(scope_name, row, "scope_name")?,
            scope_version: optional_string(scope_version, row),
            scope_schema_url: optional_string(scope_schema_url, row),
            scope_attributes_json: required_string(
                scope_attributes_json,
                row,
                "scope_attributes_json",
            )?,
            trace_flags: trace_flags.value(row),
            trace_state: required_string(trace_state, row, "trace_state")?,
            is_remote: is_remote.value(row),
            dropped_attributes_count: dropped_attributes_count.value(row),
            dropped_events_count: dropped_events_count.value(row),
            dropped_links_count: dropped_links_count.value(row),
        });
    }
    Ok(records)
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, ArrowError> {
    typed_column(batch, name)
}

fn bool_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray, ArrowError> {
    typed_column(batch, name)
}

fn i32_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array, ArrowError> {
    typed_column(batch, name)
}

fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array, ArrowError> {
    typed_column(batch, name)
}

fn u32_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array, ArrowError> {
    typed_column(batch, name)
}

fn typed_column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, ArrowError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ArrowError::SchemaError(format!("missing trace store column '{name}'")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| ArrowError::SchemaError(format!("invalid trace store column '{name}'")))
}

fn required_string(column: &StringArray, row: usize, name: &str) -> Result<String, ArrowError> {
    if column.is_null(row) {
        return Err(ArrowError::SchemaError(format!(
            "trace store column '{name}' is null"
        )));
    }
    Ok(column.value(row).to_string())
}

fn optional_string(column: &StringArray, row: usize) -> Option<String> {
    (!column.is_null(row)).then(|| column.value(row).to_string())
}

fn trace_store_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("parent_span_is_remote", DataType::Boolean, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("status_code", DataType::Utf8, false),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("start_time_unix_nanos", DataType::Int64, false),
        Field::new("end_time_unix_nanos", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("attributes_json", DataType::Utf8, false),
        Field::new("events_json", DataType::Utf8, false),
        Field::new("links_json", DataType::Utf8, false),
        Field::new("resource_json", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes_json", DataType::Utf8, false),
        Field::new("trace_flags", DataType::Int32, false),
        Field::new("trace_state", DataType::Utf8, false),
        Field::new("is_remote", DataType::Boolean, false),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
        Field::new("dropped_events_count", DataType::UInt32, false),
        Field::new("dropped_links_count", DataType::UInt32, false),
    ]))
}

fn parse_attributes(attributes_json: &str) -> Option<JsonValue> {
    serde_json::from_str(attributes_json).ok()
}

fn status_from_attributes(attributes: Option<&JsonValue>) -> Option<StoredTraceStatus> {
    match attr_string(attributes?, "status")?.as_str() {
        "ok" => Some(StoredTraceStatus::Ok),
        "error" => Some(StoredTraceStatus::Error),
        _ => None,
    }
}

fn attr_string(attributes: &JsonValue, key: &str) -> Option<String> {
    match attributes.get(key)? {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn attr_u64(attributes: &JsonValue, key: &str) -> Option<u64> {
    match attributes.get(key)? {
        JsonValue::Number(value) => value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok())),
        JsonValue::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn stored_status(status_code: &str) -> StoredTraceStatus {
    match status_code {
        "ok" => StoredTraceStatus::Ok,
        "error" => StoredTraceStatus::Error,
        _ => StoredTraceStatus::Unspecified,
    }
}

fn status_code(status: StoredTraceStatus) -> &'static str {
    match status {
        StoredTraceStatus::Unspecified => "unset",
        StoredTraceStatus::Ok => "ok",
        StoredTraceStatus::Error => "error",
    }
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn key_values_json<'a>(attributes: impl IntoIterator<Item = &'a KeyValue>) -> JsonValue {
    key_value_pairs_json(
        attributes
            .into_iter()
            .map(|kv| (kv.key.as_str(), &kv.value)),
    )
}

fn key_value_pairs_json<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a OtelValue)>,
) -> JsonValue {
    let mut map = JsonMap::new();
    for (key, value) in pairs {
        map.insert(key.to_string(), otel_value_json(value));
    }
    JsonValue::Object(map)
}

fn resource_json_from_resource(resource: &Resource) -> String {
    key_value_pairs_json(resource.iter().map(|(key, value)| (key.as_str(), value))).to_string()
}

fn events_json(span: &SpanData) -> JsonValue {
    json!({
        "dropped_count": span.events.dropped_count,
        "events": span.events.events.iter().map(|event| {
            json!({
                "name": event.name.as_ref(),
                "time_unix_nanos": unix_nanos(event.timestamp),
                "attributes": key_values_json(event.attributes.iter()),
                "dropped_attributes_count": event.dropped_attributes_count,
            })
        }).collect::<Vec<_>>(),
    })
}

fn links_json(span: &SpanData) -> JsonValue {
    json!({
        "dropped_count": span.links.dropped_count,
        "links": span.links.links.iter().map(|link| {
            let span_context = &link.span_context;
            json!({
                "trace_id": span_context.trace_id().to_string(),
                "span_id": span_context.span_id().to_string(),
                "trace_flags": span_context.trace_flags().to_u8(),
                "trace_state": span_context.trace_state().header(),
                "is_remote": span_context.is_remote(),
                "attributes": key_values_json(link.attributes.iter()),
                "dropped_attributes_count": link.dropped_attributes_count,
            })
        }).collect::<Vec<_>>(),
    })
}

fn otel_value_json(value: &OtelValue) -> JsonValue {
    match value {
        OtelValue::Bool(value) => JsonValue::Bool(*value),
        OtelValue::I64(value) => JsonValue::Number((*value).into()),
        OtelValue::F64(value) => f64_json(*value),
        OtelValue::String(value) => JsonValue::String(value.as_str().to_string()),
        OtelValue::Array(value) => otel_array_json(value),
        _ => JsonValue::String(value.to_string()),
    }
}

fn otel_array_json(value: &OtelArray) -> JsonValue {
    match value {
        OtelArray::Bool(values) => {
            JsonValue::Array(values.iter().copied().map(JsonValue::Bool).collect())
        }
        OtelArray::I64(values) => JsonValue::Array(
            values
                .iter()
                .copied()
                .map(|value| JsonValue::Number(value.into()))
                .collect(),
        ),
        OtelArray::F64(values) => JsonValue::Array(values.iter().copied().map(f64_json).collect()),
        OtelArray::String(values) => JsonValue::Array(
            values
                .iter()
                .map(|value| JsonValue::String(value.as_str().to_string()))
                .collect(),
        ),
        _ => JsonValue::String(value.to_string()),
    }
}

fn f64_json(value: f64) -> JsonValue {
    JsonNumber::from_f64(value).map_or(JsonValue::Null, JsonValue::Number)
}

fn span_kind(kind: &SpanKind) -> &'static str {
    match kind {
        SpanKind::Client => "client",
        SpanKind::Server => "server",
        SpanKind::Producer => "producer",
        SpanKind::Consumer => "consumer",
        SpanKind::Internal => "internal",
    }
}

fn status_parts(status: &Status) -> (StoredTraceStatus, Option<String>) {
    match status {
        Status::Unset => (StoredTraceStatus::Unspecified, None),
        Status::Error { description } => (StoredTraceStatus::Error, Some(description.to_string())),
        Status::Ok => (StoredTraceStatus::Ok, None),
    }
}

fn unix_nanos(time: SystemTime) -> i64 {
    let nanos = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

fn duration_nanos(start: SystemTime, end: SystemTime) -> i64 {
    let nanos = end.duration_since(start).unwrap_or_default().as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use opentelemetry::KeyValue;
    use opentelemetry::trace::{Span as _, SpanKind, Tracer, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tempfile::TempDir;

    use super::{ParquetSpanExporter, StoredTraceStatus, TraceStore};

    #[test]
    fn exports_finished_spans_to_parquet() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let exporter = ParquetSpanExporter::new(dir.clone()).expect("parquet span exporter");
        let provider = SdkTracerProvider::builder()
            .with_resource(
                opentelemetry_sdk::Resource::builder_empty()
                    .with_attribute(KeyValue::new("service.name", "coral-test"))
                    .build(),
            )
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("local-store-test");
        let mut span = tracer
            .span_builder("coral.query")
            .with_kind(SpanKind::Internal)
            .with_attributes([
                KeyValue::new("test.attribute", "value"),
                KeyValue::new("sql", "SELECT 1"),
                KeyValue::new("status", "ok"),
                KeyValue::new("row_count", 1_i64),
            ])
            .start(&tracer);
        span.end();
        provider.shutdown().expect("provider shutdown");

        let parquet_files = fs::read_dir(&dir)
            .expect("trace dir")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet")
            })
            .count();
        assert_eq!(parquet_files, 1);

        let store = TraceStore::new(dir);
        let trace_id = store
            .list_traces(10, 0)
            .expect("list traces")
            .into_iter()
            .next()
            .expect("trace summary")
            .trace_id;
        let detail = store.get_trace(&trace_id).expect("trace detail");

        assert_eq!(detail.spans[0].name, "coral.query");
        assert!(
            detail.spans[0]
                .attributes_json
                .contains(r#""test.attribute":"value""#)
        );
        assert!(
            detail.spans[0]
                .resource_json
                .contains(r#""service.name":"coral-test""#)
        );
    }

    #[test]
    fn reads_trace_summaries_and_details_from_parquet() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let exporter = ParquetSpanExporter::new(dir.clone()).expect("parquet span exporter");
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("local-store-test");
        let mut span = tracer
            .span_builder("coral.query")
            .with_kind(SpanKind::Internal)
            .with_attributes([
                KeyValue::new("sql", "SELECT 1"),
                KeyValue::new("status", "ok"),
                KeyValue::new("row_count", 1_i64),
            ])
            .start(&tracer);
        span.end();
        provider.shutdown().expect("provider shutdown");

        let store = TraceStore::new(dir);
        let summaries = store.list_traces(10, 0).expect("list traces");

        assert_eq!(summaries.len(), 1);
        let summary = &summaries[0];
        assert_eq!(summary.name, "coral.query");
        assert_eq!(summary.query, "SELECT 1");
        assert_eq!(summary.status, StoredTraceStatus::Ok);
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);

        let detail = store.get_trace(&summary.trace_id).expect("trace detail");
        assert_eq!(detail.summary, *summary);
        assert_eq!(detail.spans.len(), 1);
        assert_eq!(detail.spans[0].span_id, summary.root_span_id);
    }

    #[test]
    fn missing_trace_store_lists_empty_and_get_returns_not_found() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let store = TraceStore::new(dir);

        assert!(
            store
                .list_traces(10, 0)
                .expect("missing store list")
                .is_empty()
        );
        assert!(store.get_trace("missing").is_err());
    }
}
