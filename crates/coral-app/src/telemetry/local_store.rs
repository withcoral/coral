//! JSONL-backed span export for local trace capture.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{SpanId, SpanKind, Status};
use opentelemetry::{Array as OtelArray, KeyValue, Value as OtelValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};

const JSONL_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const JSONL_MAX_FILE_ROWS: usize = 50_000;
const JSONL_MAX_FILE_AGE: Duration = Duration::from_hours(24);
const JSONL_PRUNE_INTERVAL: Duration = Duration::from_hours(1);

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
    #[error("failed to read local trace store directory {path}: {source}")]
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to encode local trace store record: {source}")]
    EncodeRecord { source: serde_json::Error },
    #[error("failed to write local trace store file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to flush local trace store file {path}: {source}")]
    FlushFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to read local trace store file metadata {path}: {source}")]
    FileMetadata {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to remove expired local trace store file {path}: {source}")]
    RemoveFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("local trace store writer mutex poisoned")]
    WriterPoisoned,
}

#[derive(Debug, Clone)]
pub(crate) struct JsonlSpanExporter {
    writer: Arc<Mutex<RollingJsonlWriter>>,
    resource_json: Arc<Mutex<String>>,
    shutdown_called: Arc<AtomicBool>,
}

impl JsonlSpanExporter {
    pub(crate) fn new(dir: PathBuf, retention: Duration) -> Result<Self, LocalTraceStoreError> {
        Ok(Self {
            writer: Arc::new(Mutex::new(RollingJsonlWriter::new(dir, retention)?)),
            resource_json: Arc::new(Mutex::new("{}".to_string())),
            shutdown_called: Arc::new(AtomicBool::new(false)),
        })
    }

    fn resource_json(&self) -> String {
        self.resource_json
            .lock()
            .map_or_else(|_| "{}".to_string(), |resource_json| resource_json.clone())
    }
}

impl SpanExporter for JsonlSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        if self.shutdown_called.load(Ordering::Relaxed) {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        if batch.is_empty() {
            return Ok(());
        }

        let resource_json = self.resource_json();
        let records = batch
            .iter()
            .map(|span| span_record(&resource_json, span))
            .collect::<Vec<_>>();
        self.writer
            .lock()
            .map_err(|_poisoned| {
                OTelSdkError::InternalFailure(LocalTraceStoreError::WriterPoisoned.to_string())
            })?
            .write_records(&records)
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))
    }

    fn shutdown_with_timeout(&mut self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown_called.store(true, Ordering::Relaxed);
        self.writer
            .lock()
            .map_err(|_poisoned| {
                OTelSdkError::InternalFailure(LocalTraceStoreError::WriterPoisoned.to_string())
            })?
            .close_current()
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))
    }

    fn set_resource(&mut self, resource: &Resource) {
        if let Ok(mut resource_json) = self.resource_json.lock() {
            *resource_json = resource_json_from_resource(resource);
        }
    }
}

#[derive(Debug)]
struct RollingJsonlWriter {
    dir: PathBuf,
    retention: Duration,
    last_prune: Option<SystemTime>,
    file_counter: u64,
    current: Option<OpenJsonlFile>,
}

#[derive(Debug)]
struct OpenJsonlFile {
    path: PathBuf,
    created_at: SystemTime,
    writer: BufWriter<File>,
    rows_written: usize,
    bytes_written: u64,
}

impl RollingJsonlWriter {
    fn new(dir: PathBuf, retention: Duration) -> Result<Self, LocalTraceStoreError> {
        let now = SystemTime::now();
        if dir.exists() {
            prune_expired_jsonl_files(&dir, retention, now)?;
        }

        Ok(Self {
            dir,
            retention,
            last_prune: Some(now),
            file_counter: 0,
            current: None,
        })
    }

    fn write_records(&mut self, records: &[TraceSpanRecord]) -> Result<(), LocalTraceStoreError> {
        if records.is_empty() {
            return Ok(());
        }

        fs::create_dir_all(&self.dir).map_err(|source| LocalTraceStoreError::CreateDir {
            path: self.dir.clone(),
            source,
        })?;

        let now = SystemTime::now();
        self.roll_current_if_stale(now)?;
        self.prune_if_due(now)?;

        for record in records {
            let mut line = serde_json::to_vec(record)
                .map_err(|source| LocalTraceStoreError::EncodeRecord { source })?;
            line.push(b'\n');

            if self.should_roll(u64::try_from(line.len()).unwrap_or(u64::MAX)) {
                self.close_current()?;
            }

            let current = self.ensure_current(SystemTime::now())?;
            current
                .writer
                .write_all(&line)
                .map_err(|source| LocalTraceStoreError::WriteFile {
                    path: current.path.clone(),
                    source,
                })?;
            current.rows_written = current.rows_written.saturating_add(1);
            current.bytes_written = current
                .bytes_written
                .saturating_add(u64::try_from(line.len()).unwrap_or(u64::MAX));
        }

        self.flush_current()
    }

    fn roll_current_if_stale(&mut self, now: SystemTime) -> Result<(), LocalTraceStoreError> {
        if self.current.as_ref().is_some_and(|current| {
            now.duration_since(current.created_at)
                .is_ok_and(|age| age >= JSONL_MAX_FILE_AGE)
        }) {
            self.close_current()?;
        }
        Ok(())
    }

    fn prune_if_due(&mut self, now: SystemTime) -> Result<(), LocalTraceStoreError> {
        let should_prune = self.last_prune.is_none_or(|last_prune| {
            now.duration_since(last_prune)
                .is_ok_and(|age| age >= JSONL_PRUNE_INTERVAL)
        });
        if should_prune {
            prune_expired_jsonl_files(&self.dir, self.retention, now)?;
            self.last_prune = Some(now);
        }
        Ok(())
    }

    fn should_roll(&self, next_record_bytes: u64) -> bool {
        self.current.as_ref().is_some_and(|current| {
            current.rows_written > 0
                && (current.rows_written >= JSONL_MAX_FILE_ROWS
                    || current.bytes_written.saturating_add(next_record_bytes)
                        > JSONL_MAX_FILE_BYTES)
        })
    }

    fn ensure_current(
        &mut self,
        now: SystemTime,
    ) -> Result<&mut OpenJsonlFile, LocalTraceStoreError> {
        if self.current.is_none() {
            let path = self.next_file_path(now);
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|source| LocalTraceStoreError::CreateFile {
                    path: path.clone(),
                    source,
                })?;
            self.current = Some(OpenJsonlFile {
                path,
                created_at: now,
                writer: BufWriter::new(file),
                rows_written: 0,
                bytes_written: 0,
            });
        }

        Ok(self.current.as_mut().expect("current writer was just set"))
    }

    fn next_file_path(&mut self, now: SystemTime) -> PathBuf {
        let sequence = self.file_counter;
        self.file_counter = self.file_counter.saturating_add(1);
        let unix_nanos = unix_nanos(now);
        self.dir.join(format!(
            "spans-{unix_nanos:020}-{}-{sequence:016}.jsonl",
            process::id()
        ))
    }

    fn close_current(&mut self) -> Result<(), LocalTraceStoreError> {
        if let Some(mut current) = self.current.take() {
            current
                .writer
                .flush()
                .map_err(|source| LocalTraceStoreError::FlushFile {
                    path: current.path,
                    source,
                })?;
        }
        Ok(())
    }

    fn flush_current(&mut self) -> Result<(), LocalTraceStoreError> {
        if let Some(current) = &mut self.current {
            current
                .writer
                .flush()
                .map_err(|source| LocalTraceStoreError::FlushFile {
                    path: current.path.clone(),
                    source,
                })?;
        }
        Ok(())
    }
}

fn prune_expired_jsonl_files(
    dir: &Path,
    retention: Duration,
    now: SystemTime,
) -> Result<(), LocalTraceStoreError> {
    let cutoff = now.checked_sub(retention).unwrap_or(UNIX_EPOCH);
    for entry in fs::read_dir(dir).map_err(|source| LocalTraceStoreError::ReadDir {
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| LocalTraceStoreError::ReadDir {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .is_none_or(|extension| extension != "jsonl")
        {
            continue;
        }
        let timestamp = jsonl_file_timestamp(&path)?;
        if timestamp <= cutoff {
            fs::remove_file(&path)
                .map_err(|source| LocalTraceStoreError::RemoveFile { path, source })?;
        }
    }
    Ok(())
}

fn jsonl_file_timestamp(path: &Path) -> Result<SystemTime, LocalTraceStoreError> {
    if let Some(timestamp) = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .and_then(parse_jsonl_file_timestamp)
    {
        return Ok(timestamp);
    }

    path.metadata()
        .and_then(|metadata| metadata.modified())
        .map_err(|source| LocalTraceStoreError::FileMetadata {
            path: path.to_path_buf(),
            source,
        })
}

fn parse_jsonl_file_timestamp(file_name: &str) -> Option<SystemTime> {
    let unix_nanos = file_name
        .strip_prefix("spans-")?
        .split('-')
        .next()?
        .parse::<u128>()
        .ok()?;
    let secs = u64::try_from(unix_nanos / 1_000_000_000).ok()?;
    let nanos = u32::try_from(unix_nanos % 1_000_000_000).ok()?;
    UNIX_EPOCH.checked_add(Duration::new(secs, nanos))
}

#[derive(Debug, Clone)]
pub(crate) struct TraceStore {
    dir: PathBuf,
    retention: Option<Duration>,
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
    #[error("failed to read local trace store file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to decode local trace store file {path} line {line}: {source}")]
    DecodeLine {
        path: PathBuf,
        line: usize,
        source: serde_json::Error,
    },
    #[error("failed to prune expired local trace store files: {source}")]
    PruneExpired { source: LocalTraceStoreError },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoredTraceStatus {
    #[default]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TraceSpanRecord {
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) parent_span_id: Option<String>,
    pub(crate) parent_span_is_remote: bool,
    pub(crate) name: String,
    pub(crate) kind: String,
    #[serde(default)]
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
    #[cfg(test)]
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            retention: None,
        }
    }

    pub(crate) fn with_retention(dir: PathBuf, retention: Duration) -> Self {
        Self {
            dir,
            retention: Some(retention),
        }
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
        self.prune_expired()?;
        let mut spans_by_id = HashMap::new();
        for path in self.jsonl_files()? {
            for span in read_spans_file(&path)? {
                spans_by_id.insert((span.trace_id.clone(), span.span_id.clone()), span);
            }
        }
        Ok(spans_by_id.into_values().collect())
    }

    fn prune_expired(&self) -> Result<(), TraceStoreError> {
        if let Some(retention) = self.retention
            && self.dir.exists()
        {
            prune_expired_jsonl_files(&self.dir, retention, SystemTime::now())
                .map_err(|source| TraceStoreError::PruneExpired { source })?;
        }
        Ok(())
    }

    fn jsonl_files(&self) -> Result<Vec<PathBuf>, TraceStoreError> {
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
                .is_some_and(|extension| extension == "jsonl")
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
    let mut reader = BufReader::new(file);
    let mut spans = Vec::new();
    let mut line = String::new();
    let mut line_number = 0;

    loop {
        line.clear();
        let bytes_read =
            reader
                .read_line(&mut line)
                .map_err(|source| TraceStoreError::ReadFile {
                    path: path.to_path_buf(),
                    source,
                })?;
        if bytes_read == 0 {
            break;
        }

        line_number += 1;
        let complete_line = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<TraceSpanRecord>(trimmed) {
            Ok(span) => spans.push(span),
            Err(_) if !complete_line => break,
            Err(source) => {
                return Err(TraceStoreError::DecodeLine {
                    path: path.to_path_buf(),
                    line: line_number,
                    source,
                });
            }
        }
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
    }
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
        "events": span.events.events.iter().map(|event| {
            json!({
                "name": event.name.as_ref(),
                "time_unix_nanos": unix_nanos(event.timestamp),
                "attributes": key_values_json(event.attributes.iter()),
            })
        }).collect::<Vec<_>>(),
    })
}

fn links_json(span: &SpanData) -> JsonValue {
    json!({
        "links": span.links.links.iter().map(|link| {
            let span_context = &link.span_context;
            json!({
                "trace_id": span_context.trace_id().to_string(),
                "span_id": span_context.span_id().to_string(),
                "trace_flags": span_context.trace_flags().to_u8(),
                "trace_state": span_context.trace_state().header(),
                "is_remote": span_context.is_remote(),
                "attributes": key_values_json(link.attributes.iter()),
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
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use opentelemetry::KeyValue;
    use opentelemetry::trace::{Span as _, SpanKind, Tracer, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tempfile::TempDir;

    use super::{
        JSONL_MAX_FILE_AGE, JsonlSpanExporter, RollingJsonlWriter, StoredTraceStatus,
        TraceSpanRecord, TraceStore, unix_nanos,
    };

    const TRACE_RETENTION: Duration = Duration::from_hours(7 * 24);

    #[test]
    fn exports_finished_spans_to_jsonl() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let exporter =
            JsonlSpanExporter::new(dir.clone(), TRACE_RETENTION).expect("jsonl span exporter");
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
                KeyValue::new("coral.http.request.body", r#"{"query":"Ada"}"#),
                KeyValue::new("sql", "SELECT 1"),
                KeyValue::new("status", "ok"),
                KeyValue::new("row_count", 1_i64),
            ])
            .start(&tracer);
        span.end();
        provider.shutdown().expect("provider shutdown");

        assert_eq!(jsonl_file_count(&dir), 1);

        let store = TraceStore::new(dir);
        let trace_id = store
            .list_traces(10, 0)
            .expect("list traces")
            .into_iter()
            .next()
            .expect("trace summary")
            .trace_id;
        let detail = store.get_trace(&trace_id).expect("trace detail");
        let span = detail.spans.first().expect("trace span");

        assert_eq!(span.name, "coral.query");
        assert!(span.attributes_json.contains(r#""test.attribute":"value""#));
        assert!(
            span.attributes_json
                .contains(r#""coral.http.request.body":"{\"query\":\"Ada\"}""#)
        );
        assert!(
            span.resource_json
                .contains(r#""service.name":"coral-test""#)
        );
    }

    #[test]
    fn repeated_exports_append_to_one_jsonl_file() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let exporter =
            JsonlSpanExporter::new(dir.clone(), TRACE_RETENTION).expect("jsonl span exporter");
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("local-store-test");

        tracer.start("first").end();
        tracer.start("second").end();
        provider.shutdown().expect("provider shutdown");

        assert_eq!(jsonl_file_count(&dir), 1);
        assert_eq!(TraceStore::new(dir).list_traces(10, 0).unwrap().len(), 2);
    }

    #[test]
    fn reads_trace_summaries_and_details_from_jsonl() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let exporter =
            JsonlSpanExporter::new(dir.clone(), TRACE_RETENTION).expect("jsonl span exporter");
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
        let summary = summaries.first().expect("trace summary");
        assert_eq!(summary.name, "coral.query");
        assert_eq!(summary.query, "SELECT 1");
        assert_eq!(summary.status, StoredTraceStatus::Ok);
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);

        let detail = store.get_trace(&summary.trace_id).expect("trace detail");
        assert_eq!(detail.summary, *summary);
        assert_eq!(detail.spans.len(), 1);
        assert_eq!(
            detail.spans.first().expect("trace span").span_id,
            summary.root_span_id
        );
    }

    #[test]
    fn skips_incomplete_trailing_jsonl_record() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        fs::write(dir.join("spans.jsonl"), "{\"trace_id\":").expect("write partial jsonl");

        let store = TraceStore::new(dir);

        assert!(store.list_traces(10, 0).expect("list traces").is_empty());
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
        store.get_trace("missing").unwrap_err();
    }

    #[test]
    fn exporter_prunes_expired_jsonl_files_on_startup() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let old_path = dir.join(timestamped_jsonl_path(
            SystemTime::now() - Duration::from_hours(8 * 24),
        ));
        let fresh_path = dir.join(timestamped_jsonl_path(SystemTime::now()));
        fs::write(&old_path, "{}\n").expect("write expired trace file");
        fs::write(&fresh_path, "{}\n").expect("write fresh trace file");

        let _exporter =
            JsonlSpanExporter::new(dir.clone(), TRACE_RETENTION).expect("jsonl span exporter");

        assert!(!old_path.exists());
        assert!(fresh_path.exists());
    }

    #[test]
    fn trace_store_prunes_expired_jsonl_files_on_read() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let old_path = dir.join(timestamped_jsonl_path(
            SystemTime::now() - Duration::from_hours(8 * 24),
        ));
        let fresh_path = dir.join(timestamped_jsonl_path(SystemTime::now()));
        write_record_file(&old_path, &trace_record("old-trace", "old-span"));
        write_record_file(&fresh_path, &trace_record("fresh-trace", "fresh-span"));
        let store = TraceStore::with_retention(dir, TRACE_RETENTION);

        let traces = store.list_traces(10, 0).expect("list traces");

        assert!(!old_path.exists());
        assert!(fresh_path.exists());
        assert_eq!(traces.len(), 1);
        assert_eq!(traces.first().expect("fresh trace").trace_id, "fresh-trace");
    }

    #[test]
    fn rolling_writer_rolls_stale_current_file() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let mut writer =
            RollingJsonlWriter::new(dir.clone(), TRACE_RETENTION).expect("jsonl writer");

        writer
            .write_records(&[trace_record("trace-1", "span-1")])
            .expect("write first record");
        let first_path = writer.current.as_mut().expect("open file").path.clone();
        writer.current.as_mut().expect("open file").created_at =
            SystemTime::now() - JSONL_MAX_FILE_AGE - Duration::from_secs(1);

        writer
            .write_records(&[trace_record("trace-2", "span-2")])
            .expect("write second record");

        assert_ne!(
            &writer.current.as_ref().expect("open replacement").path,
            &first_path
        );
        assert_eq!(jsonl_file_count(&dir), 2);
    }

    fn jsonl_file_count(dir: &Path) -> usize {
        fs::read_dir(dir)
            .expect("trace dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
            .count()
    }

    fn timestamped_jsonl_path(timestamp: SystemTime) -> String {
        format!(
            "spans-{:020}-test-0000000000000000.jsonl",
            unix_nanos(timestamp)
        )
    }

    fn write_record_file(path: &Path, record: &TraceSpanRecord) {
        let mut line = serde_json::to_string(record).expect("serialize record");
        line.push('\n');
        fs::write(path, line).expect("write trace record");
    }

    fn trace_record(trace_id: &str, span_id: &str) -> TraceSpanRecord {
        TraceSpanRecord {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            parent_span_is_remote: false,
            name: "coral.query".to_string(),
            kind: "internal".to_string(),
            status: StoredTraceStatus::Ok,
            status_message: None,
            start_time_unix_nanos: 1,
            end_time_unix_nanos: 2,
            duration_nanos: 1,
            attributes_json: "{}".to_string(),
            events_json: "[]".to_string(),
            links_json: "[]".to_string(),
            resource_json: "{}".to_string(),
            scope_name: "test".to_string(),
            scope_version: None,
            scope_schema_url: None,
            scope_attributes_json: "{}".to_string(),
            trace_flags: 0,
            trace_state: String::new(),
            is_remote: false,
        }
    }
}
