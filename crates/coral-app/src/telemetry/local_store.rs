//! JSONL-backed span export for local trace capture.

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{SpanId, SpanKind, Status};
use opentelemetry::{Array as OtelArray, KeyValue, Value as OtelValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};
use tokio::task;

use crate::storage::fs as storage_fs;
use crate::telemetry::WORKSPACE_SPAN_ATTRIBUTE;

const JSONL_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const JSONL_MAX_FILE_ROWS: usize = 50_000;
const JSONL_MAX_FILE_AGE: Duration = Duration::from_hours(24);
const JSONL_PRUNE_INTERVAL: Duration = Duration::from_hours(1);
const JSONL_FILE_MTIME_SPAN_END_TOLERANCE: Duration = Duration::from_secs(2);
type ActiveTraceWriter = Arc<Mutex<RollingJsonlWriter>>;
type WeakActiveTraceWriter = Weak<Mutex<RollingJsonlWriter>>;
type ActiveTraceWriterRegistry = Mutex<HashMap<PathBuf, Vec<WeakActiveTraceWriter>>>;
static ACTIVE_TRACE_WRITERS: OnceLock<ActiveTraceWriterRegistry> = OnceLock::new();

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
    pub(crate) fn new(
        dir: impl Into<PathBuf>,
        retention: Duration,
    ) -> Result<Self, LocalTraceStoreError> {
        let dir = dir.into();
        let writer = Arc::new(Mutex::new(RollingJsonlWriter::new(dir.clone(), retention)?));
        register_active_trace_writer(&dir, &writer);
        Ok(Self {
            writer,
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

    fn write_records<T: Serialize>(&mut self, records: &[T]) -> Result<(), LocalTraceStoreError> {
        if records.is_empty() {
            return Ok(());
        }

        storage_fs::ensure_private_dir(&self.dir).map_err(|source| {
            LocalTraceStoreError::CreateDir {
                path: self.dir.clone(),
                source,
            }
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
            let file = storage_fs::create_new_file_private(&path).map_err(|source| {
                LocalTraceStoreError::CreateFile {
                    path: path.clone(),
                    source,
                }
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
            process::id(),
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
        let modified = jsonl_file_modified(&path)?;
        if modified <= cutoff {
            fs::remove_file(&path)
                .map_err(|source| LocalTraceStoreError::RemoveFile { path, source })?;
        }
    }
    Ok(())
}

fn jsonl_file_modified(path: &Path) -> Result<SystemTime, LocalTraceStoreError> {
    path.metadata()
        .and_then(|metadata| metadata.modified())
        .map_err(|source| LocalTraceStoreError::FileMetadata {
            path: path.to_path_buf(),
            source,
        })
}

#[derive(Debug, Clone)]
pub(crate) struct TraceStore {
    dir: PathBuf,
    retention: Option<Duration>,
}

#[derive(Debug, Clone)]
struct TraceStoreFile {
    path: PathBuf,
    modified_unix_nanos: i64,
    span_end_upper_bound_unix_nanos: i64,
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
    #[error("failed to read local trace store file metadata {path}: {source}")]
    FileMetadata {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to rewrite local trace store file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to remove local trace store file {path}: {source}")]
    RemoveFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to restore local trace store file {path} after cleanup failure: {source}")]
    RestoreFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("local trace store writer registry mutex poisoned")]
    WriterRegistryPoisoned,
    #[error("local trace store writer mutex poisoned")]
    WriterPoisoned,
    #[error("failed to close active local trace store writer before cleanup: {source}")]
    CloseActiveWriter { source: LocalTraceStoreError },
    #[error("failed to prune expired local trace store files: {source}")]
    PruneExpired { source: LocalTraceStoreError },
    #[error("local trace store worker failed before returning a response: {source}")]
    Worker { source: task::JoinError },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceQueryHistoryEntry {
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) workspace: String,
    pub(crate) sql: String,
    pub(crate) sources: Vec<String>,
    pub(crate) tables: Vec<TraceQueryTableUsage>,
    pub(crate) table_functions: Vec<TraceQueryTableFunctionUsage>,
    pub(crate) row_count: u64,
    pub(crate) end_time_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TraceQueryTableUsage {
    #[serde(rename = "source_name")]
    pub(crate) source: String,
    /// Absent on spans recorded before query provenance carried the catalog, so
    /// historical rows still parse.
    #[serde(rename = "catalog_name", default)]
    pub(crate) catalog: Option<String>,
    #[serde(rename = "schema_name")]
    pub(crate) schema: String,
    #[serde(rename = "table_name")]
    pub(crate) table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TraceQueryTableFunctionUsage {
    #[serde(rename = "source_name")]
    pub(crate) source: String,
    #[serde(rename = "schema_name")]
    pub(crate) schema: String,
    #[serde(rename = "function_name")]
    pub(crate) function: String,
}

struct TraceAggregate {
    trace_id: String,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    span_count: u32,
    error_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct TraceListSpanRecord {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    #[serde(default)]
    status: StoredTraceStatus,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    attributes_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TracePrimaryCandidate {
    span_id: String,
    name: String,
    status: StoredTraceStatus,
    start_time_unix_nanos: i64,
    attributes_json: String,
    priority: u8,
}

#[derive(Debug, Clone)]
struct TraceListAggregate {
    trace_id: String,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    span_count: u32,
    error_count: u32,
    found_root_span: bool,
    matches_workspace: bool,
    primary: Option<TracePrimaryCandidate>,
}

#[derive(Debug, Deserialize)]
struct TraceSpanIdentityRecord {
    trace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct TraceQueryHistorySpanRecord {
    trace_id: String,
    span_id: String,
    name: String,
    #[serde(default)]
    status: StoredTraceStatus,
    end_time_unix_nanos: i64,
    attributes_json: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TraceWorkspaceRecord {
    trace_id: String,
    attributes_json: String,
}

impl TraceStore {
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

    pub(crate) async fn list_traces(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        let traces = self.clone();
        task::spawn_blocking(move || traces.list_traces_sync(limit, offset))
            .await
            .map_err(|source| TraceStoreError::Worker { source })?
    }

    pub(crate) async fn list_traces_for_workspace(
        &self,
        limit: usize,
        offset: usize,
        workspace_name: String,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        let traces = self.clone();
        task::spawn_blocking(move || {
            traces.list_traces_for_workspace_sync(limit, offset, &workspace_name)
        })
        .await
        .map_err(|source| TraceStoreError::Worker { source })?
    }

    pub(crate) async fn get_trace(
        &self,
        trace_id: String,
    ) -> Result<TraceDetailRecord, TraceStoreError> {
        let traces = self.clone();
        task::spawn_blocking(move || traces.get_trace_sync(&trace_id))
            .await
            .map_err(|source| TraceStoreError::Worker { source })?
    }

    pub(crate) async fn get_trace_for_workspace(
        &self,
        trace_id: String,
        workspace_name: String,
    ) -> Result<TraceDetailRecord, TraceStoreError> {
        let traces = self.clone();
        task::spawn_blocking(move || {
            traces.get_trace_for_workspace_sync(&trace_id, &workspace_name)
        })
        .await
        .map_err(|source| TraceStoreError::Worker { source })?
    }

    pub(crate) async fn delete_traces_for_workspace(
        &self,
        workspace_name: String,
    ) -> Result<usize, TraceStoreError> {
        let traces = self.clone();
        task::spawn_blocking(move || traces.delete_traces_for_workspace_sync(&workspace_name))
            .await
            .map_err(|source| TraceStoreError::Worker { source })?
    }

    fn list_traces_sync(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        self.list_traces_filtered_sync(limit, offset, None)
    }

    fn list_traces_for_workspace_sync(
        &self,
        limit: usize,
        offset: usize,
        workspace_name: &str,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        self.list_traces_filtered_sync(limit, offset, Some(workspace_name))
    }

    fn list_traces_filtered_sync(
        &self,
        limit: usize,
        offset: usize,
        workspace_name: Option<&str>,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        self.prune_expired()?;
        let files = self.jsonl_files_by_modified()?;
        let mut spans_by_id = HashMap::new();
        let mut traces: HashMap<String, TraceListAggregate> = HashMap::new();
        let required_trace_count = offset.saturating_add(limit);
        let mut oldest_scanned_file_index = None;

        for (file_index, file) in files.iter().enumerate().rev() {
            oldest_scanned_file_index = Some(file_index);
            for span in read_list_spans_file(&file.path)? {
                record_list_span(span, workspace_name, &mut spans_by_id, &mut traces);
            }

            let Some(newest_unscanned_file) =
                file_index.checked_sub(1).and_then(|index| files.get(index))
            else {
                break;
            };
            if list_page_is_newer_than_unscanned_files(
                &traces,
                required_trace_count,
                newest_unscanned_file.span_end_upper_bound_unix_nanos,
                workspace_name,
            ) {
                break;
            }
        }

        let page_trace_ids = trace_page_ids(&traces, offset, limit, workspace_name);
        complete_list_aggregates_for_page(
            &files,
            oldest_scanned_file_index,
            &page_trace_ids,
            workspace_name,
            &mut spans_by_id,
            &mut traces,
        )?;
        let mut summaries = traces
            .into_values()
            .filter(|aggregate| page_trace_ids.contains(&aggregate.trace_id))
            .map(TraceListAggregate::into_summary)
            .collect::<Vec<_>>();
        sort_summaries(&mut summaries);

        Ok(summaries.into_iter().take(limit).collect())
    }

    fn get_trace_sync(&self, trace_id: &str) -> Result<TraceDetailRecord, TraceStoreError> {
        let mut spans_by_id = HashMap::new();
        self.prune_expired()?;
        let files = self.jsonl_files_by_modified()?;
        let mut earliest_span_start = i64::MAX;
        let mut found_root_span = false;
        for (file_index, file) in files.iter().enumerate().rev() {
            for span in read_trace_spans_file(&file.path, trace_id)? {
                earliest_span_start = earliest_span_start.min(span.start_time_unix_nanos);
                found_root_span |= is_trace_root_span(&span);
                spans_by_id
                    .entry((span.trace_id.clone(), span.span_id.clone()))
                    .or_insert(span);
            }

            let newest_unscanned_file =
                file_index.checked_sub(1).and_then(|index| files.get(index));
            if found_root_span
                && newest_unscanned_file
                    .is_some_and(|file| file.span_end_upper_bound_unix_nanos < earliest_span_start)
            {
                break;
            }
        }
        let mut spans = spans_by_id.into_values().collect::<Vec<_>>();

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

    fn get_trace_for_workspace_sync(
        &self,
        trace_id: &str,
        workspace_name: &str,
    ) -> Result<TraceDetailRecord, TraceStoreError> {
        let detail = self.get_trace_sync(trace_id)?;
        if detail
            .spans
            .iter()
            .any(|span| attributes_match_workspace(&span.attributes_json, workspace_name))
        {
            Ok(detail)
        } else {
            Err(TraceStoreError::NotFound(trace_id.to_string()))
        }
    }

    pub(crate) fn list_query_history_sync(
        &self,
    ) -> Result<Vec<TraceQueryHistoryEntry>, TraceStoreError> {
        self.prune_expired()?;
        let files = self.jsonl_files_by_modified()?;
        let mut entries_by_span = HashMap::new();

        for file in files.iter().rev() {
            for span in read_query_history_spans_file(&file.path)? {
                let key = (span.trace_id.clone(), span.span_id.clone());
                if entries_by_span.contains_key(&key) {
                    continue;
                }
                if let Some(entry) = query_history_entry_from_span(&span) {
                    entries_by_span.insert(key, entry);
                }
            }
        }

        let mut entries = entries_by_span.into_values().collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            right
                .end_time_unix_nanos
                .cmp(&left.end_time_unix_nanos)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
        Ok(entries)
    }

    fn delete_traces_for_workspace_sync(
        &self,
        workspace_name: &str,
    ) -> Result<usize, TraceStoreError> {
        if !self.dir.exists() {
            return Ok(0);
        }

        close_active_trace_writers_for_dir(&self.dir)?;

        self.prune_expired()?;
        let files = self.jsonl_files_by_modified()?;
        let trace_ids = read_workspace_trace_ids(&files, workspace_name)?;
        if trace_ids.is_empty() {
            return Ok(0);
        }

        rewrite_trace_files_excluding_trace_ids(&files, &trace_ids)?;
        Ok(trace_ids.len())
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

    fn jsonl_files_by_modified(&self) -> Result<Vec<TraceStoreFile>, TraceStoreError> {
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
            if span_jsonl_file(&path) {
                let modified = path
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .map_err(|source| TraceStoreError::FileMetadata {
                        path: path.clone(),
                        source,
                    })?;
                let modified_unix_nanos = unix_nanos(modified);
                files.push(TraceStoreFile {
                    span_end_upper_bound_unix_nanos: modified
                        .checked_add(JSONL_FILE_MTIME_SPAN_END_TOLERANCE)
                        .map_or(i64::MAX, unix_nanos),
                    modified_unix_nanos,
                    path,
                });
            }
        }
        files.sort_by(|left, right| {
            left.modified_unix_nanos
                .cmp(&right.modified_unix_nanos)
                .then_with(|| left.path.cmp(&right.path))
        });
        Ok(files)
    }
}

fn span_jsonl_file(path: &Path) -> bool {
    path.extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| extension == "jsonl")
        && path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| {
                name.strip_prefix("spans")
                    .is_some_and(|suffix| suffix.starts_with('-'))
            })
}

fn active_trace_writers() -> &'static ActiveTraceWriterRegistry {
    ACTIVE_TRACE_WRITERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_active_trace_writer(dir: &Path, writer: &ActiveTraceWriter) {
    if let Ok(mut writers) = active_trace_writers().lock() {
        writers
            .entry(dir.to_path_buf())
            .or_default()
            .push(Arc::downgrade(writer));
    }
}

fn active_trace_writers_for_dir(dir: &Path) -> Result<Vec<ActiveTraceWriter>, TraceStoreError> {
    let mut writers = active_trace_writers()
        .lock()
        .map_err(|_poisoned| TraceStoreError::WriterRegistryPoisoned)?;
    let registered = writers.entry(dir.to_path_buf()).or_default();
    let mut active = Vec::new();
    registered.retain(|writer| match writer.upgrade() {
        Some(writer) => {
            active.push(writer);
            true
        }
        None => false,
    });
    Ok(active)
}

fn close_active_trace_writers_for_dir(dir: &Path) -> Result<(), TraceStoreError> {
    for writer in active_trace_writers_for_dir(dir)? {
        writer
            .lock()
            .map_err(|_poisoned| TraceStoreError::WriterPoisoned)?
            .close_current()
            .map_err(|source| TraceStoreError::CloseActiveWriter { source })?;
    }
    Ok(())
}

impl TracePrimaryCandidate {
    fn from_span(span: &TraceListSpanRecord) -> Self {
        Self {
            span_id: span.span_id.clone(),
            name: span.name.clone(),
            status: span.status,
            start_time_unix_nanos: span.start_time_unix_nanos,
            attributes_json: span.attributes_json.clone(),
            priority: primary_priority(&span.name, span.parent_span_id.as_deref()),
        }
    }

    fn should_replace(&self, current: &Self) -> bool {
        (
            self.priority,
            self.start_time_unix_nanos,
            self.span_id.as_str(),
        ) < (
            current.priority,
            current.start_time_unix_nanos,
            current.span_id.as_str(),
        )
    }
}

impl TraceListAggregate {
    fn new(span: &TraceListSpanRecord, workspace_name: Option<&str>) -> Self {
        let mut aggregate = Self {
            trace_id: span.trace_id.clone(),
            start_time_unix_nanos: span.start_time_unix_nanos,
            end_time_unix_nanos: span.end_time_unix_nanos,
            span_count: 0,
            error_count: 0,
            found_root_span: false,
            matches_workspace: false,
            primary: None,
        };
        aggregate.record_span(span, workspace_name);
        aggregate
    }

    fn record_span(&mut self, span: &TraceListSpanRecord, workspace_name: Option<&str>) {
        self.start_time_unix_nanos = self.start_time_unix_nanos.min(span.start_time_unix_nanos);
        self.end_time_unix_nanos = self.end_time_unix_nanos.max(span.end_time_unix_nanos);
        self.span_count = self.span_count.saturating_add(1);
        if span.status == StoredTraceStatus::Error {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.found_root_span |= is_root_span_parent(span.parent_span_id.as_deref());
        self.matches_workspace |= workspace_name.is_some_and(|workspace_name| {
            attributes_match_workspace(&span.attributes_json, workspace_name)
        });

        let primary = TracePrimaryCandidate::from_span(span);
        if self
            .primary
            .as_ref()
            .is_none_or(|current| primary.should_replace(current))
        {
            self.primary = Some(primary);
        }
    }

    fn into_summary(self) -> TraceSummaryRecord {
        let aggregate = TraceAggregate {
            trace_id: self.trace_id,
            start_time_unix_nanos: self.start_time_unix_nanos,
            end_time_unix_nanos: self.end_time_unix_nanos,
            span_count: self.span_count,
            error_count: self.error_count,
        };
        summary_from_list_aggregate(&aggregate, self.primary.as_ref())
    }
}

fn record_list_span(
    span: TraceListSpanRecord,
    workspace_name: Option<&str>,
    spans_by_id: &mut HashMap<(String, String), TraceListSpanRecord>,
    traces: &mut HashMap<String, TraceListAggregate>,
) {
    let key = (span.trace_id.clone(), span.span_id.clone());
    match spans_by_id.entry(key) {
        Entry::Occupied(_) => {}
        Entry::Vacant(entry) => {
            traces
                .entry(span.trace_id.clone())
                .and_modify(|aggregate| aggregate.record_span(&span, workspace_name))
                .or_insert_with(|| TraceListAggregate::new(&span, workspace_name));
            entry.insert(span);
        }
    }
}

fn trace_page_ids(
    traces: &HashMap<String, TraceListAggregate>,
    offset: usize,
    limit: usize,
    workspace_name: Option<&str>,
) -> HashSet<String> {
    let mut aggregates = traces
        .values()
        .filter(|aggregate| trace_matches_workspace_filter(aggregate, workspace_name))
        .collect::<Vec<_>>();
    sort_trace_aggregates(&mut aggregates);
    aggregates
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|aggregate| aggregate.trace_id.clone())
        .collect()
}

fn complete_list_aggregates_for_page(
    files: &[TraceStoreFile],
    oldest_scanned_file_index: Option<usize>,
    page_trace_ids: &HashSet<String>,
    workspace_name: Option<&str>,
    spans_by_id: &mut HashMap<(String, String), TraceListSpanRecord>,
    traces: &mut HashMap<String, TraceListAggregate>,
) -> Result<(), TraceStoreError> {
    if page_trace_ids.is_empty() {
        return Ok(());
    }

    let Some(oldest_scanned_file_index) = oldest_scanned_file_index else {
        return Ok(());
    };
    let completion_start_cutoff = page_completion_start_cutoff(traces, page_trace_ids);

    for file in files.iter().take(oldest_scanned_file_index).rev() {
        if completion_start_cutoff
            .is_some_and(|cutoff| file.span_end_upper_bound_unix_nanos < cutoff)
        {
            break;
        }
        for span in read_list_spans_file_for_trace_ids(&file.path, page_trace_ids)? {
            record_list_span(span, workspace_name, spans_by_id, traces);
        }
    }

    Ok(())
}

fn page_completion_start_cutoff(
    traces: &HashMap<String, TraceListAggregate>,
    page_trace_ids: &HashSet<String>,
) -> Option<i64> {
    let mut cutoff = i64::MAX;
    for trace_id in page_trace_ids {
        let aggregate = traces.get(trace_id)?;
        if !aggregate.found_root_span {
            return None;
        }
        cutoff = cutoff.min(aggregate.start_time_unix_nanos);
    }
    Some(cutoff)
}

fn list_page_is_newer_than_unscanned_files(
    traces: &HashMap<String, TraceListAggregate>,
    required_trace_count: usize,
    newest_unscanned_span_end_upper_bound_unix_nanos: i64,
    workspace_name: Option<&str>,
) -> bool {
    if required_trace_count == 0 {
        return false;
    }

    let mut aggregates = traces
        .values()
        .filter(|aggregate| trace_matches_workspace_filter(aggregate, workspace_name))
        .collect::<Vec<_>>();
    if aggregates.len() < required_trace_count {
        return false;
    }
    sort_trace_aggregates(&mut aggregates);
    let Some(boundary) = aggregates.get(required_trace_count - 1) else {
        return false;
    };
    if boundary.end_time_unix_nanos <= newest_unscanned_span_end_upper_bound_unix_nanos {
        return false;
    }

    workspace_name.is_none_or(|_| {
        workspace_filter_is_settled_for_page_boundary(
            traces,
            boundary,
            newest_unscanned_span_end_upper_bound_unix_nanos,
        )
    })
}

fn trace_matches_workspace_filter(
    aggregate: &TraceListAggregate,
    workspace_name: Option<&str>,
) -> bool {
    workspace_name.is_none_or(|_| aggregate.matches_workspace)
}

fn workspace_filter_is_settled_for_page_boundary(
    traces: &HashMap<String, TraceListAggregate>,
    boundary: &TraceListAggregate,
    newest_unscanned_span_end_upper_bound_unix_nanos: i64,
) -> bool {
    traces.values().all(|aggregate| {
        aggregate.matches_workspace
            || !could_sort_before_or_at_boundary(aggregate, boundary)
            || trace_is_complete_before_unscanned_files(
                aggregate,
                newest_unscanned_span_end_upper_bound_unix_nanos,
            )
    })
}

fn could_sort_before_or_at_boundary(
    aggregate: &TraceListAggregate,
    boundary: &TraceListAggregate,
) -> bool {
    aggregate.end_time_unix_nanos > boundary.end_time_unix_nanos
        || (aggregate.end_time_unix_nanos == boundary.end_time_unix_nanos
            && aggregate.trace_id <= boundary.trace_id)
}

fn trace_is_complete_before_unscanned_files(
    aggregate: &TraceListAggregate,
    newest_unscanned_span_end_upper_bound_unix_nanos: i64,
) -> bool {
    aggregate.found_root_span
        && newest_unscanned_span_end_upper_bound_unix_nanos < aggregate.start_time_unix_nanos
}

fn sort_trace_aggregates(aggregates: &mut [&TraceListAggregate]) {
    aggregates.sort_by(|left, right| {
        right
            .end_time_unix_nanos
            .cmp(&left.end_time_unix_nanos)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
    });
}

fn sort_summaries(summaries: &mut [TraceSummaryRecord]) {
    summaries.sort_by(|left, right| {
        right
            .end_time_unix_nanos
            .cmp(&left.end_time_unix_nanos)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
    });
}

fn primary_priority(name: &str, parent_span_id: Option<&str>) -> u8 {
    if name == "coral.query" {
        0
    } else if parent_span_id.is_none() {
        1
    } else {
        2
    }
}

fn is_trace_root_span(span: &TraceSpanRecord) -> bool {
    is_root_span_parent(span.parent_span_id.as_deref())
}

fn is_root_span_parent(parent_span_id: Option<&str>) -> bool {
    parent_span_id.is_none()
}

fn line_trace_id(line: &str) -> Option<&str> {
    let value_start = line.find(r#""trace_id":""#)? + r#""trace_id":""#.len();
    let value = line.get(value_start..)?;
    let value_end = value.find('"')?;
    value.get(..value_end)
}

fn read_list_spans_file(path: &Path) -> Result<Vec<TraceListSpanRecord>, TraceStoreError> {
    read_list_spans_file_filtered(path, None)
}

fn read_list_spans_file_for_trace_ids(
    path: &Path,
    trace_ids: &HashSet<String>,
) -> Result<Vec<TraceListSpanRecord>, TraceStoreError> {
    read_list_spans_file_filtered(path, Some(trace_ids))
}

fn read_list_spans_file_filtered(
    path: &Path,
    trace_ids: Option<&HashSet<String>>,
) -> Result<Vec<TraceListSpanRecord>, TraceStoreError> {
    let file = File::open(path).map_err(|source| TraceStoreError::OpenFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut spans_by_id = HashMap::new();
    let mut line = String::new();

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

        let complete_line = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() {
            continue;
        }
        if trace_ids.is_some_and(|ids| {
            !line_trace_id(trimmed).is_some_and(|trace_id| ids.contains(trace_id))
        }) {
            continue;
        }

        match serde_json::from_str::<TraceListSpanRecord>(trimmed) {
            Ok(span) if trace_ids.is_none_or(|ids| ids.contains(&span.trace_id)) => {
                spans_by_id.insert((span.trace_id.clone(), span.span_id.clone()), span);
            }
            Ok(_span) => {}
            Err(_) if !complete_line => break,
            Err(_source) => {}
        }
    }

    Ok(spans_by_id.into_values().collect())
}

fn read_trace_spans_file(
    path: &Path,
    trace_id: &str,
) -> Result<Vec<TraceSpanRecord>, TraceStoreError> {
    let file = File::open(path).map_err(|source| TraceStoreError::OpenFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut spans_by_id = HashMap::new();
    let mut line = String::new();

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

        let complete_line = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() {
            continue;
        }
        if !trimmed.contains(trace_id) {
            continue;
        }

        match serde_json::from_str::<TraceSpanIdentityRecord>(trimmed) {
            Ok(identity) if identity.trace_id == trace_id => {
                match serde_json::from_str::<TraceSpanRecord>(trimmed) {
                    Ok(span) => {
                        spans_by_id.insert((span.trace_id.clone(), span.span_id.clone()), span);
                    }
                    Err(_) if !complete_line => break,
                    Err(_source) => {}
                }
            }
            Ok(_identity) => {}
            Err(_) if !complete_line => break,
            Err(_source) => {}
        }
    }

    Ok(spans_by_id.into_values().collect())
}

fn read_query_history_spans_file(
    path: &Path,
) -> Result<Vec<TraceQueryHistorySpanRecord>, TraceStoreError> {
    let file = File::open(path).map_err(|source| TraceStoreError::OpenFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut spans_by_id = HashMap::new();
    let mut line = String::new();

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

        let complete_line = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() || !trimmed.contains(r#""name":"coral.query""#) {
            continue;
        }

        match serde_json::from_str::<TraceQueryHistorySpanRecord>(trimmed) {
            Ok(span) if span.name == "coral.query" => {
                spans_by_id.insert((span.trace_id.clone(), span.span_id.clone()), span);
            }
            Ok(_span) => {}
            Err(_) if !complete_line => break,
            Err(_) => {}
        }
    }

    Ok(spans_by_id.into_values().collect())
}

fn read_workspace_trace_ids(
    files: &[TraceStoreFile],
    workspace_name: &str,
) -> Result<HashSet<String>, TraceStoreError> {
    let mut spans = Vec::new();
    for file in files {
        spans.extend(read_workspace_trace_records_file(&file.path)?);
    }
    Ok(workspace_trace_ids(spans, workspace_name))
}

fn read_workspace_trace_records_file(
    path: &Path,
) -> Result<Vec<TraceWorkspaceRecord>, TraceStoreError> {
    let file = File::open(path).map_err(|source| TraceStoreError::OpenFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut spans = Vec::new();
    let mut line = String::new();

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

        let complete_line = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<TraceWorkspaceRecord>(trimmed) {
            Ok(record) => spans.push(record),
            Err(_source) if !complete_line => break,
            // Workspace trace cleanup is best-effort. A complete malformed line
            // cannot be attributed to a workspace, so preserve it during rewrite
            // instead of blocking deletion of config-owned workspace state.
            Err(_source) => {}
        }
    }

    Ok(spans)
}

fn workspace_trace_ids(spans: Vec<TraceWorkspaceRecord>, workspace_name: &str) -> HashSet<String> {
    spans
        .into_iter()
        .filter(|span| attributes_match_workspace(&span.attributes_json, workspace_name))
        .map(|span| span.trace_id)
        .collect()
}

fn rewrite_trace_files_excluding_trace_ids(
    files: &[TraceStoreFile],
    trace_ids: &HashSet<String>,
) -> Result<(), TraceStoreError> {
    let mut rewrites = Vec::new();
    for file in files {
        if let Some(rewrite) = plan_trace_file_rewrite(&file.path, trace_ids)? {
            rewrites.push(rewrite);
        }
    }

    let mut snapshots = Vec::new();
    for rewrite in rewrites {
        let path = rewrite.snapshot.path.clone();
        let result = if rewrite.kept.is_empty() {
            fs::remove_file(&path).map_err(|source| TraceStoreError::RemoveFile {
                path: path.clone(),
                source,
            })
        } else {
            storage_fs::write_atomic(&path, &rewrite.kept).map_err(|source| {
                TraceStoreError::WriteFile {
                    path: path.clone(),
                    source,
                }
            })
        };
        if let Err(error) = result {
            restore_trace_file_snapshots(snapshots)?;
            return Err(error);
        }
        snapshots.push(rewrite.snapshot);
    }
    Ok(())
}

#[derive(Debug)]
struct TraceFileSnapshot {
    path: PathBuf,
    original: Vec<u8>,
}

#[derive(Debug)]
struct TraceFileRewrite {
    snapshot: TraceFileSnapshot,
    kept: Vec<u8>,
}

fn plan_trace_file_rewrite(
    path: &Path,
    trace_ids: &HashSet<String>,
) -> Result<Option<TraceFileRewrite>, TraceStoreError> {
    let original = fs::read(path).map_err(|source| TraceStoreError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(original.as_slice());
    let mut kept = Vec::new();
    let mut removed = false;
    let mut line = String::new();

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

        let complete_line = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() {
            kept.extend_from_slice(line.as_bytes());
            continue;
        }

        match serde_json::from_str::<TraceSpanIdentityRecord>(trimmed) {
            Ok(identity) if trace_ids.contains(&identity.trace_id) => {
                removed = true;
            }
            Ok(_identity) => kept.extend_from_slice(line.as_bytes()),
            Err(_source) if !complete_line => kept.extend_from_slice(line.as_bytes()),
            // Preserve malformed complete lines. The discovery pass applies the
            // same best-effort policy, so these lines are never attributed to a
            // workspace trace ID.
            Err(_source) => kept.extend_from_slice(line.as_bytes()),
        }
    }

    if !removed {
        return Ok(None);
    }
    let snapshot = TraceFileSnapshot {
        path: path.to_path_buf(),
        original,
    };
    Ok(Some(TraceFileRewrite { snapshot, kept }))
}

fn restore_trace_file_snapshots(snapshots: Vec<TraceFileSnapshot>) -> Result<(), TraceStoreError> {
    for snapshot in snapshots.into_iter().rev() {
        storage_fs::write_atomic(&snapshot.path, &snapshot.original).map_err(|source| {
            TraceStoreError::RestoreFile {
                path: snapshot.path,
                source,
            }
        })?;
    }
    Ok(())
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
        (
            primary_priority(&span.name, span.parent_span_id.as_deref()),
            span.start_time_unix_nanos,
            span.span_id.as_str(),
        )
    });
    summary_from_aggregate(&aggregate, primary)
}

fn summary_from_list_aggregate(
    aggregate: &TraceAggregate,
    primary: Option<&TracePrimaryCandidate>,
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

fn attributes_match_workspace(attributes_json: &str, workspace_name: &str) -> bool {
    workspace_attribute(attributes_json).is_some_and(|workspace| workspace == workspace_name)
}

fn workspace_attribute(attributes_json: &str) -> Option<String> {
    parse_attributes(attributes_json)
        .as_ref()
        .and_then(|attributes| attr_string(attributes, WORKSPACE_SPAN_ATTRIBUTE))
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

fn query_history_entry_from_span(
    span: &TraceQueryHistorySpanRecord,
) -> Option<TraceQueryHistoryEntry> {
    let attributes = parse_attributes(&span.attributes_json)?;
    let status = status_from_attributes(Some(&attributes)).unwrap_or(span.status);
    if status != StoredTraceStatus::Ok {
        return None;
    }
    let sql = attr_string(&attributes, "sql")?;
    if sql.trim().is_empty() {
        return None;
    }
    let workspace = attr_string(&attributes, WORKSPACE_SPAN_ATTRIBUTE)?;
    let row_count = attr_u64(&attributes, "row_count")?;
    let sources = attr_string_array(&attributes, super::QUERY_TRACE_SOURCES_ATTR)?;
    let tables =
        attr_json_vec::<TraceQueryTableUsage>(&attributes, super::QUERY_TRACE_TABLES_ATTR)?;
    let table_functions = attr_json_vec::<TraceQueryTableFunctionUsage>(
        &attributes,
        super::QUERY_TRACE_TABLE_FUNCTIONS_ATTR,
    )?;

    Some(TraceQueryHistoryEntry {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        workspace,
        sql,
        sources,
        tables,
        table_functions,
        row_count,
        end_time_unix_nanos: span.end_time_unix_nanos,
    })
}

fn attr_string_array(attributes: &JsonValue, key: &str) -> Option<Vec<String>> {
    match attributes.get(key)? {
        JsonValue::Array(values) => values.iter().map(attr_array_string_value).collect(),
        JsonValue::String(value) => serde_json::from_str(value).ok(),
        _ => None,
    }
}

fn attr_array_string_value(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn attr_json_vec<T: DeserializeOwned>(attributes: &JsonValue, key: &str) -> Option<Vec<T>> {
    match attributes.get(key)? {
        JsonValue::Array(values) => serde_json::from_value(JsonValue::Array(values.clone())).ok(),
        JsonValue::String(value) => serde_json::from_str(value).ok(),
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
    use std::fs::{self, FileTimes};
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use opentelemetry::KeyValue;
    use opentelemetry::trace::{Span as _, SpanKind, Tracer, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        JSONL_MAX_FILE_AGE, JsonlSpanExporter, RollingJsonlWriter, StoredTraceStatus,
        TraceSpanRecord, TraceStore, unix_nanos,
    };
    use crate::telemetry::WORKSPACE_SPAN_ATTRIBUTE;

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
            .list_traces_sync(10, 0)
            .expect("list traces")
            .into_iter()
            .next()
            .expect("trace summary")
            .trace_id;
        let detail = store.get_trace_sync(&trace_id).expect("trace detail");
        let span = detail.spans.first().expect("trace span");

        assert_eq!(span.name, "coral.query");
        assert!(span.attributes_json.contains(r#""test.attribute":"value""#));
        assert!(!span.attributes_json.contains("coral.http.request.body"));
        assert!(
            span.resource_json
                .contains(r#""service.name":"coral-test""#)
        );
    }

    #[cfg(unix)]
    #[test]
    fn rolling_writer_creates_private_dir_and_file() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755))
            .expect("make trace dir permissive");
        let mut writer =
            RollingJsonlWriter::new(dir.clone(), TRACE_RETENTION).expect("jsonl writer");

        writer
            .write_records(&[trace_record("trace-1", "span-1")])
            .expect("write record");

        let file_path = writer.current.as_ref().expect("open file").path.clone();
        let mode = |path: &Path| fs::metadata(path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&file_path), 0o600);
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
        assert_eq!(
            TraceStore::new(dir).list_traces_sync(10, 0).unwrap().len(),
            2
        );
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
        let summaries = store.list_traces_sync(10, 0).expect("list traces");

        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("trace summary");
        assert_eq!(summary.name, "coral.query");
        assert_eq!(summary.query, "SELECT 1");
        assert_eq!(summary.status, StoredTraceStatus::Ok);
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);

        let detail = store
            .get_trace_sync(&summary.trace_id)
            .expect("trace detail");
        assert_eq!(detail.summary, *summary);
        assert_eq!(detail.spans.len(), 1);
        assert_eq!(
            detail.spans.first().expect("trace span").span_id,
            summary.root_span_id
        );
    }

    #[test]
    fn list_traces_ignores_unneeded_detail_field_types() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let mut record = trace_record("trace-1", "span-1");
        record.attributes_json = r#"{"sql":"SELECT 1","status":"ok","row_count":1}"#.to_string();
        let mut value = serde_json::to_value(&record).expect("record value");
        value.as_object_mut().expect("record object").insert(
            "events_json".to_string(),
            json!({ "large_detail_payload": ["ignored by list"] }),
        );
        fs::write(
            dir.join(timestamped_jsonl_path(SystemTime::now())),
            format!("{value}\n"),
        )
        .expect("write trace record");
        fs::write(
            dir.join("http-bodies-00000000000000000001-test-0000000000000000.jsonl"),
            "{}\n",
        )
        .expect("write body record");

        let summaries = TraceStore::new(dir)
            .list_traces_sync(10, 0)
            .expect("list traces");

        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("trace summary");
        assert_eq!(summary.trace_id, "trace-1");
        assert_eq!(summary.query, "SELECT 1");
        assert_eq!(summary.status, StoredTraceStatus::Ok);
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);
    }

    #[test]
    fn query_history_reads_successful_query_provenance_leniently() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut legacy_record = trace_record("legacy-trace", "legacy-span");
        legacy_record.attributes_json =
            r#"{"sql":"SELECT old","status":"ok","row_count":1}"#.to_string();

        let mut missing_workspace_record = trace_record("missing-workspace-trace", "span");
        missing_workspace_record.attributes_json = query_history_attributes(
            None,
            "SELECT missing_workspace",
            r#"["github"]"#,
            "[]",
            "[]",
            1,
        );

        let mut malformed_record = trace_record("malformed-trace", "malformed-span");
        malformed_record.attributes_json = query_history_attributes(
            Some("default"),
            "SELECT malformed",
            "not-json",
            "[]",
            "[]",
            1,
        );

        let mut valid_record = trace_record("valid-trace", "valid-span");
        valid_record.end_time_unix_nanos = 42;
        valid_record.attributes_json = query_history_attributes(
            Some("default"),
            "SELECT title FROM github.issues",
            r#"["github"]"#,
            r#"[{"source_name":"github","schema_name":"github","table_name":"issues"}]"#,
            r#"[{"source_name":"github","schema_name":"github","function_name":"search_issues"}]"#,
            15,
        );

        let path = dir.join(timestamped_jsonl_path(SystemTime::now()));
        write_record_file_lines(
            &path,
            &[
                legacy_record,
                missing_workspace_record,
                malformed_record,
                valid_record,
            ],
        );

        let history = TraceStore::new(dir)
            .list_query_history_sync()
            .expect("query history");

        assert_eq!(history.len(), 1);
        let entry = history.first().expect("history entry");
        assert_eq!(entry.workspace, "default");
        assert_eq!(entry.sql, "SELECT title FROM github.issues");
        assert_eq!(entry.sources, ["github"]);
        assert_eq!(entry.row_count, 15);
        assert_eq!(entry.tables.len(), 1);
        let table = entry.tables.first().expect("table usage");
        assert_eq!(table.source, "github");
        assert_eq!(table.schema, "github");
        assert_eq!(table.table, "issues");
        assert_eq!(entry.table_functions.len(), 1);
        let table_function = entry.table_functions.first().expect("table function usage");
        assert_eq!(table_function.function, "search_issues");
    }

    #[test]
    fn list_traces_stops_after_enough_recent_files() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let old_time = now - Duration::from_hours(2);
        let recent_time = now - Duration::from_secs(1);
        let mut recent_record = trace_record("recent-trace", "recent-span");
        recent_record.start_time_unix_nanos = unix_nanos(recent_time);
        recent_record.end_time_unix_nanos = unix_nanos(recent_time + Duration::from_millis(1));
        let recent_path = dir.join(timestamped_jsonl_path(recent_time));
        write_record_file(&recent_path, &recent_record);
        set_modified_time(&recent_path, recent_time);
        let old_path = dir.join(timestamped_jsonl_path(old_time));
        fs::write(&old_path, "{not-json}\n").expect("write old corrupt record");
        set_modified_time(&old_path, old_time);

        let summaries = TraceStore::new(dir)
            .list_traces_sync(1, 0)
            .expect("list traces");

        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("recent trace").trace_id,
            "recent-trace"
        );
    }

    #[test]
    fn list_traces_scans_enough_recent_files_for_offset() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let old_time = now - Duration::from_hours(2);
        let first_time = now - Duration::from_secs(1);
        let second_time = now - Duration::from_secs(2);
        for (trace_id, span_id, timestamp) in [
            ("first-trace", "first-span", first_time),
            ("second-trace", "second-span", second_time),
        ] {
            let mut record = trace_record(trace_id, span_id);
            record.start_time_unix_nanos = unix_nanos(timestamp);
            record.end_time_unix_nanos = unix_nanos(timestamp + Duration::from_millis(1));
            let path = dir.join(timestamped_jsonl_path(timestamp));
            write_record_file(&path, &record);
            set_modified_time(&path, timestamp);
        }
        let old_path = dir.join(timestamped_jsonl_path(old_time));
        fs::write(&old_path, "{not-json}\n").expect("write old corrupt record");
        set_modified_time(&old_path, old_time);

        let summaries = TraceStore::new(dir)
            .list_traces_sync(1, 1)
            .expect("list traces");

        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("second trace").trace_id,
            "second-trace"
        );
    }

    #[test]
    fn list_traces_completes_returned_summaries_from_older_files() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let unrelated_time = now - Duration::from_hours(3);
        let child_time = now - Duration::from_hours(2);
        let root_time = now - Duration::from_secs(1);
        let unrelated_path = dir.join(timestamped_jsonl_path(unrelated_time));
        fs::write(
            &unrelated_path,
            r#"{"trace_id":"split-trace","span_id":"ancient""#,
        )
        .expect("write ancient corrupt record");
        set_modified_time(&unrelated_path, unrelated_time);

        let mut child_record = trace_record("split-trace", "child-span");
        child_record.parent_span_id = Some("root-span".to_string());
        child_record.name = "GET github.pulls".to_string();
        child_record.status = StoredTraceStatus::Error;
        child_record.start_time_unix_nanos = unix_nanos(child_time);
        child_record.end_time_unix_nanos = unix_nanos(child_time + Duration::from_millis(10));
        child_record.duration_nanos = 10_000_000;
        let child_path = dir.join(timestamped_jsonl_path(child_time));
        write_record_file(&child_path, &child_record);
        set_modified_time(&child_path, child_time);

        let mut root_record = trace_record("split-trace", "root-span");
        root_record.status = StoredTraceStatus::Unspecified;
        root_record.attributes_json = r#"{"sql":"SELECT 1"}"#.to_string();
        root_record.start_time_unix_nanos = unix_nanos(child_time - Duration::from_secs(1));
        root_record.end_time_unix_nanos = unix_nanos(root_time + Duration::from_millis(1));
        root_record.duration_nanos = root_record
            .end_time_unix_nanos
            .saturating_sub(root_record.start_time_unix_nanos);
        let root_path = dir.join(timestamped_jsonl_path(root_time));
        write_record_file(&root_path, &root_record);
        set_modified_time(&root_path, root_time);

        let summaries = TraceStore::new(dir)
            .list_traces_sync(1, 0)
            .expect("list traces");

        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("split trace");
        assert_eq!(summary.trace_id, "split-trace");
        assert_eq!(summary.root_span_id, "root-span");
        assert_eq!(summary.query, "SELECT 1");
        assert_eq!(summary.status, StoredTraceStatus::Error);
        assert_eq!(summary.span_count, 2);
    }

    #[test]
    fn list_traces_does_not_treat_query_primary_as_root_for_completion() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let parent_time = now - Duration::from_hours(2);
        let query_time = now - Duration::from_secs(1);

        let mut parent_record = trace_record("nested-query-trace", "parent-span");
        parent_record.name = "grpc.request".to_string();
        parent_record.start_time_unix_nanos = unix_nanos(parent_time);
        parent_record.end_time_unix_nanos = unix_nanos(parent_time + Duration::from_millis(10));
        parent_record.duration_nanos = 10_000_000;
        let parent_path = dir.join(timestamped_jsonl_path(parent_time));
        write_record_file(&parent_path, &parent_record);
        set_modified_time(&parent_path, parent_time);

        let mut query_record = trace_record("nested-query-trace", "query-span");
        query_record.parent_span_id = Some("parent-span".to_string());
        query_record.attributes_json = r#"{"sql":"SELECT nested"}"#.to_string();
        query_record.start_time_unix_nanos = unix_nanos(query_time);
        query_record.end_time_unix_nanos = unix_nanos(query_time + Duration::from_millis(1));
        query_record.duration_nanos = 1_000_000;
        let query_path = dir.join(timestamped_jsonl_path(query_time));
        write_record_file(&query_path, &query_record);
        set_modified_time(&query_path, query_time);

        let summaries = TraceStore::new(dir)
            .list_traces_sync(1, 0)
            .expect("list traces");

        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("nested query trace");
        assert_eq!(summary.root_span_id, "query-span");
        assert_eq!(summary.query, "SELECT nested");
        assert_eq!(summary.span_count, 2);
        assert_eq!(
            summary.start_time_unix_nanos,
            parent_record.start_time_unix_nanos
        );
    }

    #[test]
    fn list_traces_keeps_scanning_when_file_mtime_is_coarse() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let base_time = SystemTime::now() - Duration::from_secs(10);
        let hidden_modified = base_time;
        let visible_modified = base_time + Duration::from_millis(10);

        let mut hidden_record = trace_record("hidden-newer-trace", "hidden-span");
        hidden_record.start_time_unix_nanos = unix_nanos(base_time);
        hidden_record.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(900));
        let hidden_path = dir.join(timestamped_jsonl_path(hidden_modified));
        write_record_file(&hidden_path, &hidden_record);
        set_modified_time(&hidden_path, hidden_modified);

        let mut visible_record = trace_record("visible-older-trace", "visible-span");
        visible_record.start_time_unix_nanos = unix_nanos(base_time);
        visible_record.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(100));
        let visible_path = dir.join(timestamped_jsonl_path(visible_modified));
        write_record_file(&visible_path, &visible_record);
        set_modified_time(&visible_path, visible_modified);

        let summaries = TraceStore::new(dir)
            .list_traces_sync(1, 0)
            .expect("list traces");

        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("newer trace").trace_id,
            "hidden-newer-trace"
        );
    }

    #[test]
    fn skips_incomplete_trailing_jsonl_record() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        fs::write(
            dir.join(timestamped_jsonl_path(SystemTime::now())),
            "{\"trace_id\":",
        )
        .expect("write partial jsonl");

        let store = TraceStore::new(dir);

        assert!(
            store
                .list_traces_sync(10, 0)
                .expect("list traces")
                .is_empty()
        );
    }

    #[test]
    fn get_trace_skips_unrelated_lines_before_decoding() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        fs::write(
            dir.join(timestamped_jsonl_path(SystemTime::now())),
            "{not-json}\n",
        )
        .expect("write unrelated corrupt record");
        let target_path = dir.join(timestamped_jsonl_path(
            SystemTime::now() + Duration::from_secs(1),
        ));
        write_record_file(&target_path, &trace_record("target-trace", "target-span"));

        let detail = TraceStore::new(dir)
            .get_trace_sync("target-trace")
            .expect("trace detail");

        assert_eq!(detail.summary.trace_id, "target-trace");
        assert_eq!(detail.spans.len(), 1);
    }

    #[test]
    fn get_trace_stops_after_trace_is_newer_than_remaining_files() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let old_time = now - Duration::from_hours(2);
        let recent_time = now - Duration::from_secs(1);
        let mut target_record = trace_record("target-trace", "target-span");
        target_record.start_time_unix_nanos = unix_nanos(recent_time);
        target_record.end_time_unix_nanos = unix_nanos(recent_time + Duration::from_millis(1));
        let target_path = dir.join(timestamped_jsonl_path(recent_time));
        write_record_file(&target_path, &target_record);
        set_modified_time(&target_path, recent_time);
        let old_path = dir.join(timestamped_jsonl_path(old_time));
        fs::write(&old_path, r#"{"trace_id":"target-trace""#).expect("write old corrupt record");
        set_modified_time(&old_path, old_time);

        let detail = TraceStore::new(dir)
            .get_trace_sync("target-trace")
            .expect("trace detail");

        assert_eq!(detail.summary.trace_id, "target-trace");
        assert_eq!(detail.spans.len(), 1);
    }

    #[test]
    fn get_trace_does_not_treat_query_primary_as_root_span() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let parent_time = now - Duration::from_hours(2);
        let query_time = now - Duration::from_secs(1);

        let mut parent_record = trace_record("nested-detail-trace", "parent-span");
        parent_record.name = "grpc.request".to_string();
        parent_record.start_time_unix_nanos = unix_nanos(parent_time);
        parent_record.end_time_unix_nanos = unix_nanos(parent_time + Duration::from_millis(10));
        parent_record.duration_nanos = 10_000_000;
        let parent_path = dir.join(timestamped_jsonl_path(parent_time));
        write_record_file(&parent_path, &parent_record);
        set_modified_time(&parent_path, parent_time);

        let mut query_record = trace_record("nested-detail-trace", "query-span");
        query_record.parent_span_id = Some("parent-span".to_string());
        query_record.attributes_json = r#"{"sql":"SELECT nested"}"#.to_string();
        query_record.start_time_unix_nanos = unix_nanos(query_time);
        query_record.end_time_unix_nanos = unix_nanos(query_time + Duration::from_millis(1));
        query_record.duration_nanos = 1_000_000;
        let query_path = dir.join(timestamped_jsonl_path(query_time));
        write_record_file(&query_path, &query_record);
        set_modified_time(&query_path, query_time);

        let detail = TraceStore::new(dir)
            .get_trace_sync("nested-detail-trace")
            .expect("trace detail");

        assert_eq!(detail.summary.root_span_id, "query-span");
        assert_eq!(detail.summary.query, "SELECT nested");
        assert_eq!(detail.summary.span_count, 2);
        assert_eq!(
            detail.summary.start_time_unix_nanos,
            parent_record.start_time_unix_nanos
        );
        assert_eq!(detail.spans.len(), 2);
    }

    #[test]
    fn get_trace_keeps_newer_duplicate_span_record() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let base_time = SystemTime::now() - Duration::from_secs(10);
        let older_modified = base_time;
        let newer_modified = base_time + Duration::from_millis(10);

        let mut older_record = trace_record("duplicate-trace", "duplicate-span");
        older_record.attributes_json = r#"{"sql":"SELECT 'old'"}"#.to_string();
        older_record.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(500));
        older_record.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(600));
        let older_path = dir.join(timestamped_jsonl_path(older_modified));
        write_record_file(&older_path, &older_record);
        set_modified_time(&older_path, older_modified);

        let mut newer_record = trace_record("duplicate-trace", "duplicate-span");
        newer_record.attributes_json = r#"{"sql":"SELECT 'new'"}"#.to_string();
        newer_record.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(500));
        newer_record.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(700));
        let newer_path = dir.join(timestamped_jsonl_path(newer_modified));
        write_record_file(&newer_path, &newer_record);
        set_modified_time(&newer_path, newer_modified);

        let detail = TraceStore::new(dir)
            .get_trace_sync("duplicate-trace")
            .expect("trace detail");

        assert_eq!(detail.spans.len(), 1);
        assert_eq!(detail.summary.query, "SELECT 'new'");
        assert_eq!(
            detail
                .spans
                .first()
                .expect("duplicate span")
                .attributes_json,
            r#"{"sql":"SELECT 'new'"}"#
        );
    }

    #[test]
    fn workspace_filtered_traces_include_complete_matching_traces() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut alpha_query = trace_record("alpha-new", "alpha-query");
        alpha_query.attributes_json =
            r#"{"workspace":"alpha","sql":"SELECT alpha_new"}"#.to_string();
        alpha_query.start_time_unix_nanos = 10;
        alpha_query.end_time_unix_nanos = 15;

        let mut alpha_child = trace_record("alpha-new", "alpha-child");
        alpha_child.parent_span_id = Some("alpha-query".to_string());
        alpha_child.name = "http.request".to_string();
        alpha_child.start_time_unix_nanos = 15;
        alpha_child.end_time_unix_nanos = 20;

        let mut beta_query = trace_record("beta-trace", "beta-query");
        beta_query.attributes_json = r#"{"workspace":"beta","sql":"SELECT beta"}"#.to_string();
        beta_query.start_time_unix_nanos = 30;
        beta_query.end_time_unix_nanos = 40;

        let mut alpha_old = trace_record("alpha-old", "alpha-old-query");
        alpha_old.attributes_json = r#"{"workspace":"alpha","sql":"SELECT alpha_old"}"#.to_string();
        alpha_old.start_time_unix_nanos = 1;
        alpha_old.end_time_unix_nanos = 2;

        let mut older_duplicate = trace_record("duplicate-workspace", "duplicate-span");
        older_duplicate.attributes_json =
            r#"{"workspace":"alpha","sql":"SELECT duplicate_old"}"#.to_string();
        older_duplicate.start_time_unix_nanos = 50;
        older_duplicate.end_time_unix_nanos = 60;

        let mut newer_duplicate = trace_record("duplicate-workspace", "duplicate-span");
        newer_duplicate.attributes_json =
            r#"{"workspace":"beta","sql":"SELECT duplicate_new"}"#.to_string();
        newer_duplicate.start_time_unix_nanos = 50;
        newer_duplicate.end_time_unix_nanos = 70;

        let path = dir.join(timestamped_jsonl_path(SystemTime::now()));
        write_record_file_lines(
            &path,
            &[
                alpha_query,
                alpha_child,
                beta_query,
                alpha_old,
                older_duplicate,
                newer_duplicate,
            ],
        );

        let store = TraceStore::new(dir);
        let summaries = store
            .list_traces_for_workspace_sync(10, 0, "alpha")
            .expect("list alpha traces");

        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.trace_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha-new", "alpha-old"]
        );
        assert_eq!(summaries.first().expect("alpha-new").span_count, 2);
        assert_eq!(
            summaries.first().expect("alpha-new").end_time_unix_nanos,
            20
        );

        let paged = store
            .list_traces_for_workspace_sync(1, 1, "alpha")
            .expect("list second alpha trace");
        assert_eq!(paged.first().expect("paged alpha").trace_id, "alpha-old");

        let detail = store
            .get_trace_for_workspace_sync("alpha-new", "alpha")
            .expect("alpha trace detail");
        assert_eq!(detail.spans.len(), 2);
        store
            .get_trace_for_workspace_sync("beta-trace", "alpha")
            .expect_err("beta trace must not be visible through alpha filter");
        store
            .get_trace_for_workspace_sync("duplicate-workspace", "alpha")
            .expect_err("newer beta duplicate must not be visible through alpha filter");
    }

    #[test]
    fn trace_store_keeps_later_duplicate_span_record_in_same_file() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let base_time = SystemTime::now() - Duration::from_secs(10);

        let mut older_record = trace_record("same-file-duplicate-trace", "duplicate-span");
        older_record.attributes_json = r#"{"sql":"SELECT 'old'"}"#.to_string();
        older_record.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(500));
        older_record.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(600));

        let mut newer_record = trace_record("same-file-duplicate-trace", "duplicate-span");
        newer_record.attributes_json = r#"{"sql":"SELECT 'new'"}"#.to_string();
        newer_record.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(500));
        newer_record.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(700));
        let path = dir.join(timestamped_jsonl_path(base_time));
        write_record_file_lines(&path, &[older_record, newer_record]);
        set_modified_time(&path, base_time);

        let store = TraceStore::new(dir);
        let summary = store
            .list_traces_sync(1, 0)
            .expect("list traces")
            .into_iter()
            .next()
            .expect("trace summary");
        let detail = store
            .get_trace_sync("same-file-duplicate-trace")
            .expect("trace detail");

        assert_eq!(summary.query, "SELECT 'new'");
        assert_eq!(detail.summary.query, "SELECT 'new'");
        assert_eq!(
            detail
                .spans
                .first()
                .expect("duplicate span")
                .attributes_json,
            r#"{"sql":"SELECT 'new'"}"#
        );
    }

    #[test]
    fn missing_trace_store_lists_empty_and_get_returns_not_found() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        let store = TraceStore::new(dir);

        assert!(
            store
                .list_traces_sync(10, 0)
                .expect("missing store list")
                .is_empty()
        );
        store.get_trace_sync("missing").unwrap_err();
    }

    #[test]
    fn exporter_prunes_expired_jsonl_files_on_startup() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let expired_path = dir.join(timestamped_jsonl_path(SystemTime::now()));
        let old_name_fresh_path = dir.join(timestamped_jsonl_path(
            SystemTime::now() - Duration::from_hours(8 * 24),
        ));
        fs::write(&expired_path, "{}\n").expect("write expired trace file");
        fs::write(&old_name_fresh_path, "{}\n").expect("write fresh trace file");
        set_modified_time(
            &expired_path,
            SystemTime::now() - Duration::from_hours(8 * 24),
        );

        let _exporter =
            JsonlSpanExporter::new(dir.clone(), TRACE_RETENTION).expect("jsonl span exporter");

        assert!(!expired_path.exists());
        assert!(old_name_fresh_path.exists());
    }

    #[test]
    fn trace_store_prunes_expired_jsonl_files_on_read() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let expired_path = dir.join(timestamped_jsonl_path(SystemTime::now()));
        let old_name_fresh_path = dir.join(timestamped_jsonl_path(
            SystemTime::now() - Duration::from_hours(8 * 24),
        ));
        write_record_file(&expired_path, &trace_record("old-trace", "old-span"));
        write_record_file(
            &old_name_fresh_path,
            &trace_record("fresh-trace", "fresh-span"),
        );
        set_modified_time(
            &expired_path,
            SystemTime::now() - Duration::from_hours(8 * 24),
        );
        let store = TraceStore::with_retention(dir, TRACE_RETENTION);

        let traces = store.list_traces_sync(10, 0).expect("list traces");

        assert!(!expired_path.exists());
        assert!(old_name_fresh_path.exists());
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

    fn write_record_file_lines(path: &Path, records: &[TraceSpanRecord]) {
        let mut lines = String::new();
        for record in records {
            lines.push_str(&serde_json::to_string(record).expect("serialize record"));
            lines.push('\n');
        }
        fs::write(path, lines).expect("write trace records");
    }

    fn set_modified_time(path: &Path, modified: SystemTime) {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open trace file for timestamp update");
        file.set_times(FileTimes::new().set_modified(modified))
            .expect("set trace file modified time");
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

    fn query_history_attributes(
        workspace: Option<&str>,
        sql: &str,
        sources_json: &str,
        tables_json: &str,
        table_functions_json: &str,
        row_count: u64,
    ) -> String {
        let mut attributes = serde_json::Map::new();
        if let Some(workspace) = workspace {
            attributes.insert(WORKSPACE_SPAN_ATTRIBUTE.to_string(), json!(workspace));
        }
        attributes.insert("sql".to_string(), json!(sql));
        attributes.insert("status".to_string(), json!("ok"));
        attributes.insert("row_count".to_string(), json!(row_count));
        attributes.insert(
            crate::telemetry::QUERY_TRACE_SOURCES_ATTR.to_string(),
            json!(sources_json),
        );
        attributes.insert(
            crate::telemetry::QUERY_TRACE_TABLES_ATTR.to_string(),
            json!(tables_json),
        );
        attributes.insert(
            crate::telemetry::QUERY_TRACE_TABLE_FUNCTIONS_ATTR.to_string(),
            json!(table_functions_json),
        );
        serde_json::Value::Object(attributes).to_string()
    }
}
