//! SQLite-backed span export for local trace capture.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{SpanId, SpanKind, Status};
use opentelemetry::{Array as OtelArray, KeyValue, Value as OtelValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use rusqlite::{Connection, OptionalExtension as _, Row, Statement, params};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};

const DB_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const CREATE_SCHEMA_SQL: &str = r"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    parent_span_is_remote INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    status_code TEXT NOT NULL,
    status_message TEXT,
    start_time_unix_nanos INTEGER NOT NULL,
    end_time_unix_nanos INTEGER NOT NULL,
    duration_nanos INTEGER NOT NULL,
    attributes_json TEXT NOT NULL,
    events_json TEXT NOT NULL,
    links_json TEXT NOT NULL,
    resource_json TEXT NOT NULL,
    scope_name TEXT NOT NULL,
    scope_version TEXT,
    scope_schema_url TEXT,
    scope_attributes_json TEXT NOT NULL,
    trace_flags INTEGER NOT NULL,
    trace_state TEXT NOT NULL,
    is_remote INTEGER NOT NULL,
    dropped_attributes_count INTEGER NOT NULL,
    dropped_events_count INTEGER NOT NULL,
    dropped_links_count INTEGER NOT NULL,
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS spans_trace_start_idx
    ON spans(trace_id, start_time_unix_nanos);
CREATE INDEX IF NOT EXISTS spans_name_start_idx
    ON spans(name, start_time_unix_nanos DESC);
CREATE INDEX IF NOT EXISTS spans_start_idx
    ON spans(start_time_unix_nanos DESC);
";

const INSERT_SPAN_SQL: &str = r"
INSERT OR REPLACE INTO spans (
    trace_id,
    span_id,
    parent_span_id,
    parent_span_is_remote,
    name,
    kind,
    status_code,
    status_message,
    start_time_unix_nanos,
    end_time_unix_nanos,
    duration_nanos,
    attributes_json,
    events_json,
    links_json,
    resource_json,
    scope_name,
    scope_version,
    scope_schema_url,
    scope_attributes_json,
    trace_flags,
    trace_state,
    is_remote,
    dropped_attributes_count,
    dropped_events_count,
    dropped_links_count
) VALUES (
    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
    ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
    ?21, ?22, ?23, ?24, ?25
);
";

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalTraceStoreError {
    #[error("failed to create local trace store directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to initialize local trace store {path}: {source}")]
    Sqlite {
        path: PathBuf,
        source: rusqlite::Error,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct SqliteSpanExporter {
    path: PathBuf,
    resource_json: Arc<Mutex<String>>,
    shutdown_called: Arc<AtomicBool>,
}

impl SqliteSpanExporter {
    pub(crate) fn new(path: PathBuf) -> Result<Self, LocalTraceStoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| LocalTraceStoreError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        open_trace_store_connection(&path).map_err(|source| LocalTraceStoreError::Sqlite {
            path: path.clone(),
            source,
        })?;

        Ok(Self {
            path,
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

impl SpanExporter for SqliteSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        if self.shutdown_called.load(Ordering::Relaxed) {
            return Err(OTelSdkError::AlreadyShutdown);
        }

        write_batch(&self.path, &self.resource_json(), &batch)
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
    path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraceStoreError {
    #[error("trace '{0}' not found")]
    NotFound(String),
    #[error("failed to read local trace store {path}: {source}")]
    Sqlite {
        path: PathBuf,
        source: rusqlite::Error,
    },
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
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn list_traces(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
        let Some(connection) = self.open_existing_connection()? else {
            return Ok(Vec::new());
        };

        let mut statement = connection
            .prepare(
                r"
SELECT
    trace_id,
    MIN(start_time_unix_nanos) AS start_time_unix_nanos,
    MAX(end_time_unix_nanos) AS end_time_unix_nanos,
    COUNT(*) AS span_count,
    SUM(CASE WHEN status_code = 'error' THEN 1 ELSE 0 END) AS error_count
FROM spans
GROUP BY trace_id
ORDER BY end_time_unix_nanos DESC
LIMIT ?1 OFFSET ?2
",
            )
            .map_err(|source| self.sqlite_error(source))?;
        let aggregates = statement
            .query_map(params![usize_to_i64(limit), usize_to_i64(offset)], |row| {
                Ok(TraceAggregate {
                    trace_id: row.get(0)?,
                    start_time_unix_nanos: row.get(1)?,
                    end_time_unix_nanos: row.get(2)?,
                    span_count: i64_to_u32(row.get(3)?),
                    error_count: i64_to_u32(row.get(4)?),
                })
            })
            .map_err(|source| self.sqlite_error(source))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| self.sqlite_error(source))?;
        drop(statement);

        aggregates
            .into_iter()
            .map(|aggregate| {
                let primary = primary_span(&connection, &aggregate.trace_id)
                    .map_err(|source| self.sqlite_error(source))?;
                Ok(summary_from_aggregate(&aggregate, primary.as_ref()))
            })
            .collect()
    }

    pub(crate) fn get_trace(&self, trace_id: &str) -> Result<TraceDetailRecord, TraceStoreError> {
        let Some(connection) = self.open_existing_connection()? else {
            return Err(TraceStoreError::NotFound(trace_id.to_string()));
        };

        let mut statement = connection
            .prepare(
                r"
SELECT
    trace_id,
    span_id,
    parent_span_id,
    parent_span_is_remote,
    name,
    kind,
    status_code,
    status_message,
    start_time_unix_nanos,
    end_time_unix_nanos,
    duration_nanos,
    attributes_json,
    events_json,
    links_json,
    resource_json,
    scope_name,
    scope_version,
    scope_schema_url,
    scope_attributes_json,
    trace_flags,
    trace_state,
    is_remote,
    dropped_attributes_count,
    dropped_events_count,
    dropped_links_count
FROM spans
WHERE trace_id = ?1
ORDER BY start_time_unix_nanos ASC, span_id ASC
",
            )
            .map_err(|source| self.sqlite_error(source))?;
        let spans = statement
            .query_map(params![trace_id], span_from_row)
            .map_err(|source| self.sqlite_error(source))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| self.sqlite_error(source))?;

        if spans.is_empty() {
            return Err(TraceStoreError::NotFound(trace_id.to_string()));
        }

        let summary = summary_from_spans(trace_id, &spans);
        Ok(TraceDetailRecord { summary, spans })
    }

    fn open_existing_connection(&self) -> Result<Option<Connection>, TraceStoreError> {
        if !self.path.exists() {
            return Ok(None);
        }

        let connection =
            Connection::open(&self.path).map_err(|source| self.sqlite_error(source))?;
        connection
            .busy_timeout(DB_BUSY_TIMEOUT)
            .map_err(|source| self.sqlite_error(source))?;
        if !spans_table_exists(&connection).map_err(|source| self.sqlite_error(source))? {
            return Ok(None);
        }
        Ok(Some(connection))
    }

    fn sqlite_error(&self, source: rusqlite::Error) -> TraceStoreError {
        TraceStoreError::Sqlite {
            path: self.path.clone(),
            source,
        }
    }
}

fn spans_table_exists(connection: &Connection) -> rusqlite::Result<bool> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'spans'",
        [],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn primary_span(
    connection: &Connection,
    trace_id: &str,
) -> rusqlite::Result<Option<TraceSpanRecord>> {
    connection
        .query_row(
            r"
SELECT
    trace_id,
    span_id,
    parent_span_id,
    parent_span_is_remote,
    name,
    kind,
    status_code,
    status_message,
    start_time_unix_nanos,
    end_time_unix_nanos,
    duration_nanos,
    attributes_json,
    events_json,
    links_json,
    resource_json,
    scope_name,
    scope_version,
    scope_schema_url,
    scope_attributes_json,
    trace_flags,
    trace_state,
    is_remote,
    dropped_attributes_count,
    dropped_events_count,
    dropped_links_count
FROM spans
WHERE trace_id = ?1
ORDER BY
    CASE
        WHEN name = 'coral.query' THEN 0
        WHEN parent_span_id IS NULL THEN 1
        ELSE 2
    END,
    start_time_unix_nanos ASC,
    span_id ASC
LIMIT 1
",
            params![trace_id],
            span_from_row,
        )
        .optional()
}

fn span_from_row(row: &Row<'_>) -> rusqlite::Result<TraceSpanRecord> {
    Ok(TraceSpanRecord {
        trace_id: row.get(0)?,
        span_id: row.get(1)?,
        parent_span_id: row.get(2)?,
        parent_span_is_remote: i64_to_bool(row.get(3)?),
        name: row.get(4)?,
        kind: row.get(5)?,
        status: stored_status(row.get::<_, String>(6)?.as_str()),
        status_message: row.get(7)?,
        start_time_unix_nanos: row.get(8)?,
        end_time_unix_nanos: row.get(9)?,
        duration_nanos: row.get(10)?,
        attributes_json: row.get(11)?,
        events_json: row.get(12)?,
        links_json: row.get(13)?,
        resource_json: row.get(14)?,
        scope_name: row.get(15)?,
        scope_version: row.get(16)?,
        scope_schema_url: row.get(17)?,
        scope_attributes_json: row.get(18)?,
        trace_flags: i64_to_i32(row.get(19)?),
        trace_state: row.get(20)?,
        is_remote: i64_to_bool(row.get(21)?),
        dropped_attributes_count: i64_to_u32(row.get(22)?),
        dropped_events_count: i64_to_u32(row.get(23)?),
        dropped_links_count: i64_to_u32(row.get(24)?),
    })
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

fn i64_to_bool(value: i64) -> bool {
    value != 0
}

fn i64_to_i32(value: i64) -> i32 {
    i32::try_from(value).unwrap_or(if value.is_negative() {
        i32::MIN
    } else {
        i32::MAX
    })
}

fn i64_to_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(if value.is_negative() { 0 } else { u32::MAX })
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn open_trace_store_connection(path: &Path) -> rusqlite::Result<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(DB_BUSY_TIMEOUT)?;
    connection.execute_batch(CREATE_SCHEMA_SQL)?;
    Ok(connection)
}

fn write_batch(path: &Path, resource_json: &str, batch: &[SpanData]) -> rusqlite::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut connection = open_trace_store_connection(path)?;
    let transaction = connection.transaction()?;
    {
        let mut statement = transaction.prepare(INSERT_SPAN_SQL)?;
        for span in batch {
            insert_span(&mut statement, resource_json, span)?;
        }
    }
    transaction.commit()
}

fn insert_span(
    statement: &mut Statement<'_>,
    resource_json: &str,
    span: &SpanData,
) -> rusqlite::Result<()> {
    let span_context = &span.span_context;
    let parent_span_id =
        (span.parent_span_id != SpanId::INVALID).then(|| span.parent_span_id.to_string());
    let (status_code, status_message) = status_parts(&span.status);
    let attributes_json = key_values_json(span.attributes.iter()).to_string();
    let events_json = events_json(span).to_string();
    let links_json = links_json(span).to_string();
    let scope_attributes_json =
        key_values_json(span.instrumentation_scope.attributes()).to_string();

    statement.execute(params![
        span_context.trace_id().to_string(),
        span_context.span_id().to_string(),
        parent_span_id,
        span.parent_span_is_remote,
        span.name.as_ref(),
        span_kind(&span.span_kind),
        status_code,
        status_message,
        unix_nanos(span.start_time),
        unix_nanos(span.end_time),
        duration_nanos(span.start_time, span.end_time),
        attributes_json,
        events_json,
        links_json,
        resource_json,
        span.instrumentation_scope.name(),
        span.instrumentation_scope.version(),
        span.instrumentation_scope.schema_url(),
        scope_attributes_json,
        i64::from(span_context.trace_flags().to_u8()),
        span_context.trace_state().header(),
        span_context.is_remote(),
        i64::from(span.dropped_attributes_count),
        i64::from(span.events.dropped_count),
        i64::from(span.links.dropped_count),
    ])?;

    Ok(())
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

fn status_parts(status: &Status) -> (&'static str, Option<String>) {
    match status {
        Status::Unset => ("unset", None),
        Status::Error { description } => ("error", Some(description.to_string())),
        Status::Ok => ("ok", None),
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
    use opentelemetry::KeyValue;
    use opentelemetry::trace::{Span as _, SpanKind, Tracer, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tempfile::TempDir;

    use super::{SqliteSpanExporter, StoredTraceStatus, TraceStore};

    #[test]
    fn exports_finished_spans_to_sqlite() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("telemetry").join("traces.sqlite3");
        let exporter = SqliteSpanExporter::new(path.clone()).expect("sqlite span exporter");
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

        let connection = rusqlite::Connection::open(path).expect("open trace db");
        let (name, attributes_json, resource_json): (String, String, String) = connection
            .query_row(
                "SELECT name, attributes_json, resource_json FROM spans",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("span row");

        assert_eq!(name, "coral.query");
        assert!(attributes_json.contains(r#""test.attribute":"value""#));
        assert!(resource_json.contains(r#""service.name":"coral-test""#));
    }

    #[test]
    fn reads_trace_summaries_and_details_from_sqlite() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("telemetry").join("traces.sqlite3");
        let exporter = SqliteSpanExporter::new(path.clone()).expect("sqlite span exporter");
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

        let store = TraceStore::new(path);
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
        let path = temp.path().join("telemetry").join("traces.sqlite3");
        let store = TraceStore::new(path);

        assert!(
            store
                .list_traces(10, 0)
                .expect("missing store list")
                .is_empty()
        );
        assert!(store.get_trace("missing").is_err());
    }
}
