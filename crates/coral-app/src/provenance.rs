//! Local episode provenance receipts and queryable read tables.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use coral_engine::RuntimeTable;
use opentelemetry::trace::TraceContextExt as _;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use uuid::Uuid;

pub(crate) const SCHEMA_NAME: &str = "coral_provenance";

const EPISODES_TABLE: &str = "episodes";
const CALLS_TABLE: &str = "calls";
const OCCURRENCES_TABLE: &str = "occurrences";
const BINDINGS_TABLE: &str = "bindings";
const MAX_PREVIEW_CHARS: usize = 160;

static LOCAL_EPISODE_ID: OnceLock<String> = OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) struct ProvenanceRecorder {
    inner: Arc<RecorderInner>,
}

#[derive(Debug)]
struct RecorderInner {
    events_file: PathBuf,
    lock: Mutex<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct CallTiming {
    started_at_unix_nanos: i64,
    started_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct ProvenanceCall {
    pub(crate) workspace: String,
    pub(crate) operation: String,
    pub(crate) input_json: Value,
    pub(crate) output_summary_json: Value,
    pub(crate) status: String,
    pub(crate) row_count: Option<i64>,
    pub(crate) input_occurrences: Vec<OccurrenceDraft>,
    pub(crate) output_occurrences: Vec<OccurrenceDraft>,
    pub(crate) timing: CallTiming,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OccurrenceDraft {
    role: OccurrenceRole,
    path: String,
    entity_kind: String,
    entity_key: String,
    value_hash: Option<String>,
    value_preview: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OccurrenceRole {
    Input,
    Output,
}

#[derive(Debug, Clone)]
struct CallIdentity {
    episode_id: String,
    call_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredRecord {
    Episode(EpisodeRecord),
    Call(CallRecord),
    Occurrence(OccurrenceRecord),
    Binding(BindingRecord),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EpisodeRecord {
    episode_id: String,
    workspace: String,
    started_at_unix_nanos: i64,
    id_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CallRecord {
    call_id: String,
    episode_id: String,
    workspace: String,
    operation: String,
    status: String,
    started_at_unix_nanos: i64,
    duration_ms: i64,
    row_count: Option<i64>,
    input_json: Value,
    output_summary_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OccurrenceRecord {
    occurrence_id: String,
    episode_id: String,
    call_id: String,
    role: String,
    path: String,
    entity_kind: String,
    entity_key: String,
    value_hash: Option<String>,
    value_preview: Option<String>,
    observed_at_unix_nanos: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BindingRecord {
    binding_id: String,
    episode_id: String,
    source_occurrence_id: String,
    target_occurrence_id: String,
    source_call_id: String,
    target_call_id: String,
    entity_key: String,
    evidence_kind: String,
    observed_at_unix_nanos: i64,
}

impl ProvenanceRecorder {
    pub(crate) fn new(events_file: PathBuf) -> Self {
        Self {
            inner: Arc::new(RecorderInner {
                events_file,
                lock: Mutex::new(()),
            }),
        }
    }

    pub(crate) fn record_call(&self, span: &tracing::Span, call: ProvenanceCall) {
        let identity = call_identity(span);
        let records = self.records_for_call(&identity, call);
        let _guard = self.inner.lock.lock().expect("provenance mutex poisoned");
        if let Err(error) = append_records(&self.inner.events_file, &records) {
            tracing::warn!(
                path = %self.inner.events_file.display(),
                detail = %error,
                "failed to write local provenance records"
            );
        }
    }

    pub(crate) fn runtime_tables(&self) -> Vec<RuntimeTable> {
        let _guard = self.inner.lock.lock().expect("provenance mutex poisoned");
        let records = read_records(&self.inner.events_file);
        runtime_tables_from_records(&records)
    }

    fn records_for_call(&self, identity: &CallIdentity, call: ProvenanceCall) -> Vec<StoredRecord> {
        let existing = read_records(&self.inner.events_file);
        let episode_exists = existing.iter().any(|record| match record {
            StoredRecord::Episode(episode) => episode.episode_id == identity.episode_id,
            StoredRecord::Call(_) | StoredRecord::Occurrence(_) | StoredRecord::Binding(_) => false,
        });
        let duration_ms =
            i64::try_from(call.timing.started_at.elapsed().as_millis()).unwrap_or(i64::MAX);
        let mut records = Vec::new();
        if !episode_exists {
            records.push(StoredRecord::Episode(EpisodeRecord {
                episode_id: identity.episode_id.clone(),
                workspace: call.workspace.clone(),
                started_at_unix_nanos: call.timing.started_at_unix_nanos,
                id_source: "trace".to_string(),
            }));
        }
        let call_record = CallRecord {
            call_id: identity.call_id.clone(),
            episode_id: identity.episode_id.clone(),
            workspace: call.workspace,
            operation: call.operation,
            status: call.status,
            started_at_unix_nanos: call.timing.started_at_unix_nanos,
            duration_ms,
            row_count: call.row_count,
            input_json: call.input_json,
            output_summary_json: call.output_summary_json,
        };
        records.push(StoredRecord::Call(call_record.clone()));

        let input_records = occurrence_records(
            &identity.episode_id,
            &identity.call_id,
            call.timing.started_at_unix_nanos,
            call.input_occurrences,
        );
        let output_records = occurrence_records(
            &identity.episode_id,
            &identity.call_id,
            call.timing.started_at_unix_nanos,
            call.output_occurrences,
        );
        let bindings = nearest_prior_bindings(&existing, &call_record, &input_records);
        records.extend(
            input_records
                .into_iter()
                .chain(output_records)
                .map(StoredRecord::Occurrence),
        );
        records.extend(bindings.into_iter().map(StoredRecord::Binding));
        records
    }
}

impl CallTiming {
    pub(crate) fn start_now() -> Self {
        Self {
            started_at_unix_nanos: unix_nanos_now(),
            started_at: Instant::now(),
        }
    }
}

impl OccurrenceDraft {
    pub(crate) fn input_entity(path: impl Into<String>, entity_key: impl Into<String>) -> Self {
        let entity_key = entity_key.into();
        Self {
            role: OccurrenceRole::Input,
            path: path.into(),
            entity_kind: entity_kind(&entity_key).to_string(),
            entity_key,
            value_hash: None,
            value_preview: None,
        }
    }

    pub(crate) fn output_entity(path: impl Into<String>, entity_key: impl Into<String>) -> Self {
        let entity_key = entity_key.into();
        Self {
            role: OccurrenceRole::Output,
            path: path.into(),
            entity_kind: entity_kind(&entity_key).to_string(),
            entity_key,
            value_hash: None,
            value_preview: None,
        }
    }
}

pub(crate) fn sql_input_occurrences(sql: &str) -> Vec<OccurrenceDraft> {
    let mut occurrences = Vec::new();
    for (index, literal) in sql_string_literals(sql).into_iter().enumerate() {
        occurrences.push(value_occurrence(
            OccurrenceRole::Input,
            format!("sql.literal[{index}]"),
            &Value::String(literal),
        ));
    }
    for (index, table_ref) in sql_table_refs(sql).into_iter().enumerate() {
        occurrences.push(OccurrenceDraft::input_entity(
            format!("sql.table[{index}]"),
            format!("table:{table_ref}"),
        ));
    }
    occurrences
}

pub(crate) fn json_output_occurrences(value: &Value) -> Vec<OccurrenceDraft> {
    let mut occurrences = Vec::new();
    collect_json_occurrences(value, "output", OccurrenceRole::Output, &mut occurrences);
    occurrences
}

pub(crate) fn json_input_occurrences(value: &Value) -> Vec<OccurrenceDraft> {
    let mut occurrences = Vec::new();
    collect_json_occurrences(value, "input", OccurrenceRole::Input, &mut occurrences);
    occurrences
}

#[cfg(test)]
fn value_output_occurrence(path: impl Into<String>, value: impl Into<Value>) -> OccurrenceDraft {
    value_occurrence(OccurrenceRole::Output, path.into(), &value.into())
}

fn call_identity(span: &tracing::Span) -> CallIdentity {
    let context = span.context();
    let span_context = context.span().span_context().clone();
    if span_context.is_valid() {
        let episode_id = span_context.trace_id().to_string();
        let span_id = span_context.span_id().to_string();
        return CallIdentity {
            call_id: format!("{episode_id}:{span_id}"),
            episode_id,
        };
    }
    let episode_id = LOCAL_EPISODE_ID
        .get_or_init(|| format!("local:{}", Uuid::new_v4()))
        .clone();
    CallIdentity {
        call_id: format!("{episode_id}:{}", Uuid::new_v4()),
        episode_id,
    }
}

fn occurrence_records(
    episode_id: &str,
    call_id: &str,
    observed_at_unix_nanos: i64,
    occurrences: Vec<OccurrenceDraft>,
) -> Vec<OccurrenceRecord> {
    occurrences
        .into_iter()
        .map(|occurrence| {
            let role = match occurrence.role {
                OccurrenceRole::Input => "input",
                OccurrenceRole::Output => "output",
            }
            .to_string();
            let occurrence_id = stable_id(&[
                "occurrence",
                episode_id,
                call_id,
                &role,
                &occurrence.path,
                &occurrence.entity_key,
            ]);
            OccurrenceRecord {
                occurrence_id,
                episode_id: episode_id.to_string(),
                call_id: call_id.to_string(),
                role,
                path: occurrence.path,
                entity_kind: occurrence.entity_kind,
                entity_key: occurrence.entity_key,
                value_hash: occurrence.value_hash,
                value_preview: occurrence.value_preview,
                observed_at_unix_nanos,
            }
        })
        .collect()
}

fn nearest_prior_bindings(
    existing: &[StoredRecord],
    target_call: &CallRecord,
    input_records: &[OccurrenceRecord],
) -> Vec<BindingRecord> {
    let call_times = existing
        .iter()
        .filter_map(|record| match record {
            StoredRecord::Call(call) => Some((call.call_id.clone(), call.started_at_unix_nanos)),
            StoredRecord::Episode(_) | StoredRecord::Occurrence(_) | StoredRecord::Binding(_) => {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    let mut latest_outputs: HashMap<&str, (&OccurrenceRecord, i64)> = HashMap::new();
    for record in existing {
        let StoredRecord::Occurrence(occurrence) = record else {
            continue;
        };
        if occurrence.episode_id != target_call.episode_id || occurrence.role != "output" {
            continue;
        }
        let Some(started_at) = call_times.get(&occurrence.call_id).copied() else {
            continue;
        };
        if started_at >= target_call.started_at_unix_nanos {
            continue;
        }
        let slot = latest_outputs
            .entry(occurrence.entity_key.as_str())
            .or_insert((occurrence, started_at));
        if started_at > slot.1 {
            *slot = (occurrence, started_at);
        }
    }

    input_records
        .iter()
        .filter_map(|target| {
            let (source, _) = latest_outputs.get(target.entity_key.as_str())?;
            let binding_id = stable_id(&[
                "binding",
                &target_call.episode_id,
                &source.occurrence_id,
                &target.occurrence_id,
            ]);
            Some(BindingRecord {
                binding_id,
                episode_id: target_call.episode_id.clone(),
                source_occurrence_id: source.occurrence_id.clone(),
                target_occurrence_id: target.occurrence_id.clone(),
                source_call_id: source.call_id.clone(),
                target_call_id: target.call_id.clone(),
                entity_key: target.entity_key.clone(),
                evidence_kind: "nearest_earlier_output_value".to_string(),
                observed_at_unix_nanos: target_call.started_at_unix_nanos,
            })
        })
        .collect()
}

fn append_records(path: &Path, records: &[StoredRecord]) -> Result<(), std::io::Error> {
    if records.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    for record in records {
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn read_records(path: &Path) -> Vec<StoredRecord> {
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }
            match serde_json::from_str::<StoredRecord>(trimmed) {
                Ok(record) => Some(record),
                Err(error) => {
                    tracing::warn!(detail = %error, "skipping malformed provenance record");
                    None
                }
            }
        })
        .collect()
}

fn runtime_tables_from_records(records: &[StoredRecord]) -> Vec<RuntimeTable> {
    vec![
        runtime_table(
            EPISODES_TABLE,
            "Episodes inferred from traced Coral calls.",
            "Join episode_id to coral_provenance.calls, occurrences, and bindings.",
            episodes_schema(),
            episode_batches(records),
        ),
        runtime_table(
            CALLS_TABLE,
            "Immutable local receipts for Coral query and catalog calls.",
            "Filter by episode_id and order by started_at_unix_nanos to replay an episode.",
            calls_schema(),
            call_batches(records),
        ),
        runtime_table(
            OCCURRENCES_TABLE,
            "Input and output entity/value occurrences observed inside calls.",
            "Bind on entity_key to see where a value or catalog object appeared.",
            occurrences_schema(),
            occurrence_batches(records),
        ),
        runtime_table(
            BINDINGS_TABLE,
            "Nearest-earlier input/output continuity evidence inside one episode.",
            "Each row links a later input occurrence to the nearest earlier output occurrence with the same entity_key.",
            bindings_schema(),
            binding_batches(records),
        ),
    ]
}

fn episode_batches(records: &[StoredRecord]) -> Vec<RecordBatch> {
    #[derive(Default)]
    struct EpisodeRow {
        workspace: String,
        started_at: i64,
        last_call: i64,
        call_count: i64,
        binding_count: i64,
    }

    let mut rows: HashMap<String, EpisodeRow> = HashMap::new();
    for record in records {
        match record {
            StoredRecord::Episode(episode) => {
                rows.entry(episode.episode_id.clone())
                    .or_insert(EpisodeRow {
                        workspace: episode.workspace.clone(),
                        started_at: episode.started_at_unix_nanos,
                        last_call: episode.started_at_unix_nanos,
                        call_count: 0,
                        binding_count: 0,
                    });
            }
            StoredRecord::Call(call) => {
                let row = rows.entry(call.episode_id.clone()).or_insert(EpisodeRow {
                    workspace: call.workspace.clone(),
                    started_at: call.started_at_unix_nanos,
                    last_call: call.started_at_unix_nanos,
                    call_count: 0,
                    binding_count: 0,
                });
                row.started_at = row.started_at.min(call.started_at_unix_nanos);
                row.last_call = row.last_call.max(call.started_at_unix_nanos);
                row.call_count += 1;
            }
            StoredRecord::Binding(binding) => {
                rows.entry(binding.episode_id.clone())
                    .or_default()
                    .binding_count += 1;
            }
            StoredRecord::Occurrence(_) => {}
        }
    }
    let mut rows = rows.into_iter().collect::<Vec<_>>();
    rows.sort_by_key(|(_, row)| row.started_at);
    let batch = RecordBatch::try_new(
        episodes_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, row)| row.workspace.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, row)| row.started_at)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, row)| row.last_call)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, row)| row.call_count)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, row)| row.binding_count)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("episode batch should match schema");
    (batch.num_rows() > 0)
        .then_some(batch)
        .into_iter()
        .collect()
}

fn call_batches(records: &[StoredRecord]) -> Vec<RecordBatch> {
    let calls = records
        .iter()
        .filter_map(|record| match record {
            StoredRecord::Call(call) => Some(call),
            StoredRecord::Episode(_) | StoredRecord::Occurrence(_) | StoredRecord::Binding(_) => {
                None
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        calls_schema(),
        vec![
            string_array(calls.iter().map(|call| Some(call.call_id.as_str()))),
            string_array(calls.iter().map(|call| Some(call.episode_id.as_str()))),
            string_array(calls.iter().map(|call| Some(call.workspace.as_str()))),
            string_array(calls.iter().map(|call| Some(call.operation.as_str()))),
            string_array(calls.iter().map(|call| Some(call.status.as_str()))),
            Arc::new(Int64Array::from(
                calls
                    .iter()
                    .map(|call| call.started_at_unix_nanos)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                calls
                    .iter()
                    .map(|call| call.duration_ms)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                calls.iter().map(|call| call.row_count).collect::<Vec<_>>(),
            )) as ArrayRef,
            string_array(calls.iter().map(|call| Some(json_string(&call.input_json)))),
            string_array(
                calls
                    .iter()
                    .map(|call| Some(json_string(&call.output_summary_json))),
            ),
        ],
    )
    .expect("call batch should match schema");
    (batch.num_rows() > 0)
        .then_some(batch)
        .into_iter()
        .collect()
}

fn occurrence_batches(records: &[StoredRecord]) -> Vec<RecordBatch> {
    let occurrences = records
        .iter()
        .filter_map(|record| match record {
            StoredRecord::Occurrence(occurrence) => Some(occurrence),
            StoredRecord::Episode(_) | StoredRecord::Call(_) | StoredRecord::Binding(_) => None,
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        occurrences_schema(),
        vec![
            string_array(
                occurrences
                    .iter()
                    .map(|row| Some(row.occurrence_id.as_str())),
            ),
            string_array(occurrences.iter().map(|row| Some(row.episode_id.as_str()))),
            string_array(occurrences.iter().map(|row| Some(row.call_id.as_str()))),
            string_array(occurrences.iter().map(|row| Some(row.role.as_str()))),
            string_array(occurrences.iter().map(|row| Some(row.path.as_str()))),
            string_array(occurrences.iter().map(|row| Some(row.entity_kind.as_str()))),
            string_array(occurrences.iter().map(|row| Some(row.entity_key.as_str()))),
            string_array(occurrences.iter().map(|row| row.value_hash.as_deref())),
            string_array(occurrences.iter().map(|row| row.value_preview.as_deref())),
            Arc::new(Int64Array::from(
                occurrences
                    .iter()
                    .map(|row| row.observed_at_unix_nanos)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("occurrence batch should match schema");
    (batch.num_rows() > 0)
        .then_some(batch)
        .into_iter()
        .collect()
}

fn binding_batches(records: &[StoredRecord]) -> Vec<RecordBatch> {
    let bindings = records
        .iter()
        .filter_map(|record| match record {
            StoredRecord::Binding(binding) => Some(binding),
            StoredRecord::Episode(_) | StoredRecord::Call(_) | StoredRecord::Occurrence(_) => None,
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        bindings_schema(),
        vec![
            string_array(bindings.iter().map(|row| Some(row.binding_id.as_str()))),
            string_array(bindings.iter().map(|row| Some(row.episode_id.as_str()))),
            string_array(
                bindings
                    .iter()
                    .map(|row| Some(row.source_occurrence_id.as_str())),
            ),
            string_array(
                bindings
                    .iter()
                    .map(|row| Some(row.target_occurrence_id.as_str())),
            ),
            string_array(bindings.iter().map(|row| Some(row.source_call_id.as_str()))),
            string_array(bindings.iter().map(|row| Some(row.target_call_id.as_str()))),
            string_array(bindings.iter().map(|row| Some(row.entity_key.as_str()))),
            string_array(bindings.iter().map(|row| Some(row.evidence_kind.as_str()))),
            Arc::new(Int64Array::from(
                bindings
                    .iter()
                    .map(|row| row.observed_at_unix_nanos)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("binding batch should match schema");
    (batch.num_rows() > 0)
        .then_some(batch)
        .into_iter()
        .collect()
}

fn runtime_table(
    table_name: &str,
    description: &str,
    guide: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> RuntimeTable {
    RuntimeTable {
        schema_name: SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        description: description.to_string(),
        guide: guide.to_string(),
        schema,
        batches,
    }
}

fn episodes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("episode_id", DataType::Utf8, false),
        Field::new("workspace", DataType::Utf8, false),
        Field::new("started_at_unix_nanos", DataType::Int64, false),
        Field::new("last_call_unix_nanos", DataType::Int64, false),
        Field::new("call_count", DataType::Int64, false),
        Field::new("binding_count", DataType::Int64, false),
    ]))
}

fn calls_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("call_id", DataType::Utf8, false),
        Field::new("episode_id", DataType::Utf8, false),
        Field::new("workspace", DataType::Utf8, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("started_at_unix_nanos", DataType::Int64, false),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("row_count", DataType::Int64, true),
        Field::new("input_json", DataType::Utf8, false),
        Field::new("output_summary_json", DataType::Utf8, false),
    ]))
}

fn occurrences_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("occurrence_id", DataType::Utf8, false),
        Field::new("episode_id", DataType::Utf8, false),
        Field::new("call_id", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("entity_kind", DataType::Utf8, false),
        Field::new("entity_key", DataType::Utf8, false),
        Field::new("value_hash", DataType::Utf8, true),
        Field::new("value_preview", DataType::Utf8, true),
        Field::new("observed_at_unix_nanos", DataType::Int64, false),
    ]))
}

fn bindings_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("binding_id", DataType::Utf8, false),
        Field::new("episode_id", DataType::Utf8, false),
        Field::new("source_occurrence_id", DataType::Utf8, false),
        Field::new("target_occurrence_id", DataType::Utf8, false),
        Field::new("source_call_id", DataType::Utf8, false),
        Field::new("target_call_id", DataType::Utf8, false),
        Field::new("entity_key", DataType::Utf8, false),
        Field::new("evidence_kind", DataType::Utf8, false),
        Field::new("observed_at_unix_nanos", DataType::Int64, false),
    ]))
}

fn string_array<I, S>(values: I) -> ArrayRef
where
    I: IntoIterator<Item = Option<S>>,
    S: AsRef<str>,
{
    Arc::new(StringArray::from(
        values
            .into_iter()
            .map(|value| value.map(|value| value.as_ref().to_string()))
            .collect::<Vec<_>>(),
    )) as ArrayRef
}

fn json_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

fn collect_json_occurrences(
    value: &Value,
    path: &str,
    role: OccurrenceRole,
    occurrences: &mut Vec<OccurrenceDraft>,
) {
    match value {
        Value::Null => {}
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            occurrences.push(value_occurrence(role, path.to_string(), value));
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                collect_json_occurrences(value, &format!("{path}[{index}]"), role, occurrences);
            }
        }
        Value::Object(map) => {
            for (key, value) in map {
                collect_json_occurrences(value, &format!("{path}.{key}"), role, occurrences);
            }
        }
    }
}

fn value_occurrence(
    role: OccurrenceRole,
    path: impl Into<String>,
    value: &Value,
) -> OccurrenceDraft {
    let canonical = canonical_scalar(value);
    let value_hash = canonical.as_ref().map(|value| hash_hex(value.as_bytes()));
    let entity_kind = scalar_entity_kind(value);
    let entity_key = value_hash.as_ref().map_or_else(
        || "value:null".to_string(),
        |hash| format!("{entity_kind}:{hash}"),
    );
    OccurrenceDraft {
        role,
        path: path.into(),
        entity_kind: entity_kind.to_string(),
        entity_key,
        value_hash,
        value_preview: scalar_preview(value),
    }
}

fn canonical_scalar(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::Bool(_) | Value::Number(_) | Value::String(_) => serde_json::to_string(value).ok(),
    }
}

fn scalar_entity_kind(value: &Value) -> &'static str {
    match value {
        Value::String(value) if value.starts_with("http://") || value.starts_with("https://") => {
            "url"
        }
        Value::String(_) | Value::Null | Value::Array(_) | Value::Object(_) => "value",
        Value::Number(_) => "number",
        Value::Bool(_) => "bool",
    }
}

fn scalar_preview(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::String(value) => Some(truncate_chars(value, MAX_PREVIEW_CHARS)),
        Value::Bool(_) | Value::Number(_) => Some(value.to_string()),
    }
}

fn sql_string_literals(sql: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\'' {
            continue;
        }
        let mut literal = String::new();
        while let Some(next) = chars.next() {
            if next == '\'' {
                if chars.peek() == Some(&'\'') {
                    literal.push('\'');
                    chars.next();
                    continue;
                }
                break;
            }
            literal.push(next);
        }
        literals.push(literal);
    }
    literals
}

fn sql_table_refs(sql: &str) -> Vec<String> {
    static TABLE_REF: OnceLock<Regex> = OnceLock::new();
    let regex = TABLE_REF.get_or_init(|| {
        Regex::new(r"(?i)\b(?:from|join)\s+([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)")
            .expect("table ref regex should compile")
    });
    regex
        .captures_iter(sql)
        .filter_map(|captures| captures.get(1).map(|value| value.as_str().to_string()))
        .collect()
}

fn entity_kind(entity_key: &str) -> &str {
    entity_key
        .split_once(':')
        .map_or("entity", |(kind, _)| kind)
}

fn stable_id(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    format!("sha256:{}", hex_encode(&hasher.finalize()))
}

fn hash_hex(bytes: &[u8]) -> String {
    hex_encode(&Sha256::digest(bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from_digit(u32::from(byte >> 4), 16).expect("hex digit"));
        encoded.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("hex digit"));
    }
    encoded
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn unix_nanos_now() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn records_nearest_earlier_binding_for_matching_value() {
        let dir = tempfile::tempdir().expect("temp dir");
        let recorder = ProvenanceRecorder::new(dir.path().join("events.jsonl"));
        let span = tracing::info_span!("test");
        let timing = CallTiming::start_now();
        recorder.record_call(
            &span,
            ProvenanceCall {
                workspace: "default".to_string(),
                operation: "catalog.list_columns".to_string(),
                input_json: json!({"schema": "github", "table": "pulls"}),
                output_summary_json: json!({"column_count": 1}),
                status: "ok".to_string(),
                row_count: Some(1),
                input_occurrences: Vec::new(),
                output_occurrences: vec![value_output_occurrence(
                    "columns[0].example",
                    "https://github.com/withcoral/coral/pull/1197",
                )],
                timing,
            },
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
        recorder.record_call(
            &span,
            ProvenanceCall {
                workspace: "default".to_string(),
                operation: "query.execute_sql".to_string(),
                input_json: json!({"sql": "select * from linear.attachments where url = 'https://github.com/withcoral/coral/pull/1197'"}),
                output_summary_json: json!({"row_count": 1}),
                status: "ok".to_string(),
                row_count: Some(1),
                input_occurrences: sql_input_occurrences(
                    "select * from linear.attachments where url = 'https://github.com/withcoral/coral/pull/1197'",
                ),
                output_occurrences: Vec::new(),
                timing: CallTiming::start_now(),
            },
        );

        let records = read_records(&dir.path().join("events.jsonl"));
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, StoredRecord::Binding(_)))
                .count(),
            1
        );
    }

    #[test]
    fn sql_input_occurrences_extracts_literals_and_table_refs() {
        let occurrences = sql_input_occurrences(
            "select * from github.pulls join linear.attachments on url = html_url where html_url = 'https://example.com/pr/1'",
        );
        assert!(
            occurrences
                .iter()
                .any(|occurrence| occurrence.entity_key.starts_with("url:"))
        );
        assert!(
            occurrences
                .iter()
                .any(|occurrence| occurrence.entity_key == "table:github.pulls")
        );
        assert!(
            occurrences
                .iter()
                .any(|occurrence| occurrence.entity_key == "table:linear.attachments")
        );
    }
}
