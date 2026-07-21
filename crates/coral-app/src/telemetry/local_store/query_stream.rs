//! Query Stream LIST projection over locally captured JSONL spans.

use std::collections::{BTreeMap, HashMap};

use coral_telemetry::{
    QUERY_STREAM_ENTRY_ATTRIBUTE, QUERY_STREAM_KIND_ATTRIBUTE, QUERY_STREAM_KIND_QUERY,
    QUERY_STREAM_KIND_SEARCH, QUERY_STREAM_KIND_TOOL, QUERY_STREAM_NAME_ATTRIBUTE,
};
use serde_json::Value as JsonValue;

use super::{
    StoredTraceOperationKind, StoredTraceStatus, TraceListSpanRecord, TraceStore, TraceStoreError,
    TraceStoreFile, TraceSummaryRecord, attr_bool, attr_string, attr_u64, parse_attributes,
    read_list_spans_file, status_from_attributes, usize_to_u32,
};
use crate::telemetry::WORKSPACE_SPAN_ATTRIBUTE;

const MCP_METHOD_ATTRIBUTE: &str = "mcp.method";
const MCP_TOOL_NAME_ATTRIBUTE: &str = "mcp.tool.name";
const MCP_CALL_TOOL_METHOD: &str = "tools/call";
const UNKNOWN_TOOL_OPERATION_NAME: &str = "unknown_tool";
const MAX_LEGACY_TOOL_OPERATION_NAME_LEN: usize = 128;

pub(super) fn list(
    store: &TraceStore,
    limit: usize,
    offset: usize,
    workspace_name: Option<&str>,
) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    store.prune_expired()?;
    let files = store.jsonl_files_by_modified()?;
    let required_entry_count = offset.saturating_add(limit);
    let mut projector = QueryStreamProjector::new(required_entry_count, workspace_name);
    let mut scanned_all_files = true;
    let mut bucket_end = files.len();
    while bucket_end > 0 {
        let Some(newest_bucket_file) = files.get(bucket_end - 1) else {
            break;
        };
        let bucket = equal_mtime_file_bucket(&files, newest_bucket_file.modified_unix_nanos);
        let Some(bucket_files) = files.get(bucket.clone()) else {
            break;
        };
        let mut spans = Vec::new();
        for file in bucket_files {
            // Files are read in ascending path order so that the last record
            // retains the existing highest-path precedence for duplicate span
            // IDs while the projector can resolve the entire bucket at once.
            spans.extend(read_list_spans_file(&file.path)?);
        }
        projector.record_file(spans);

        let Some(newest_unscanned_file) = bucket
            .start
            .checked_sub(1)
            .and_then(|index| files.get(index))
        else {
            break;
        };
        let watermark = newest_unscanned_file.span_end_upper_bound_unix_nanos;
        projector.advance_watermark(watermark);
        if projector.page_is_newer_than(watermark) {
            scanned_all_files = false;
            break;
        }
        bucket_end = bucket.start;
    }

    if scanned_all_files {
        projector.finish();
    }
    Ok(projector.into_page(offset, limit))
}

fn equal_mtime_file_bucket(
    files: &[TraceStoreFile],
    modified_unix_nanos: i64,
) -> std::ops::Range<usize> {
    let start = files.partition_point(|file| file.modified_unix_nanos < modified_unix_nanos);
    let end = files.partition_point(|file| file.modified_unix_nanos <= modified_unix_nanos);
    start..end
}

// Shared operation semantics used by the LIST projector.

trait QueryStreamSpan {
    fn name(&self) -> &str;
}

impl QueryStreamSpan for TraceListSpanRecord {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryStreamMetadata {
    explicit: bool,
    kind: StoredTraceOperationKind,
    name: String,
}

fn query_stream_metadata<T>(span: &T, attributes: Option<&JsonValue>) -> Option<QueryStreamMetadata>
where
    T: QueryStreamSpan,
{
    match attributes.and_then(|attributes| attr_bool(attributes, QUERY_STREAM_ENTRY_ATTRIBUTE)) {
        Some(false) => None,
        Some(true) => {
            let kind_name = attributes
                .and_then(|attributes| attr_string(attributes, QUERY_STREAM_KIND_ATTRIBUTE));
            let kind = operation_kind_from_name(kind_name.as_deref());
            let name = attributes
                .and_then(|attributes| attr_string(attributes, QUERY_STREAM_NAME_ATTRIBUTE))
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| fallback_operation_name(span, attributes, kind));
            Some(QueryStreamMetadata {
                explicit: true,
                kind,
                name,
            })
        }
        None => legacy_query_stream_metadata(span, attributes),
    }
}

fn legacy_query_stream_metadata<T>(
    span: &T,
    attributes: Option<&JsonValue>,
) -> Option<QueryStreamMetadata>
where
    T: QueryStreamSpan,
{
    if let Some(tool_name) = attributes
        .filter(|attributes| {
            attr_string(attributes, MCP_METHOD_ATTRIBUTE).as_deref() == Some(MCP_CALL_TOOL_METHOD)
        })
        .and_then(|attributes| attr_string(attributes, MCP_TOOL_NAME_ATTRIBUTE))
        .filter(|tool_name| !tool_name.trim().is_empty())
    {
        return Some(QueryStreamMetadata {
            explicit: true,
            kind: StoredTraceOperationKind::Tool,
            name: privacy_safe_legacy_tool_operation_name(tool_name),
        });
    }
    let (kind, default_name) = match span.name() {
        "coral.query" => (StoredTraceOperationKind::Query, "sql"),
        "coral.search" => (StoredTraceOperationKind::Search, "search"),
        _ => return None,
    };
    let name = attributes
        .and_then(|attributes| attr_string(attributes, "operation"))
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| default_name.to_string());
    Some(QueryStreamMetadata {
        explicit: false,
        kind,
        name,
    })
}

fn operation_kind_from_name(kind: Option<&str>) -> StoredTraceOperationKind {
    match kind {
        Some(QUERY_STREAM_KIND_QUERY) => StoredTraceOperationKind::Query,
        Some(QUERY_STREAM_KIND_SEARCH) => StoredTraceOperationKind::Search,
        Some(QUERY_STREAM_KIND_TOOL) => StoredTraceOperationKind::Tool,
        _ => StoredTraceOperationKind::Other,
    }
}

fn privacy_safe_legacy_tool_operation_name(tool_name: String) -> String {
    let has_identifier_shape = !tool_name.is_empty()
        && tool_name.len() <= MAX_LEGACY_TOOL_OPERATION_NAME_LEN
        && tool_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
    if has_identifier_shape {
        tool_name
    } else {
        UNKNOWN_TOOL_OPERATION_NAME.to_string()
    }
}

fn fallback_operation_name<T>(
    span: &T,
    attributes: Option<&JsonValue>,
    kind: StoredTraceOperationKind,
) -> String
where
    T: QueryStreamSpan,
{
    match kind {
        StoredTraceOperationKind::Query => attributes
            .and_then(|attributes| attr_string(attributes, "operation"))
            .unwrap_or_else(|| "sql".to_string()),
        StoredTraceOperationKind::Search => "search".to_string(),
        StoredTraceOperationKind::Tool => attributes
            .and_then(|attributes| attr_string(attributes, MCP_TOOL_NAME_ATTRIBUTE))
            .unwrap_or_else(|| span.name().to_string()),
        StoredTraceOperationKind::Other | StoredTraceOperationKind::Unspecified => {
            span.name().to_string()
        }
    }
}

fn is_unmarked_mcp_protocol_attributes(attributes: &JsonValue) -> bool {
    let is_marked = attr_bool(attributes, QUERY_STREAM_ENTRY_ATTRIBUTE).unwrap_or(false);
    !is_marked
        && attributes.get(MCP_METHOD_ATTRIBUTE).is_some()
        && attributes.get(MCP_TOOL_NAME_ATTRIBUTE).is_none()
}

fn query_enrichment_is_semantic(
    entry_kind: StoredTraceOperationKind,
    primary_descendant: Option<&QueryStreamPrimaryOperation>,
) -> bool {
    entry_kind == StoredTraceOperationKind::Query
        || (entry_kind == StoredTraceOperationKind::Tool
            && primary_descendant
                .is_some_and(|operation| operation.kind == StoredTraceOperationKind::Query))
}

// LIST projection. This section owns bounded JSONL scanning, completion
// watermarks, pagination, and streaming aggregation.

fn sort_query_stream_summaries(summaries: &mut [TraceSummaryRecord]) {
    summaries.sort_by(|left, right| {
        right
            .end_time_unix_nanos
            .cmp(&left.end_time_unix_nanos)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
            .then_with(|| left.root_span_id.cmp(&right.root_span_id))
    });
}

type QueryStreamSpanKey = (String, String);

#[derive(Debug)]
struct ProjectedQueryStreamSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    parent_span_is_remote: bool,
    name: String,
    status: StoredTraceStatus,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    metadata: Option<QueryStreamMetadata>,
    protocol: bool,
    workspace: Option<String>,
    sql: Option<String>,
    row_count: Option<u64>,
}

impl ProjectedQueryStreamSpan {
    fn from_record(span: TraceListSpanRecord) -> Self {
        let attributes = parse_attributes(&span.attributes_json);
        let metadata = query_stream_metadata(&span, attributes.as_ref());
        let protocol = attributes
            .as_ref()
            .is_some_and(is_unmarked_mcp_protocol_attributes);
        let status = status_from_attributes(attributes.as_ref()).unwrap_or(span.status);
        let workspace = attributes
            .as_ref()
            .and_then(|attributes| attr_string(attributes, WORKSPACE_SPAN_ATTRIBUTE));
        let sql = attributes
            .as_ref()
            .and_then(|attributes| attr_string(attributes, "sql"));
        let row_count = attributes
            .as_ref()
            .and_then(|attributes| attr_u64(attributes, "row_count"));
        Self {
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            parent_span_is_remote: span.parent_span_is_remote,
            name: span.name,
            status,
            start_time_unix_nanos: span.start_time_unix_nanos,
            end_time_unix_nanos: span.end_time_unix_nanos,
            metadata,
            protocol,
            workspace,
            sql,
            row_count,
        }
    }

    fn key(&self) -> QueryStreamSpanKey {
        (self.trace_id.clone(), self.span_id.clone())
    }

    fn parent_key(&self) -> Option<QueryStreamSpanKey> {
        self.parent_span_id
            .as_ref()
            .map(|parent_span_id| (self.trace_id.clone(), parent_span_id.clone()))
    }
}

#[derive(Debug, Clone, Copy)]
struct QueryStreamNodeState {
    start_time_unix_nanos: i64,
    owner: Option<u64>,
    owner_depth: Option<usize>,
    suppresses_entries: bool,
}

#[derive(Debug)]
struct QueryStreamEntrySnapshot {
    trace_id: String,
    span_id: String,
    name: String,
    status: StoredTraceStatus,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    metadata: QueryStreamMetadata,
    workspace: Option<String>,
    sql: Option<String>,
    row_count: Option<u64>,
}

impl QueryStreamEntrySnapshot {
    fn from_span(span: &ProjectedQueryStreamSpan, metadata: QueryStreamMetadata) -> Self {
        Self {
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            name: span.name.clone(),
            status: span.status,
            start_time_unix_nanos: span.start_time_unix_nanos,
            end_time_unix_nanos: span.end_time_unix_nanos,
            metadata,
            workspace: span.workspace.clone(),
            sql: span.sql.clone(),
            row_count: span.row_count,
        }
    }
}

#[derive(Debug)]
struct QueryStreamQueryEnrichment {
    depth: usize,
    start_time_unix_nanos: i64,
    span_id: String,
    sql: String,
    row_count: Option<u64>,
}

impl QueryStreamQueryEnrichment {
    fn sort_key(&self) -> (usize, i64, &str) {
        (self.depth, self.start_time_unix_nanos, &self.span_id)
    }
}

#[derive(Debug)]
struct QueryStreamPrimaryOperation {
    kind: StoredTraceOperationKind,
    depth: usize,
    start_time_unix_nanos: i64,
    span_id: String,
}

impl QueryStreamPrimaryOperation {
    fn new(
        kind: StoredTraceOperationKind,
        depth: usize,
        start_time_unix_nanos: i64,
        span_id: &str,
    ) -> Self {
        Self {
            kind,
            depth,
            start_time_unix_nanos,
            span_id: span_id.to_string(),
        }
    }

    fn sort_key(&self) -> (usize, i64, &str) {
        (self.depth, self.start_time_unix_nanos, &self.span_id)
    }
}

#[derive(Debug, Default)]
enum QueryStreamWorkspaceEvidence {
    #[default]
    None,
    One(String),
    Conflict,
}

impl QueryStreamWorkspaceEvidence {
    fn record(&mut self, workspace: Option<&str>) {
        let Some(workspace) = workspace.filter(|workspace| !workspace.trim().is_empty()) else {
            return;
        };
        match self {
            Self::None => *self = Self::One(workspace.to_string()),
            Self::One(current) if current != workspace => *self = Self::Conflict,
            Self::One(_) | Self::Conflict => {}
        }
    }

    fn unique(&self) -> Option<&str> {
        match self {
            Self::One(workspace) => Some(workspace),
            Self::None | Self::Conflict => None,
        }
    }
}

#[derive(Debug)]
struct StreamingQueryStreamAggregate {
    entry: QueryStreamEntrySnapshot,
    span_count: usize,
    primary_descendant_operation: Option<QueryStreamPrimaryOperation>,
    query_enrichment: Option<QueryStreamQueryEnrichment>,
    workspace_evidence: QueryStreamWorkspaceEvidence,
}

impl StreamingQueryStreamAggregate {
    fn new(entry: QueryStreamEntrySnapshot) -> Self {
        Self {
            entry,
            span_count: 0,
            primary_descendant_operation: None,
            query_enrichment: None,
            workspace_evidence: QueryStreamWorkspaceEvidence::default(),
        }
    }

    fn record_span(&mut self, span: &ProjectedQueryStreamSpan, depth: usize) {
        self.span_count = self.span_count.saturating_add(1);
        self.workspace_evidence.record(span.workspace.as_deref());
        let Some(metadata) = span.metadata.as_ref() else {
            return;
        };
        if depth > 0 {
            let operation = QueryStreamPrimaryOperation::new(
                metadata.kind,
                depth,
                span.start_time_unix_nanos,
                &span.span_id,
            );
            if self
                .primary_descendant_operation
                .as_ref()
                .is_none_or(|current| operation.sort_key() < current.sort_key())
            {
                self.primary_descendant_operation = Some(operation);
            }
        }
        if metadata.kind != StoredTraceOperationKind::Query {
            return;
        }
        let Some(sql) = span.sql.as_deref().filter(|sql| !sql.trim().is_empty()) else {
            return;
        };
        let enrichment = QueryStreamQueryEnrichment {
            depth,
            start_time_unix_nanos: span.start_time_unix_nanos,
            span_id: span.span_id.clone(),
            sql: sql.to_string(),
            row_count: span.row_count,
        };
        if self
            .query_enrichment
            .as_ref()
            .is_none_or(|current| enrichment.sort_key() < current.sort_key())
        {
            self.query_enrichment = Some(enrichment);
        }
    }

    fn workspace(&self) -> Option<&str> {
        self.entry
            .workspace
            .as_deref()
            .or_else(|| self.workspace_evidence.unique())
    }

    fn into_summary(self) -> TraceSummaryRecord {
        let query_enrichment = query_enrichment_is_semantic(
            self.entry.metadata.kind,
            self.primary_descendant_operation.as_ref(),
        )
        .then_some(self.query_enrichment.as_ref())
        .flatten();
        let query = self
            .entry
            .sql
            .or_else(|| query_enrichment.map(|enrichment| enrichment.sql.clone()));
        let row_count = self
            .entry
            .row_count
            .or_else(|| query_enrichment.and_then(|enrichment| enrichment.row_count));
        TraceSummaryRecord {
            trace_id: self.entry.trace_id,
            root_span_id: self.entry.span_id,
            name: self.entry.name,
            query: query.unwrap_or_default(),
            status: self.entry.status,
            start_time_unix_nanos: self.entry.start_time_unix_nanos,
            end_time_unix_nanos: self.entry.end_time_unix_nanos,
            duration_nanos: self
                .entry
                .end_time_unix_nanos
                .saturating_sub(self.entry.start_time_unix_nanos),
            span_count: usize_to_u32(self.span_count),
            row_count: row_count.unwrap_or_default(),
            row_count_recorded: row_count.is_some(),
            operation_kind: self.entry.metadata.kind,
            operation_name: self.entry.metadata.name,
        }
    }
}

/// Projects completed spans while scanning files from newest to oldest.
///
/// The local exporter writes a span only after it ends, so a parent is written
/// no earlier than its descendants. File mtimes plus the store tolerance form
/// the same conservative span-end watermark used by the normal trace list. Once
/// that watermark moves before a span's start, no older file can contain one of
/// its descendants (or a valid duplicate), and its compact ancestry state can
/// be discarded. A completed child with an unresolved local parent remains
/// hidden until that owning ancestry is exported, avoiding an incomplete row
/// that would later need to be replaced by its outer operation.
struct QueryStreamProjector {
    required_entry_count: usize,
    workspace_name: Option<String>,
    nodes: HashMap<QueryStreamSpanKey, QueryStreamNodeState>,
    node_starts: BTreeMap<i64, Vec<QueryStreamSpanKey>>,
    aggregates: HashMap<u64, StreamingQueryStreamAggregate>,
    aggregate_starts: BTreeMap<i64, Vec<u64>>,
    finalized: Vec<TraceSummaryRecord>,
    next_operation_id: u64,
}

impl QueryStreamProjector {
    fn new(required_entry_count: usize, workspace_name: Option<&str>) -> Self {
        Self {
            required_entry_count,
            workspace_name: workspace_name.map(str::to_string),
            nodes: HashMap::new(),
            node_starts: BTreeMap::new(),
            aggregates: HashMap::new(),
            aggregate_starts: BTreeMap::new(),
            finalized: Vec::new(),
            next_operation_id: 0,
        }
    }

    fn record_file(&mut self, spans: Vec<TraceListSpanRecord>) {
        let mut pending = spans
            .into_iter()
            .map(ProjectedQueryStreamSpan::from_record)
            .map(|span| (span.key(), span))
            .collect::<HashMap<_, _>>();

        while let Some(start_key) = pending.keys().next().cloned() {
            if self.nodes.contains_key(&start_key) {
                pending.remove(&start_key);
                continue;
            }

            let mut chain = Vec::new();
            let mut current_key = start_key;
            loop {
                if self.nodes.contains_key(&current_key) {
                    break;
                }
                let Some(span) = pending.remove(&current_key) else {
                    break;
                };
                let parent_key = span.parent_key();
                chain.push(span);
                let Some(parent_key) = parent_key else {
                    break;
                };
                if self.nodes.contains_key(&parent_key) || !pending.contains_key(&parent_key) {
                    break;
                }
                current_key = parent_key;
            }

            for span in chain.into_iter().rev() {
                self.record_span(&span);
            }
        }
    }

    fn record_span(&mut self, span: &ProjectedQueryStreamSpan) {
        let key = span.key();
        if self.nodes.contains_key(&key) {
            return;
        }
        let parent = span
            .parent_key()
            .as_ref()
            .and_then(|parent_key| self.nodes.get(parent_key))
            .copied();
        let unresolved_local_parent =
            span.parent_span_id.is_some() && !span.parent_span_is_remote && parent.is_none();
        let ancestor_suppresses_entries = parent.is_some_and(|parent| parent.suppresses_entries);
        let visible_metadata = (!ancestor_suppresses_entries && !unresolved_local_parent)
            .then(|| span.metadata.clone())
            .flatten();
        let (owner, owner_depth) = if let Some(metadata) = visible_metadata {
            let operation_id = self.next_operation_id;
            self.next_operation_id = self.next_operation_id.saturating_add(1);
            self.aggregate_starts
                .entry(span.start_time_unix_nanos)
                .or_default()
                .push(operation_id);
            self.aggregates.insert(
                operation_id,
                StreamingQueryStreamAggregate::new(QueryStreamEntrySnapshot::from_span(
                    span, metadata,
                )),
            );
            (Some(operation_id), Some(0))
        } else {
            (
                parent.and_then(|parent| parent.owner),
                parent
                    .and_then(|parent| parent.owner_depth)
                    .map(|depth| depth.saturating_add(1)),
            )
        };
        let suppresses_entries = ancestor_suppresses_entries
            || unresolved_local_parent
            || span.protocol
            || span
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.explicit);
        let node = QueryStreamNodeState {
            start_time_unix_nanos: span.start_time_unix_nanos,
            owner,
            owner_depth,
            suppresses_entries,
        };
        self.node_starts
            .entry(node.start_time_unix_nanos)
            .or_default()
            .push(key.clone());
        self.nodes.insert(key, node);
        if let Some((owner, depth)) = owner.zip(owner_depth)
            && let Some(aggregate) = self.aggregates.get_mut(&owner)
        {
            aggregate.record_span(span, depth);
        }
    }

    fn advance_watermark(&mut self, watermark: i64) {
        for operation_id in take_indexed_values_after(&mut self.aggregate_starts, watermark) {
            self.finalize_operation(operation_id);
        }
        for key in take_indexed_values_after(&mut self.node_starts, watermark) {
            self.nodes.remove(&key);
        }
        self.trim_finalized();
    }

    fn finish(&mut self) {
        let operation_ids = self.aggregates.keys().copied().collect::<Vec<_>>();
        for operation_id in operation_ids {
            self.finalize_operation(operation_id);
        }
        self.aggregate_starts.clear();
        self.nodes.clear();
        self.node_starts.clear();
        self.trim_finalized();
    }

    fn finalize_operation(&mut self, operation_id: u64) {
        let Some(aggregate) = self.aggregates.remove(&operation_id) else {
            return;
        };
        if self
            .workspace_name
            .as_deref()
            .is_none_or(|workspace_name| aggregate.workspace() == Some(workspace_name))
        {
            self.finalized.push(aggregate.into_summary());
        }
    }

    fn trim_finalized(&mut self) {
        sort_query_stream_summaries(&mut self.finalized);
        self.finalized.truncate(self.required_entry_count);
    }

    fn page_is_newer_than(&self, watermark: i64) -> bool {
        let Some(boundary) = self
            .finalized
            .get(self.required_entry_count.saturating_sub(1))
        else {
            return false;
        };
        boundary.end_time_unix_nanos > watermark
            && self
                .aggregates
                .values()
                .all(|aggregate| aggregate.entry.end_time_unix_nanos < boundary.end_time_unix_nanos)
    }

    fn into_page(mut self, offset: usize, limit: usize) -> Vec<TraceSummaryRecord> {
        sort_query_stream_summaries(&mut self.finalized);
        self.finalized
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect()
    }
}

fn take_indexed_values_after<T>(index: &mut BTreeMap<i64, Vec<T>>, watermark: i64) -> Vec<T> {
    let Some(split_at) = watermark.checked_add(1) else {
        return Vec::new();
    };
    index.split_off(&split_at).into_values().flatten().collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::time::{Duration, SystemTime};

    use serde_json::json;
    use tempfile::TempDir;

    use super::super::tests::{
        set_modified_time, timestamped_jsonl_path, trace_record, write_record_file,
        write_record_file_lines,
    };
    use super::super::{
        StoredTraceOperationKind, StoredTraceStatus, TraceListSpanRecord, TraceStore, unix_nanos,
    };
    use super::{
        MAX_LEGACY_TOOL_OPERATION_NAME_LEN, QueryStreamProjector, UNKNOWN_TOOL_OPERATION_NAME,
        privacy_safe_legacy_tool_operation_name,
    };

    // Shared classification and ownership behavior.

    #[test]
    fn query_stream_projects_outer_operations() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut sql_tool = trace_record("shared-trace", "sql-tool");
        sql_tool.parent_span_id = Some("remote-parent".to_string());
        sql_tool.parent_span_is_remote = true;
        sql_tool.name = "coral.mcp.call_tool".to_string();
        sql_tool.status = StoredTraceStatus::Error;
        sql_tool.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "tool",
            "coral.stream.name": "sql",
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
            "workspace": "alpha",
            "status": "error",
        })
        .to_string();
        sql_tool.start_time_unix_nanos = 10;
        sql_tool.end_time_unix_nanos = 40;

        let mut nested_query = trace_record("shared-trace", "nested-query");
        nested_query.parent_span_id = Some(sql_tool.span_id.clone());
        nested_query.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 42",
            "row_count": 7,
            "status": "ok",
        })
        .to_string();
        nested_query.start_time_unix_nanos = 20;
        nested_query.end_time_unix_nanos = 30;

        let mut search_tool = trace_record("shared-trace", "search-tool");
        search_tool.parent_span_id = Some("remote-parent".to_string());
        search_tool.parent_span_is_remote = true;
        search_tool.name = "coral.mcp.call_tool".to_string();
        search_tool.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "tool",
            "coral.stream.name": "search",
            "mcp.method": "tools/call",
            "mcp.tool.name": "search",
            "workspace": "beta",
            "status": "ok",
        })
        .to_string();
        search_tool.start_time_unix_nanos = 50;
        search_tool.end_time_unix_nanos = 60;

        write_record_file_lines(
            &dir.join("spans-shared.jsonl"),
            &[sql_tool, nested_query, search_tool],
        );

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(10, 0, None)
            .expect("list query stream");
        assert_eq!(summaries.len(), 2);
        assert_eq!(
            summaries.first().expect("search summary").root_span_id,
            "search-tool"
        );

        let sql_summary = summaries.get(1).expect("SQL summary");
        assert_eq!(sql_summary.root_span_id, "sql-tool");
        assert_eq!(sql_summary.operation_kind, StoredTraceOperationKind::Tool);
        assert_eq!(sql_summary.operation_name, "sql");
        assert_eq!(sql_summary.query, "SELECT 42");
        assert_eq!(sql_summary.row_count, 7);
        assert!(sql_summary.row_count_recorded);
        assert_eq!(sql_summary.status, StoredTraceStatus::Error);
        assert_eq!(sql_summary.start_time_unix_nanos, 10);
        assert_eq!(sql_summary.end_time_unix_nanos, 40);
        assert_eq!(sql_summary.duration_nanos, 30);
        assert_eq!(sql_summary.span_count, 2);
    }

    #[test]
    fn query_stream_entry_false_overrides_legacy_tool_shape() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut suppressed = trace_record("suppressed-trace", "suppressed-tool");
        suppressed.parent_span_id = Some("remote-parent".to_string());
        suppressed.parent_span_is_remote = true;
        suppressed.name = "coral.mcp.call_tool".to_string();
        suppressed.attributes_json = json!({
            "coral.stream.entry": false,
            "mcp.method": "tools/call",
            "mcp.tool.name": "list_catalog",
            "workspace": "alpha",
        })
        .to_string();
        write_record_file(&dir.join("spans-suppressed.jsonl"), &suppressed);

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(10, 0, Some("alpha"))
            .expect("list query stream");
        assert!(summaries.is_empty());
    }

    #[test]
    fn query_stream_entry_without_kind_is_other() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut entry = trace_record("other-trace", "other-entry");
        entry.parent_span_id = Some("remote-parent".to_string());
        entry.parent_span_is_remote = true;
        entry.name = "coral.vector_lookup".to_string();
        entry.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.name": "semantic_lookup",
            "workspace": "alpha",
        })
        .to_string();
        write_record_file(&dir.join("spans-other.jsonl"), &entry);

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(10, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("other summary");
        assert_eq!(summary.operation_kind, StoredTraceOperationKind::Other);
        assert_eq!(summary.operation_name, "semantic_lookup");
        assert!(summary.query.is_empty());
    }

    #[test]
    fn query_stream_hides_entries_with_unfinished_local_parents() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut unfinished_child = trace_record("unfinished-trace", "unfinished-query");
        unfinished_child.parent_span_id = Some("unfinished-protocol-parent".to_string());
        unfinished_child.parent_span_is_remote = false;
        unfinished_child.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'unfinished'",
        })
        .to_string();

        let mut remote_root = trace_record("remote-root-trace", "remote-query");
        remote_root.parent_span_id = Some("remote-parent".to_string());
        remote_root.parent_span_is_remote = true;
        remote_root.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'remote'",
        })
        .to_string();

        write_record_file_lines(
            &dir.join("spans-unfinished-parent.jsonl"),
            &[unfinished_child, remote_root],
        );

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(10, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("remote summary").root_span_id,
            "remote-query"
        );
    }

    // LIST projection, pagination, and streaming behavior.

    #[test]
    fn query_stream_resolves_equal_mtime_files_as_one_bucket() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let base_time = SystemTime::now() - Duration::from_secs(10);
        let common_modified = base_time + Duration::from_secs(1);

        let mut parent = trace_record("equal-mtime-trace", "local-parent");
        parent.name = "http.request".to_string();
        parent.start_time_unix_nanos = unix_nanos(base_time);
        parent.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(900));

        let mut root = trace_record("equal-mtime-trace", "selected-tool");
        root.parent_span_id = Some(parent.span_id.clone());
        root.name = "coral.mcp.call_tool".to_string();
        root.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "tool",
            "coral.stream.name": "sql",
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
            "workspace": "alpha",
        })
        .to_string();
        root.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(100));
        root.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(800));

        let mut child = trace_record("equal-mtime-trace", "nested-query");
        child.parent_span_id = Some(root.span_id.clone());
        child.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 42",
            "row_count": 1,
        })
        .to_string();
        child.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(200));
        child.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(700));

        for (name, span) in [
            ("spans-a-parent.jsonl", &parent),
            ("spans-b-root.jsonl", &root),
            ("spans-c-child.jsonl", &child),
        ] {
            let path = dir.join(name);
            write_record_file(&path, span);
            set_modified_time(&path, common_modified);
        }

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(10, 0, Some("alpha"))
            .expect("list equal-mtime operation");
        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("equal-mtime summary");
        assert_eq!(summary.root_span_id, "selected-tool");
        assert_eq!(summary.query, "SELECT 42");
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);
        assert_eq!(summary.span_count, 2);
    }

    #[test]
    fn query_stream_projects_many_operations_from_one_distributed_trace() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let mut records = Vec::new();

        for index in 0..128 {
            let tool_span_id = format!("tool-{index:03}");
            let mut tool = trace_record("shared-trace", &tool_span_id);
            tool.parent_span_id = Some("remote-parent".to_string());
            tool.parent_span_is_remote = true;
            tool.name = "coral.mcp.call_tool".to_string();
            tool.attributes_json = json!({
                "coral.stream.entry": true,
                "coral.stream.kind": "tool",
                "coral.stream.name": "sql",
                "mcp.method": "tools/call",
                "mcp.tool.name": "sql",
                "workspace": "alpha",
            })
            .to_string();
            tool.start_time_unix_nanos = index * 10;
            tool.end_time_unix_nanos = index * 10 + 9;

            let mut query = trace_record("shared-trace", &format!("query-{index:03}"));
            query.parent_span_id = Some(tool_span_id);
            query.attributes_json = json!({
                "coral.stream.entry": true,
                "coral.stream.kind": "query",
                "coral.stream.name": "sql",
                "workspace": "alpha",
                "sql": format!("SELECT {index}"),
                "row_count": index,
            })
            .to_string();
            query.start_time_unix_nanos = index * 10 + 1;
            query.end_time_unix_nanos = index * 10 + 8;
            records.extend([tool, query]);
        }
        write_record_file_lines(&dir.join("spans-many-entries.jsonl"), &records);

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(200, 0, Some("alpha"))
            .expect("project many entries");
        assert_eq!(summaries.len(), 128);
        assert!(summaries.iter().all(|summary| {
            summary.operation_kind == StoredTraceOperationKind::Tool
                && summary.span_count == 2
                && !summary.query.is_empty()
        }));
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.root_span_id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            128
        );
    }

    #[test]
    fn query_stream_suppresses_protocol_descendants_without_method_enumeration() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut protocol = trace_record("future-trace", "protocol");
        protocol.name = "coral.mcp.future_protocol".to_string();
        protocol.attributes_json = r#"{"mcp.method":"future/negotiate"}"#.to_string();

        let mut protocol_child = trace_record("future-trace", "protocol-child");
        protocol_child.parent_span_id = Some(protocol.span_id.clone());
        protocol_child.name = "coral.vector_lookup".to_string();
        protocol_child.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "vector_lookup",
            "coral.stream.name": "semantic_lookup",
            "workspace": "alpha",
        })
        .to_string();

        let mut direct_future = trace_record("future-trace", "direct-future");
        direct_future.parent_span_id = Some("remote-parent".to_string());
        direct_future.parent_span_is_remote = true;
        direct_future.name = "coral.vector_lookup".to_string();
        direct_future.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "vector_lookup",
            "coral.stream.name": "semantic_lookup",
            "workspace": "alpha",
        })
        .to_string();
        direct_future.start_time_unix_nanos = 10;
        direct_future.end_time_unix_nanos = 11;

        write_record_file_lines(
            &dir.join("spans-future.jsonl"),
            &[protocol, protocol_child, direct_future],
        );

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(10, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("future summary");
        assert_eq!(summary.root_span_id, "direct-future");
        assert_eq!(summary.operation_kind, StoredTraceOperationKind::Other);
        assert_eq!(summary.operation_name, "semantic_lookup");
    }

    #[test]
    fn query_stream_supports_legacy_operations_and_protocol_suppression() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut protocol = trace_record("legacy-trace", "protocol");
        protocol.name = "coral.mcp.protocol".to_string();
        protocol.attributes_json = r#"{"mcp.method":"some/future_method"}"#.to_string();

        let mut hidden_query = trace_record("legacy-trace", "hidden-query");
        hidden_query.parent_span_id = Some(protocol.span_id.clone());
        hidden_query.attributes_json = r#"{"workspace":"alpha","sql":"LIST CATALOG"}"#.to_string();

        let mut legacy_tool = trace_record("legacy-trace", "legacy-tool");
        legacy_tool.name = "coral.mcp.call_tool".to_string();
        legacy_tool.attributes_json =
            r#"{"mcp.method":"tools/call","mcp.tool.name":"list_catalog"}"#.to_string();
        legacy_tool.start_time_unix_nanos = 10;
        legacy_tool.end_time_unix_nanos = 13;

        let mut visible_query = trace_record("legacy-trace", "visible-query");
        visible_query.parent_span_id = Some(legacy_tool.span_id.clone());
        visible_query.attributes_json =
            r#"{"workspace":"alpha","operation":"list_catalog","sql":"LIST CATALOG"}"#.to_string();
        visible_query.start_time_unix_nanos = 11;
        visible_query.end_time_unix_nanos = 12;

        let mut search_tool = trace_record("search-trace", "legacy-search-tool");
        search_tool.name = "coral.mcp.call_tool".to_string();
        search_tool.attributes_json =
            r#"{"mcp.method":"tools/call","mcp.tool.name":"search"}"#.to_string();
        search_tool.start_time_unix_nanos = 20;
        search_tool.end_time_unix_nanos = 25;

        let mut search = trace_record("search-trace", "legacy-search");
        search.parent_span_id = Some(search_tool.span_id.clone());
        search.name = "coral.search".to_string();
        search.attributes_json = r#"{"workspace":"alpha","operation":"search"}"#.to_string();
        search.start_time_unix_nanos = 21;
        search.end_time_unix_nanos = 24;

        let mut search_catalog_query = trace_record("search-trace", "legacy-search-catalog-query");
        search_catalog_query.parent_span_id = Some(search.span_id.clone());
        search_catalog_query.attributes_json =
            r#"{"workspace":"alpha","operation":"list_catalog","sql":"LIST CATALOG"}"#.to_string();
        search_catalog_query.start_time_unix_nanos = 22;
        search_catalog_query.end_time_unix_nanos = 23;

        write_record_file_lines(
            &dir.join("spans-legacy.jsonl"),
            &[
                protocol,
                hidden_query,
                legacy_tool,
                visible_query,
                search_tool,
                search,
                search_catalog_query,
            ],
        );

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(10, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 2);
        let search_summary = summaries.first().expect("legacy search summary");
        assert_eq!(search_summary.root_span_id, "legacy-search-tool");
        assert_eq!(
            search_summary.operation_kind,
            StoredTraceOperationKind::Tool
        );
        assert_eq!(search_summary.operation_name, "search");
        assert!(search_summary.query.is_empty());
        let tool_summary = summaries.get(1).expect("legacy tool summary");
        assert_eq!(tool_summary.root_span_id, "legacy-tool");
        assert_eq!(tool_summary.operation_kind, StoredTraceOperationKind::Tool);
        assert_eq!(tool_summary.operation_name, "list_catalog");
        assert_eq!(tool_summary.query, "LIST CATALOG");
    }

    #[test]
    fn query_stream_legacy_tool_requires_consistent_descendant_workspace() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut tool = trace_record("legacy-conflict", "legacy-tool");
        tool.name = "coral.mcp.call_tool".to_string();
        tool.attributes_json =
            r#"{"mcp.method":"tools/call","mcp.tool.name":"future_tool"}"#.to_string();
        tool.start_time_unix_nanos = 10;
        tool.end_time_unix_nanos = 30;

        let mut alpha = trace_record("legacy-conflict", "alpha-child");
        alpha.parent_span_id = Some(tool.span_id.clone());
        alpha.name = "internal.alpha".to_string();
        alpha.attributes_json = r#"{"workspace":"alpha"}"#.to_string();
        alpha.start_time_unix_nanos = 15;
        alpha.end_time_unix_nanos = 16;

        let mut beta = trace_record("legacy-conflict", "beta-child");
        beta.parent_span_id = Some(tool.span_id.clone());
        beta.name = "internal.beta".to_string();
        beta.attributes_json = r#"{"workspace":"beta"}"#.to_string();
        beta.start_time_unix_nanos = 20;
        beta.end_time_unix_nanos = 21;

        write_record_file_lines(&dir.join("spans-conflict.jsonl"), &[tool, alpha, beta]);
        let store = TraceStore::new(dir);
        assert!(
            store
                .list_query_stream_sync(10, 0, Some("alpha"))
                .expect("list alpha entries")
                .is_empty()
        );
        let global = store
            .list_query_stream_sync(10, 0, None)
            .expect("list query stream");
        assert_eq!(global.len(), 1);
        assert_eq!(
            global.first().expect("legacy tool").operation_name,
            "future_tool"
        );
    }

    #[test]
    fn query_stream_redacts_prose_shaped_legacy_tool_name() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let sentinel = "SENSITIVE prose-shaped legacy tool";

        let mut tool = trace_record("legacy-private-trace", "legacy-private-tool");
        tool.name = "coral.mcp.call_tool".to_string();
        tool.attributes_json = json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": sentinel,
        })
        .to_string();
        write_record_file(&dir.join("spans-legacy-private.jsonl"), &tool);

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(10, 0, None)
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("legacy tool summary");
        assert_eq!(summary.operation_kind, StoredTraceOperationKind::Tool);
        assert_eq!(summary.operation_name, UNKNOWN_TOOL_OPERATION_NAME);
        assert!(!format!("{summary:?}").contains(sentinel));
    }

    #[test]
    fn legacy_tool_operation_name_policy_bounds_identifiers() {
        let longest_valid = format!("2.{}", "a".repeat(MAX_LEGACY_TOOL_OPERATION_NAME_LEN - 2));
        assert_eq!(
            privacy_safe_legacy_tool_operation_name(longest_valid.clone()),
            longest_valid
        );

        let overlong = format!("2.{}", "a".repeat(MAX_LEGACY_TOOL_OPERATION_NAME_LEN - 1));
        assert_eq!(
            privacy_safe_legacy_tool_operation_name(overlong),
            UNKNOWN_TOOL_OPERATION_NAME
        );
    }

    #[test]
    fn query_stream_filters_workspace_before_pagination() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");

        let mut records = Vec::new();
        for (span_id, workspace, end_time) in [
            ("alpha-new", "alpha", 30),
            ("beta", "beta", 20),
            ("alpha-old", "alpha", 10),
        ] {
            let mut record = trace_record("pagination-trace", span_id);
            record.parent_span_id = Some("remote-parent".to_string());
            record.parent_span_is_remote = true;
            record.attributes_json = json!({
                "coral.stream.entry": true,
                "coral.stream.kind": "query",
                "coral.stream.name": "sql",
                "workspace": workspace,
                "sql": format!("SELECT '{span_id}'"),
            })
            .to_string();
            record.start_time_unix_nanos = end_time - 1;
            record.end_time_unix_nanos = end_time;
            records.push(record);
        }
        write_record_file_lines(&dir.join("spans-pagination.jsonl"), &records);

        let store = TraceStore::new(dir);
        let first = store
            .list_query_stream_sync(1, 0, Some("alpha"))
            .expect("first page");
        let second = store
            .list_query_stream_sync(1, 1, Some("alpha"))
            .expect("second page");
        assert_eq!(
            first.first().expect("first entry").root_span_id,
            "alpha-new"
        );
        assert_eq!(
            second.first().expect("second entry").root_span_id,
            "alpha-old"
        );
    }

    #[test]
    fn query_stream_stops_after_a_complete_recent_page() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let recent_time = now - Duration::from_secs(1);
        let old_time = now - Duration::from_hours(2);

        let mut recent = trace_record("recent-trace", "recent-entry");
        recent.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'recent'",
        })
        .to_string();
        recent.start_time_unix_nanos = unix_nanos(recent_time);
        recent.end_time_unix_nanos = unix_nanos(recent_time + Duration::from_millis(1));
        let recent_path = dir.join(timestamped_jsonl_path(recent_time));
        write_record_file(&recent_path, &recent);
        set_modified_time(&recent_path, recent_time);

        let old_path = dir.join(timestamped_jsonl_path(old_time));
        fs::write(&old_path, [0xff]).expect("write unreadable old JSONL text");
        set_modified_time(&old_path, old_time);

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(1, 0, Some("alpha"))
            .expect("recent page does not read old file");
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("recent summary").root_span_id,
            "recent-entry"
        );
    }

    #[test]
    fn query_stream_offset_page_reads_enough_recent_files_then_stops() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let now = SystemTime::now();
        let newest_time = now - Duration::from_secs(1);
        let second_time = now - Duration::from_secs(5);
        let unreadable_time = now - Duration::from_hours(2);

        let mut newest = trace_record("newest-trace", "newest-entry");
        newest.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'newest'",
        })
        .to_string();
        newest.start_time_unix_nanos = unix_nanos(newest_time);
        newest.end_time_unix_nanos = unix_nanos(newest_time + Duration::from_millis(1));
        let newest_path = dir.join(timestamped_jsonl_path(newest_time));
        write_record_file(&newest_path, &newest);
        set_modified_time(&newest_path, newest_time);

        let mut second = trace_record("second-trace", "second-entry");
        second.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'second'",
        })
        .to_string();
        second.start_time_unix_nanos = unix_nanos(second_time);
        second.end_time_unix_nanos = unix_nanos(second_time + Duration::from_millis(1));
        let second_path = dir.join(timestamped_jsonl_path(second_time));
        write_record_file(&second_path, &second);
        set_modified_time(&second_path, second_time);

        let unreadable_path = dir.join(timestamped_jsonl_path(unreadable_time));
        fs::write(&unreadable_path, [0xff]).expect("write unreadable old JSONL text");
        set_modified_time(&unreadable_path, unreadable_time);

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(1, 1, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("offset summary").root_span_id,
            "second-entry"
        );
    }

    #[test]
    fn query_stream_completes_returned_operations_from_older_files() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let operation_start = SystemTime::now() - Duration::from_secs(20);
        let child_end = operation_start + Duration::from_secs(2);
        let operation_end = operation_start + Duration::from_secs(10);

        let mut child = trace_record("split-trace", "query-child");
        child.parent_span_id = Some("tool-root".to_string());
        child.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 42",
            "row_count": 1,
        })
        .to_string();
        child.start_time_unix_nanos = unix_nanos(operation_start + Duration::from_secs(1));
        child.end_time_unix_nanos = unix_nanos(child_end);
        let child_path = dir.join(timestamped_jsonl_path(child_end));
        write_record_file(&child_path, &child);
        set_modified_time(&child_path, child_end);

        let mut root = trace_record("split-trace", "tool-root");
        root.parent_span_id = Some("remote-parent".to_string());
        root.parent_span_is_remote = true;
        root.name = "coral.mcp.call_tool".to_string();
        root.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "tool",
            "coral.stream.name": "sql",
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
            "workspace": "alpha",
        })
        .to_string();
        root.start_time_unix_nanos = unix_nanos(operation_start);
        root.end_time_unix_nanos = unix_nanos(operation_end);
        let root_path = dir.join(timestamped_jsonl_path(operation_end));
        write_record_file(&root_path, &root);
        set_modified_time(&root_path, operation_end);

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(1, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("completed operation summary");
        assert_eq!(summary.root_span_id, "tool-root");
        assert_eq!(summary.query, "SELECT 42");
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);
        assert_eq!(summary.span_count, 2);
    }

    #[test]
    fn query_stream_projector_releases_completed_discovery_trees() {
        let mut projector = QueryStreamProjector::new(1, Some("alpha"));

        for index in (0..128_i64).rev() {
            let base = index * 10;
            let protocol_span_id = format!("protocol-{index}");
            let protocol = TraceListSpanRecord {
                trace_id: "shared-discovery-trace".to_string(),
                span_id: protocol_span_id.clone(),
                parent_span_id: Some("remote-parent".to_string()),
                parent_span_is_remote: true,
                name: "coral.mcp.list_tools".to_string(),
                status: StoredTraceStatus::Ok,
                start_time_unix_nanos: base,
                end_time_unix_nanos: base + 4,
                attributes_json: r#"{"mcp.method":"tools/list"}"#.to_string(),
            };
            let child = TraceListSpanRecord {
                trace_id: "shared-discovery-trace".to_string(),
                span_id: format!("discovery-query-{index}"),
                parent_span_id: Some(protocol_span_id),
                parent_span_is_remote: false,
                name: "coral.query".to_string(),
                status: StoredTraceStatus::Ok,
                start_time_unix_nanos: base + 1,
                end_time_unix_nanos: base + 2,
                attributes_json: json!({
                    "coral.stream.entry": true,
                    "coral.stream.kind": "query",
                    "coral.stream.name": "list_catalog",
                    "workspace": "alpha",
                    "sql": "LIST CATALOG",
                })
                .to_string(),
            };

            projector.record_file(vec![child, protocol]);
            projector.advance_watermark(base - 1);
            assert!(projector.nodes.is_empty());
            assert!(projector.aggregates.is_empty());
            assert!(projector.finalized.is_empty());
        }
    }

    #[test]
    fn query_stream_keeps_newer_duplicate_span_across_files() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let base_time = SystemTime::now() - Duration::from_secs(10);

        let mut older = trace_record("duplicate-query-stream", "duplicate-entry");
        older.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'old'",
        })
        .to_string();
        older.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(500));
        older.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(600));
        let older_path = dir.join(timestamped_jsonl_path(base_time));
        write_record_file(&older_path, &older);
        set_modified_time(&older_path, base_time);

        let mut newer = older;
        newer.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'new'",
        })
        .to_string();
        newer.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(700));
        let newer_modified = base_time + Duration::from_millis(10);
        let newer_path = dir.join(timestamped_jsonl_path(newer_modified));
        write_record_file(&newer_path, &newer);
        set_modified_time(&newer_path, newer_modified);

        let store = TraceStore::new(dir);
        let summaries = store
            .list_query_stream_sync(1, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("newer duplicate summary").query,
            "SELECT 'new'"
        );
    }

    #[test]
    fn query_stream_keeps_scanning_when_file_mtime_is_coarse() {
        let temp = TempDir::new().expect("temp dir");
        let dir = temp.path().join("telemetry").join("traces");
        fs::create_dir_all(&dir).expect("trace dir");
        let base_time = SystemTime::now() - Duration::from_secs(10);
        let hidden_modified = base_time;
        let visible_modified = base_time + Duration::from_millis(10);

        let mut hidden_newer = trace_record("hidden-newer", "hidden-newer-entry");
        hidden_newer.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'newer'",
        })
        .to_string();
        hidden_newer.start_time_unix_nanos = unix_nanos(base_time);
        hidden_newer.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(900));
        let hidden_path = dir.join(timestamped_jsonl_path(hidden_modified));
        write_record_file(&hidden_path, &hidden_newer);
        set_modified_time(&hidden_path, hidden_modified);

        let mut visible_older = trace_record("visible-older", "visible-older-entry");
        visible_older.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": "alpha",
            "sql": "SELECT 'older'",
        })
        .to_string();
        visible_older.start_time_unix_nanos = unix_nanos(base_time);
        visible_older.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(100));
        let visible_path = dir.join(timestamped_jsonl_path(visible_modified));
        write_record_file(&visible_path, &visible_older);
        set_modified_time(&visible_path, visible_modified);

        let summaries = TraceStore::new(dir)
            .list_query_stream_sync(1, 0, Some("alpha"))
            .expect("list query stream");
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries.first().expect("newest summary").trace_id,
            "hidden-newer"
        );
    }
}
