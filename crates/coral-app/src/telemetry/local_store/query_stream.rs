//! Query Stream projection over locally captured JSONL spans.

use std::collections::{BTreeMap, HashMap, HashSet};

mod classification;

use classification::{
    QueryStreamMetadata, QueryStreamPrimaryOperation, QueryStreamWorkspaceEvidence,
    is_unmarked_mcp_protocol_attributes, is_unmarked_mcp_protocol_span,
    operation_text_from_attributes, operation_text_is_semantic, query_stream_metadata,
    query_stream_metadata_from_attributes, query_stream_summaries,
};

use super::{
    StoredTraceOperationKind, StoredTraceStatus, TraceDetailRecord, TraceListSpanRecord,
    TraceSpanRecord, TraceStore, TraceStoreError, TraceStoreFile, TraceSummaryRecord, attr_string,
    attr_u64, parse_attributes, read_list_spans_file, status_from_attributes, usize_to_u32,
};
use crate::telemetry::WORKSPACE_SPAN_ATTRIBUTE;

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

pub(super) fn get(
    store: &TraceStore,
    trace_id: &str,
    root_span_id: &str,
    workspace_name: Option<&str>,
) -> Result<TraceDetailRecord, TraceStoreError> {
    let trace = store.get_trace_sync(trace_id)?;
    let spans_by_id = trace
        .spans
        .iter()
        .map(|span| (span.span_id.as_str(), span))
        .collect::<HashMap<_, _>>();
    let root = spans_by_id
        .get(root_span_id)
        .copied()
        .ok_or_else(|| TraceStoreError::NotFound(trace_id.to_string()))?;
    if query_stream_metadata(root).is_none() || !query_stream_root_is_visible(root, &spans_by_id) {
        return Err(TraceStoreError::NotFound(trace_id.to_string()));
    }

    let mut spans = collect_query_stream_operation_spans(trace.spans, trace_id, root_span_id)?;
    let span_refs = spans.iter().collect::<Vec<_>>();
    let summary = query_stream_summaries(&span_refs, workspace_name)
        .into_iter()
        .find(|summary| summary.root_span_id == root_span_id)
        .ok_or_else(|| TraceStoreError::NotFound(trace_id.to_string()))?;
    spans.sort_by(|left, right| {
        left.start_time_unix_nanos
            .cmp(&right.start_time_unix_nanos)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
    debug_assert_eq!(spans.len(), summary.span_count as usize);
    Ok(TraceDetailRecord { summary, spans })
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
    operation_text: Option<String>,
    row_count: Option<u64>,
}

impl ProjectedQueryStreamSpan {
    fn from_record(span: TraceListSpanRecord) -> Self {
        let attributes = parse_attributes(&span.attributes_json);
        let metadata = query_stream_metadata_from_attributes(&span, attributes.as_ref());
        let protocol = attributes
            .as_ref()
            .is_some_and(is_unmarked_mcp_protocol_attributes);
        let status = status_from_attributes(attributes.as_ref()).unwrap_or(span.status);
        let workspace = attributes
            .as_ref()
            .and_then(|attributes| attr_string(attributes, WORKSPACE_SPAN_ATTRIBUTE));
        let operation_text = metadata.as_ref().and_then(|metadata| {
            attributes
                .as_ref()
                .and_then(|attributes| operation_text_from_attributes(metadata.kind, attributes))
        });
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
            operation_text,
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
    operation_text: Option<String>,
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
            operation_text: span.operation_text.clone(),
            row_count: span.row_count,
        }
    }
}

#[derive(Debug)]
struct QueryStreamTextEnrichment {
    kind: StoredTraceOperationKind,
    depth: usize,
    start_time_unix_nanos: i64,
    span_id: String,
    text: String,
    row_count: Option<u64>,
}

impl QueryStreamTextEnrichment {
    fn sort_key(&self) -> (usize, i64, &str) {
        (self.depth, self.start_time_unix_nanos, &self.span_id)
    }
}

#[derive(Debug)]
struct StreamingQueryStreamAggregate {
    entry: QueryStreamEntrySnapshot,
    span_count: usize,
    primary_descendant_operation: Option<QueryStreamPrimaryOperation>,
    text_enrichment: Option<QueryStreamTextEnrichment>,
    workspace_evidence: QueryStreamWorkspaceEvidence,
}

impl StreamingQueryStreamAggregate {
    fn new(entry: QueryStreamEntrySnapshot) -> Self {
        Self {
            entry,
            span_count: 0,
            primary_descendant_operation: None,
            text_enrichment: None,
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
        let Some(text) = span.operation_text.as_deref() else {
            return;
        };
        let enrichment = QueryStreamTextEnrichment {
            kind: metadata.kind,
            depth,
            start_time_unix_nanos: span.start_time_unix_nanos,
            span_id: span.span_id.clone(),
            text: text.to_string(),
            row_count: (metadata.kind == StoredTraceOperationKind::Query)
                .then_some(span.row_count)
                .flatten(),
        };
        if self
            .text_enrichment
            .as_ref()
            .is_none_or(|current| enrichment.sort_key() < current.sort_key())
        {
            self.text_enrichment = Some(enrichment);
        }
    }

    fn workspace(&self) -> Option<&str> {
        self.entry
            .workspace
            .as_deref()
            .or_else(|| self.workspace_evidence.unique())
    }

    fn into_summary(self) -> TraceSummaryRecord {
        let text_enrichment = self.text_enrichment.as_ref().filter(|enrichment| {
            operation_text_is_semantic(
                self.entry.metadata.kind,
                enrichment.kind,
                self.primary_descendant_operation.as_ref(),
            )
        });
        let query = self
            .entry
            .operation_text
            .or_else(|| text_enrichment.map(|enrichment| enrichment.text.clone()));
        let row_count = self
            .entry
            .row_count
            .or_else(|| text_enrichment.and_then(|enrichment| enrichment.row_count));
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

// Selected-operation DETAIL. This layer applies operation ownership semantics
// to a complete locally stored trace. A later stack layer narrows the file
// reads without changing the selection result.

fn query_stream_root_is_visible(
    root: &TraceSpanRecord,
    spans_by_id: &HashMap<&str, &TraceSpanRecord>,
) -> bool {
    let mut parent_span_id = root.parent_span_id.as_deref();
    let mut parent_span_is_remote = root.parent_span_is_remote;
    let mut visited = HashSet::new();
    while let Some(current_parent_span_id) = parent_span_id {
        if parent_span_is_remote {
            return true;
        }
        if !visited.insert(current_parent_span_id) {
            return false;
        }
        let Some(parent) = spans_by_id.get(current_parent_span_id).copied() else {
            return false;
        };
        if query_stream_metadata(parent).is_some_and(|metadata| metadata.explicit)
            || is_unmarked_mcp_protocol_span(parent)
        {
            return false;
        }
        parent_span_id = parent.parent_span_id.as_deref();
        parent_span_is_remote = parent.parent_span_is_remote;
    }
    true
}

#[derive(Debug, Clone, Copy)]
struct SelectedOperationNodeState {
    suppresses_entries: bool,
}

fn collect_query_stream_operation_spans(
    spans: Vec<TraceSpanRecord>,
    trace_id: &str,
    root_span_id: &str,
) -> Result<Vec<TraceSpanRecord>, TraceStoreError> {
    let mut candidates = spans
        .into_iter()
        .map(|span| (span.span_id.clone(), span))
        .collect::<HashMap<_, _>>();
    let root = candidates
        .remove(root_span_id)
        .ok_or_else(|| TraceStoreError::NotFound(trace_id.to_string()))?;
    let root_metadata = query_stream_metadata(&root)
        .expect("query stream root metadata was validated before collecting descendants");
    let root_state = SelectedOperationNodeState {
        suppresses_entries: root_metadata.explicit || is_unmarked_mcp_protocol_span(&root),
    };
    let mut node_states = HashMap::from([(root_span_id.to_string(), root_state)]);
    let mut selected = HashMap::from([(root_span_id.to_string(), root)]);
    let mut children_by_parent = HashMap::<String, Vec<TraceSpanRecord>>::new();
    for span in candidates.into_values() {
        let Some(parent_span_id) = span.parent_span_id.as_deref() else {
            continue;
        };
        children_by_parent
            .entry(parent_span_id.to_string())
            .or_default()
            .push(span);
    }
    let mut ready = children_by_parent.remove(root_span_id).unwrap_or_default();

    while let Some(span) = ready.pop() {
        if node_states.contains_key(&span.span_id) {
            continue;
        }
        let Some(parent) = span
            .parent_span_id
            .as_deref()
            .and_then(|parent_span_id| node_states.get(parent_span_id))
            .copied()
        else {
            continue;
        };
        let metadata = query_stream_metadata(&span);
        let starts_nested_visible_operation = metadata.is_some() && !parent.suppresses_entries;
        if starts_nested_visible_operation {
            // This is the root of a separate visible operation. Prune its
            // branch instead of retaining false ownership state for it.
            continue;
        }
        let state = SelectedOperationNodeState {
            suppresses_entries: parent.suppresses_entries
                || is_unmarked_mcp_protocol_span(&span)
                || metadata.as_ref().is_some_and(|metadata| metadata.explicit),
        };
        let span_id = span.span_id.clone();
        node_states.insert(span_id.clone(), state);
        selected.insert(span_id.clone(), span);
        if let Some(children) = children_by_parent.remove(&span_id) {
            ready.extend(children);
        }
    }

    debug_assert_eq!(
        node_states.len(),
        selected.len(),
        "detail projection must not retain state for pruned operation branches"
    );
    Ok(selected.into_values().collect())
}

#[cfg(test)]
mod semantics_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod test_support;
