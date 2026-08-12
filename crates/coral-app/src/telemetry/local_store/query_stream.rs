//! Query Stream operation projection over locally captured JSONL spans.

use std::collections::{BTreeMap, HashMap, HashSet};

mod classification;

use classification::{
    QueryStreamMetadata, QueryStreamPrimaryOperation, QueryStreamWorkspaceEvidence,
    is_unmarked_mcp_protocol_attributes, operation_text_from_attributes,
    operation_text_is_semantic, query_stream_metadata,
};

use super::{
    StoredTraceOperationKind, StoredTraceStatus, TraceListSpanRecord, TraceSpanRecord, TraceStore,
    TraceStoreError, TraceSummaryRecord, attr_string, attr_u64, parse_attributes,
    read_list_spans_file, status_from_attributes, usize_to_u32,
};
use coral_telemetry::WORKSPACE_SPAN_ATTRIBUTE;

// Bounded LIST storage scanning.

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
    let mut remaining_files = files.as_slice();
    while let Some(newest_bucket_file) = remaining_files.last() {
        let bucket_start = remaining_files.partition_point(|file| {
            file.modified_unix_nanos < newest_bucket_file.modified_unix_nanos
        });
        let (older_files, bucket_files) = remaining_files.split_at(bucket_start);
        let mut spans = Vec::new();
        // Equal-mtime files are one atomic unit for duplicate precedence and
        // ancestry resolution. A timestamp-preserving restore can therefore
        // make this bucket contain the entire retained store.
        for file in bucket_files {
            // Files are read in ascending path order so that the last record
            // retains the existing highest-path precedence for duplicate span
            // IDs while the projector can resolve the entire bucket at once.
            spans.extend(read_list_spans_file(&file.path)?);
        }
        projector.record_file(spans);

        let Some(newest_unscanned_file) = older_files.last() else {
            break;
        };
        let watermark = newest_unscanned_file.span_end_upper_bound_unix_nanos;
        projector.advance_watermark(watermark);
        if workspace_name.is_none() && projector.page_is_newer_than(watermark) {
            scanned_all_files = false;
            break;
        }
        remaining_files = older_files;
    }

    if scanned_all_files {
        projector.finish();
    }
    Ok(projector.into_page(offset, limit))
}

// Projection shared by LIST and detail retrieval.

fn sort_and_deduplicate_query_stream_summaries(summaries: &mut Vec<TraceSummaryRecord>) {
    summaries.sort_by(|left, right| {
        right
            .end_time_unix_nanos
            .cmp(&left.end_time_unix_nanos)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
            .then_with(|| left.root_span_id.cmp(&right.root_span_id))
    });
    let mut seen_trace_ids = HashSet::new();
    summaries.retain(|summary| seen_trace_ids.insert(summary.trace_id.clone()));
}

pub(super) fn summary(
    spans: &[TraceSpanRecord],
    workspace_name: Option<&str>,
) -> Option<TraceSummaryRecord> {
    summaries(spans, workspace_name).into_iter().next()
}

fn summaries(spans: &[TraceSpanRecord], workspace_name: Option<&str>) -> Vec<TraceSummaryRecord> {
    let records = spans
        .iter()
        .map(|span| TraceListSpanRecord {
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            parent_span_id: span.parent_span_id.clone(),
            parent_span_is_remote: span.parent_span_is_remote,
            name: span.name.clone(),
            status: span.status,
            start_time_unix_nanos: span.start_time_unix_nanos,
            end_time_unix_nanos: span.end_time_unix_nanos,
            attributes_json: span.attributes_json.clone(),
        })
        .collect::<Vec<_>>();
    let required_entry_count = records.len();
    let mut projector = QueryStreamProjector::new(required_entry_count, workspace_name);
    projector.record_file(records);
    projector.finish();
    projector.into_page(0, required_entry_count)
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
        let metadata = query_stream_metadata(&span.name, attributes.as_ref());
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

    fn may_match_workspace(&self, workspace_name: Option<&str>) -> bool {
        workspace_name.is_none_or(|workspace_name| {
            self.workspace()
                .is_none_or(|operation_workspace| operation_workspace == workspace_name)
        })
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
        let row_count = (self.entry.metadata.kind == StoredTraceOperationKind::Query)
            .then_some(self.entry.row_count)
            .flatten()
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
            invocation_kind: self.entry.metadata.invocation_kind,
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
    trace_workspace_evidence: HashMap<String, QueryStreamWorkspaceEvidence>,
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
            trace_workspace_evidence: HashMap::new(),
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
        self.trace_workspace_evidence
            .entry(span.trace_id.clone())
            .or_default()
            .record(span.workspace.as_deref());
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
        self.finalized.push(aggregate.into_summary());
    }

    fn trim_finalized(&mut self) {
        sort_and_deduplicate_query_stream_summaries(&mut self.finalized);
        if self.workspace_name.is_none() {
            self.finalized.truncate(self.required_entry_count);
        }
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
                .filter(|aggregate| aggregate.may_match_workspace(self.workspace_name.as_deref()))
                .all(|aggregate| aggregate.entry.end_time_unix_nanos < boundary.end_time_unix_nanos)
    }

    fn into_page(mut self, offset: usize, limit: usize) -> Vec<TraceSummaryRecord> {
        if let Some(workspace_name) = self.workspace_name.as_deref() {
            self.finalized.retain(|summary| {
                self.trace_workspace_evidence
                    .get(&summary.trace_id)
                    .is_some_and(|evidence| evidence.matches(workspace_name))
            });
        }
        sort_and_deduplicate_query_stream_summaries(&mut self.finalized);
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
mod semantics_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod test_support;
