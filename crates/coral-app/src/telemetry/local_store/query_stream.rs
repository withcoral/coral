//! Query Stream operation projection over locally captured spans.

use std::collections::{HashMap, HashSet};

mod classification;

use classification::{
    QueryStreamMetadata, QueryStreamPrimaryOperation, QueryStreamWorkspaceEvidence,
    is_unmarked_mcp_protocol_attributes, operation_text_from_attributes,
    operation_text_is_semantic, query_stream_metadata,
};

use super::{
    StoredTraceOperationKind, StoredTraceStatus, TraceListSpanRecord, TraceSpanRecord,
    TraceSummaryRecord, attr_string, attr_u64, parse_attributes, status_from_attributes,
    usize_to_u32,
};
use coral_telemetry::WORKSPACE_SPAN_ATTRIBUTE;

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
        .collect();
    let mut projector = QueryStreamProjector::new(workspace_name);
    projector.record_file(records);
    projector.finish();
    projector.into_summaries()
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

/// Projects completed spans into their outer Query Stream operations.
///
/// A completed child with an unresolved local parent remains hidden until that
/// owning ancestry is available, avoiding an incomplete row that would later
/// need to be replaced by its outer operation.
struct QueryStreamProjector {
    workspace_name: Option<String>,
    nodes: HashMap<QueryStreamSpanKey, QueryStreamNodeState>,
    aggregates: HashMap<u64, StreamingQueryStreamAggregate>,
    finalized: Vec<TraceSummaryRecord>,
    next_operation_id: u64,
}

impl QueryStreamProjector {
    fn new(workspace_name: Option<&str>) -> Self {
        Self {
            workspace_name: workspace_name.map(str::to_string),
            nodes: HashMap::new(),
            aggregates: HashMap::new(),
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
            owner,
            owner_depth,
            suppresses_entries,
        };
        self.nodes.insert(key, node);
        if let Some((owner, depth)) = owner.zip(owner_depth)
            && let Some(aggregate) = self.aggregates.get_mut(&owner)
        {
            aggregate.record_span(span, depth);
        }
    }

    fn finish(&mut self) {
        let operation_ids = self.aggregates.keys().copied().collect::<Vec<_>>();
        for operation_id in operation_ids {
            self.finalize_operation(operation_id);
        }
        self.nodes.clear();
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

    fn into_summaries(mut self) -> Vec<TraceSummaryRecord> {
        sort_and_deduplicate_query_stream_summaries(&mut self.finalized);
        self.finalized
    }
}

#[cfg(test)]
mod semantics_tests;
#[cfg(test)]
mod test_support;
