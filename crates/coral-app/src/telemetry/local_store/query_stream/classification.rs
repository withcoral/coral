//! Shared Query Stream operation classification and aggregation policy.

use std::collections::{HashMap, HashSet};

use coral_telemetry::{
    QUERY_STREAM_ENTRY_ATTRIBUTE, QUERY_STREAM_KIND_ATTRIBUTE, QUERY_STREAM_KIND_QUERY,
    QUERY_STREAM_KIND_SEARCH, QUERY_STREAM_KIND_TOOL, QUERY_STREAM_NAME_ATTRIBUTE,
    QUERY_STREAM_SEARCH_QUERY_ATTRIBUTE,
};
use serde_json::Value as JsonValue;

use super::super::{
    StoredTraceOperationKind, StoredTraceStatus, TraceListSpanRecord, TraceSpanRecord,
    TraceSummaryRecord, attr_bool, attr_string, attr_u64, parse_attributes, status_from_attributes,
    usize_to_u32, workspace_attribute,
};

const MCP_METHOD_ATTRIBUTE: &str = "mcp.method";
const MCP_TOOL_NAME_ATTRIBUTE: &str = "mcp.tool.name";
const MCP_CALL_TOOL_METHOD: &str = "tools/call";
pub(super) const UNKNOWN_TOOL_OPERATION_NAME: &str = "unknown_tool";
pub(super) const MAX_LEGACY_TOOL_OPERATION_NAME_LEN: usize = 128;

// Shared operation semantics used by LIST and selected DETAIL.

pub(super) trait QueryStreamSpan {
    fn trace_id(&self) -> &str;
    fn span_id(&self) -> &str;
    fn parent_span_id(&self) -> Option<&str>;
    fn name(&self) -> &str;
    fn status(&self) -> StoredTraceStatus;
    fn start_time_unix_nanos(&self) -> i64;
    fn end_time_unix_nanos(&self) -> i64;
    fn attributes_json(&self) -> &str;
}

impl QueryStreamSpan for TraceListSpanRecord {
    fn trace_id(&self) -> &str {
        &self.trace_id
    }

    fn span_id(&self) -> &str {
        &self.span_id
    }

    fn parent_span_id(&self) -> Option<&str> {
        self.parent_span_id.as_deref()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn status(&self) -> StoredTraceStatus {
        self.status
    }

    fn start_time_unix_nanos(&self) -> i64 {
        self.start_time_unix_nanos
    }

    fn end_time_unix_nanos(&self) -> i64 {
        self.end_time_unix_nanos
    }

    fn attributes_json(&self) -> &str {
        &self.attributes_json
    }
}

impl QueryStreamSpan for TraceSpanRecord {
    fn trace_id(&self) -> &str {
        &self.trace_id
    }

    fn span_id(&self) -> &str {
        &self.span_id
    }

    fn parent_span_id(&self) -> Option<&str> {
        self.parent_span_id.as_deref()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn status(&self) -> StoredTraceStatus {
        self.status
    }

    fn start_time_unix_nanos(&self) -> i64 {
        self.start_time_unix_nanos
    }

    fn end_time_unix_nanos(&self) -> i64 {
        self.end_time_unix_nanos
    }

    fn attributes_json(&self) -> &str {
        &self.attributes_json
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryStreamMetadata {
    pub(super) explicit: bool,
    pub(super) kind: StoredTraceOperationKind,
    pub(super) name: String,
}

struct QueryStreamEntryAggregate<'a, T> {
    span_count: usize,
    primary_descendant_operation: Option<QueryStreamPrimaryOperation>,
    text_enrichment: Option<(StoredTraceOperationKind, usize, &'a T)>,
    workspace_evidence: QueryStreamWorkspaceEvidence,
}

impl<T> Default for QueryStreamEntryAggregate<'_, T> {
    fn default() -> Self {
        Self {
            span_count: 0,
            primary_descendant_operation: None,
            text_enrichment: None,
            workspace_evidence: QueryStreamWorkspaceEvidence::default(),
        }
    }
}

pub(super) fn query_stream_summaries<T>(
    spans: &[&T],
    workspace_name: Option<&str>,
) -> Vec<TraceSummaryRecord>
where
    T: QueryStreamSpan,
{
    let mut spans_by_trace = HashMap::<&str, Vec<&T>>::new();
    for span in spans {
        spans_by_trace
            .entry(span.trace_id())
            .or_default()
            .push(*span);
    }

    spans_by_trace
        .into_values()
        .flat_map(|trace_spans| query_stream_summaries_for_trace(&trace_spans, workspace_name))
        .collect()
}

fn query_stream_summaries_for_trace<T>(
    spans: &[&T],
    workspace_name: Option<&str>,
) -> Vec<TraceSummaryRecord>
where
    T: QueryStreamSpan,
{
    let spans_by_id = spans
        .iter()
        .map(|span| (span.span_id(), *span))
        .collect::<HashMap<_, _>>();
    let metadata_by_id = spans
        .iter()
        .map(|span| (span.span_id(), query_stream_metadata(*span)))
        .collect::<HashMap<_, _>>();
    let protocol_span_ids = spans
        .iter()
        .filter(|span| is_unmarked_mcp_protocol_span(**span))
        .map(|span| span.span_id())
        .collect::<HashSet<_>>();
    let visible_entries = spans
        .iter()
        .filter_map(|span| {
            let metadata = metadata_by_id.get(span.span_id())?.as_ref()?.clone();
            is_visible_query_stream_entry(*span, &spans_by_id, &metadata_by_id, &protocol_span_ids)
                .then_some((*span, metadata))
        })
        .collect::<Vec<_>>();
    let visible_entry_ids = visible_entries
        .iter()
        .map(|(entry, _metadata)| entry.span_id())
        .collect::<HashSet<_>>();
    let mut aggregates: HashMap<&str, QueryStreamEntryAggregate<'_, T>> = visible_entry_ids
        .iter()
        .map(|span_id| (*span_id, QueryStreamEntryAggregate::default()))
        .collect::<HashMap<_, _>>();

    for span in spans {
        let Some((owner_span_id, depth)) =
            nearest_visible_entry(*span, &spans_by_id, &visible_entry_ids)
        else {
            continue;
        };
        let Some(aggregate) = aggregates.get_mut(owner_span_id) else {
            continue;
        };
        aggregate.span_count = aggregate.span_count.saturating_add(1);
        let span_workspace = workspace_attribute(span.attributes_json());
        aggregate
            .workspace_evidence
            .record(span_workspace.as_deref());
        let metadata = metadata_by_id.get(span.span_id()).and_then(Option::as_ref);
        if depth > 0
            && let Some(metadata) = metadata
        {
            let operation = QueryStreamPrimaryOperation::new(
                metadata.kind,
                depth,
                span.start_time_unix_nanos(),
                span.span_id(),
            );
            if aggregate
                .primary_descendant_operation
                .as_ref()
                .is_none_or(|current| operation.sort_key() < current.sort_key())
            {
                aggregate.primary_descendant_operation = Some(operation);
            }
        }
        let text_kind = metadata.and_then(|metadata| {
            parse_attributes(span.attributes_json())
                .as_ref()
                .and_then(|attributes| operation_text_from_attributes(metadata.kind, attributes))
                .map(|_text| metadata.kind)
        });
        if let Some(text_kind) = text_kind
            && aggregate
                .text_enrichment
                .is_none_or(|(_current_kind, current_depth, current)| {
                    (depth, span.start_time_unix_nanos(), span.span_id())
                        < (
                            current_depth,
                            current.start_time_unix_nanos(),
                            current.span_id(),
                        )
                })
        {
            aggregate.text_enrichment = Some((text_kind, depth, *span));
        }
    }

    visible_entries
        .into_iter()
        .filter_map(|(entry, metadata)| {
            let aggregate = aggregates.remove(entry.span_id()).unwrap_or_default();
            let workspace = workspace_attribute(entry.attributes_json());
            let workspace = workspace
                .as_deref()
                .or_else(|| aggregate.workspace_evidence.unique());
            workspace_name
                .is_none_or(|workspace_name| workspace == Some(workspace_name))
                .then(|| query_stream_summary(entry, metadata, &aggregate))
        })
        .collect()
}

pub(super) fn query_stream_metadata<T>(span: &T) -> Option<QueryStreamMetadata>
where
    T: QueryStreamSpan,
{
    let attributes = parse_attributes(span.attributes_json());
    query_stream_metadata_from_attributes(span, attributes.as_ref())
}

pub(super) fn query_stream_metadata_from_attributes<T>(
    span: &T,
    attributes: Option<&JsonValue>,
) -> Option<QueryStreamMetadata>
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

pub(super) fn privacy_safe_legacy_tool_operation_name(tool_name: String) -> String {
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

/// Precondition: every candidate entry has complete local ancestry in
/// `spans_by_id`, unless omitted ancestry was already validated against the
/// full store. DETAIL does this with `query_stream_root_is_visible`. A missing
/// parent is treated as a batch boundary, not as an unfinished local parent.
fn is_visible_query_stream_entry<T>(
    span: &T,
    spans_by_id: &HashMap<&str, &T>,
    metadata_by_id: &HashMap<&str, Option<QueryStreamMetadata>>,
    protocol_span_ids: &HashSet<&str>,
) -> bool
where
    T: QueryStreamSpan,
{
    let mut ancestor_id = span.parent_span_id();
    let mut visited = HashSet::new();
    while let Some(span_id) = ancestor_id {
        if !visited.insert(span_id) {
            return false;
        }
        let Some(ancestor) = spans_by_id.get(span_id).copied() else {
            break;
        };
        if metadata_by_id
            .get(span_id)
            .and_then(Option::as_ref)
            .is_some_and(|metadata| metadata.explicit)
        {
            return false;
        }
        if protocol_span_ids.contains(span_id) {
            return false;
        }
        ancestor_id = ancestor.parent_span_id();
    }
    true
}

fn nearest_visible_entry<'a, T>(
    span: &'a T,
    spans_by_id: &HashMap<&str, &'a T>,
    visible_entry_ids: &HashSet<&str>,
) -> Option<(&'a str, usize)>
where
    T: QueryStreamSpan,
{
    let mut span_id = Some(span.span_id());
    let mut visited = HashSet::new();
    let mut depth = 0_usize;
    while let Some(current_span_id) = span_id {
        if !visited.insert(current_span_id) {
            return None;
        }
        if visible_entry_ids.contains(current_span_id) {
            return Some((current_span_id, depth));
        }
        let current = spans_by_id.get(current_span_id).copied()?;
        span_id = current.parent_span_id();
        depth = depth.saturating_add(1);
    }
    None
}

pub(super) fn is_unmarked_mcp_protocol_span<T>(span: &T) -> bool
where
    T: QueryStreamSpan,
{
    let Some(attributes) = parse_attributes(span.attributes_json()) else {
        return false;
    };
    is_unmarked_mcp_protocol_attributes(&attributes)
}

pub(super) fn is_unmarked_mcp_protocol_attributes(attributes: &JsonValue) -> bool {
    let is_marked = attr_bool(attributes, QUERY_STREAM_ENTRY_ATTRIBUTE).unwrap_or(false);
    !is_marked
        && attributes.get(MCP_METHOD_ATTRIBUTE).is_some()
        && attributes.get(MCP_TOOL_NAME_ATTRIBUTE).is_none()
}

pub(super) fn operation_text_from_attributes(
    kind: StoredTraceOperationKind,
    attributes: &JsonValue,
) -> Option<String> {
    let attribute = match kind {
        StoredTraceOperationKind::Query => "sql",
        StoredTraceOperationKind::Search => QUERY_STREAM_SEARCH_QUERY_ATTRIBUTE,
        StoredTraceOperationKind::Tool
        | StoredTraceOperationKind::Other
        | StoredTraceOperationKind::Unspecified => return None,
    };
    attr_string(attributes, attribute).filter(|text| !text.trim().is_empty())
}

pub(super) fn operation_text_is_semantic(
    entry_kind: StoredTraceOperationKind,
    text_kind: StoredTraceOperationKind,
    primary_descendant: Option<&QueryStreamPrimaryOperation>,
) -> bool {
    entry_kind == text_kind
        || (entry_kind == StoredTraceOperationKind::Tool
            && primary_descendant.is_some_and(|operation| operation.kind == text_kind))
}

fn query_stream_summary<T>(
    entry: &T,
    metadata: QueryStreamMetadata,
    aggregate: &QueryStreamEntryAggregate<'_, T>,
) -> TraceSummaryRecord
where
    T: QueryStreamSpan,
{
    let entry_attributes = parse_attributes(entry.attributes_json());
    let enrichment = aggregate.text_enrichment.and_then(|(kind, _depth, span)| {
        operation_text_is_semantic(
            metadata.kind,
            kind,
            aggregate.primary_descendant_operation.as_ref(),
        )
        .then_some((kind, span))
    });
    let enrichment_attributes =
        enrichment.and_then(|(_kind, span)| parse_attributes(span.attributes_json()));
    let query = entry_attributes
        .as_ref()
        .and_then(|attributes| operation_text_from_attributes(metadata.kind, attributes))
        .or_else(|| {
            enrichment.zip(enrichment_attributes.as_ref()).and_then(
                |((kind, _span), attributes)| operation_text_from_attributes(kind, attributes),
            )
        })
        .unwrap_or_default();
    let row_count = (metadata.kind == StoredTraceOperationKind::Query)
        .then(|| {
            entry_attributes
                .as_ref()
                .and_then(|attributes| attr_u64(attributes, "row_count"))
        })
        .flatten()
        .or_else(|| {
            enrichment
                .filter(|(kind, _span)| *kind == StoredTraceOperationKind::Query)
                .zip(enrichment_attributes.as_ref())
                .and_then(|((_kind, _span), attributes)| attr_u64(attributes, "row_count"))
        });
    let status = status_from_attributes(entry_attributes.as_ref()).unwrap_or(entry.status());
    TraceSummaryRecord {
        trace_id: entry.trace_id().to_string(),
        root_span_id: entry.span_id().to_string(),
        name: entry.name().to_string(),
        query,
        status,
        start_time_unix_nanos: entry.start_time_unix_nanos(),
        end_time_unix_nanos: entry.end_time_unix_nanos(),
        duration_nanos: entry
            .end_time_unix_nanos()
            .saturating_sub(entry.start_time_unix_nanos()),
        span_count: usize_to_u32(aggregate.span_count),
        row_count: row_count.unwrap_or_default(),
        row_count_recorded: row_count.is_some(),
        operation_kind: metadata.kind,
        operation_name: metadata.name,
    }
}

#[derive(Debug)]
pub(super) struct QueryStreamPrimaryOperation {
    kind: StoredTraceOperationKind,
    depth: usize,
    start_time_unix_nanos: i64,
    span_id: String,
}

impl QueryStreamPrimaryOperation {
    pub(super) fn new(
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

    pub(super) fn sort_key(&self) -> (usize, i64, &str) {
        (self.depth, self.start_time_unix_nanos, &self.span_id)
    }
}

#[derive(Debug, Default)]
pub(super) enum QueryStreamWorkspaceEvidence {
    #[default]
    None,
    One(String),
    Conflict,
}

impl QueryStreamWorkspaceEvidence {
    pub(super) fn record(&mut self, workspace: Option<&str>) {
        let Some(workspace) = workspace.filter(|workspace| !workspace.trim().is_empty()) else {
            return;
        };
        match self {
            Self::None => *self = Self::One(workspace.to_string()),
            Self::One(current) if current != workspace => *self = Self::Conflict,
            Self::One(_) | Self::Conflict => {}
        }
    }

    pub(super) fn unique(&self) -> Option<&str> {
        match self {
            Self::One(workspace) => Some(workspace),
            Self::None | Self::Conflict => None,
        }
    }
}
