//! Shared Query Stream operation classification and aggregation policy.

use coral_telemetry::{
    QUERY_STREAM_ENTRY_ATTRIBUTE, QUERY_STREAM_KIND_ATTRIBUTE, QUERY_STREAM_KIND_QUERY,
    QUERY_STREAM_KIND_SEARCH, QUERY_STREAM_KIND_TOOL, QUERY_STREAM_NAME_ATTRIBUTE,
    QUERY_STREAM_SEARCH_QUERY_ATTRIBUTE,
};
use serde_json::Value as JsonValue;

use super::super::{StoredTraceInvocationKind, StoredTraceOperationKind, attr_bool, attr_string};

const MCP_METHOD_ATTRIBUTE: &str = "mcp.method";
const MCP_TOOL_NAME_ATTRIBUTE: &str = "mcp.tool.name";
const MCP_CALL_TOOL_METHOD: &str = "tools/call";
pub(super) const UNKNOWN_TOOL_OPERATION_NAME: &str = "unknown_tool";
pub(super) const MAX_LEGACY_TOOL_OPERATION_NAME_LEN: usize = 128;

// Shared operation semantics used by the Query Stream projector.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryStreamMetadata {
    pub(super) explicit: bool,
    pub(super) kind: StoredTraceOperationKind,
    pub(super) name: String,
    pub(super) invocation_kind: StoredTraceInvocationKind,
}

pub(super) fn query_stream_metadata(
    span_name: &str,
    attributes: Option<&JsonValue>,
) -> Option<QueryStreamMetadata> {
    match attributes.and_then(|attributes| attr_bool(attributes, QUERY_STREAM_ENTRY_ATTRIBUTE)) {
        Some(false) => None,
        Some(true) => {
            let kind_name = attributes
                .and_then(|attributes| attr_string(attributes, QUERY_STREAM_KIND_ATTRIBUTE));
            let raw_kind = operation_kind_from_name(kind_name.as_deref());
            let name = attributes
                .and_then(|attributes| attr_string(attributes, QUERY_STREAM_NAME_ATTRIBUTE))
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| fallback_operation_name(span_name, attributes, raw_kind));
            let invocation_kind = invocation_kind_from_attributes(attributes);
            let kind = normalize_legacy_mcp_capability(raw_kind, invocation_kind, &name);
            Some(QueryStreamMetadata {
                explicit: true,
                kind,
                name,
                invocation_kind,
            })
        }
        None => legacy_query_stream_metadata(span_name, attributes),
    }
}

fn legacy_query_stream_metadata(
    span_name: &str,
    attributes: Option<&JsonValue>,
) -> Option<QueryStreamMetadata> {
    if let Some(tool_name) = attributes
        .filter(|attributes| {
            attr_string(attributes, MCP_METHOD_ATTRIBUTE).as_deref() == Some(MCP_CALL_TOOL_METHOD)
        })
        .and_then(|attributes| attr_string(attributes, MCP_TOOL_NAME_ATTRIBUTE))
        .filter(|tool_name| !tool_name.trim().is_empty())
    {
        let name = privacy_safe_tool_operation_name(tool_name);
        return Some(QueryStreamMetadata {
            explicit: true,
            kind: normalize_legacy_mcp_capability(
                StoredTraceOperationKind::Tool,
                StoredTraceInvocationKind::Mcp,
                &name,
            ),
            name,
            invocation_kind: StoredTraceInvocationKind::Mcp,
        });
    }
    let (kind, default_name) = match span_name {
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
        invocation_kind: StoredTraceInvocationKind::Direct,
    })
}

fn invocation_kind_from_attributes(attributes: Option<&JsonValue>) -> StoredTraceInvocationKind {
    if attributes.is_some_and(|attributes| {
        attr_string(attributes, MCP_METHOD_ATTRIBUTE)
            .is_some_and(|method| !method.trim().is_empty())
    }) {
        StoredTraceInvocationKind::Mcp
    } else {
        StoredTraceInvocationKind::Direct
    }
}

/// Normalizes spans retained before capability and invocation were separated.
/// New MCP producers record SQL and Search as semantic kinds on the outer span,
/// while older producers recorded every MCP operation as `tool`.
fn normalize_legacy_mcp_capability(
    kind: StoredTraceOperationKind,
    invocation_kind: StoredTraceInvocationKind,
    operation_name: &str,
) -> StoredTraceOperationKind {
    if kind != StoredTraceOperationKind::Tool || invocation_kind != StoredTraceInvocationKind::Mcp {
        return kind;
    }
    match operation_name {
        "sql" => StoredTraceOperationKind::Query,
        "search" => StoredTraceOperationKind::Search,
        _ => kind,
    }
}

fn operation_kind_from_name(kind: Option<&str>) -> StoredTraceOperationKind {
    match kind {
        Some(QUERY_STREAM_KIND_QUERY) => StoredTraceOperationKind::Query,
        Some(QUERY_STREAM_KIND_SEARCH) => StoredTraceOperationKind::Search,
        Some(QUERY_STREAM_KIND_TOOL) => StoredTraceOperationKind::Tool,
        _ => StoredTraceOperationKind::Other,
    }
}

pub(super) fn privacy_safe_tool_operation_name(tool_name: String) -> String {
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

fn fallback_operation_name(
    span_name: &str,
    attributes: Option<&JsonValue>,
    kind: StoredTraceOperationKind,
) -> String {
    match kind {
        StoredTraceOperationKind::Query => attributes
            .and_then(|attributes| attr_string(attributes, "operation"))
            .unwrap_or_else(|| "sql".to_string()),
        StoredTraceOperationKind::Search => "search".to_string(),
        StoredTraceOperationKind::Tool => privacy_safe_tool_operation_name(
            attributes
                .and_then(|attributes| attr_string(attributes, MCP_TOOL_NAME_ATTRIBUTE))
                .unwrap_or_else(|| span_name.to_string()),
        ),
        StoredTraceOperationKind::Other | StoredTraceOperationKind::Unspecified => {
            span_name.to_string()
        }
    }
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

    pub(super) fn resolve<'a>(&'a self, entry_workspace: Option<&'a str>) -> Option<&'a str> {
        match self {
            Self::None => entry_workspace,
            Self::One(workspace)
                if entry_workspace.is_none_or(|entry| entry == workspace.as_str()) =>
            {
                Some(workspace)
            }
            Self::One(_) | Self::Conflict => None,
        }
    }

    pub(super) fn matches(&self, workspace: &str) -> bool {
        matches!(self, Self::One(actual) if actual == workspace)
    }
}
