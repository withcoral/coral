//! OpenTelemetry helpers for MCP protocol spans.

use std::collections::HashMap;
use std::future::Future;

use coral_api::grpc_response_status_code;
use coral_client::{DecodedStatusError, decode_status_error};
use opentelemetry::{propagation::Extractor, trace::Status as OtelStatus};
use rmcp::{
    ErrorData,
    model::{CallToolResult, ErrorCode},
};
use tracing::{Instrument as _, field};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

struct StringMapExtractor<'a>(&'a HashMap<String, String>);

impl Extractor for StringMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

pub(crate) async fn instrument<T, F>(span: tracing::Span, future: F) -> T
where
    F: Future<Output = T>,
{
    future.instrument(span).await
}

pub(crate) async fn instrument_protocol<T, F>(
    span: tracing::Span,
    future: F,
) -> Result<T, ErrorData>
where
    F: Future<Output = Result<T, ErrorData>>,
{
    let result = instrument(span.clone(), future).await;
    record_protocol_result(&span, &result);
    result
}

pub(crate) fn list_tools_span(trace_parent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        target: "coral_mcp::server",
        "coral.mcp.list_tools",
        error.type = field::Empty,
        exception.message = field::Empty,
        mcp.surface.feedback.enabled = field::Empty,
        mcp.surface.source_count = field::Empty,
        mcp.surface.table_count = field::Empty,
        mcp.surface.table_function_count = field::Empty,
        mcp.surface.tool_count = field::Empty,
        mcp.method = "tools/list",
        otel.kind = "server",
        otel.name = "coral.mcp.list_tools",
        status = field::Empty,
    );
    apply_trace_parent(&span, trace_parent);
    span
}

pub(crate) fn call_tool_span(tool_name: &str, trace_parent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        target: "coral_mcp::server",
        "coral.mcp.call_tool",
        error.type = field::Empty,
        exception.message = field::Empty,
        mcp.method = "tools/call",
        mcp.response.content_bytes = field::Empty,
        mcp.response.is_error = field::Empty,
        mcp.result_get.has_more = field::Empty,
        mcp.result_get.limit = field::Empty,
        mcp.result_get.next_offset = field::Empty,
        mcp.result_get.offset = field::Empty,
        mcp.result_get.projected_column_count = field::Empty,
        mcp.result_get.requested_column_count = field::Empty,
        mcp.result_get.row_count = field::Empty,
        mcp.result_get.rows_returned = field::Empty,
        mcp.result_get.uses_projection = field::Empty,
        mcp.sql.result.column_count = field::Empty,
        mcp.sql.result.guidance_included = field::Empty,
        mcp.sql.result.has_more = field::Empty,
        mcp.sql.result.preview_rows = field::Empty,
        mcp.sql.result.row_count = field::Empty,
        mcp.sql.result.shape = field::Empty,
        mcp.tool.name = tool_name,
        otel.kind = "server",
        otel.name = "coral.mcp.call_tool",
        status = field::Empty,
    );
    apply_trace_parent(&span, trace_parent);
    span
}

pub(crate) fn list_resources_span(trace_parent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        target: "coral_mcp::server",
        "coral.mcp.list_resources",
        error.type = field::Empty,
        exception.message = field::Empty,
        mcp.surface.resource_count = field::Empty,
        mcp.surface.source_count = field::Empty,
        mcp.surface.table_count = field::Empty,
        mcp.surface.table_function_count = field::Empty,
        mcp.method = "resources/list",
        otel.kind = "server",
        otel.name = "coral.mcp.list_resources",
        status = field::Empty,
    );
    apply_trace_parent(&span, trace_parent);
    span
}

pub(crate) fn read_resource_span(uri: &str, trace_parent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        target: "coral_mcp::server",
        "coral.mcp.read_resource",
        error.type = field::Empty,
        exception.message = field::Empty,
        mcp.method = "resources/read",
        mcp.resource.uri = uri,
        otel.kind = "server",
        otel.name = "coral.mcp.read_resource",
        status = field::Empty,
    );
    apply_trace_parent(&span, trace_parent);
    span
}

fn apply_trace_parent(span: &tracing::Span, trace_parent: Option<&str>) {
    let Some(trace_parent) = trace_parent else {
        return;
    };
    let carrier = HashMap::from([("traceparent".to_string(), trace_parent.to_string())]);
    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&StringMapExtractor(&carrier))
    });
    drop(span.set_parent(parent_cx));
}

pub(crate) fn record_protocol_result<T>(span: &tracing::Span, result: &Result<T, ErrorData>) {
    match result {
        Ok(_) => record_success(span),
        Err(error) => record_protocol_error(span, error),
    }
}

pub(crate) fn record_list_tools_surface(
    span: &tracing::Span,
    source_count: usize,
    table_count: usize,
    table_function_count: usize,
    tool_count: usize,
    feedback_enabled: bool,
) {
    span.record("mcp.surface.source_count", usize_to_u64(source_count));
    span.record("mcp.surface.table_count", usize_to_u64(table_count));
    span.record(
        "mcp.surface.table_function_count",
        usize_to_u64(table_function_count),
    );
    span.record("mcp.surface.tool_count", usize_to_u64(tool_count));
    span.record("mcp.surface.feedback.enabled", feedback_enabled);
}

pub(crate) fn record_list_resources_surface(
    span: &tracing::Span,
    source_count: usize,
    table_count: usize,
    table_function_count: usize,
    resource_count: usize,
) {
    span.record("mcp.surface.source_count", usize_to_u64(source_count));
    span.record("mcp.surface.table_count", usize_to_u64(table_count));
    span.record(
        "mcp.surface.table_function_count",
        usize_to_u64(table_function_count),
    );
    span.record("mcp.surface.resource_count", usize_to_u64(resource_count));
}

pub(crate) fn record_tool_response(span: &tracing::Span, result: &CallToolResult) {
    let content_bytes = result
        .content
        .iter()
        .filter_map(|content| content.as_text())
        .map(|text| text.text.len())
        .sum::<usize>();
    span.record("mcp.response.content_bytes", usize_to_u64(content_bytes));
    span.record("mcp.response.is_error", result.is_error.unwrap_or(false));
}

pub(crate) fn record_sql_result(
    span: &tracing::Span,
    shape: &'static str,
    row_count: usize,
    column_count: usize,
    preview_rows: usize,
    has_more: Option<bool>,
    guidance_included: bool,
) {
    span.record("mcp.sql.result.shape", shape);
    span.record("mcp.sql.result.row_count", usize_to_u64(row_count));
    span.record("mcp.sql.result.column_count", usize_to_u64(column_count));
    span.record("mcp.sql.result.preview_rows", usize_to_u64(preview_rows));
    if let Some(has_more) = has_more {
        span.record("mcp.sql.result.has_more", has_more);
    }
    span.record("mcp.sql.result.guidance_included", guidance_included);
}

pub(crate) fn record_result_get_request(
    span: &tracing::Span,
    offset: usize,
    limit: usize,
    requested_column_count: Option<usize>,
) {
    span.record("mcp.result_get.offset", usize_to_u64(offset));
    span.record("mcp.result_get.limit", usize_to_u64(limit));
    span.record(
        "mcp.result_get.uses_projection",
        requested_column_count.is_some(),
    );
    if let Some(requested_column_count) = requested_column_count {
        span.record(
            "mcp.result_get.requested_column_count",
            usize_to_u64(requested_column_count),
        );
    }
}

pub(crate) fn record_result_get_page(
    span: &tracing::Span,
    row_count: usize,
    rows_returned: usize,
    projected_column_count: usize,
    has_more: bool,
    next_offset: Option<usize>,
) {
    span.record("mcp.result_get.row_count", usize_to_u64(row_count));
    span.record("mcp.result_get.rows_returned", usize_to_u64(rows_returned));
    span.record(
        "mcp.result_get.projected_column_count",
        usize_to_u64(projected_column_count),
    );
    span.record("mcp.result_get.has_more", has_more);
    if let Some(next_offset) = next_offset {
        span.record("mcp.result_get.next_offset", usize_to_u64(next_offset));
    }
}

pub(crate) fn record_protocol_error(span: &tracing::Span, error: &ErrorData) {
    record_error(span, mcp_error_type(error.code), error.message.as_ref());
}

pub(crate) fn record_tonic_status(span: &tracing::Span, status: &tonic::Status) {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => {
            record_error(span, error.reason.as_str(), error.summary);
        }
        DecodedStatusError::Plain(message) => {
            record_error(span, grpc_response_status_code(status.code()), message);
        }
    }
}

pub(crate) fn record_success(span: &tracing::Span) {
    span.record("status", "ok");
    span.set_status(OtelStatus::Ok);
}

fn record_error(span: &tracing::Span, error_type: &str, message: impl std::fmt::Display) {
    let message = message.to_string();
    span.record("status", "error");
    span.record("error.type", error_type);
    span.record("exception.message", field::display(&message));
    span.set_status(OtelStatus::error(message));
}

fn mcp_error_type(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::RESOURCE_NOT_FOUND => "RESOURCE_NOT_FOUND",
        ErrorCode::INVALID_REQUEST => "INVALID_REQUEST",
        ErrorCode::METHOD_NOT_FOUND => "METHOD_NOT_FOUND",
        ErrorCode::INVALID_PARAMS => "INVALID_PARAMS",
        ErrorCode::INTERNAL_ERROR => "INTERNAL_ERROR",
        ErrorCode::PARSE_ERROR => "PARSE_ERROR",
        ErrorCode::URL_ELICITATION_REQUIRED => "URL_ELICITATION_REQUIRED",
        _ => "MCP_PROTOCOL",
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
