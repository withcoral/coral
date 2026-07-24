//! OpenTelemetry helpers for MCP protocol spans.

use std::future::Future;

use coral_api::grpc_response_status_code;
use coral_client::{DecodedStatusError, decode_status_error};
use coral_telemetry::record_failure;
use opentelemetry::trace::Status as OtelStatus;
use rmcp::{ErrorData, model::ErrorCode};
use tracing::{Instrument as _, field};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

pub(crate) const MCP_PROTOCOL_ERROR_MESSAGE: &str = "MCP request failed";
pub(crate) const MCP_TOOL_ERROR_MESSAGE: &str = "MCP tool call failed";

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
        mcp.tool.name = tool_name,
        otel.kind = "server",
        otel.name = "coral.mcp.call_tool",
        task.id = field::Empty,
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
    coral_telemetry::set_parent_from_trace_headers(span, trace_parent, None);
}

pub(crate) fn record_protocol_result<T>(span: &tracing::Span, result: &Result<T, ErrorData>) {
    match result {
        Ok(_) => record_success(span),
        Err(error) => record_protocol_error(span, error),
    }
}

pub(crate) fn record_protocol_error(span: &tracing::Span, error: &ErrorData) {
    record_failure(span, mcp_error_type(error.code), MCP_PROTOCOL_ERROR_MESSAGE);
}

pub(crate) fn record_tonic_status(span: &tracing::Span, status: &tonic::Status) {
    let error_type = match decode_status_error(status) {
        DecodedStatusError::Structured(error) => error.reason,
        DecodedStatusError::Plain(_) => grpc_response_status_code(status.code()).to_string(),
    };
    record_failure(span, error_type.as_str(), MCP_TOOL_ERROR_MESSAGE);
}

pub(crate) fn record_sql_batch_partial_failure(span: &tracing::Span) {
    record_failure(
        span,
        "sql_batch_partial_failure",
        "One or more SQL queries failed",
    );
}

pub(crate) fn record_success(span: &tracing::Span) {
    span.record("status", "ok");
    span.set_status(OtelStatus::Ok);
}

pub(crate) fn record_task_id(span: &tracing::Span, task_id: &str) {
    span.record("task.id", task_id);
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_METADATA_SUMMARY};
    use opentelemetry::Value;
    use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{MCP_TOOL_ERROR_MESSAGE, call_tool_span, record_tonic_status};

    #[test]
    fn tool_call_span_does_not_record_intent_or_arguments() {
        let (exporter, provider, subscriber) = telemetry_fixture();
        let _guard = tracing::subscriber::set_default(subscriber);

        let span = call_tool_span("future_tool", None);
        span.in_scope(|| {});
        drop(span);

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let tool_call = spans
            .iter()
            .find(|span| span.name == "coral.mcp.call_tool")
            .expect("tool call span");

        assert_eq!(
            string_attribute(tool_call, "mcp.tool.name"),
            Some("future_tool".to_string())
        );
        assert_eq!(attribute(tool_call, "mcp.tool.intent"), None);
        assert!(
            tool_call
                .attributes
                .iter()
                .all(|attribute| !attribute.key.as_str().contains("argument"))
        );

        provider.shutdown().expect("provider shutdown");
    }

    #[test]
    fn tonic_error_details_are_not_recorded_on_tool_spans() {
        let (exporter, provider, subscriber) = telemetry_fixture();
        let _guard = tracing::subscriber::set_default(subscriber);
        let sentinel = "SENSITIVE_TONIC_ERROR_MARKER";

        let span = call_tool_span("list_catalog", None);
        record_tonic_status(
            &span,
            &tonic::Status::invalid_argument(format!("invalid kind: {sentinel}")),
        );
        drop(span);

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let tool_call = spans
            .iter()
            .find(|span| span.name == "coral.mcp.call_tool")
            .expect("tool call span");

        assert_eq!(
            string_attribute(tool_call, "error.type"),
            Some("INVALID_ARGUMENT".to_string())
        );
        assert_eq!(
            string_attribute(tool_call, "exception.message"),
            Some(MCP_TOOL_ERROR_MESSAGE.to_string())
        );
        assert_eq!(tool_call.status, OtelStatus::error(MCP_TOOL_ERROR_MESSAGE));
        assert!(!format!("{tool_call:?}").contains(sentinel));

        provider.shutdown().expect("provider shutdown");
    }

    #[test]
    fn structured_tonic_error_keeps_only_its_categorical_reason() {
        let (exporter, provider, subscriber) = telemetry_fixture();
        let _guard = tracing::subscriber::set_default(subscriber);
        let sentinel = "SENSITIVE_STRUCTURED_ERROR_MARKER";
        let metadata = HashMap::from([(
            CORAL_ERROR_METADATA_SUMMARY.to_string(),
            format!("summary containing {sentinel}"),
        )]);
        let status = Status::with_error_details_vec(
            Code::InvalidArgument,
            format!("fallback containing {sentinel}"),
            vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
                "INVALID_CATALOG_KIND",
                CORAL_ERROR_DOMAIN,
                metadata,
            ))],
        );

        let span = call_tool_span("list_catalog", None);
        record_tonic_status(&span, &status);
        drop(span);

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let tool_call = spans
            .iter()
            .find(|span| span.name == "coral.mcp.call_tool")
            .expect("tool call span");

        assert_eq!(
            string_attribute(tool_call, "error.type"),
            Some("INVALID_CATALOG_KIND".to_string())
        );
        assert_eq!(
            string_attribute(tool_call, "exception.message"),
            Some(MCP_TOOL_ERROR_MESSAGE.to_string())
        );
        assert_eq!(tool_call.status, OtelStatus::error(MCP_TOOL_ERROR_MESSAGE));
        assert!(!format!("{tool_call:?}").contains(sentinel));

        provider.shutdown().expect("provider shutdown");
    }

    fn telemetry_fixture() -> (
        InMemorySpanExporter,
        SdkTracerProvider,
        impl tracing::Subscriber + Send + Sync,
    ) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("mcp-error-privacy-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        (exporter, provider, subscriber)
    }

    fn attribute<'a>(span: &'a SpanData, name: &str) -> Option<&'a Value> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == name)
            .map(|attribute| &attribute.value)
    }

    fn string_attribute(span: &SpanData, name: &str) -> Option<String> {
        attribute(span, name).map(|value| value.as_str().into_owned())
    }
}
