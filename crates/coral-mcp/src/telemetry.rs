//! OpenTelemetry helpers for MCP protocol spans.

use std::collections::HashMap;
use std::future::Future;

use coral_api::grpc_response_status_code;
use coral_client::{DecodedStatusError, decode_status_error};
use opentelemetry::{propagation::Extractor, trace::Status as OtelStatus};
use rmcp::{ErrorData, model::ErrorCode};
use serde_json::{Map as JsonMap, Value as JsonValue};
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
        code_mode.cell_id = field::Empty,
        code_mode.source_length = field::Empty,
        mcp.method = "tools/call",
        mcp.tool.name = tool_name,
        otel.kind = "server",
        otel.name = "coral.mcp.call_tool",
        status = field::Empty,
    );
    apply_trace_parent(&span, trace_parent);
    span
}

pub(crate) fn record_tool_request_details(
    span: &tracing::Span,
    tool_name: &str,
    arguments: Option<&JsonMap<String, JsonValue>>,
    code_mode_enabled: bool,
) {
    if tool_name != "exec" || !code_mode_enabled {
        return;
    }
    let Some(source) = arguments
        .and_then(|arguments| arguments.get("source"))
        .and_then(JsonValue::as_str)
    else {
        return;
    };
    span.record(
        "code_mode.source_length",
        i64::try_from(source.len()).unwrap_or(i64::MAX),
    );
}

#[cfg(feature = "code-mode")]
pub(crate) fn record_code_mode_cell_id(span: &tracing::Span, cell_id: &str) {
    span.record("code_mode.cell_id", cell_id);
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

#[cfg(test)]
mod tests {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::{KeyValue, Value as OtelValue};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde_json::json;
    use tracing_subscriber::prelude::*;

    #[cfg(feature = "code-mode")]
    use super::record_code_mode_cell_id;
    use super::{call_tool_span, record_tool_request_details};

    #[cfg(feature = "code-mode")]
    #[test]
    fn call_tool_exec_span_records_source_length_and_cell_id() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("coral-mcp-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let source = "return { ok: true };";
        let arguments = json!({ "source": source });
        let arguments = arguments.as_object().expect("object arguments");

        tracing::subscriber::with_default(subscriber, || {
            let span = call_tool_span("exec", None);
            record_tool_request_details(&span, "exec", Some(arguments), true);
            record_code_mode_cell_id(&span, "1");
            let _entered = span.enter();
        });

        provider.force_flush().expect("spans should flush");
        let spans = exporter
            .get_finished_spans()
            .expect("finished spans should be readable");
        let span = spans
            .iter()
            .find(|span| span.name == "coral.mcp.call_tool")
            .expect("call tool span should export");

        assert_eq!(string_attr(&span.attributes, "mcp.tool.name"), Some("exec"));
        assert_eq!(string_attr(&span.attributes, "code_mode.source"), None);
        assert_eq!(
            i64_attr(&span.attributes, "code_mode.source_length"),
            i64::try_from(source.len()).ok()
        );
        assert_eq!(
            string_attr(&span.attributes, "code_mode.cell_id"),
            Some("1")
        );
        provider.shutdown().expect("provider shutdown");
    }

    #[test]
    fn call_tool_exec_span_omits_source_when_code_mode_runtime_disabled() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("coral-mcp-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let arguments = json!({ "source": "return { ok: true };" });
        let arguments = arguments.as_object().expect("object arguments");

        tracing::subscriber::with_default(subscriber, || {
            let span = call_tool_span("exec", None);
            record_tool_request_details(&span, "exec", Some(arguments), false);
            let _entered = span.enter();
        });

        provider.force_flush().expect("spans should flush");
        let spans = exporter
            .get_finished_spans()
            .expect("finished spans should be readable");
        let span = spans
            .iter()
            .find(|span| span.name == "coral.mcp.call_tool")
            .expect("call tool span should export");

        assert_eq!(string_attr(&span.attributes, "mcp.tool.name"), Some("exec"));
        assert_eq!(string_attr(&span.attributes, "code_mode.source"), None);
        assert_eq!(i64_attr(&span.attributes, "code_mode.source_length"), None);
        provider.shutdown().expect("provider shutdown");
    }

    fn i64_attr(attributes: &[KeyValue], key: &str) -> Option<i64> {
        attributes.iter().find_map(|attribute| {
            if attribute.key.as_str() == key
                && let OtelValue::I64(value) = &attribute.value
            {
                Some(*value)
            } else {
                None
            }
        })
    }

    fn string_attr<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a str> {
        attributes.iter().find_map(|attribute| {
            if attribute.key.as_str() == key
                && let OtelValue::String(value) = &attribute.value
            {
                Some(value.as_ref())
            } else {
                None
            }
        })
    }
}
