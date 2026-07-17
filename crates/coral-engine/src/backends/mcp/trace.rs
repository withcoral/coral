//! MCP-specific tracing helpers for tool-call execution.
//!
//! Mirrors the shape of `backends::http::trace`: an opt-in body capture
//! emits child preview spans for request arguments and response payloads,
//! and an error classifier maps [`McpProviderQueryError`] variants to
//! stable span error labels. Generic helpers (URL sanitization, endpoint
//! attributes, W3C trace-context propagation) come from
//! `backends::shared::trace`.

use std::sync::atomic::{AtomicU64, Ordering};

use rmcp::model::JsonObject;
use serde_json::Value;

use super::error::McpProviderQueryError;

pub(super) const MCP_BODY_TRACE_TARGET: &str = "coral.mcp.body";

/// Monotonic per-process counter for the `coral.mcp.request_id` span
/// attribute. Lets a trace consumer correlate the parent `mcp.tool.call`
/// span with its body-preview child spans.
static NEXT_MCP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_mcp_request_id() -> u64 {
    NEXT_MCP_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceBodyContent {
    body: String,
    truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpBodyDirection {
    Request,
    Response,
}

impl McpBodyDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

/// Opt-in capture for MCP tool arguments (request) and result payloads
/// (response). When configured, [`record_request`] and [`record_response`]
/// emit child `coral.mcp.request.body` / `coral.mcp.response.body` trace
/// spans carrying a UTF-8-safe preview truncated to `max_bytes` bytes.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct McpBodyCapture {
    max_bytes: Option<usize>,
}

impl McpBodyCapture {
    pub(super) fn new(max_bytes: Option<usize>) -> Self {
        Self { max_bytes }
    }

    fn enabled_max_bytes(&self) -> Option<usize> {
        self.max_bytes
    }

    pub(super) fn record_request(
        &self,
        span: &tracing::Span,
        request_id: u64,
        arguments: &JsonObject,
    ) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        let Ok(body) = serde_json::to_string(arguments) else {
            return;
        };
        let content = trace_body_content(&body, max_bytes);
        Self::record(span, request_id, McpBodyDirection::Request, &content);
    }

    pub(super) fn record_response(&self, span: &tracing::Span, request_id: u64, payload: &Value) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        let Ok(body) = serde_json::to_string(payload) else {
            return;
        };
        let content = trace_body_content(&body, max_bytes);
        Self::record(span, request_id, McpBodyDirection::Response, &content);
    }

    fn record(
        span: &tracing::Span,
        request_id: u64,
        direction: McpBodyDirection,
        content: &TraceBodyContent,
    ) {
        span.in_scope(|| match direction {
            McpBodyDirection::Request => {
                let body_span = tracing::trace_span!(
                    target: MCP_BODY_TRACE_TARGET,
                    "coral.mcp.request.body",
                    coral.mcp.request_id = request_id,
                    coral.mcp.body.direction = direction.as_str(),
                    coral.mcp.request.body = content.body.as_str(),
                    coral.mcp.request.body.truncated = content.truncated,
                );
                body_span.in_scope(|| {});
            }
            McpBodyDirection::Response => {
                let body_span = tracing::trace_span!(
                    target: MCP_BODY_TRACE_TARGET,
                    "coral.mcp.response.body",
                    coral.mcp.request_id = request_id,
                    coral.mcp.body.direction = direction.as_str(),
                    coral.mcp.response.body = content.body.as_str(),
                    coral.mcp.response.body.truncated = content.truncated,
                );
                body_span.in_scope(|| {});
            }
        });
    }
}

fn trace_body_content(body: &str, max_bytes: usize) -> TraceBodyContent {
    if body.len() <= max_bytes {
        return TraceBodyContent {
            body: body.to_string(),
            truncated: false,
        };
    }

    let mut end = max_bytes;
    while !body.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    TraceBodyContent {
        body: body
            .get(..end)
            .expect("trace body truncation end is a UTF-8 boundary")
            .to_string(),
        truncated: true,
    }
}

/// Stable span `error.type` label for an [`McpProviderQueryError`].
/// Returns the same canonical reason string that
/// `McpProviderQueryError::to_structured` emits, so a span's `error.type`
/// matches the query error reason that ends up on the root invocation span.
pub(super) fn mcp_error_type(error: &McpProviderQueryError) -> &'static str {
    match error {
        McpProviderQueryError::MissingRequiredFilter { .. } => "MISSING_REQUIRED_FILTER",
        McpProviderQueryError::MissingRequiredFunctionArg { .. } => "MISSING_REQUIRED_FUNCTION_ARG",
        McpProviderQueryError::ServerStart { .. } => "MCP_SERVER_START_FAILED",
        McpProviderQueryError::Initialize { .. } => "MCP_INITIALIZE_FAILED",
        McpProviderQueryError::AuthRequired { .. } => "MCP_AUTH_REQUIRED",
        McpProviderQueryError::AuthFailed { .. } => "MCP_AUTH_FAILED",
        McpProviderQueryError::ToolCall { .. } => "MCP_TOOL_CALL_FAILED",
        McpProviderQueryError::ToolReturnedError { .. } => "MCP_TOOL_RETURNED_ERROR",
        McpProviderQueryError::ResultDecode { .. } => "MCP_RESULT_DECODE_FAILED",
        McpProviderQueryError::Pagination { .. } => "MCP_PAGINATION_FAILED",
        McpProviderQueryError::HttpRequestFailed { .. } => "MCP_HTTP_REQUEST_FAILED",
        McpProviderQueryError::HttpStatusFailed { .. } => "MCP_HTTP_STATUS_FAILED",
        McpProviderQueryError::HttpSseDecodeFailed { .. } => "MCP_HTTP_SSE_DECODE_FAILED",
        McpProviderQueryError::SessionExpired { .. } => "MCP_SESSION_EXPIRED",
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::Value as OtelValue;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use rmcp::model::JsonObject;
    use serde_json::{Value, json};
    use tracing_subscriber::layer::SubscriberExt;

    use super::{McpBodyCapture, trace_body_content};

    #[test]
    fn trace_body_content_truncates_on_utf8_boundary() {
        let content = trace_body_content("a💚b", 3);

        assert_eq!(content.body, "a");
        assert!(content.truncated);
    }

    #[test]
    fn body_capture_emits_child_spans_for_request_and_response() {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let tracer = provider.tracer("mcp-body-capture-test");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_target(true)
            .with_level(true);
        let subscriber = tracing_subscriber::Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!(target: "coral_engine::mcp", "mcp.tool.call");
            let _entered = parent.enter();
            let capture = McpBodyCapture::new(Some(6));
            let mut arguments = JsonObject::new();
            arguments.insert("state".to_string(), Value::String("open".to_string()));
            capture.record_request(&parent, 9, &arguments);
            capture.record_response(&parent, 9, &json!({"issues": [{"title": "x"}]}));
        });
        provider.force_flush().expect("flush spans");

        let spans = memory.get_finished_spans().expect("finished spans");
        let request = spans
            .iter()
            .find(|span| span.name == "coral.mcp.request.body")
            .expect("request body span");
        assert_eq!(
            span_string_attr(request, "coral.mcp.body.direction").as_deref(),
            Some("request")
        );
        assert_eq!(
            span_string_attr(request, "coral.mcp.request_id").as_deref(),
            Some("9")
        );
        assert_eq!(
            span_string_attr(request, "coral.mcp.request.body").as_deref(),
            Some(r#"{"stat"#),
        );
        assert_eq!(
            span_bool_attr(request, "coral.mcp.request.body.truncated"),
            Some(true)
        );

        let response = spans
            .iter()
            .find(|span| span.name == "coral.mcp.response.body")
            .expect("response body span");
        assert_eq!(
            span_string_attr(response, "coral.mcp.body.direction").as_deref(),
            Some("response")
        );
        assert_eq!(
            span_bool_attr(response, "coral.mcp.response.body.truncated"),
            Some(true)
        );
        provider.shutdown().expect("provider shutdown");
    }

    fn span_string_attr(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                OtelValue::String(value) => Some(value.to_string()),
                OtelValue::I64(value) => Some(value.to_string()),
                _ => None,
            })
    }

    fn span_bool_attr(span: &SpanData, key: &str) -> Option<bool> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                OtelValue::Bool(value) => Some(*value),
                _ => None,
            })
    }
}
