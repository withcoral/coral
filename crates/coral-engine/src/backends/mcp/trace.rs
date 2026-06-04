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

use crate::backends::shared::trace::{
    TraceBodySpanKind, record_trace_body_span, trace_body_content,
};

use super::error::McpProviderQueryError;

/// Monotonic per-process counter for the `coral.mcp.request_id` span
/// attribute. Lets a trace consumer correlate the parent `mcp.tool.call`
/// span with its body-preview child spans.
static NEXT_MCP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_mcp_request_id() -> u64 {
    NEXT_MCP_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
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
        record_trace_body_span(span, TraceBodySpanKind::McpRequest, request_id, &content);
    }

    pub(super) fn record_response(&self, span: &tracing::Span, request_id: u64, payload: &Value) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        let Ok(body) = serde_json::to_string(payload) else {
            return;
        };
        let content = trace_body_content(&body, max_bytes);
        record_trace_body_span(span, TraceBodySpanKind::McpResponse, request_id, &content);
    }
}

/// Stable span `error.type` label for an [`McpProviderQueryError`].
/// Returns the same canonical reason string that
/// `McpProviderQueryError::to_structured` emits, so a span's `error.type`
/// matches the query error reason that ends up on the root invocation span.
pub(super) fn mcp_error_type(error: &McpProviderQueryError) -> &'static str {
    error.reason()
}

#[cfg(test)]
mod tests {
    use rmcp::model::JsonObject;
    use serde_json::{Value, json};

    use super::McpBodyCapture;
    use crate::backends::shared::trace::test_support::{
        TraceCapture, span_attr_bool as span_bool_attr, span_attr_string as span_string_attr,
    };

    #[test]
    fn body_capture_emits_child_spans_for_request_and_response() {
        let capture = TraceCapture::install("mcp-body-capture-test");
        {
            let parent = tracing::info_span!(target: "coral_engine::mcp", "mcp.tool.call");
            let _entered = parent.enter();
            let capture = McpBodyCapture::new(Some(6));
            let mut arguments = JsonObject::new();
            arguments.insert("state".to_string(), Value::String("open".to_string()));
            capture.record_request(&parent, 9, &arguments);
            capture.record_response(&parent, 9, &json!({"issues": [{"title": "x"}]}));
        }

        let spans = capture.finished_spans();
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
        capture.shutdown();
    }
}
