//! HTTP tracing helpers for request execution.

use std::time::{Duration, Instant};

use tracing::Instrument as _;
use tracing::field;

use crate::backends::http::request::RequestBody;
pub(super) use crate::backends::shared::trace::{
    inject_trace_context, record_processing_error as record_http_processing_error,
    record_trace_http_endpoint, sanitize_trace_url, trace_http_endpoint,
};

const HTTP_BODY_CAPTURE_IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const HTTP_BODY_CAPTURE_TOTAL_TIMEOUT: Duration = Duration::from_millis(200);
const HTTP_BODY_TRACE_TARGET: &str = "coral.http.body";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceBodyContent {
    body: String,
    truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnconsumedTraceBody {
    content: TraceBodyContent,
    complete_body_size: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpBodyDirection {
    Request,
    Response,
}

impl HttpBodyDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct HttpBodyCapture {
    max_bytes: Option<usize>,
}

impl HttpBodyCapture {
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
        body: Option<&RequestBody>,
    ) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        let Some(content) = trace_request_body_content(body, max_bytes) else {
            return;
        };
        Self::record(span, request_id, HttpBodyDirection::Request, &content);
    }

    pub(super) fn record_response(&self, span: &tracing::Span, request_id: u64, body: &str) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        Self::record(
            span,
            request_id,
            HttpBodyDirection::Response,
            &trace_body_content(body, max_bytes),
        );
    }

    pub(super) async fn record_unconsumed_response(
        &self,
        response_span: &tracing::Span,
        request_id: u64,
        response: reqwest::Response,
    ) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        if let Ok(body) = read_unconsumed_response_body(response, max_bytes)
            .instrument(response_span.clone())
            .await
        {
            if let Some(body_size) = body.complete_body_size {
                response_span.record(
                    "http.response.body.size",
                    i64::try_from(body_size).unwrap_or(i64::MAX),
                );
            }
            Self::record(
                response_span,
                request_id,
                HttpBodyDirection::Response,
                &body.content,
            );
        }
    }

    fn record(
        span: &tracing::Span,
        request_id: u64,
        direction: HttpBodyDirection,
        content: &TraceBodyContent,
    ) {
        span.in_scope(|| match direction {
            HttpBodyDirection::Request => {
                let body_span = tracing::trace_span!(
                    target: HTTP_BODY_TRACE_TARGET,
                    "coral.http.request.body",
                    coral.http.request_id = request_id,
                    coral.http.body.direction = direction.as_str(),
                    coral.http.request.body = content.body.as_str(),
                    coral.http.request.body.truncated = content.truncated,
                );
                body_span.in_scope(|| {});
            }
            HttpBodyDirection::Response => {
                let body_span = tracing::trace_span!(
                    target: HTTP_BODY_TRACE_TARGET,
                    "coral.http.response.body",
                    coral.http.request_id = request_id,
                    coral.http.body.direction = direction.as_str(),
                    coral.http.response.body = content.body.as_str(),
                    coral.http.response.body.truncated = content.truncated,
                );
                body_span.in_scope(|| {});
            }
        });
    }
}

pub(super) fn trace_reqwest_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "source API request timed out"
    } else if error.is_connect() {
        "source API connection failed"
    } else if error.is_request() {
        "source API request failed before a response was received"
    } else {
        "source API request failed"
    }
}

pub(super) fn trace_reqwest_error_type(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "TIMEOUT"
    } else if error.is_connect() {
        "CONNECT"
    } else if error.is_request() {
        "REQUEST"
    } else {
        "OTHER"
    }
}

pub(super) fn record_http_status_error(
    span: &tracing::Span,
    status: reqwest::StatusCode,
    message: impl std::fmt::Display,
) {
    span.record("error", true);
    span.record("otel.status_code", "error");
    span.record("error.type", field::display(status.as_u16()));
    span.record("otel.status_description", field::display(&message));
    span.record("exception.message", field::display(&message));
}

pub(super) fn request_body_size(body: Option<&RequestBody>) -> Option<usize> {
    match body {
        Some(RequestBody::Json(value)) => serde_json::to_vec(value).ok().map(|body| body.len()),
        Some(RequestBody::Text(text)) => Some(text.len()),
        None => None,
    }
}

fn trace_request_body_content(
    body: Option<&RequestBody>,
    max_bytes: usize,
) -> Option<TraceBodyContent> {
    let body = match body? {
        RequestBody::Json(value) => serde_json::to_string(value).ok()?,
        RequestBody::Text(text) => text.clone(),
    };
    Some(trace_body_content(&body, max_bytes))
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

async fn read_unconsumed_response_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> reqwest::Result<UnconsumedTraceBody> {
    let read_limit = max_bytes.saturating_add(1);
    let complete_body_size = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok());
    let read_started_at = Instant::now();
    let mut bytes = Vec::new();
    while bytes.len() < read_limit {
        if complete_body_size.is_some_and(|body_size| bytes.len() >= body_size) {
            return Ok(trace_body_from_bytes(
                &bytes,
                max_bytes,
                Some(bytes.len()),
                false,
            ));
        }
        let Some(total_remaining) =
            HTTP_BODY_CAPTURE_TOTAL_TIMEOUT.checked_sub(read_started_at.elapsed())
        else {
            return Ok(trace_body_from_bytes(&bytes, max_bytes, None, true));
        };
        if total_remaining.is_zero() {
            return Ok(trace_body_from_bytes(&bytes, max_bytes, None, true));
        }
        let chunk_timeout = HTTP_BODY_CAPTURE_IDLE_TIMEOUT.min(total_remaining);
        let chunk = match tokio::time::timeout(chunk_timeout, response.chunk()).await {
            Ok(chunk) => chunk?,
            Err(_elapsed) => {
                return Ok(trace_body_from_bytes(&bytes, max_bytes, None, true));
            }
        };
        let Some(chunk) = chunk else {
            return Ok(trace_body_from_bytes(
                &bytes,
                max_bytes,
                Some(bytes.len()),
                false,
            ));
        };
        let remaining = read_limit.saturating_sub(bytes.len());
        let take = chunk.len().min(remaining);
        bytes.extend_from_slice(
            chunk
                .get(..take)
                .expect("chunk capture length is bounded by chunk length"),
        );
    }

    Ok(trace_body_from_bytes(&bytes, max_bytes, None, true))
}

fn trace_body_from_bytes(
    bytes: &[u8],
    max_bytes: usize,
    complete_body_size: Option<usize>,
    force_truncated: bool,
) -> UnconsumedTraceBody {
    let body_len = bytes.len().min(max_bytes);
    let body = String::from_utf8_lossy(
        bytes
            .get(..body_len)
            .expect("body capture length is bounded by buffer length"),
    );
    let mut content = trace_body_content(body.as_ref(), max_bytes);
    if force_truncated {
        content.truncated = true;
    }
    UnconsumedTraceBody {
        content,
        complete_body_size,
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::Value as OtelValue;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use serde_json::json;
    use tracing_subscriber::layer::SubscriberExt;

    use super::{HttpBodyCapture, trace_body_content, trace_request_body_content};
    use crate::backends::http::request::RequestBody;

    #[test]
    fn trace_request_body_content_records_compact_json() {
        let body = RequestBody::Json(json!({
            "query": "query { viewer { login } }",
            "variables": { "first": 10 }
        }));

        let content = trace_request_body_content(Some(&body), 1024).expect("body content");

        assert_eq!(
            content.body,
            r#"{"query":"query { viewer { login } }","variables":{"first":10}}"#
        );
        assert!(!content.truncated);
    }

    #[test]
    fn trace_body_content_truncates_on_utf8_boundary() {
        let content = trace_body_content("a💚b", 3);

        assert_eq!(content.body, "a");
        assert!(content.truncated);
    }

    #[test]
    fn body_capture_emits_child_span_with_preview_attributes() {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let tracer = provider.tracer("body-capture-test");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_target(true)
            .with_level(true);
        let subscriber = tracing_subscriber::Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!(target: "coral_engine::http", "http.request");
            let _entered = parent.enter();
            let capture = HttpBodyCapture::new(Some(4));
            capture.record_request(&parent, 7, Some(&RequestBody::Text("abcdef".to_string())));
        });
        provider.force_flush().expect("flush spans");

        let spans = memory.get_finished_spans().expect("finished spans");
        let body = spans
            .iter()
            .find(|span| span.name == "coral.http.request.body")
            .expect("body span");
        assert_eq!(
            span_string_attr(body, "coral.http.request.body").as_deref(),
            Some("abcd")
        );
        assert_eq!(
            span_string_attr(body, "target").as_deref(),
            Some("coral.http.body")
        );
        assert_eq!(
            span_string_attr(body, "coral.http.body.direction").as_deref(),
            Some("request")
        );
        assert_eq!(
            span_string_attr(body, "coral.http.request_id").as_deref(),
            Some("7")
        );
        assert_eq!(
            span_bool_attr(body, "coral.http.request.body.truncated"),
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
