//! HTTP tracing helpers for request execution.

use std::time::{Duration, Instant};

use tracing::Instrument as _;
use tracing::field;

use crate::backends::http::request::RequestBody;
use crate::backends::shared::trace::{
    TraceBodyContent, TraceBodySpanKind, record_trace_body_span, trace_body_content,
};
pub(super) use crate::backends::shared::trace::{
    inject_trace_context, record_processing_error as record_http_processing_error,
    record_trace_http_endpoint, sanitize_trace_url, trace_http_endpoint,
};

const HTTP_BODY_CAPTURE_IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const HTTP_BODY_CAPTURE_TOTAL_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnconsumedTraceBody {
    content: TraceBodyContent,
    complete_body_size: Option<usize>,
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
        record_trace_body_span(span, TraceBodySpanKind::HttpRequest, request_id, &content);
    }

    pub(super) fn record_response(&self, span: &tracing::Span, request_id: u64, body: &str) {
        let Some(max_bytes) = self.enabled_max_bytes() else {
            return;
        };
        record_trace_body_span(
            span,
            TraceBodySpanKind::HttpResponse,
            request_id,
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
            record_trace_body_span(
                response_span,
                TraceBodySpanKind::HttpResponse,
                request_id,
                &body.content,
            );
        }
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
    use serde_json::json;

    use super::{HttpBodyCapture, trace_request_body_content};
    use crate::backends::http::request::RequestBody;
    use crate::backends::shared::trace::test_support::{
        TraceCapture, span_attr_bool as span_bool_attr, span_attr_string as span_string_attr,
    };

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
    fn body_capture_emits_child_span_with_preview_attributes() {
        let capture = TraceCapture::install("body-capture-test");
        {
            let parent = tracing::info_span!(target: "coral_engine::http", "http.request");
            let _entered = parent.enter();
            let capture = HttpBodyCapture::new(Some(4));
            capture.record_request(&parent, 7, Some(&RequestBody::Text("abcdef".to_string())));
        }

        let spans = capture.finished_spans();
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
        capture.shutdown();
    }
}
