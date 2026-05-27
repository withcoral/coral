//! HTTP tracing helpers for request execution.

use std::time::{Duration, Instant};

use opentelemetry::propagation::Injector;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing::Instrument as _;
use tracing::field;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::backends::http::request::RequestBody;

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

pub(super) fn record_http_processing_error(
    span: &tracing::Span,
    error_type: &'static str,
    message: impl std::fmt::Display,
) {
    span.record("error", true);
    span.record("otel.status_code", "error");
    span.record("error.type", error_type);
    span.record("otel.status_description", field::display(&message));
    span.record("exception.message", field::display(&message));
}

pub(super) fn sanitize_trace_url(raw: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw) else {
        let without_fragment = raw.split_once('#').map_or(raw, |(before, _)| before);
        let without_query = without_fragment
            .split_once('?')
            .map_or(without_fragment, |(before, _)| before);
        return strip_userinfo_from_unparsed_url(without_query);
    };
    url.set_query(None);
    url.set_fragment(None);
    #[expect(
        clippy::let_underscore_must_use,
        reason = "set_username/set_password only fail for cannot-be-a-base URLs; HTTP URLs always have a host"
    )]
    let _ = url.set_username("");
    #[expect(
        clippy::let_underscore_must_use,
        reason = "set_username/set_password only fail for cannot-be-a-base URLs; HTTP URLs always have a host"
    )]
    let _ = url.set_password(None);
    url.to_string()
}

fn strip_userinfo_from_unparsed_url(raw: &str) -> String {
    let Some(authority_start) = authority_start(raw) else {
        return raw.to_string();
    };
    let Some(prefix) = raw.get(..authority_start) else {
        return raw.to_string();
    };
    let Some(after_authority_start) = raw.get(authority_start..) else {
        return raw.to_string();
    };
    let authority_len = after_authority_start
        .find('/')
        .unwrap_or(after_authority_start.len());
    let Some(authority) = after_authority_start.get(..authority_len) else {
        return raw.to_string();
    };
    let Some(userinfo_end) = authority.rfind('@') else {
        return raw.to_string();
    };
    let Some(authority_without_userinfo) = authority.get(userinfo_end + 1..) else {
        return raw.to_string();
    };
    let Some(suffix) = after_authority_start.get(authority_len..) else {
        return raw.to_string();
    };

    format!("{prefix}{authority_without_userinfo}{suffix}")
}

fn authority_start(raw: &str) -> Option<usize> {
    raw.find("://")
        .map(|scheme_end| scheme_end + 3)
        .or_else(|| raw.starts_with("//").then_some(2))
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct TraceHttpEndpoint {
    server_address: Option<String>,
    server_port: Option<u16>,
}

pub(super) fn trace_http_endpoint(raw: &str) -> TraceHttpEndpoint {
    let Ok(url) = reqwest::Url::parse(raw) else {
        return TraceHttpEndpoint::default();
    };
    TraceHttpEndpoint {
        server_address: url.host_str().map(str::to_string),
        server_port: url.port_or_known_default(),
    }
}

pub(super) fn record_trace_http_endpoint(span: &tracing::Span, endpoint: &TraceHttpEndpoint) {
    if let Some(address) = &endpoint.server_address {
        span.record("server.address", address.as_str());
        span.record("peer.service", address.as_str());
        span.record("http.host", address.as_str());
        span.record("net.peer.name", address.as_str());
    }
    if let Some(port) = endpoint.server_port {
        span.record("server.port", i64::from(port));
    }
}

struct HeaderMapInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = HeaderName::try_from(key)
            && let Ok(value) = HeaderValue::try_from(value)
        {
            self.0.insert(name, value);
        }
    }
}

pub(super) fn inject_trace_context(span: &tracing::Span, headers: &mut HeaderMap) {
    let cx = span.context();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut HeaderMapInjector(headers));
    });
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

    use super::{
        HttpBodyCapture, sanitize_trace_url, trace_body_content, trace_http_endpoint,
        trace_request_body_content,
    };
    use crate::backends::http::request::RequestBody;

    #[test]
    fn sanitize_trace_url_removes_userinfo_when_url_parses() {
        assert_eq!(
            sanitize_trace_url("https://user:secret@api.example.com/v1/items?token=hidden"),
            "https://api.example.com/v1/items"
        );
    }

    #[test]
    fn sanitize_trace_url_removes_userinfo_when_url_does_not_parse() {
        assert_eq!(
            sanitize_trace_url("https://user:secret@bad host/v1/items?token=hidden"),
            "https://bad host/v1/items"
        );
    }

    #[test]
    fn trace_http_endpoint_extracts_host_and_port() {
        let endpoint = trace_http_endpoint("https://api.example.com/v1/items");
        assert_eq!(endpoint.server_address.as_deref(), Some("api.example.com"));
        assert_eq!(endpoint.server_port, Some(443));

        let endpoint = trace_http_endpoint("http://localhost:8080/v1/items");
        assert_eq!(endpoint.server_address.as_deref(), Some("localhost"));
        assert_eq!(endpoint.server_port, Some(8080));
    }

    #[test]
    fn trace_http_endpoint_ignores_unparseable_urls() {
        let endpoint = trace_http_endpoint("/v1/items");
        assert!(endpoint.server_address.is_none());
        assert!(endpoint.server_port.is_none());
    }

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
