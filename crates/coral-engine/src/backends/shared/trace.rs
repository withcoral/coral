//! Backend-agnostic tracing helpers.
//!
//! Both the HTTP and MCP Streamable HTTP transports share the same span
//! conventions (OpenTelemetry HTTP client semantic conventions for the
//! endpoint and W3C trace-context propagation). These helpers keep the
//! conventions consistent across backends; backend-specific extras (body
//! extraction, reqwest error classification, MCP error classification) live
//! in each backend's own `trace` module.

use opentelemetry::propagation::Injector;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing::field;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Mark a span as errored with a generic processing failure.
///
/// Records the OpenTelemetry status fields plus an `exception.message`
/// attribute. `error_type` is a stable category label (e.g. `INITIALIZE`,
/// `TOOL_CALL`, `DECODE`) — distinct from the human-readable `message`.
pub(crate) fn record_processing_error(
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

/// Strip userinfo, query string, and fragment from a URL before recording
/// it on a span. Falls back to manual parsing if the URL isn't valid so
/// even malformed URLs don't leak secrets.
pub(crate) fn sanitize_trace_url(raw: &str) -> String {
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

/// HTTP endpoint attributes derived from a request URL, ready for span
/// recording.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TraceHttpEndpoint {
    server_address: Option<String>,
    server_port: Option<u16>,
}

pub(crate) fn trace_http_endpoint(raw: &str) -> TraceHttpEndpoint {
    let Ok(url) = reqwest::Url::parse(raw) else {
        return TraceHttpEndpoint::default();
    };
    TraceHttpEndpoint {
        server_address: url.host_str().map(str::to_string),
        server_port: url.port_or_known_default(),
    }
}

pub(crate) fn record_trace_http_endpoint(span: &tracing::Span, endpoint: &TraceHttpEndpoint) {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceBodyContent {
    pub(crate) body: String,
    pub(crate) truncated: bool,
}

pub(crate) fn trace_body_content(body: &str, max_bytes: usize) -> TraceBodyContent {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceBodySpanKind {
    HttpRequest,
    HttpResponse,
    McpRequest,
    McpResponse,
}

pub(crate) fn record_trace_body_span(
    span: &tracing::Span,
    kind: TraceBodySpanKind,
    request_id: u64,
    content: &TraceBodyContent,
) {
    span.in_scope(|| match kind {
        TraceBodySpanKind::HttpRequest => {
            let body_span = tracing::trace_span!(
                target: "coral.http.body",
                "coral.http.request.body",
                coral.http.request_id = request_id,
                coral.http.body.direction = "request",
                coral.http.request.body = content.body.as_str(),
                coral.http.request.body.truncated = content.truncated,
            );
            body_span.in_scope(|| {});
        }
        TraceBodySpanKind::HttpResponse => {
            let body_span = tracing::trace_span!(
                target: "coral.http.body",
                "coral.http.response.body",
                coral.http.request_id = request_id,
                coral.http.body.direction = "response",
                coral.http.response.body = content.body.as_str(),
                coral.http.response.body.truncated = content.truncated,
            );
            body_span.in_scope(|| {});
        }
        TraceBodySpanKind::McpRequest => {
            let body_span = tracing::trace_span!(
                target: "coral.mcp.body",
                "coral.mcp.request.body",
                coral.mcp.request_id = request_id,
                coral.mcp.body.direction = "request",
                coral.mcp.request.body = content.body.as_str(),
                coral.mcp.request.body.truncated = content.truncated,
            );
            body_span.in_scope(|| {});
        }
        TraceBodySpanKind::McpResponse => {
            let body_span = tracing::trace_span!(
                target: "coral.mcp.body",
                "coral.mcp.response.body",
                coral.mcp.request_id = request_id,
                coral.mcp.body.direction = "response",
                coral.mcp.response.body = content.body.as_str(),
                coral.mcp.response.body.truncated = content.truncated,
            );
            body_span.in_scope(|| {});
        }
    });
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

/// Inject the current span's W3C trace context into the supplied header
/// map. Used to propagate the trace into downstream HTTP services.
pub(crate) fn inject_trace_context(span: &tracing::Span, headers: &mut HeaderMap) {
    let cx = span.context();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut HeaderMapInjector(headers));
    });
}

#[cfg(test)]
pub(crate) mod test_support {
    use opentelemetry::Value as OtelValue;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::layer::SubscriberExt;

    pub(crate) struct TraceCapture {
        memory: InMemorySpanExporter,
        provider: SdkTracerProvider,
        _guard: DefaultGuard,
    }

    impl TraceCapture {
        pub(crate) fn install(tracer_name: &'static str) -> Self {
            let memory = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(memory.clone())
                .build();
            let tracer = provider.tracer(tracer_name);
            let layer = tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_target(true)
                .with_level(true);
            let subscriber = tracing_subscriber::Registry::default().with(layer);
            let guard = tracing::subscriber::set_default(subscriber);
            Self {
                memory,
                provider,
                _guard: guard,
            }
        }

        pub(crate) fn finished_spans(&self) -> Vec<SpanData> {
            self.provider.force_flush().expect("flush spans");
            self.memory.get_finished_spans().expect("finished spans")
        }

        pub(crate) fn shutdown(self) {
            self.provider.shutdown().expect("provider shutdown");
        }
    }

    pub(crate) fn span_attr_string(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                OtelValue::String(value) => Some(value.to_string()),
                OtelValue::I64(value) => Some(value.to_string()),
                _ => None,
            })
    }

    pub(crate) fn span_attr_bool(span: &SpanData, key: &str) -> Option<bool> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                OtelValue::Bool(value) => Some(*value),
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{sanitize_trace_url, trace_body_content, trace_http_endpoint};

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
    fn trace_body_content_truncates_on_utf8_boundary() {
        let content = trace_body_content("a💚b", 3);

        assert_eq!(content.body, "a");
        assert!(content.truncated);
    }
}
