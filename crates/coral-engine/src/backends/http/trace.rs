//! HTTP tracing helpers for request execution.

use opentelemetry::propagation::Injector;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing::field;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::backends::http::request::RequestBody;

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

#[cfg(test)]
mod tests {
    use super::{sanitize_trace_url, trace_http_endpoint};

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
}
