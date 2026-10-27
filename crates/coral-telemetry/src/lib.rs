//! Cross-crate telemetry helpers.
//!
//! This crate owns transport-neutral W3C propagation, safe telemetry utilities
//! shared by application, domain, and adapter crates, and the filter that
//! decides what spans may leave the host.

mod export;

pub use export::OtlpSpanFilter;

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator as _};
use opentelemetry::trace::{Status as OtelStatus, TraceContextExt as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Safe lowercase W3C trace identifier used for cross-crate correlation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceId(String);

impl TraceId {
    /// Parses one lowercase nonzero W3C trace identifier.
    ///
    /// # Errors
    ///
    /// Returns a safe static message when the identifier is not exactly 32
    /// lowercase hexadecimal characters or is the invalid all-zero value.
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        if value.len() != 32
            || value == "00000000000000000000000000000000"
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("trace id must be a lowercase nonzero 32-character W3C trace id");
        }
        Ok(Self(value.to_string()))
    }

    /// Uses the current trace, or starts correlation for uninstrumented host work.
    /// This does not configure or require an exporter.
    #[must_use]
    pub fn current_or_new() -> Self {
        use opentelemetry_sdk::trace::IdGenerator as _;
        let context = tracing::Span::current().context();
        let span = context.span();
        let id = if span.span_context().is_valid() {
            span.span_context().trace_id()
        } else {
            opentelemetry_sdk::trace::RandomIdGenerator::default().new_trace_id()
        };
        Self(id.to_string())
    }

    /// Returns the safe trace identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

const TRACEPARENT_HEADER: &str = "traceparent";
const TRACESTATE_HEADER: &str = "tracestate";

/// Stable message recorded for failed gRPC requests.
pub const GRPC_REQUEST_ERROR_MESSAGE: &str = "gRPC request failed";

/// Prefix for host-only span attributes that must not be exported over OTLP.
pub const LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX: &str = "coral.local.";
/// Records a categorical failure without accepting caller-controlled message text.
pub fn record_failure(
    span: &tracing::Span,
    error_type: impl AsRef<str>,
    stable_message: &'static str,
) {
    span.record("status", "error");
    span.record("error.type", error_type.as_ref());
    span.record("exception.message", stable_message);
    span.set_status(OtelStatus::error(stable_message));
}

struct TraceHeaders<'a> {
    traceparent: &'a str,
    tracestate: Option<&'a str>,
}

impl Extractor for TraceHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            TRACEPARENT_HEADER => Some(self.traceparent),
            TRACESTATE_HEADER => self.tracestate,
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        if self.tracestate.is_some() {
            vec![TRACEPARENT_HEADER, TRACESTATE_HEADER]
        } else {
            vec![TRACEPARENT_HEADER]
        }
    }
}

fn context_from_trace_headers(
    traceparent: Option<&str>,
    tracestate: Option<&str>,
) -> Option<Context> {
    let traceparent = traceparent?;
    context_from_extractor(&TraceHeaders {
        traceparent,
        tracestate,
    })
}

fn context_from_extractor(extractor: &dyn Extractor) -> Option<Context> {
    let context = TraceContextPropagator::new().extract_with_context(&Context::new(), extractor);
    context_has_valid_span(&context).then_some(context)
}

/// Sets `span`'s parent from W3C trace-context headers.
pub fn set_parent_from_trace_headers(
    span: &tracing::Span,
    traceparent: Option<&str>,
    tracestate: Option<&str>,
) {
    set_parent_from_context(span, context_from_trace_headers(traceparent, tracestate));
}

/// Sets `span`'s parent from an inbound HTTP request's W3C trace-context headers.
///
/// More than one `traceparent` line is invalid and starts a new trace. Every
/// `tracestate` line is kept, joined in order. `baggage` is never read.
pub fn set_parent_from_http_headers(span: &tracing::Span, headers: &http::HeaderMap) {
    let mut traceparents = headers.get_all(TRACEPARENT_HEADER).iter();
    let (Some(traceparent), None) = (traceparents.next(), traceparents.next()) else {
        return;
    };
    let Ok(traceparent) = traceparent.to_str() else {
        return;
    };
    let tracestate = headers
        .get_all(TRACESTATE_HEADER)
        .iter()
        .map(http::HeaderValue::to_str)
        .collect::<Result<Vec<_>, _>>()
        .ok()
        .filter(|lines| !lines.is_empty())
        .map(|lines| lines.join(","));
    set_parent_from_trace_headers(span, Some(traceparent), tracestate.as_deref());
}

/// Stable message recorded for HTTP requests that end in a server error.
const HTTP_REQUEST_ERROR_MESSAGE: &str = "HTTP request failed";

/// Opens the server span of one inbound HTTP request, under the caller's
/// target, and continues a valid inbound W3C trace.
///
/// `$name` names the listener's request span in Coral's own logs; exported,
/// it is named `{method} {route}`, as OpenTelemetry's HTTP conventions ask.
/// `$request` is the inbound `http::Request`, and `$route` its matched route
/// template, never the request path. The span carries only the method, route
/// and response status; headers, body, query, cookies and the full URL stay
/// off it. Record the response with [`record_http_response`].
#[macro_export]
macro_rules! http_request_span {
    ($name:literal, $request:expr, $route:expr) => {{
        let request: &$crate::__http::Request<_> = $request;
        let route: ::core::option::Option<&str> = $route;
        let method = $crate::http_method(request.method());
        let span = ::tracing::info_span!(
            parent: None,
            $name,
            otel.name = $crate::http_span_name(method, route),
            otel.kind = "server",
            http.request.method = method,
            http.route = route,
            http.response.status_code = ::tracing::field::Empty,
            error.type = ::tracing::field::Empty,
            exception.message = ::tracing::field::Empty,
            status = ::tracing::field::Empty,
        );
        $crate::set_parent_from_http_headers(&span, request.headers());
        span
    }};
}

#[doc(hidden)]
pub use http as __http;

/// The exported name of an HTTP server span: `{method} {route}`, the method
/// alone without a route, and `HTTP` for a method outside the standard set.
/// [`http_request_span!`] records it.
#[doc(hidden)]
#[must_use]
pub fn http_span_name(method: &'static str, route: Option<&str>) -> String {
    let method = if method == "_OTHER" { "HTTP" } else { method };
    route.map_or_else(|| method.to_owned(), |route| format!("{method} {route}"))
}

/// The request method as a span field: a standard method's name, else
/// `_OTHER`, so a caller cannot choose the text. [`http_request_span!`]
/// records it.
#[doc(hidden)]
#[must_use]
pub fn http_method(method: &http::Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "CONNECT" => "CONNECT",
        "OPTIONS" => "OPTIONS",
        "TRACE" => "TRACE",
        "PATCH" => "PATCH",
        _ => "_OTHER",
    }
}

/// Records the response status on a span opened by [`http_request_span!`].
/// A server error fails the span; a client error is the caller's.
pub fn record_http_response(span: &tracing::Span, status: http::StatusCode) {
    span.record("http.response.status_code", i64::from(status.as_u16()));
    if status.is_server_error() {
        record_failure(span, status.as_str(), HTTP_REQUEST_ERROR_MESSAGE);
    } else {
        span.record("status", "ok");
    }
}

/// Sets `span`'s parent from a text-map carrier.
pub fn set_parent_from_extractor(span: &tracing::Span, extractor: &dyn Extractor) {
    set_parent_from_context(span, context_from_extractor(extractor));
}

fn set_parent_from_context(span: &tracing::Span, context: Option<Context>) {
    let Some(context) = context else {
        return;
    };
    if let Err(error) = span.set_parent(context) {
        tracing::debug!(%error, "failed to set parent trace context");
    }
}

fn inject_context(context: &Context, injector: &mut dyn Injector) {
    TraceContextPropagator::new().inject_context(context, injector);
}

/// Injects `span`'s OpenTelemetry context into a W3C trace-context carrier.
pub fn inject_span_context(span: &tracing::Span, injector: &mut dyn Injector) {
    inject_context(&span.context(), injector);
}

/// Injects the current tracing span's OpenTelemetry context into a carrier.
pub fn inject_current_context(injector: &mut dyn Injector) {
    inject_context(&tracing::Span::current().context(), injector);
}

fn context_has_valid_span(context: &Context) -> bool {
    context.span().span_context().is_valid()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use opentelemetry::baggage::BaggageExt as _;
    use opentelemetry::trace::{SpanId, Status as OtelStatus, TraceId, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;

    use super::{
        context_from_extractor, context_from_trace_headers, record_failure,
        set_parent_from_trace_headers,
    };

    /// Persisted trace IDs are decoded through this parser, so it admits only
    /// the one spelling Coral writes.
    #[test]
    fn trace_id_accepts_only_lowercase_nonzero_32_hex() {
        let valid = "0123456789abcdef0123456789abcdef";
        assert_eq!(super::TraceId::parse(valid).unwrap().as_str(), valid);
        for invalid in [
            "0123456789ABCDEF0123456789ABCDEF",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "00000000000000000000000000000000",
            "0123456789abcdef0123456789abcdeg",
            "0123456789abcdef-123456789abcdef",
            "",
        ] {
            assert_eq!(
                super::TraceId::parse(invalid),
                Err("trace id must be a lowercase nonzero 32-character W3C trace id"),
                "{invalid}"
            );
        }
    }

    #[test]
    fn record_failure_sets_shared_error_fields() {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let tracer = provider.tracer("failure-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "failure",
                error.type = tracing::field::Empty,
                exception.message = tracing::field::Empty,
                status = tracing::field::Empty,
            );
            record_failure(&span, "INVALID_ARGUMENT", "request failed");
            span.in_scope(|| {});
        });
        provider.force_flush().expect("flush spans");

        let spans = memory.get_finished_spans().expect("finished spans");
        let failure = spans
            .iter()
            .find(|span| span.name == "failure")
            .expect("failure span");
        let string_attribute = |name: &str| {
            failure
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .map(|attribute| attribute.value.as_str().into_owned())
        };
        assert_eq!(string_attribute("status").as_deref(), Some("error"));
        assert_eq!(
            string_attribute("error.type").as_deref(),
            Some("INVALID_ARGUMENT")
        );
        assert_eq!(
            string_attribute("exception.message").as_deref(),
            Some("request failed")
        );
        assert_eq!(failure.status, OtelStatus::error("request failed"));
        provider.shutdown().expect("provider shutdown");
    }

    #[test]
    fn rejects_missing_or_invalid_traceparent_under_active_span() {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let tracer = provider.tracer("trace-context-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let ambient = tracing::info_span!("ambient");
            let _guard = ambient.enter();

            assert!(context_from_trace_headers(None, Some("vendor=value")).is_none());
            assert!(context_from_trace_headers(Some("not-a-traceparent"), None).is_none());
            assert!(context_from_extractor(&HashMap::new()).is_none());
        });
        provider.shutdown().expect("provider shutdown");
    }

    /// A request span carries the method, route template and status, never
    /// the path, query, headers or cookies. A server error fails it.
    #[test]
    fn http_request_spans_carry_only_safe_fields() {
        for (method, status, expected_method, expected_name, failed) in [
            (
                "GET",
                200,
                "GET",
                "GET /workspaces/{workspace_id}/mcp",
                false,
            ),
            (
                "BREW-CANARY",
                404,
                "_OTHER",
                "HTTP /workspaces/{workspace_id}/mcp",
                false,
            ),
            (
                "POST",
                503,
                "POST",
                "POST /workspaces/{workspace_id}/mcp",
                true,
            ),
        ] {
            let request = http::Request::builder()
                .method(method)
                .uri("/workspaces/canary-path/mcp?query=canary-query")
                .header("cookie", "session=canary-cookie")
                .header("authorization", "Bearer canary-token")
                .body(())
                .expect("request");
            let memory = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(memory.clone())
                .build();
            let subscriber = tracing_subscriber::Registry::default()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("http-test")));
            tracing::subscriber::with_default(subscriber, || {
                let span = crate::http_request_span!(
                    "request",
                    &request,
                    Some("/workspaces/{workspace_id}/mcp")
                );
                crate::record_http_response(
                    &span,
                    http::StatusCode::from_u16(status).expect("status"),
                );
                span.in_scope(|| {});
            });
            provider.force_flush().expect("flush spans");
            let spans = memory.get_finished_spans().expect("finished spans");
            let span = spans.first().expect("request span");
            assert_eq!(span.name, expected_name, "{method}");
            let attribute = |name: &str| {
                span.attributes
                    .iter()
                    .find(|attribute| attribute.key.as_str() == name)
                    .map(|attribute| attribute.value.to_string())
            };
            assert_eq!(
                attribute("http.request.method").as_deref(),
                Some(expected_method),
                "{method}"
            );
            assert_eq!(
                attribute("http.route").as_deref(),
                Some("/workspaces/{workspace_id}/mcp"),
                "{method}"
            );
            assert_eq!(
                attribute("http.response.status_code"),
                Some(status.to_string()),
                "{method}"
            );
            assert_eq!(
                span.status == OtelStatus::error("HTTP request failed"),
                failed,
                "{method}"
            );
            assert!(
                !format!("{span:?}").contains("canary"),
                "{method}: {span:?}"
            );
            provider.shutdown().expect("provider shutdown");
        }
    }

    /// A public listener continues only one well-formed inbound trace, keeps
    /// every `tracestate` line, and never takes `baggage`.
    #[test]
    fn http_headers_continue_only_one_valid_trace_and_never_baggage() {
        const PARENT: &str = "00-00000000000000000000000000000077-0000000000000088-01";
        let continued = TraceId::from_hex("00000000000000000000000000000077").expect("trace id");
        for (case, headers, continues, trace_state) in [
            ("one traceparent", vec![("traceparent", PARENT)], true, ""),
            (
                "tracestate lines join in order",
                vec![
                    ("traceparent", PARENT),
                    ("tracestate", "first=1"),
                    ("tracestate", "second=2"),
                ],
                true,
                "first=1,second=2",
            ),
            (
                "two traceparents are invalid",
                vec![
                    ("traceparent", PARENT),
                    (
                        "traceparent",
                        "00-00000000000000000000000000000099-0000000000000088-01",
                    ),
                ],
                false,
                "",
            ),
            (
                "a malformed traceparent is ignored",
                vec![("traceparent", "00-not-a-trace-01")],
                false,
                "",
            ),
            (
                "a malformed tracestate is dropped and the trace kept",
                vec![("traceparent", PARENT), ("tracestate", "not a list member")],
                true,
                "",
            ),
            (
                "baggage is never taken",
                vec![("traceparent", PARENT), ("baggage", "tenant=canary")],
                true,
                "",
            ),
        ] {
            let mut map = http::HeaderMap::new();
            for (name, value) in headers {
                map.append(name, http::HeaderValue::from_static(value));
            }
            let memory = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(memory.clone())
                .build();
            let subscriber = tracing_subscriber::Registry::default()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("http-test")));
            let baggage = tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!("request");
                super::set_parent_from_http_headers(&span, &map);
                let baggage = span.context().baggage().len();
                span.in_scope(|| {});
                baggage
            });
            provider.force_flush().expect("flush spans");
            let spans = memory.get_finished_spans().expect("finished spans");
            let request = spans.first().expect("request span");
            assert_eq!(
                request.span_context.trace_id() == continued,
                continues,
                "{case}"
            );
            assert_eq!(request.parent_span_is_remote, continues, "{case}");
            assert_eq!(
                request.span_context.trace_state().header(),
                trace_state,
                "{case}"
            );
            assert_eq!(baggage, 0, "{case}");
            provider.shutdown().expect("provider shutdown");
        }
    }

    #[test]
    fn sets_span_parent_from_trace_headers() {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let tracer = provider.tracer("trace-context-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("child");
            set_parent_from_trace_headers(
                &span,
                Some("00-00000000000000000000000000000077-0000000000000088-01"),
                Some("vendor=value"),
            );
            let _guard = span.enter();
        });
        provider.force_flush().expect("flush spans");

        let spans = memory.get_finished_spans().expect("finished spans");
        let child = spans
            .iter()
            .find(|span| span.name == "child")
            .expect("child span");
        assert_eq!(
            child.span_context.trace_id(),
            TraceId::from_hex("00000000000000000000000000000077").expect("trace id")
        );
        assert_eq!(
            child.parent_span_id,
            SpanId::from_hex("0000000000000088").expect("span id")
        );
        assert!(child.parent_span_is_remote);
        assert_eq!(child.span_context.trace_state().header(), "vendor=value");
        provider.shutdown().expect("provider shutdown");
    }
}
