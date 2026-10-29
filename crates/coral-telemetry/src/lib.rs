//! Cross-crate telemetry helpers.
//!
//! This crate owns transport-neutral W3C propagation, safe telemetry utilities
//! shared by application, domain, and adapter crates, the safe fields, and
//! the filters that decide what spans and log records may leave the host.

mod export;
mod field;
mod request;
mod run;
#[cfg(test)]
mod source_scan;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use export::{OtlpLogFilter, OtlpSpanFilter};
pub use request::{REQUEST_TARGET, request_span, trace_grpc_routes, trace_http_routes};
pub use run::RunTrace;

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

/// Prefix for host-only span attributes that must not be exported over OTLP.
pub const LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX: &str = "coral.local.";

/// Marks `span` failed with the categorical `error_type` and no message
/// text, so no caller-controlled text reaches its status. A span that
/// succeeds leaves its status unset, as the OpenTelemetry conventions ask.
pub fn record_failure(span: &tracing::Span, error_type: impl AsRef<str>) {
    span.record("error.type", error_type.as_ref());
    span.set_status(OtelStatus::error(""));
}

/// Links `span`, a span not yet entered, to the current span, when there is
/// one. Work that a request starts and does not wait for runs in a root span
/// of its own, so it does not hold the request's span open, and keeps a path
/// back to the request this way.
pub fn link_to_current(span: &tracing::Span) {
    let current = tracing::Span::current().context();
    let current = current.span().span_context().clone();
    if current.is_valid() {
        span.add_link(current);
    }
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
/// More than one `traceparent` line is invalid and starts a new trace. The
/// `tracestate` lines are joined in order, and all are dropped when any is
/// invalid. `baggage` is never read.
fn set_parent_from_http_headers(span: &tracing::Span, headers: &http::HeaderMap) {
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

/// Sets `span`'s parent from a text-map carrier.
pub fn set_parent_from_extractor(span: &tracing::Span, extractor: &dyn Extractor) {
    set_parent_from_context(span, context_from_extractor(extractor));
}

fn set_parent_from_context(span: &tracing::Span, context: Option<Context>) {
    let Some(context) = context else {
        return;
    };
    if span.set_parent(context).is_err() {
        tracing::debug!(error.type = "set_parent", "failed to set parent trace context");
    }
}

fn inject_context(context: &Context, injector: &mut dyn Injector) {
    TraceContextPropagator::new().inject_context(context, injector);
}

/// Injects `span`'s OpenTelemetry context into a W3C trace-context carrier.
pub fn inject_span_context(span: &tracing::Span, injector: &mut dyn Injector) {
    inject_context(&span.context(), injector);
}

const BAGGAGE_HEADER: &str = "baggage";

/// Gives the headers of a request Coral sends in `span`, such as a Provider
/// Tool dispatch, `span`'s W3C `traceparent` and no other trace context. A
/// `traceparent`, `tracestate` or `baggage` header already there, such as one
/// from Program arguments, never leaves, and outside a valid trace none is
/// sent.
pub fn set_outbound_trace_context(span: &tracing::Span, headers: &mut http::HeaderMap) {
    for name in [TRACEPARENT_HEADER, TRACESTATE_HEADER, BAGGAGE_HEADER] {
        headers.remove(name);
    }
    let mut carrier = std::collections::HashMap::new();
    inject_span_context(span, &mut carrier);
    if let Some(value) = carrier
        .remove(TRACEPARENT_HEADER)
        .and_then(|value| http::HeaderValue::from_str(&value).ok())
    {
        headers.insert(TRACEPARENT_HEADER, value);
    }
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

    use opentelemetry::trace::{SpanId, Status as OtelStatus, TraceId};

    use super::{
        context_from_extractor, context_from_trace_headers, record_failure,
        set_parent_from_trace_headers,
    };
    use crate::testing::{SpanCapture, attribute};

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
        let capture = SpanCapture::default();
        tracing::subscriber::with_default(capture.subscriber(), || {
            let span = tracing::info_span!(
                "failure",
                error.type = tracing::field::Empty,
            );
            record_failure(&span, "INVALID_ARGUMENT");
            span.in_scope(|| {});
        });
        let spans = capture.finished();
        let failure = spans
            .iter()
            .find(|span| span.name == "failure")
            .expect("failure span");
        let string_attribute = |name: &str| attribute(failure, name);
        assert_eq!(
            string_attribute("error.type").as_deref(),
            Some("INVALID_ARGUMENT")
        );
        assert_eq!(failure.status, OtelStatus::error(""));
    }

    #[test]
    fn rejects_missing_or_invalid_traceparent_under_active_span() {
        let capture = SpanCapture::default();
        tracing::subscriber::with_default(capture.subscriber(), || {
            let ambient = tracing::info_span!("ambient");
            let _guard = ambient.enter();

            assert!(context_from_trace_headers(None, Some("vendor=value")).is_none());
            assert!(context_from_trace_headers(Some("not-a-traceparent"), None).is_none());
            assert!(context_from_extractor(&HashMap::new()).is_none());
        });
    }

    #[test]
    fn sets_span_parent_from_trace_headers() {
        let capture = SpanCapture::default();
        tracing::subscriber::with_default(capture.subscriber(), || {
            let span = tracing::info_span!("child");
            set_parent_from_trace_headers(
                &span,
                Some("00-00000000000000000000000000000077-0000000000000088-01"),
                Some("vendor=value"),
            );
            let _guard = span.enter();
        });
        let spans = capture.finished();
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
    }
}

#[cfg(test)]
mod outbound_tests {
    use super::set_outbound_trace_context;
    use crate::testing::SpanCapture;

    fn headers() -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        for (name, value) in [
            (
                "traceparent",
                "00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-01",
            ),
            ("tracestate", "caller=value"),
            ("baggage", "user=private"),
            ("x-kept", "kept"),
        ] {
            headers.insert(name, http::HeaderValue::from_static(value));
        }
        headers
    }

    /// A request Coral sends carries only the trace context of the span it
    /// sends it in: never a `traceparent`, `tracestate` or `baggage` that
    /// came from elsewhere, and none at all outside a valid trace.
    #[test]
    fn only_the_sending_span_trace_context_leaves() {
        let capture = SpanCapture::default();
        let (inside, expected) = tracing::subscriber::with_default(capture.subscriber(), || {
            let span = tracing::info_span!("client");
            let mut headers = headers();
            set_outbound_trace_context(&span, &mut headers);
            let expected = {
                use opentelemetry::trace::TraceContextExt as _;
                use tracing_opentelemetry::OpenTelemetrySpanExt as _;
                let context = span.context();
                let span = context.span();
                format!(
                    "00-{}-{}-01",
                    span.span_context().trace_id(),
                    span.span_context().span_id()
                )
            };
            (headers, Some(expected))
        });
        let mut outside = headers();
        set_outbound_trace_context(&tracing::Span::none(), &mut outside);
        for (headers, expected) in [(inside, expected), (outside, None)] {
            assert_eq!(
                headers
                    .get_all("traceparent")
                    .iter()
                    .map(|value| value.to_str().expect("ASCII").to_string())
                    .collect::<Vec<_>>(),
                expected.into_iter().collect::<Vec<_>>()
            );
            assert!(headers.get("tracestate").is_none());
            assert!(headers.get("baggage").is_none());
            assert_eq!(
                headers.get("x-kept").map(http::HeaderValue::as_bytes),
                Some(&b"kept"[..])
            );
        }
    }
}
