//! Cross-crate telemetry helpers.
//!
//! This crate owns transport-neutral W3C propagation, safe telemetry utilities
//! shared by application, domain, and adapter crates, and the filter that
//! decides what spans may leave the host.

mod export;
mod request;
mod run;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use export::OtlpSpanFilter;
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

/// `span`'s W3C `traceparent`, for a request Coral sends on its behalf;
/// `None` when it has no valid trace context. Nothing else of the trace
/// context, such as `tracestate`, is returned.
#[must_use]
pub fn traceparent(span: &tracing::Span) -> Option<String> {
    let mut carrier = std::collections::HashMap::new();
    inject_span_context(span, &mut carrier);
    carrier.remove(TRACEPARENT_HEADER)
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
                exception.message = tracing::field::Empty,
                status = tracing::field::Empty,
            );
            record_failure(&span, "INVALID_ARGUMENT", "request failed");
            span.in_scope(|| {});
        });
        let spans = capture.finished();
        let failure = spans
            .iter()
            .find(|span| span.name == "failure")
            .expect("failure span");
        let string_attribute = |name: &str| attribute(failure, name);
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
