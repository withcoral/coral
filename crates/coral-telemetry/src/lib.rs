//! Cross-crate telemetry helpers.
//!
//! This crate owns small telemetry utilities that need to be shared by the
//! app, client, engine, and MCP adapter without adding dependency edges between
//! those crates.

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator as _};
use opentelemetry::trace::TraceContextExt as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

const TRACEPARENT_HEADER: &str = "traceparent";
const TRACESTATE_HEADER: &str = "tracestate";

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

    use opentelemetry::trace::{SpanId, TraceContextExt as _, TraceId, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::prelude::*;

    use super::{
        TRACEPARENT_HEADER, TRACESTATE_HEADER, context_from_extractor, context_from_trace_headers,
        inject_context, set_parent_from_trace_headers,
    };

    #[test]
    fn extracts_valid_traceparent_and_tracestate() {
        let trace_id = "00000000000000000000000000000077";
        let span_id = "0000000000000088";
        let context = context_from_trace_headers(
            Some(&format!("00-{trace_id}-{span_id}-01")),
            Some("vendor=value"),
        )
        .expect("valid trace context");

        let span_context = context.span().span_context().clone();
        assert_eq!(
            span_context.trace_id(),
            TraceId::from_hex(trace_id).expect("trace id")
        );
        assert_eq!(
            span_context.span_id(),
            SpanId::from_hex(span_id).expect("span id")
        );
        assert!(span_context.is_remote());
        assert_eq!(span_context.trace_state().header(), "vendor=value");
    }

    #[test]
    fn rejects_missing_or_invalid_traceparent() {
        assert!(context_from_trace_headers(None, Some("vendor=value")).is_none());
        assert!(context_from_trace_headers(Some("not-a-traceparent"), None).is_none());
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

    #[test]
    fn extracts_from_generic_carrier() {
        let carrier = HashMap::from([(
            TRACEPARENT_HEADER.to_string(),
            "00-00000000000000000000000000000077-0000000000000088-01".to_string(),
        )]);

        assert!(context_from_extractor(&carrier).is_some());
    }

    #[test]
    fn injects_trace_context_headers() {
        let context = context_from_trace_headers(
            Some("00-00000000000000000000000000000077-0000000000000088-01"),
            Some("vendor=value"),
        )
        .expect("valid trace context");
        let mut carrier = HashMap::new();

        inject_context(&context, &mut carrier);

        assert_eq!(
            carrier.get(TRACEPARENT_HEADER).map(String::as_str),
            Some("00-00000000000000000000000000000077-0000000000000088-01")
        );
        assert_eq!(
            carrier.get(TRACESTATE_HEADER).map(String::as_str),
            Some("vendor=value")
        );
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
                None,
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
        provider.shutdown().expect("provider shutdown");
    }
}
