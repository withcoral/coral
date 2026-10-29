//! The Run Trace: the Coral-owned trace every span of one Program Run joins.

use std::collections::HashMap;

use opentelemetry::Context;
use opentelemetry::trace::{SpanContext, TraceContextExt as _, TraceState};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{TRACEPARENT_HEADER, TraceId, context_from_trace_headers, inject_context};

/// The stored context of one Run Trace: its `coral.run.accept` span, as one
/// W3C `traceparent`. It carries the root's sampling decision and no
/// `tracestate`, so nothing a caller sent reaches the Run's spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTrace {
    traceparent: String,
    accept: SpanContext,
}

impl RunTrace {
    /// Begins a Run Trace at `accept`, a span not yet entered: makes it the
    /// root of a new trace, links it to the current span (the request that
    /// accepts the Run), and returns its context. `None` when no
    /// OpenTelemetry layer gives `accept` a context.
    #[must_use]
    pub fn begin(accept: &tracing::Span) -> Option<Self> {
        let request = tracing::Span::current().context();
        if accept.set_parent(Context::new()).is_err() {
            return None;
        }
        accept.add_link(request.span().span_context().clone());
        Self::from_context(&accept.context())
    }

    /// Reads a stored Run Trace.
    ///
    /// # Errors
    ///
    /// Returns a safe static message unless `stored` is one valid lowercase
    /// W3C `traceparent`, as [`Self::as_str`] writes it.
    pub fn parse(stored: &str) -> Result<Self, &'static str> {
        context_from_trace_headers(Some(stored), None)
            .and_then(|context| Self::from_context(&context))
            .filter(|trace| trace.traceparent == stored)
            .ok_or("Run Trace must be one lowercase W3C traceparent")
    }

    /// The `traceparent` to store with the accepted Run.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.traceparent
    }

    /// The Run Trace's identifier, for safe errors about the Run's work.
    #[must_use]
    pub fn trace_id(&self) -> TraceId {
        TraceId(self.accept.trace_id().to_string())
    }

    /// Makes `span`, a span not yet entered, a child of the accept span.
    /// Every Run phase span joins the Run Trace this way, on any Coral
    /// Instance; no span of an earlier phase is reopened.
    pub fn adopt(&self, span: &tracing::Span) {
        if let Err(error) =
            span.set_parent(Context::new().with_remote_span_context(self.accept.clone()))
        {
            tracing::debug!(%error, "failed to join the Run Trace");
        }
    }

    /// Links `span`, a request about the Run, to the Run Trace. The request
    /// keeps its own trace.
    pub fn link(&self, span: &tracing::Span) {
        span.add_link(self.accept.clone());
    }

    fn from_context(context: &Context) -> Option<Self> {
        let span = context.span();
        let found = span.span_context();
        if !found.is_valid() {
            return None;
        }
        let accept = SpanContext::new(
            found.trace_id(),
            found.span_id(),
            found.trace_flags(),
            true,
            TraceState::default(),
        );
        let mut carrier = HashMap::new();
        inject_context(
            &Context::new().with_remote_span_context(accept.clone()),
            &mut carrier,
        );
        let traceparent = carrier.remove(TRACEPARENT_HEADER)?;
        Some(Self {
            traceparent,
            accept,
        })
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanKind, TraceContextExt as _};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    use super::RunTrace;
    use crate::testing::SpanCapture;

    /// A Run's phases join the accept span's trace as its children, on any
    /// Coral Instance, while the request that accepted the Run keeps its own
    /// trace and is reached only through a link.
    #[test]
    fn phases_join_the_accept_span_and_the_request_is_only_linked() {
        let capture = SpanCapture::default();
        let stored = tracing::subscriber::with_default(capture.subscriber(), || {
            let request = tracing::info_span!("request");
            let _request = request.enter();
            let accept = tracing::info_span!("coral.run.accept");
            let trace =
                RunTrace::begin(&accept).expect("a Run Trace under the OpenTelemetry layer");
            drop(accept);
            trace.as_str().to_string()
        });
        tracing::subscriber::with_default(capture.subscriber(), || {
            let trace = RunTrace::parse(&stored).expect("the stored Run Trace parses");
            let phase = tracing::info_span!("coral.run.recover");
            trace.adopt(&phase);
            phase.in_scope(|| {});
        });

        let spans = capture.finished();
        let named = |name: &str| {
            spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| panic!("{name} span"))
                .clone()
        };
        let (request, accept, phase) = (
            named("request"),
            named("coral.run.accept"),
            named("coral.run.recover"),
        );
        let run_trace = accept.span_context.trace_id();
        assert_ne!(run_trace, request.span_context.trace_id());
        assert_eq!(accept.parent_span_id, opentelemetry::trace::SpanId::INVALID);
        assert_eq!(
            accept
                .links
                .iter()
                .map(|link| link.span_context.clone())
                .collect::<Vec<_>>(),
            vec![request.span_context.clone()]
        );
        assert_eq!(phase.span_context.trace_id(), run_trace);
        assert_eq!(phase.parent_span_id, accept.span_context.span_id());
        assert_eq!(accept.span_kind, SpanKind::Internal);
        assert_eq!(
            RunTrace::parse(&stored).expect("parse").trace_id().as_str(),
            run_trace.to_string()
        );
    }

    /// A later request links to the Run Trace and stays in its own trace.
    #[test]
    fn a_request_links_to_the_run_trace_without_joining_it() {
        let capture = SpanCapture::default();
        tracing::subscriber::with_default(capture.subscriber(), || {
            let trace = RunTrace::parse("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
                .expect("stored Run Trace");
            let cancel = tracing::info_span!("cancel");
            let request_trace = cancel.context().span().span_context().trace_id();
            trace.link(&cancel);
            assert_eq!(
                cancel.context().span().span_context().trace_id(),
                request_trace
            );
        });
        let spans = capture.finished();
        let cancel = spans
            .iter()
            .find(|span| span.name == "cancel")
            .expect("cancel");
        assert_eq!(
            cancel
                .links
                .iter()
                .map(|link| link.span_context.trace_id().to_string())
                .collect::<Vec<_>>(),
            vec!["0123456789abcdef0123456789abcdef".to_string()]
        );
    }

    #[test]
    fn parse_accepts_only_a_valid_traceparent() {
        for invalid in [
            "",
            "not-a-traceparent",
            "00-00000000000000000000000000000000-0123456789abcdef-01",
            "00-0123456789abcdef0123456789abcdef-0000000000000000-01",
            "00-0123456789ABCDEF0123456789ABCDEF-0123456789abcdef-01",
        ] {
            assert!(RunTrace::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn begin_without_the_opentelemetry_layer_gives_no_run_trace() {
        let accept = tracing::info_span!("coral.run.accept");
        assert_eq!(RunTrace::begin(&accept), None);
    }
}
