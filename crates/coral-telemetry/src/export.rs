//! Filtering for spans that leave the host over OTLP.

use std::time::Duration;

use opentelemetry::{Context, KeyValue, Value};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};
use tracing_subscriber::filter::Targets;

use crate::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX;

/// Targets whose spans carry request or response bodies. They never leave the
/// host, whatever the operator's trace filter admits.
const DENIED_TARGETS: &[&str] = &["coral.http.body", "coral.mcp.body"];

/// Wraps the span processor of an off-host exporter so that it receives only
/// what may leave the host.
///
/// A span leaves only when the operator's trace filter admits its target and
/// level and its target is not a body target. The rest never reach `inner`,
/// so they take no room in its export queue. Attributes named `coral.local.*`
/// are removed from every exported span, event and link.
#[derive(Debug)]
pub struct OtlpSpanFilter<E> {
    inner: E,
    targets: Targets,
}

impl<E> OtlpSpanFilter<E> {
    /// Filters the spans `inner` receives with the operator's trace filter `targets`.
    pub const fn new(inner: E, targets: Targets) -> Self {
        Self { inner, targets }
    }
}

impl<P> SpanProcessor for OtlpSpanFilter<P>
where
    P: SpanProcessor,
{
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        if !span_matches_targets(&span, &self.targets) || span_has_denied_target(&span) {
            return;
        }
        strip_local_attributes(&mut span.attributes);
        for event in &mut span.events.events {
            strip_local_attributes(&mut event.attributes);
        }
        for link in &mut span.links.links {
            strip_local_attributes(&mut link.attributes);
        }
        self.inner.on_end(span);
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

fn strip_local_attributes(attributes: &mut Vec<KeyValue>) {
    attributes.retain(|attribute| {
        !attribute
            .key
            .as_str()
            .starts_with(LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX)
    });
}

fn span_matches_targets(span: &SpanData, targets: &Targets) -> bool {
    let Some(target) = span_string_attribute(span, "target") else {
        return false;
    };
    let Some(level) = span_level_attribute(span) else {
        return false;
    };
    targets.would_enable(&target, &level)
}

fn span_has_denied_target(span: &SpanData) -> bool {
    span_string_attribute(span, "target").is_some_and(|target| DENIED_TARGETS.contains(&&*target))
}

fn span_string_attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .and_then(|attribute| match &attribute.value {
            Value::String(value) => Some(value.to_string()),
            _ => None,
        })
}

fn span_level_attribute(span: &SpanData) -> Option<tracing::Level> {
    match span_string_attribute(span, "level")?.as_str() {
        "TRACE" => Some(tracing::Level::TRACE),
        "DEBUG" => Some(tracing::Level::DEBUG),
        "INFO" => Some(tracing::Level::INFO),
        "WARN" => Some(tracing::Level::WARN),
        "ERROR" => Some(tracing::Level::ERROR),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceFlags, TraceId, TraceState, TracerProvider as _,
    };
    use opentelemetry::{KeyValue, Value};
    use opentelemetry_sdk::trace::{
        InMemorySpanExporter, SdkTracerProvider, SimpleSpanProcessor, SpanData,
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;

    use super::OtlpSpanFilter;

    /// Exports the spans below through the filter and returns what left the host.
    fn exported(targets: &str) -> Vec<SpanData> {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(OtlpSpanFilter::new(
                SimpleSpanProcessor::new(memory.clone()),
                targets.parse().expect("trace filter"),
            ))
            .build();
        let subscriber = tracing_subscriber::Registry::default().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("export-filter-test"))
                .with_level(true),
        );
        tracing::subscriber::with_default(subscriber, || {
            let kept = tracing::info_span!(
                target: "coral_app",
                "kept",
                coral.local.path = "/home/secret",
                safe = "visible",
            );
            kept.add_link_with_attributes(
                SpanContext::new(
                    TraceId::from_hex("00000000000000000000000000000077").unwrap(),
                    SpanId::from_hex("0000000000000088").unwrap(),
                    TraceFlags::SAMPLED,
                    true,
                    TraceState::default(),
                ),
                vec![
                    KeyValue::new("coral.local.link", "secret"),
                    KeyValue::new("link.safe", "visible"),
                ],
            );
            kept.in_scope(|| {
                tracing::info!(target: "coral_app", detail = "visible", coral.local.event = "secret", "event");
            });
            for body in [
                tracing::info_span!(target: "coral.http.body", "http body"),
                tracing::info_span!(target: "coral.mcp.body", "mcp body"),
                tracing::info_span!(target: "unlisted", "unlisted"),
                tracing::trace_span!(target: "coral_app", "too verbose"),
            ] {
                body.in_scope(|| {});
            }
        });
        provider.force_flush().expect("flush spans");
        let spans = memory.get_finished_spans().expect("finished spans");
        provider.shutdown().expect("provider shutdown");
        spans
    }

    fn keys(attributes: &[KeyValue]) -> Vec<&str> {
        attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect()
    }

    #[test]
    fn only_admitted_spans_leave_and_never_with_local_attributes() {
        // The filter admits both body targets, so only the deny list keeps them home.
        let spans = exported("coral_app=info,coral.http.body=trace,coral.mcp.body=trace");
        let names = spans
            .iter()
            .map(|span| span.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(names, ["kept"]);
        let kept = spans.first().expect("kept span");

        assert!(keys(&kept.attributes).contains(&"safe"));
        let event = kept.events.events.first().expect("event");
        assert!(keys(&event.attributes).contains(&"detail"));
        let link = kept.links.links.first().expect("link");
        assert_eq!(keys(&link.attributes), ["link.safe"]);
        for attributes in [&kept.attributes, &event.attributes, &link.attributes] {
            assert!(
                !keys(attributes)
                    .iter()
                    .any(|key| key.starts_with("coral.local.")),
                "{:?}",
                keys(attributes)
            );
            assert!(
                !attributes
                    .iter()
                    .any(|attribute| attribute.value == Value::from("/home/secret")
                        || attribute.value == Value::from("secret"))
            );
        }
    }
}
