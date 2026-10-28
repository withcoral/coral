//! Span capture for tests that assert what Coral traces.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracer, SdkTracerProvider, SpanData};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;

/// Records every finished span in memory.
///
/// In a test binary with other tests, install [`SpanCapture::subscriber`]
/// once for the process with `set_global_default` and select the test's own
/// spans, by trace or attribute: a subscriber set for one thread with
/// `set_default` misses a callsite another test's thread registered first.
/// Add [`SpanCapture::layer`] to a subscriber that needs other layers too.
#[derive(Debug, Clone)]
pub struct SpanCapture {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
}

impl Default for SpanCapture {
    fn default() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        Self { provider, exporter }
    }
}

impl SpanCapture {
    /// A capture whose spans also go to `other`, such as an exporter that
    /// fails, so tests can show what export does not change.
    #[must_use]
    pub fn beside<E>(other: E) -> Self
    where
        E: opentelemetry_sdk::trace::SpanExporter + 'static,
    {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .with_simple_exporter(other)
            .build();
        Self { provider, exporter }
    }

    /// A subscriber that gives every span trace context and captures it.
    #[must_use]
    pub fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + use<> {
        tracing_subscriber::Registry::default().with(self.layer())
    }

    /// A layer that gives every span it sees trace context and captures it.
    #[must_use]
    pub fn layer<S>(&self) -> OpenTelemetryLayer<S, SdkTracer>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_opentelemetry::layer()
            .with_tracer(self.provider.tracer("coral-test"))
            .with_level(true)
    }

    /// Every span that has finished so far.
    ///
    /// # Panics
    ///
    /// Panics when the in-memory exporter fails.
    #[must_use]
    pub fn finished(&self) -> Vec<SpanData> {
        self.provider.force_flush().expect("flush captured spans");
        self.exporter
            .get_finished_spans()
            .expect("read captured spans")
    }
}

/// The value of `span`'s attribute `key`, as text.
#[must_use]
pub fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.to_string())
}
