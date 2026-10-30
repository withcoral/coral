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

/// Records every Coral metric in memory.
///
/// Scope [`MetricCapture::subscriber`] to a test with
/// `tracing::subscriber::set_default`, or add [`MetricCapture::layer`] to a
/// subscriber that needs other layers too. Domain code records through the
/// current dispatcher, so work on another thread, such as a blocking task,
/// records nothing unless it runs under the same subscriber.
#[derive(Debug, Clone)]
pub struct MetricCapture {
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    exporter: opentelemetry_sdk::metrics::InMemoryMetricExporter,
    metrics: crate::metric::Metrics,
}

impl Default for MetricCapture {
    fn default() -> Self {
        let exporter = opentelemetry_sdk::metrics::InMemoryMetricExporter::default();
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(
                opentelemetry_sdk::metrics::PeriodicReader::builder(exporter.clone()).build(),
            )
            .build();
        let metrics = crate::metric::Metrics::new(&provider);
        Self {
            provider,
            exporter,
            metrics,
        }
    }
}

/// One exported data point of a Coral metric.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPoint {
    /// The instrument's name.
    pub name: String,
    /// The instrument's unit.
    pub unit: String,
    /// `counter`, `gauge` or `histogram`.
    pub kind: &'static str,
    /// The point's attributes, by key.
    pub attributes: std::collections::BTreeMap<String, String>,
    /// A counter's sum, a gauge's value, or a histogram's sum.
    pub value: f64,
    /// A histogram's count, or 1.
    pub count: u64,
}

impl MetricCapture {
    /// The catalog this capture records, for code that takes it explicitly.
    #[must_use]
    pub fn metrics(&self) -> crate::metric::Metrics {
        self.metrics.clone()
    }

    /// A subscriber through which domain code records to this capture. It
    /// keeps no spans: a span that ends on another thread closes its parent
    /// through that thread's subscriber, which would not hold a span kept
    /// here.
    #[must_use]
    pub fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + use<> {
        Spanless.with(self.layer())
    }

    /// The layer through which domain code records to this capture.
    #[must_use]
    pub fn layer(&self) -> crate::metric::MetricsLayer {
        self.metrics.layer()
    }

    /// Every point recorded so far, cumulatively.
    ///
    /// # Panics
    ///
    /// Panics when the in-memory exporter fails.
    #[must_use]
    pub fn points(&self) -> Vec<MetricPoint> {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        self.provider.force_flush().expect("flush captured metrics");
        let exported = self
            .exporter
            .get_finished_metrics()
            .expect("read captured metrics");
        let Some(last) = exported.last() else {
            return Vec::new();
        };
        let mut points = Vec::new();
        for scope in last.scope_metrics() {
            for metric in scope.metrics() {
                let point = |kind, attributes: Vec<&opentelemetry::KeyValue>, value: f64, count| {
                    MetricPoint {
                        name: metric.name().to_string(),
                        unit: metric.unit().to_string(),
                        kind,
                        attributes: attributes
                            .into_iter()
                            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                            .collect(),
                        value,
                        count,
                    }
                };
                macro_rules! points {
                    ($data:expr, $as_f64:expr) => {
                        match $data {
                            MetricData::Sum(sum) => points.extend(sum.data_points().map(|p| {
                                point("counter", p.attributes().collect(), $as_f64(p.value()), 1)
                            })),
                            MetricData::Gauge(gauge) => {
                                points.extend(gauge.data_points().map(|p| {
                                    point("gauge", p.attributes().collect(), $as_f64(p.value()), 1)
                                }))
                            }
                            MetricData::Histogram(histogram) => {
                                points.extend(histogram.data_points().map(|p| {
                                    point(
                                        "histogram",
                                        p.attributes().collect(),
                                        $as_f64(p.sum()),
                                        p.count(),
                                    )
                                }));
                            }
                            MetricData::ExponentialHistogram(_) => {}
                        }
                    };
                }
                match metric.data() {
                    AggregatedMetrics::F64(data) => points!(data, |v: f64| v),
                    AggregatedMetrics::U64(data) => points!(data, u64_value),
                    AggregatedMetrics::I64(data) => points!(data, i64_value),
                }
            }
        }
        points
    }

    /// The captured values of the instrument `name`, in export order.
    #[must_use]
    pub fn values(&self, name: &str) -> Vec<f64> {
        self.named(name)
            .into_iter()
            .map(|point| point.value)
            .collect()
    }

    /// The points of the instrument `name`.
    #[must_use]
    pub fn named(&self, name: &str) -> Vec<MetricPoint> {
        self.points()
            .into_iter()
            .filter(|point| point.name == name)
            .collect()
    }
}

#[expect(clippy::cast_precision_loss, reason = "test values are small")]
const fn u64_value(value: u64) -> f64 {
    value as f64
}

#[expect(clippy::cast_precision_loss, reason = "test values are small")]
const fn i64_value(value: i64) -> f64 {
    value as f64
}

/// A subscriber that enables no span or event.
struct Spanless;

impl tracing::Subscriber for Spanless {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}
