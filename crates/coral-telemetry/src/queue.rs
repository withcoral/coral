//! Finite export queues that never delay the work that ends a signal, and
//! count every signal they drop in `coral.telemetry.dropped`.
//!
//! The SDK's batch processors drop a span or log record when their queue is
//! full but do not say how many. Here a gate in front of each holds the count
//! of signals admitted and not yet through export, and admits none past the
//! queue's capacity. The queue itself holds no more than that count, so it is
//! never full when the gate admits: the gate alone drops, and counts. A
//! signal whose export fails is counted too. After shutdown the SDK rejects
//! signals itself, uncounted, since no export follows.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use opentelemetry::{Context, InstrumentationScope};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{
    self, BatchLogProcessor, LogBatch, LogExporter, LogProcessor, SdkLogRecord,
};
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{
    MeterProviderBuilder, PeriodicReader, SdkMeterProvider, Temporality,
};
use opentelemetry_sdk::trace::{
    self, BatchSpanProcessor, Span, SpanData, SpanExporter, SpanProcessor,
};

use crate::metric::{DropReason, Metrics, SignalKind};

/// A batch span processor for `exporter` whose queue holds `capacity`
/// spans. A span that finds it full is dropped at once and counted.
pub fn batch_span_processor<E>(
    exporter: E,
    capacity: usize,
    metrics: Metrics,
) -> Queue<BatchSpanProcessor>
where
    E: SpanExporter + 'static,
{
    let gate = Gate::new(capacity, SignalKind::Span, metrics);
    let config = trace::BatchConfigBuilder::default()
        .with_max_queue_size(capacity)
        .build();
    let inner = BatchSpanProcessor::builder(Counted {
        inner: exporter,
        gate: gate.clone(),
    })
    .with_batch_config(config)
    .build();
    Queue { inner, gate }
}

/// A batch log processor for `exporter` whose queue holds `capacity` log
/// records. A record that finds it full is dropped at once and counted.
pub fn batch_log_processor<E>(
    exporter: E,
    capacity: usize,
    metrics: Metrics,
) -> Queue<BatchLogProcessor>
where
    E: LogExporter + 'static,
{
    let gate = Gate::new(capacity, SignalKind::Log, metrics);
    let config = logs::BatchConfigBuilder::default()
        .with_max_queue_size(capacity)
        .build();
    let inner = BatchLogProcessor::builder(Counted {
        inner: exporter,
        gate: gate.clone(),
    })
    .with_batch_config(config)
    .build();
    Queue { inner, gate }
}

/// The meter provider `builder` makes with a reader that exports through
/// `exporter` every `interval`, and the catalog of Coral's metrics built from
/// it. A failed export's delta points are counted in that same catalog.
/// Metrics have no queue: the reader aggregates them in place.
pub fn meter_provider<E>(
    builder: MeterProviderBuilder,
    exporter: E,
    interval: Duration,
) -> (SdkMeterProvider, Metrics)
where
    E: PushMetricExporter + 'static,
{
    // The exporter counts to the catalog of the provider it exports for,
    // which exists only once the provider is built.
    let catalog = Arc::new(OnceLock::new());
    let provider = builder
        .with_reader(
            PeriodicReader::builder(counted_metric_exporter(exporter, Arc::clone(&catalog)))
                .with_interval(interval)
                .build(),
        )
        .build();
    let metrics = catalog.get_or_init(|| Metrics::new(&provider)).clone();
    (provider, metrics)
}

/// Counts the delta points of each export `exporter` fails, to the catalog
/// `metrics` holds by then.
pub(crate) fn counted_metric_exporter<E>(
    exporter: E,
    metrics: Arc<OnceLock<Metrics>>,
) -> CountedMetrics<E>
where
    E: PushMetricExporter,
{
    CountedMetrics {
        inner: exporter,
        metrics,
    }
}

/// How many signals are admitted and not yet through export.
struct Gate {
    pending: AtomicUsize,
    capacity: usize,
    signal: SignalKind,
    metrics: Metrics,
}

impl Gate {
    fn new(capacity: usize, signal: SignalKind, metrics: Metrics) -> Arc<Self> {
        Arc::new(Self {
            pending: AtomicUsize::new(0),
            capacity,
            signal,
            metrics,
        })
    }

    /// Admits one signal, or counts it dropped.
    fn admit(&self) -> bool {
        let admitted = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending < self.capacity).then_some(pending + 1)
            })
            .is_ok();
        if !admitted {
            self.dropped(DropReason::QueueFull, 1);
        }
        admitted
    }

    fn dropped(&self, reason: DropReason, count: usize) {
        self.metrics.telemetry_dropped(
            self.signal,
            reason,
            u64::try_from(count).unwrap_or(u64::MAX),
        );
    }
}

impl fmt::Debug for Gate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Gate")
            .field("pending", &self.pending)
            .field("capacity", &self.capacity)
            .field("signal", &self.signal)
            .finish_non_exhaustive()
    }
}

/// Releases `count` signals from the gate once their export is over, however
/// it ends.
struct Released<'a> {
    gate: &'a Gate,
    count: usize,
}

impl Drop for Released<'_> {
    fn drop(&mut self) {
        self.gate.pending.fetch_sub(self.count, Ordering::AcqRel);
    }
}

/// A batch processor behind its gate.
#[derive(Debug)]
pub struct Queue<P> {
    inner: P,
    gate: Arc<Gate>,
}

impl SpanProcessor for Queue<BatchSpanProcessor> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        if self.gate.admit() {
            self.inner.on_end(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

impl LogProcessor for Queue<BatchLogProcessor> {
    fn emit(&self, record: &mut SdkLogRecord, instrumentation: &InstrumentationScope) {
        if self.gate.admit() {
            self.inner.emit(record, instrumentation);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn event_enabled(
        &self,
        level: opentelemetry::logs::Severity,
        target: &str,
        name: Option<&str>,
    ) -> bool {
        self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// An exporter that releases what it exports from the gate, and counts what
/// it fails to export.
#[derive(Debug)]
struct Counted<E> {
    inner: E,
    gate: Arc<Gate>,
}

impl<E: SpanExporter> SpanExporter for Counted<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let count = batch.len();
        let _released = Released {
            gate: &self.gate,
            count,
        };
        let result = self.inner.export(batch).await;
        if result.is_err() {
            self.gate.dropped(DropReason::ExportFailed, count);
        }
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

impl<E: LogExporter> LogExporter for Counted<E> {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let count = batch.iter().count();
        let _released = Released {
            gate: &self.gate,
            count,
        };
        let result = self.inner.export(batch).await;
        if result.is_err() {
            self.gate.dropped(DropReason::ExportFailed, count);
        }
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn event_enabled(
        &self,
        level: opentelemetry::logs::Severity,
        target: &str,
        name: Option<&str>,
    ) -> bool {
        self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// A metric exporter that counts the delta points of each export it fails.
/// A cumulative point loses nothing: the next export carries its total.
#[derive(Debug)]
pub(crate) struct CountedMetrics<E> {
    inner: E,
    metrics: Arc<OnceLock<Metrics>>,
}

impl<E: PushMetricExporter> PushMetricExporter for CountedMetrics<E> {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let result = self.inner.export(metrics).await;
        if result.is_err()
            && let Some(catalog) = self.metrics.get()
            && let lost @ 1.. = delta_points(metrics)
        {
            catalog.telemetry_dropped(
                SignalKind::Metric,
                DropReason::ExportFailed,
                u64::try_from(lost).unwrap_or(u64::MAX),
            );
        }
        result
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

/// How many delta data points `metrics` holds. A gauge is never delta.
fn delta_points(metrics: &ResourceMetrics) -> usize {
    fn count<T>(data: &MetricData<T>) -> usize {
        let delta = |temporality, points: usize| {
            if temporality == Temporality::Delta {
                points
            } else {
                0
            }
        };
        match data {
            MetricData::Gauge(_) => 0,
            MetricData::Sum(sum) => delta(sum.temporality(), sum.data_points().count()),
            MetricData::Histogram(histogram) => {
                delta(histogram.temporality(), histogram.data_points().count())
            }
            MetricData::ExponentialHistogram(histogram) => {
                delta(histogram.temporality(), histogram.data_points().count())
            }
        }
    }
    metrics
        .scope_metrics()
        .flat_map(ScopeMetrics::metrics)
        .map(|metric| match metric.data() {
            AggregatedMetrics::F64(data) => count(data),
            AggregatedMetrics::U64(data) => count(data),
            AggregatedMetrics::I64(data) => count(data),
        })
        .sum()
}

#[cfg(test)]
mod tests;
