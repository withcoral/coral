use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::logs::{LogRecord as _, Logger as _, LoggerProvider as _};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{Tracer as _, TracerProvider as _};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::logs::{LogBatch, LogExporter, SdkLoggerProvider};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

use super::{batch_log_processor, batch_span_processor, counted_metric_exporter};
use crate::testing::MetricCapture;

/// An exporter that holds every export until it is opened, then fails or
/// succeeds.
#[derive(Debug, Clone, Default)]
struct Held {
    open: Arc<(Mutex<bool>, Condvar)>,
    fail: bool,
    temporality: Temporality,
}

impl Held {
    fn failing() -> Self {
        let held = Self {
            fail: true,
            ..Self::default()
        };
        held.release();
        held
    }

    fn release(&self) {
        let (open, changed) = &*self.open;
        *open.lock().expect("lock") = true;
        changed.notify_all();
    }

    fn export(&self) -> OTelSdkResult {
        let (open, changed) = &*self.open;
        let poisoned = |_| OTelSdkError::InternalFailure("poisoned".into());
        let _open = changed
            .wait_while(open.lock().map_err(poisoned)?, |open| !*open)
            .map_err(poisoned)?;
        if self.fail {
            Err(OTelSdkError::InternalFailure("canary".into()))
        } else {
            Ok(())
        }
    }
}

impl SpanExporter for Held {
    fn export(&self, _batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        std::future::ready(Held::export(self))
    }
}

impl LogExporter for Held {
    fn export(&self, _batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
        std::future::ready(Held::export(self))
    }
}

impl PushMetricExporter for Held {
    fn export(&self, _metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
        std::future::ready(Held::export(self))
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}

/// The `coral.telemetry.dropped` points, as signal kind, reason and count.
fn dropped(capture: &MetricCapture) -> Vec<(String, String, f64)> {
    let mut points = capture
        .named("coral.telemetry.dropped")
        .into_iter()
        .map(|point| {
            let attribute = |key: &str| point.attributes.get(key).cloned().unwrap_or_default();
            (
                attribute("signal.kind"),
                attribute("drop.reason"),
                point.value,
            )
        })
        .collect::<Vec<_>>();
    points.sort_by(|left, right| left.partial_cmp(right).expect("comparable"));
    points
}

/// Ends `count` spans, each in well under the time an export holds.
fn end_spans(provider: &SdkTracerProvider, count: usize) {
    let tracer = provider.tracer("test");
    for _ in 0..count {
        let started = Instant::now();
        tracer.in_span("span", |_| {});
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "ending a span never waits"
        );
    }
}

/// With its queue full behind a held export, a span is dropped at once and
/// counted, and a span whose export fails is counted too.
#[test]
fn spans_beyond_a_full_queue_or_a_failed_export_are_counted() {
    let capture = MetricCapture::default();
    let held = Held::default();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(batch_span_processor(held.clone(), 2, capture.metrics()))
        .build();
    // Two spans fill the queue and are held in export; three more find it full.
    end_spans(&provider, 5);
    assert_eq!(
        dropped(&capture),
        [("span".into(), "queue_full".into(), 3.0)]
    );
    held.release();
    provider.force_flush().expect("flush");
    end_spans(&provider, 2);
    provider.force_flush().expect("flush");
    assert_eq!(
        dropped(&capture),
        [("span".into(), "queue_full".into(), 3.0)],
        "a freed queue takes spans again"
    );

    let failing = SdkTracerProvider::builder()
        .with_span_processor(batch_span_processor(Held::failing(), 8, capture.metrics()))
        .build();
    end_spans(&failing, 2);
    assert!(failing.force_flush().is_err(), "the export failed");
    assert_eq!(
        dropped(&capture),
        [
            ("span".into(), "export_failed".into(), 2.0),
            ("span".into(), "queue_full".into(), 3.0),
        ]
    );
}

#[test]
fn log_records_beyond_a_full_queue_or_a_failed_export_are_counted() {
    let capture = MetricCapture::default();
    let held = Held::default();
    let provider = SdkLoggerProvider::builder()
        .with_log_processor(batch_log_processor(held.clone(), 2, capture.metrics()))
        .build();
    let logger = provider.logger("test");
    for _ in 0..5 {
        let started = Instant::now();
        let mut record = logger.create_log_record();
        record.set_body("logged".into());
        logger.emit(record);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "emitting never waits"
        );
    }
    assert_eq!(
        dropped(&capture),
        [("log".into(), "queue_full".into(), 3.0)]
    );
    held.release();

    let failing = SdkLoggerProvider::builder()
        .with_log_processor(batch_log_processor(Held::failing(), 8, capture.metrics()))
        .build();
    failing
        .logger("test")
        .emit(failing.logger("test").create_log_record());
    assert!(failing.force_flush().is_err(), "the export failed");
    assert_eq!(
        dropped(&capture),
        [
            ("log".into(), "export_failed".into(), 1.0),
            ("log".into(), "queue_full".into(), 3.0),
        ]
    );
}

/// A failed metric export drops the delta points it carried. A cumulative
/// point loses nothing: the next export carries its total.
#[test]
fn delta_metric_points_of_a_failed_export_are_counted() {
    let cases = [
        (
            Temporality::Delta,
            vec![("metric".into(), "export_failed".into(), 2.0)],
        ),
        (Temporality::Cumulative, vec![]),
    ];
    for (temporality, expected) in cases {
        let capture = MetricCapture::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(counted_metric_exporter(
                    Held {
                        temporality,
                        ..Held::failing()
                    },
                    Arc::new(capture.metrics().into()),
                ))
                .build(),
            )
            .build();
        let counter = provider.meter("test").u64_counter("test.counter").build();
        counter.add(1, &[opentelemetry::KeyValue::new("outcome", "ok")]);
        counter.add(1, &[opentelemetry::KeyValue::new("outcome", "error")]);
        assert!(provider.force_flush().is_err(), "the export failed");
        assert_eq!(dropped(&capture), expected, "{temporality:?}");
    }
}
