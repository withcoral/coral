//! Shared query metric instruments.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use coral_engine::QueryMemoryOutcome;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, ObservableGauge};

const QUERY_MEMORY_BUCKETS: [f64; 12] = [
    4_096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
    1_073_741_824.0,
    4_294_967_296.0,
    17_179_869_184.0,
];
const EXECUTE_SQL: &str = "execute_sql";

#[derive(Clone)]
pub(crate) struct Metrics {
    count: Counter<u64>,
    duration: Histogram<f64>,
    rows: Histogram<u64>,
    active_queries: Arc<AtomicU64>,
    _active_query_gauge: ObservableGauge<u64>,
    datafusion_reserved_peak: Histogram<u64>,
    arrow_estimated_occupied_memory: Histogram<u64>,
}

impl Metrics {
    pub(crate) fn begin_query(&self) -> ActiveQueryGuard {
        self.active_queries.fetch_add(1, Ordering::Relaxed);
        ActiveQueryGuard {
            active_queries: Arc::clone(&self.active_queries),
        }
    }

    pub(crate) fn record_query(
        &self,
        operation: &'static str,
        duration: Duration,
        row_count: Option<u64>,
        ok: bool,
    ) {
        let status = status_attr(ok);
        let attributes = [status, KeyValue::new("operation", operation)];
        self.count.add(1, &attributes);
        self.duration.record(duration.as_secs_f64(), &attributes);

        if let Some(row_count) = row_count {
            self.rows.record(row_count, &attributes);
        }
    }

    pub(crate) fn record_datafusion_reserved_peak(
        &self,
        bytes: Option<u64>,
        outcome: QueryMemoryOutcome,
    ) {
        let Some(bytes) = bytes else {
            return;
        };
        self.datafusion_reserved_peak
            .record(bytes, &query_memory_attributes(outcome));
    }

    pub(crate) fn record_arrow_estimated_occupied_memory(&self, bytes: Option<u64>) {
        let Some(bytes) = bytes else {
            return;
        };
        self.arrow_estimated_occupied_memory
            .record(bytes, &query_memory_attributes(QueryMemoryOutcome::Success));
    }
}

pub(crate) struct ActiveQueryGuard {
    active_queries: Arc<AtomicU64>,
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        let previous = self.active_queries.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "active query count underflow");
    }
}

fn status_attr(ok: bool) -> KeyValue {
    KeyValue::new("status", if ok { "ok" } else { "error" })
}

fn query_memory_attributes(outcome: QueryMemoryOutcome) -> [KeyValue; 2] {
    let ok = matches!(outcome, QueryMemoryOutcome::Success);
    [KeyValue::new("operation", EXECUTE_SQL), status_attr(ok)]
}

static METRICS: RwLock<Option<Metrics>> = RwLock::new(None);

fn build_metrics(meter: &Meter) -> Metrics {
    let active_queries = Arc::new(AtomicU64::new(0));
    let active_query_count = Arc::clone(&active_queries);

    Metrics {
        count: meter
            .u64_counter("coral.query.count")
            .with_unit("{queries}")
            .with_description("Total queries executed")
            .build(),
        duration: meter
            .f64_histogram("coral.query.duration")
            .with_unit("s")
            .with_description("Query execution latency")
            .build(),
        rows: meter
            .u64_histogram("coral.query.rows")
            .with_unit("{rows}")
            .with_description("Rows returned per query")
            .build(),
        active_queries,
        _active_query_gauge: meter
            .u64_observable_gauge("coral.query.active")
            .with_unit("{queries}")
            .with_description("Current query operations in flight")
            .with_callback(move |observer| {
                observer.observe(active_query_count.load(Ordering::Relaxed), &[]);
            })
            .build(),
        datafusion_reserved_peak: meter
            .u64_histogram("coral.query.memory.datafusion_reserved_peak")
            .with_unit("By")
            .with_description("Peak bytes reserved by DataFusion per query")
            .with_boundaries(QUERY_MEMORY_BUCKETS.to_vec())
            .build(),
        arrow_estimated_occupied_memory: meter
            .u64_histogram("coral.query.result.arrow.estimated_occupied_memory")
            .with_unit("By")
            .with_description("Estimated Arrow occupied memory per query result")
            .with_boundaries(QUERY_MEMORY_BUCKETS.to_vec())
            .build(),
    }
}

pub(crate) fn init(meter: &Meter) {
    let mut metrics = METRICS
        .write()
        .expect("metrics lock poisoned during initialization");
    *metrics = Some(build_metrics(meter));
}

pub(crate) fn init_global() {
    let meter = opentelemetry::global::meter("coral");
    init(&meter);
}

pub(crate) fn metrics() -> Metrics {
    #[cfg(test)]
    if let Some(metrics) = test_support::metrics_for_test() {
        return metrics;
    }

    if let Some(metrics) = METRICS
        .read()
        .expect("metrics lock poisoned during read")
        .clone()
    {
        return metrics;
    }

    let mut metrics = METRICS
        .write()
        .expect("metrics lock poisoned during initialization");
    if metrics.is_none() {
        let meter = opentelemetry::global::meter("coral");
        *metrics = Some(build_metrics(&meter));
    }

    metrics
        .clone()
        .expect("metrics must be initialized before use")
}

#[cfg(test)]
pub(crate) mod test_support {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    use super::METRICS;

    std::thread_local! {
        static TEST_METER_PROVIDER: std::cell::RefCell<Option<SdkMeterProvider>> =
            const { std::cell::RefCell::new(None) };
        static TEST_METRICS: std::cell::RefCell<Option<super::Metrics>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(crate) fn metrics_for_test() -> Option<super::Metrics> {
        TEST_METRICS.with(|metrics| metrics.borrow().clone())
    }

    fn install_provider(provider: SdkMeterProvider) {
        let meter = provider.meter("coral");
        let metrics = super::build_metrics(&meter);
        TEST_METRICS.with(|slot| {
            *slot.borrow_mut() = Some(metrics);
        });
        TEST_METER_PROVIDER.with(|slot| {
            *slot.borrow_mut() = Some(provider);
        });
    }

    pub(crate) fn install_metrics_exporter() -> InMemoryMetricExporter {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        install_provider(provider);
        exporter
    }

    pub(crate) fn flush_metrics() {
        TEST_METER_PROVIDER.with(|slot| {
            if let Some(provider) = slot.borrow().as_ref() {
                provider
                    .force_flush()
                    .expect("test metrics flush should work");
            }
        });
    }

    pub(crate) fn reset_metrics() {
        TEST_METRICS.with(|slot| {
            *slot.borrow_mut() = None;
        });
        TEST_METER_PROVIDER.with(|slot| {
            *slot.borrow_mut() = None;
        });
        *METRICS
            .write()
            .expect("metrics lock poisoned during test reset") = None;
    }
}

#[cfg(test)]
mod tests {
    use coral_engine::QueryMemoryOutcome;
    use opentelemetry::{KeyValue, Value};
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};

    use super::{QUERY_MEMORY_BUCKETS, metrics};

    fn find_metric<'a>(
        metrics: &'a [ResourceMetrics],
        name: &str,
    ) -> Option<&'a opentelemetry_sdk::metrics::data::Metric> {
        metrics
            .iter()
            .rev()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
    }

    #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct MetricPoint {
        attributes: Vec<(String, String)>,
        value: u64,
    }

    struct ExpectedMetricPoint<'a> {
        attributes: &'a [(&'a str, &'a str)],
        value: u64,
    }

    #[test]
    fn query_memory_active_gauge_emits_zero_and_tracks_overlaps() {
        super::test_support::reset_metrics();
        let exporter = super::test_support::install_metrics_exporter();
        let metrics = metrics();

        assert_active_queries(&exporter, 0);
        let first = metrics.begin_query();
        assert_active_queries(&exporter, 1);
        let second = metrics.begin_query();
        assert_active_queries(&exporter, 2);
        drop(first);
        assert_active_queries(&exporter, 1);
        drop(second);
        assert_active_queries(&exporter, 0);
    }

    #[test]
    fn query_memory_histograms_export_contract_and_preserve_zero() {
        super::test_support::reset_metrics();
        let exporter = super::test_support::install_metrics_exporter();
        let metrics = metrics();

        metrics.record_datafusion_reserved_peak(Some(0), QueryMemoryOutcome::Success);
        metrics.record_datafusion_reserved_peak(Some(4_096), QueryMemoryOutcome::Error);
        metrics
            .record_datafusion_reserved_peak(Some(17_179_869_185), QueryMemoryOutcome::Cancelled);
        metrics.record_datafusion_reserved_peak(None, QueryMemoryOutcome::Success);
        metrics.record_arrow_estimated_occupied_memory(Some(0));
        metrics.record_arrow_estimated_occupied_memory(None);

        super::test_support::flush_metrics();
        let finished = exporter.get_finished_metrics().expect("finished metrics");
        assert_query_memory_histogram(
            &finished,
            "coral.query.memory.datafusion_reserved_peak",
            "Peak bytes reserved by DataFusion per query",
            &[
                ExpectedMetricPoint {
                    attributes: &[("operation", "execute_sql"), ("status", "error")],
                    value: 2,
                },
                ExpectedMetricPoint {
                    attributes: &[("operation", "execute_sql"), ("status", "ok")],
                    value: 1,
                },
            ],
        );
        let reservation = find_metric(&finished, "coral.query.memory.datafusion_reserved_peak")
            .expect("reservation peak metric");
        let AggregatedMetrics::U64(MetricData::Histogram(histogram)) = reservation.data() else {
            panic!("reservation peak should be a u64 histogram");
        };
        let error_point = histogram
            .data_points()
            .find(|point| {
                attributes(point.attributes())
                    == expected_attributes(&[("operation", "execute_sql"), ("status", "error")])
            })
            .expect("reservation error point");
        assert_eq!(
            error_point.bucket_counts().collect::<Vec<_>>().last(),
            Some(&1),
            "overflow bucket should contain the cancelled observation"
        );

        assert_query_memory_histogram(
            &finished,
            "coral.query.result.arrow.estimated_occupied_memory",
            "Estimated Arrow occupied memory per query result",
            &[ExpectedMetricPoint {
                attributes: &[("operation", "execute_sql"), ("status", "ok")],
                value: 1,
            }],
        );
        assert_u64_histogram_points(
            &finished,
            "coral.query.result.arrow.estimated_occupied_memory",
            &[ExpectedMetricPoint {
                attributes: &[("operation", "execute_sql"), ("status", "ok")],
                value: 0,
            }],
        );
    }

    fn assert_active_queries(
        exporter: &opentelemetry_sdk::metrics::InMemoryMetricExporter,
        expected: u64,
    ) {
        super::test_support::flush_metrics();
        let finished = exporter.get_finished_metrics().expect("finished metrics");
        let metric = find_metric(&finished, "coral.query.active").expect("active query metric");
        assert_eq!(metric.unit(), "{queries}");
        assert_eq!(metric.description(), "Current query operations in flight");
        let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
            panic!("active query metric should be a u64 gauge");
        };
        let points = gauge.data_points().collect::<Vec<_>>();
        assert_eq!(points.len(), 1);
        let point = points.first().expect("active query gauge point");
        assert!(point.attributes().next().is_none());
        assert_eq!(point.value(), expected);
    }

    fn assert_query_memory_histogram(
        metrics: &[ResourceMetrics],
        name: &str,
        description: &str,
        expected: &[ExpectedMetricPoint<'_>],
    ) {
        let metric = find_metric(metrics, name).unwrap_or_else(|| panic!("metric {name} missing"));
        assert_eq!(metric.unit(), "By");
        assert_eq!(metric.description(), description);
        let AggregatedMetrics::U64(MetricData::Histogram(histogram)) = metric.data() else {
            panic!("metric {name} should be a u64 histogram");
        };
        for point in histogram.data_points() {
            assert_eq!(point.bounds().collect::<Vec<_>>(), QUERY_MEMORY_BUCKETS);
            assert_eq!(
                point.bucket_counts().count(),
                QUERY_MEMORY_BUCKETS.len() + 1
            );
        }
        let mut actual = histogram
            .data_points()
            .map(|point| MetricPoint {
                attributes: attributes(point.attributes()),
                value: point.count(),
            })
            .collect::<Vec<_>>();
        assert_metric_points(name, &mut actual, expected);
    }

    #[test]
    fn query_metrics_record_counts_and_rows_with_status() {
        super::test_support::reset_metrics();
        let exporter = super::test_support::install_metrics_exporter();
        let metrics = metrics();

        metrics.record_query(
            "execute_sql",
            std::time::Duration::from_millis(500),
            Some(7),
            true,
        );
        metrics.record_query(
            "execute_sql",
            std::time::Duration::from_millis(100),
            None,
            false,
        );
        metrics.record_query(
            "explain_sql",
            std::time::Duration::from_millis(250),
            None,
            true,
        );

        super::test_support::flush_metrics();
        let finished = exporter.get_finished_metrics().expect("finished metrics");
        assert_counter_points(
            &finished,
            "coral.query.count",
            &[
                ExpectedMetricPoint {
                    attributes: &[("operation", "execute_sql"), ("status", "error")],
                    value: 1,
                },
                ExpectedMetricPoint {
                    attributes: &[("operation", "execute_sql"), ("status", "ok")],
                    value: 1,
                },
                ExpectedMetricPoint {
                    attributes: &[("operation", "explain_sql"), ("status", "ok")],
                    value: 1,
                },
            ],
        );
        assert_histogram_counts(
            &finished,
            "coral.query.duration",
            &[
                ExpectedMetricPoint {
                    attributes: &[("operation", "execute_sql"), ("status", "error")],
                    value: 1,
                },
                ExpectedMetricPoint {
                    attributes: &[("operation", "execute_sql"), ("status", "ok")],
                    value: 1,
                },
                ExpectedMetricPoint {
                    attributes: &[("operation", "explain_sql"), ("status", "ok")],
                    value: 1,
                },
            ],
        );
        assert_u64_histogram_points(
            &finished,
            "coral.query.rows",
            &[ExpectedMetricPoint {
                attributes: &[("operation", "execute_sql"), ("status", "ok")],
                value: 7,
            }],
        );
    }

    fn assert_counter_points(
        metrics: &[ResourceMetrics],
        name: &str,
        expected: &[ExpectedMetricPoint<'_>],
    ) {
        let metric = find_metric(metrics, name).unwrap_or_else(|| panic!("metric {name} missing"));
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
            panic!("metric {name} should be a u64 sum");
        };
        let mut actual = sum
            .data_points()
            .map(|point| MetricPoint {
                attributes: attributes(point.attributes()),
                value: point.value(),
            })
            .collect::<Vec<_>>();
        assert_metric_points(name, &mut actual, expected);
    }

    fn assert_histogram_counts(
        metrics: &[ResourceMetrics],
        name: &str,
        expected: &[ExpectedMetricPoint<'_>],
    ) {
        let metric = find_metric(metrics, name).unwrap_or_else(|| panic!("metric {name} missing"));
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
            panic!("metric {name} should be an f64 histogram");
        };
        let mut actual = histogram
            .data_points()
            .map(|point| MetricPoint {
                attributes: attributes(point.attributes()),
                value: point.count(),
            })
            .collect::<Vec<_>>();
        assert_metric_points(name, &mut actual, expected);
    }

    fn assert_u64_histogram_points(
        metrics: &[ResourceMetrics],
        name: &str,
        expected: &[ExpectedMetricPoint<'_>],
    ) {
        let metric = find_metric(metrics, name).unwrap_or_else(|| panic!("metric {name} missing"));
        let AggregatedMetrics::U64(MetricData::Histogram(histogram)) = metric.data() else {
            panic!("metric {name} should be a u64 histogram");
        };
        let mut actual = histogram
            .data_points()
            .map(|point| MetricPoint {
                attributes: attributes(point.attributes()),
                value: point.sum(),
            })
            .collect::<Vec<_>>();
        assert_metric_points(name, &mut actual, expected);
    }

    fn assert_metric_points(
        name: &str,
        actual: &mut Vec<MetricPoint>,
        expected: &[ExpectedMetricPoint<'_>],
    ) {
        actual.sort();
        let mut expected = expected_points(expected);
        expected.sort();
        assert_eq!(actual, &expected, "metric {name} data points");
    }

    fn expected_points(expected: &[ExpectedMetricPoint<'_>]) -> Vec<MetricPoint> {
        expected
            .iter()
            .map(|point| MetricPoint {
                attributes: expected_attributes(point.attributes),
                value: point.value,
            })
            .collect()
    }

    fn attributes<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> Vec<(String, String)> {
        let mut attributes = attributes
            .map(|attribute| {
                (
                    attribute.key.as_str().to_string(),
                    value_string(&attribute.value),
                )
            })
            .collect::<Vec<_>>();
        attributes.sort();
        attributes
    }

    fn expected_attributes(attributes: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut attributes = attributes
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<Vec<_>>();
        attributes.sort();
        attributes
    }

    fn value_string(value: &Value) -> String {
        match value {
            Value::String(value) => value.to_string(),
            _ => value.to_string(),
        }
    }
}
