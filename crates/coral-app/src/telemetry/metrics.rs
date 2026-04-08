//! Shared query metric instruments.

use std::sync::RwLock;

use opentelemetry::metrics::{Counter, Histogram, Meter};

#[derive(Clone)]
pub(crate) struct Metrics {
    pub(crate) count: Counter<u64>,
    pub(crate) duration: Histogram<f64>,
    pub(crate) errors: Counter<u64>,
    pub(crate) rows: Histogram<u64>,
}

static METRICS: RwLock<Option<Metrics>> = RwLock::new(None);

fn build_metrics(meter: &Meter) -> Metrics {
    Metrics {
        count: meter
            .u64_counter("coral.query.count")
            .with_unit("{queries}")
            .with_description("Total queries executed")
            .build(),
        duration: meter
            .f64_histogram("coral.query.duration")
            .with_unit("ms")
            .with_description("Query execution latency")
            .build(),
        errors: meter
            .u64_counter("coral.query.errors")
            .with_unit("{errors}")
            .with_description("Failed queries")
            .build(),
        rows: meter
            .u64_histogram("coral.query.rows")
            .with_unit("{rows}")
            .with_description("Rows returned per query")
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

    let initialized = METRICS
        .read()
        .expect("metrics lock poisoned during read")
        .is_some();
    if !initialized {
        init_global();
    }

    METRICS
        .read()
        .expect("metrics lock poisoned during read")
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
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};

    use super::metrics;

    fn sum_u64(metrics: &[ResourceMetrics], name: &str) -> u64 {
        metrics
            .iter()
            .rev()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
            .and_then(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                    .data_points()
                    .next()
                    .map(opentelemetry_sdk::metrics::data::SumDataPoint::value),
                _ => None,
            })
            .unwrap_or(0)
    }

    fn histogram_count(metrics: &[ResourceMetrics], name: &str) -> u64 {
        metrics
            .iter()
            .rev()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
            .and_then(|metric| match metric.data() {
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                    .data_points()
                    .next()
                    .map(opentelemetry_sdk::metrics::data::HistogramDataPoint::count),
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                    .data_points()
                    .next()
                    .map(opentelemetry_sdk::metrics::data::HistogramDataPoint::count),
                _ => None,
            })
            .unwrap_or(0)
    }

    #[test]
    fn query_metrics_record_counts_errors_and_rows() {
        super::test_support::reset_metrics();
        let exporter = super::test_support::install_metrics_exporter();
        let metrics = metrics();

        metrics.count.add(2, &[]);
        metrics.errors.add(1, &[]);
        metrics.duration.record(12.5, &[]);
        metrics.rows.record(7, &[]);

        super::test_support::flush_metrics();
        let finished = exporter.get_finished_metrics().expect("finished metrics");
        assert_eq!(sum_u64(&finished, "coral.query.count"), 2);
        assert_eq!(sum_u64(&finished, "coral.query.errors"), 1);
        assert_eq!(histogram_count(&finished, "coral.query.duration"), 1);
        assert_eq!(histogram_count(&finished, "coral.query.rows"), 1);
    }
}
