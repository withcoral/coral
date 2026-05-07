//! Shared query metric instruments.

use std::sync::RwLock;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

#[derive(Clone)]
pub(crate) struct Metrics {
    pub(crate) count: Counter<u64>,
    pub(crate) duration: Histogram<f64>,
    pub(crate) rows: Histogram<u64>,
}

pub(crate) fn status_attr(ok: bool) -> KeyValue {
    KeyValue::new("status", if ok { "ok" } else { "error" })
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
            .with_unit("s")
            .with_description("Query execution latency")
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
    use opentelemetry::Value;
    use opentelemetry_sdk::metrics::data::{
        AggregatedMetrics, Histogram, MetricData, ResourceMetrics,
    };

    use super::metrics;

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

    fn histogram_total_count(metrics: &[ResourceMetrics], name: &str) -> u64 {
        find_metric(metrics, name).map_or(0, |metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(opentelemetry_sdk::metrics::data::HistogramDataPoint::count)
                .sum(),
            AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(opentelemetry_sdk::metrics::data::HistogramDataPoint::count)
                .sum(),
            _ => 0,
        })
    }

    fn histogram_has_status(metrics: &[ResourceMetrics], name: &str, status: &str) -> bool {
        find_metric(metrics, name).is_some_and(|metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                histogram_data_has_status(histogram, status)
            }
            AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                histogram_data_has_status(histogram, status)
            }
            _ => false,
        })
    }

    fn histogram_data_has_status<T>(histogram: &Histogram<T>, status: &str) -> bool {
        histogram.data_points().any(|point| {
            point.attributes().any(|attr| {
                attr.key.as_str() == "status"
                    && matches!(&attr.value, Value::String(value) if value.as_str() == status)
            })
        })
    }

    fn counter_total(metrics: &[ResourceMetrics], name: &str) -> u64 {
        find_metric(metrics, name).map_or(0, |metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(opentelemetry_sdk::metrics::data::SumDataPoint::value)
                .sum(),
            _ => 0,
        })
    }

    #[test]
    fn query_metrics_record_counts_and_rows_with_status() {
        super::test_support::reset_metrics();
        let exporter = super::test_support::install_metrics_exporter();
        let metrics = metrics();

        let ok = super::status_attr(true);
        let err = super::status_attr(false);
        metrics.count.add(2, std::slice::from_ref(&ok));
        metrics.count.add(1, std::slice::from_ref(&err));
        metrics.duration.record(0.5, std::slice::from_ref(&ok));
        metrics.duration.record(0.1, std::slice::from_ref(&err));
        metrics.rows.record(7, std::slice::from_ref(&ok));

        super::test_support::flush_metrics();
        let finished = exporter.get_finished_metrics().expect("finished metrics");
        assert_eq!(counter_total(&finished, "coral.query.count"), 3);
        assert_eq!(histogram_total_count(&finished, "coral.query.duration"), 2);
        assert_eq!(histogram_total_count(&finished, "coral.query.rows"), 1);
        assert!(histogram_has_status(&finished, "coral.query.rows", "ok"));
    }
}
