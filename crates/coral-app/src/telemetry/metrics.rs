//! App-owned query and bounded Universal Search metric instruments.

use std::sync::RwLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

#[derive(Clone)]
pub(crate) struct Metrics {
    count: Counter<u64>,
    duration: Histogram<f64>,
    rows: Histogram<u64>,
    search_native_count: Counter<u64>,
    search_native_duration: Histogram<f64>,
    search_native_selected_calls: Histogram<u64>,
    search_native_started_calls: Histogram<u64>,
    search_native_rows: Histogram<u64>,
    search_native_diagnostics: Counter<u64>,
}

impl Metrics {
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

    /// Records only bounded Universal Search fanout dimensions. Callers must
    /// supply stable enum-derived labels; source identities, query text,
    /// arguments, URLs, provider messages, and raw errors never enter metrics.
    pub(crate) fn record_search_native_fanout(
        &self,
        disposition: &'static str,
        duration: Duration,
        selected_calls: u64,
        started_calls: u64,
        returned_rows: u64,
        diagnostics: &[(&'static str, &'static str)],
    ) {
        let attributes = [KeyValue::new("disposition", disposition)];
        self.search_native_count.add(1, &attributes);
        self.search_native_duration
            .record(duration.as_secs_f64(), &attributes);
        self.search_native_selected_calls
            .record(selected_calls, &attributes);
        self.search_native_started_calls
            .record(started_calls, &attributes);
        self.search_native_rows.record(returned_rows, &attributes);

        for (state, reason) in diagnostics {
            self.search_native_diagnostics.add(
                1,
                &[
                    KeyValue::new("state", *state),
                    KeyValue::new("reason", *reason),
                ],
            );
        }
    }
}

fn status_attr(ok: bool) -> KeyValue {
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
        search_native_count: meter
            .u64_counter("coral.search.native.count")
            .with_unit("{searches}")
            .with_description("Universal Search requests by bounded native fanout disposition")
            .build(),
        search_native_duration: meter
            .f64_histogram("coral.search.native.duration")
            .with_unit("s")
            .with_description("Universal Search request latency by native fanout disposition")
            .build(),
        search_native_selected_calls: meter
            .u64_histogram("coral.search.native.selected_calls")
            .with_unit("{calls}")
            .with_description("Provider functions selected for bounded native fanout")
            .build(),
        search_native_started_calls: meter
            .u64_histogram("coral.search.native.started_calls")
            .with_unit("{calls}")
            .with_description("Provider function calls started by bounded native fanout")
            .build(),
        search_native_rows: meter
            .u64_histogram("coral.search.native.rows")
            .with_unit("{rows}")
            .with_description("Safe native rows returned by Universal Search")
            .build(),
        search_native_diagnostics: meter
            .u64_counter("coral.search.native.diagnostic.count")
            .with_unit("{diagnostics}")
            .with_description("Bounded native fanout diagnostic categories")
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
    use opentelemetry::{KeyValue, Value};
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};

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

    #[test]
    fn native_search_metrics_use_only_bounded_dispositions_and_diagnostics() {
        super::test_support::reset_metrics();
        let exporter = super::test_support::install_metrics_exporter();
        let metrics = metrics();

        metrics.record_search_native_fanout(
            "enabled",
            std::time::Duration::from_millis(350),
            3,
            2,
            5,
            &[("timed_out", "call_timeout"), ("error", "rate_limited")],
        );

        super::test_support::flush_metrics();
        let finished = exporter.get_finished_metrics().expect("finished metrics");
        let disposition = [ExpectedMetricPoint {
            attributes: &[("disposition", "enabled")],
            value: 1,
        }];
        assert_counter_points(&finished, "coral.search.native.count", &disposition);
        assert_histogram_counts(&finished, "coral.search.native.duration", &disposition);
        assert_u64_histogram_points(
            &finished,
            "coral.search.native.selected_calls",
            &[ExpectedMetricPoint {
                attributes: &[("disposition", "enabled")],
                value: 3,
            }],
        );
        assert_u64_histogram_points(
            &finished,
            "coral.search.native.started_calls",
            &[ExpectedMetricPoint {
                attributes: &[("disposition", "enabled")],
                value: 2,
            }],
        );
        assert_u64_histogram_points(
            &finished,
            "coral.search.native.rows",
            &[ExpectedMetricPoint {
                attributes: &[("disposition", "enabled")],
                value: 5,
            }],
        );
        assert_counter_points(
            &finished,
            "coral.search.native.diagnostic.count",
            &[
                ExpectedMetricPoint {
                    attributes: &[("reason", "call_timeout"), ("state", "timed_out")],
                    value: 1,
                },
                ExpectedMetricPoint {
                    attributes: &[("reason", "rate_limited"), ("state", "error")],
                    value: 1,
                },
            ],
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
