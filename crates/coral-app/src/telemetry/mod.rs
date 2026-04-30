//! Tracing and OpenTelemetry initialization for the local Coral process.

use std::collections::HashMap;
use std::sync::{Mutex, Once};
use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::propagation::{Extractor, TextMapPropagator as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

pub mod config;
pub mod metrics;

use config::DEFAULT_TRACE_FILTER;
pub use config::TelemetryConfig;

static INIT: Once = Once::new();
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);
static LOGGER_PROVIDER: Mutex<Option<SdkLoggerProvider>> = Mutex::new(None);
static METER_PROVIDER: Mutex<Option<SdkMeterProvider>> = Mutex::new(None);

const METRICS_INTERVAL: Duration = Duration::from_secs(5);

fn build_filter(log: &str) -> EnvFilter {
    EnvFilter::new(log)
}

fn build_trace_targets(filter: &str) -> (Targets, Option<String>) {
    match filter.parse() {
        Ok(targets) => (targets, None),
        Err(error) => (
            DEFAULT_TRACE_FILTER
                .parse()
                .expect("default trace filter must be valid"),
            Some(error.to_string()),
        ),
    }
}

fn initialize_metrics(meter_provider: Option<&SdkMeterProvider>) {
    if let Some(provider) = meter_provider {
        let meter = provider.meter("coral");
        metrics::init(&meter);
    } else {
        metrics::init_global();
    }
}

fn normalize_otlp_endpoint(endpoint: &str, signal: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    let base = ["traces", "logs", "metrics"]
        .into_iter()
        .find_map(|existing_signal| {
            base.strip_suffix(&format!("/v1/{existing_signal}"))
                .map(str::to_string)
        })
        .unwrap_or_else(|| base.to_string());
    format!("{base}/v1/{signal}")
}

fn parse_headers(raw: &str) -> HashMap<String, String> {
    raw.split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            let (key, value) = pair.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

/// Enters a root CLI span, optionally parented to the W3C `traceparent` value
/// in the `CORAL_TRACE_PARENT` environment variable.
///
/// The returned [`tracing::span::EnteredSpan`] must be kept alive for the
/// duration of the CLI invocation — dropping it exits the span.
#[allow(
    clippy::disallowed_methods,
    reason = "CORAL_TRACE_PARENT is intentionally read from the environment as a per-invocation CLI seed"
)]
pub fn seed_root_span() -> tracing::span::EnteredSpan {
    let span = tracing::info_span!("coral.cli");
    if let Ok(traceparent) = std::env::var("CORAL_TRACE_PARENT") {
        struct StringMapExtractor<'a>(&'a HashMap<String, String>);
        impl Extractor for StringMapExtractor<'_> {
            fn get(&self, key: &str) -> Option<&str> {
                self.0.get(key).map(String::as_str)
            }
            fn keys(&self) -> Vec<&str> {
                self.0.keys().map(String::as_str).collect()
            }
        }
        let carrier = HashMap::from([("traceparent".to_string(), traceparent)]);
        let parent_cx = TraceContextPropagator::new().extract(&StringMapExtractor(&carrier));
        let _ = span.set_parent(parent_cx);
    }
    span.entered()
}

#[allow(
    clippy::too_many_lines,
    reason = "Initialization configures three OTLP pipelines in one place"
)]
pub(crate) fn init_tracing(config: &TelemetryConfig) {
    INIT.call_once(|| {
        let endpoint = config
            .otel_endpoint
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let stderr_layer = config.otel_log_filter.as_deref().map(|log_filter| {
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .compact()
                .with_writer(std::io::stderr)
                .with_filter(build_filter(log_filter))
        });

        if let Some(ref endpoint) = endpoint {
            let resource = opentelemetry_sdk::Resource::builder()
                .with_attribute(opentelemetry::KeyValue::new(
                    "service.name",
                    config.otel_service_name.clone(),
                ))
                .build();

            let headers = parse_headers(config.otel_headers.as_deref().unwrap_or_default());

            let trace_exporter = SpanExporter::builder()
                .with_http()
                .with_endpoint(normalize_otlp_endpoint(endpoint, "traces"))
                .with_headers(headers.clone())
                .build()
                .expect("failed to build OTLP span exporter");
            let builder = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(
                    opentelemetry_sdk::trace::BatchSpanProcessor::builder(trace_exporter).build(),
                );

            let log_exporter = LogExporter::builder()
                .with_http()
                .with_endpoint(normalize_otlp_endpoint(endpoint, "logs"))
                .with_headers(headers.clone())
                .build()
                .expect("failed to build OTLP log exporter");
            let logger_provider = SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_log_processor(
                    opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter).build(),
                )
                .build();
            if let Ok(mut guard) = LOGGER_PROVIDER.lock() {
                *guard = Some(logger_provider);
            }

            let metric_exporter = MetricExporter::builder()
                .with_http()
                .with_endpoint(normalize_otlp_endpoint(endpoint, "metrics"))
                .with_headers(headers)
                .build()
                .expect("failed to build OTLP metric exporter");
            let meter_provider = SdkMeterProvider::builder()
                .with_resource(resource.clone())
                .with_reader(
                    opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
                        .with_interval(METRICS_INTERVAL)
                        .build(),
                )
                .build();
            opentelemetry::global::set_meter_provider(meter_provider.clone());
            initialize_metrics(Some(&meter_provider));
            if let Ok(mut guard) = METER_PROVIDER.lock() {
                *guard = Some(meter_provider);
            }

            let provider = builder.build();
            let tracer = provider.tracer("coral");
            let (trace_targets, trace_filter_error) =
                build_trace_targets(&config.otel_trace_filter);
            let otel_trace_layer = tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(trace_targets.clone());
            let otel_log_layer = LOGGER_PROVIDER.lock().ok().and_then(|guard| {
                guard.as_ref().map(|provider| {
                    opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(provider)
                        .with_filter(trace_targets)
                })
            });

            if let Ok(mut guard) = PROVIDER.lock() {
                *guard = Some(provider);
            }

            Registry::default()
                .with(stderr_layer)
                .with(otel_trace_layer)
                .with(otel_log_layer)
                .init();
            if let Some(error) = trace_filter_error {
                tracing::warn!(
                    provided_filter = %config.otel_trace_filter,
                    fallback_filter = DEFAULT_TRACE_FILTER,
                    detail = %error,
                    "invalid otel_trace_filter; falling back to default filter"
                );
            }
        } else {
            Registry::default().with(stderr_layer).init();
            initialize_metrics(None);
        }
    });
}

/// Flush any pending tracing, log, and metric exports before process exit.
pub fn shutdown_tracing() {
    if let Ok(mut guard) = METER_PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!("OTEL meter provider shutdown error: {error}");
    }
    if let Ok(mut guard) = PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!("OTEL trace provider shutdown error: {error}");
    }
    if let Ok(mut guard) = LOGGER_PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!("OTEL logger provider shutdown error: {error}");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        DEFAULT_TRACE_FILTER, build_trace_targets, normalize_otlp_endpoint, parse_headers,
    };

    #[test]
    fn normalize_otlp_endpoint_handles_signal_paths() {
        assert_eq!(
            normalize_otlp_endpoint("http://localhost:4318", "traces"),
            "http://localhost:4318/v1/traces"
        );
        assert_eq!(
            normalize_otlp_endpoint("http://localhost:4318/v1/traces", "logs"),
            "http://localhost:4318/v1/logs"
        );
        assert_eq!(
            normalize_otlp_endpoint("http://localhost:4318/", "metrics"),
            "http://localhost:4318/v1/metrics"
        );
    }

    #[test]
    fn parse_headers_ignores_invalid_pairs() {
        let headers = parse_headers("x-api-key=secret, invalid, user = coral ");

        assert_eq!(
            headers,
            HashMap::from([
                ("x-api-key".to_string(), "secret".to_string()),
                ("user".to_string(), "coral".to_string()),
            ])
        );
    }

    #[test]
    fn invalid_trace_filter_falls_back_to_default() {
        let (targets, error) = build_trace_targets("coral_app=[");
        let (expected, default_error) = build_trace_targets(DEFAULT_TRACE_FILTER);

        assert_eq!(format!("{targets:?}"), format!("{expected:?}"));
        assert!(error.is_some());
        assert!(default_error.is_none());
    }
}
