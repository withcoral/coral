//! Tracing and OpenTelemetry initialization for the local Coral process.

use std::collections::HashMap;
use std::sync::{Mutex, Once};
use std::time::Duration;

use opentelemetry::Context;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _,
};
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator, SdkTracerProvider};
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

pub mod config;
pub mod metrics;

pub use config::TelemetryConfig;

static INIT: Once = Once::new();
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);
static LOGGER_PROVIDER: Mutex<Option<SdkLoggerProvider>> = Mutex::new(None);
static METER_PROVIDER: Mutex<Option<SdkMeterProvider>> = Mutex::new(None);
static REMOTE_CONTEXT: Mutex<Option<Context>> = Mutex::new(None);

const METRICS_INTERVAL: Duration = Duration::from_secs(5);

fn build_filter(log: &str) -> EnvFilter {
    EnvFilter::new(log)
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

fn parse_traceparent(value: &str) -> Option<(TraceId, SpanContext)> {
    let parts: Vec<&str> = value.split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    let trace_id = TraceId::from_hex(parts[1]).ok()?;
    let span_id = SpanId::from_hex(parts[2]).ok()?;
    let flags = u8::from_str_radix(parts[3], 16).ok()?;
    let span_context = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(flags),
        true,
        TraceState::default(),
    );
    Some((trace_id, span_context))
}

pub(crate) fn remote_parent_context() -> Context {
    REMOTE_CONTEXT
        .lock()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_default()
}

#[derive(Debug)]
struct FixedTraceIdGenerator {
    trace_id: TraceId,
    inner: RandomIdGenerator,
}

impl IdGenerator for FixedTraceIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        self.trace_id
    }

    fn new_span_id(&self) -> SpanId {
        self.inner.new_span_id()
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "CORAL_TRACE_PARENT is intentionally read from the environment as a per-invocation override"
)]
#[allow(
    clippy::too_many_lines,
    reason = "Initialization configures three OTLP pipelines in one place"
)]
pub(crate) fn init_tracing(config: &TelemetryConfig) {
    INIT.call_once(|| {
        let trace_parent = std::env::var("CORAL_TRACE_PARENT").ok();
        let endpoint = config
            .otel_endpoint
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let stderr_layer = config.log_filter.as_deref().map(|log_filter| {
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .compact()
                .with_writer(std::io::stderr)
                .with_filter(build_filter(log_filter))
        });

        if endpoint.is_some() || trace_parent.is_some() {
            let resource = opentelemetry_sdk::Resource::builder()
                .with_attribute(opentelemetry::KeyValue::new(
                    "service.name",
                    config.otel_service_name.clone(),
                ))
                .build();
            let mut builder = SdkTracerProvider::builder().with_resource(resource.clone());

            if let Some(ref trace_parent) = trace_parent {
                if let Some((trace_id, span_context)) = parse_traceparent(trace_parent) {
                    builder = builder.with_id_generator(FixedTraceIdGenerator {
                        trace_id,
                        inner: RandomIdGenerator::default(),
                    });
                    let parent_context = Context::current().with_remote_span_context(span_context);
                    if let Ok(mut guard) = REMOTE_CONTEXT.lock() {
                        *guard = Some(parent_context);
                    }
                } else {
                    tracing::warn!("invalid CORAL_TRACE_PARENT format: {trace_parent}");
                }
            }

            if let Some(ref endpoint) = endpoint {
                let headers = parse_headers(config.otel_headers.as_deref().unwrap_or_default());

                let trace_exporter = SpanExporter::builder()
                    .with_http()
                    .with_endpoint(normalize_otlp_endpoint(endpoint, "traces"))
                    .with_headers(headers.clone())
                    .build()
                    .expect("failed to build OTLP span exporter");
                builder = builder.with_span_processor(
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
            }

            let provider = builder.build();
            let tracer = provider.tracer("coral");
            let trace_targets: Targets = config
                .trace_filter
                .parse()
                .expect("trace filter must be valid");
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
        } else {
            Registry::default().with(stderr_layer).init();
        }

        if endpoint.is_none() {
            initialize_metrics(None);
        }
    });
}

/// Flush any pending tracing, log, and metric exports before process exit.
pub fn shutdown_tracing() {
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
    if let Ok(mut guard) = METER_PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!("OTEL meter provider shutdown error: {error}");
    }
    if let Ok(mut guard) = REMOTE_CONTEXT.lock() {
        *guard = None;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use opentelemetry::trace::TraceContextExt as _;

    use super::{normalize_otlp_endpoint, parse_headers, parse_traceparent, remote_parent_context};

    #[test]
    fn parse_traceparent_valid() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let (trace_id, span_context) = parse_traceparent(traceparent).expect("traceparent");

        assert_eq!(trace_id.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert!(span_context.trace_flags().is_sampled());
    }

    #[test]
    fn parse_traceparent_not_sampled() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
        let (_, span_context) = parse_traceparent(traceparent).expect("traceparent");

        assert!(!span_context.trace_flags().is_sampled());
    }

    #[test]
    fn parse_traceparent_invalid_format() {
        assert!(parse_traceparent("not-a-traceparent").is_none());
        assert!(parse_traceparent("").is_none());
        assert!(parse_traceparent("too-few-parts").is_none());
        assert!(parse_traceparent("00-xyz-00f067aa0ba902b7-01").is_none());
    }

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
    fn remote_parent_context_defaults_to_empty_context() {
        assert!(!remote_parent_context().has_active_span());
    }
}
