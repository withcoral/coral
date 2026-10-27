//! Tracing and OpenTelemetry initialization for the local Coral process.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use coral_telemetry::OtlpSpanFilter;
use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter as OtlpSpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, SdkTracer, SdkTracerProvider, TracerProviderBuilder,
};
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt as _};
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::{self, FilterExt as _, Filtered, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

pub mod config;

use crate::bootstrap::AppError;
pub use config::TelemetryConfig;
use config::{DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER, REQUEST_CONTEXT_FILTER};
static INIT: OnceLock<Result<(), String>> = OnceLock::new();
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);
static LOGGER_PROVIDER: Mutex<Option<SdkLoggerProvider>> = Mutex::new(None);
static METER_PROVIDER: Mutex<Option<SdkMeterProvider>> = Mutex::new(None);

const METRICS_INTERVAL: Duration = Duration::from_secs(5);
fn build_log_filter(filter: Option<&str>) -> (EnvFilter, Option<String>) {
    let Some(filter) = filter else {
        return (EnvFilter::new(DEFAULT_LOG_FILTER), None);
    };

    match EnvFilter::try_new(filter) {
        Ok(filter) => (filter, None),
        Err(error) => (EnvFilter::new(DEFAULT_LOG_FILTER), Some(error.to_string())),
    }
}

fn build_trace_targets(filter: &str, fallback_filter: &str) -> (Targets, Option<String>) {
    match filter.parse() {
        Ok(targets) => (targets, None),
        Err(error) => (
            fallback_filter
                .parse()
                .expect("fallback trace filter must be valid"),
            Some(error.to_string()),
        ),
    }
}

fn trace_layer_filter(otlp_filter: &str) -> (Targets, Option<String>) {
    build_trace_targets(otlp_filter, DEFAULT_TRACE_FILTER)
}

fn request_context_targets() -> Targets {
    REQUEST_CONTEXT_FILTER
        .parse()
        .expect("request context filter must be valid")
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

fn configured_otlp_endpoint(config: &TelemetryConfig) -> Option<&str> {
    config
        .otel
        .endpoint
        .as_deref()
        .filter(|value| !value.is_empty())
}

/// The process's resource: its service name, and as `service.instance.id`
/// the id of the Coral Instance whose start initialized telemetry, so every
/// exported span, log and metric names that Coral Instance.
fn telemetry_resource(service_name: &str, instance_id: &str) -> Resource {
    Resource::builder()
        .with_attribute(opentelemetry::KeyValue::new(
            "service.name",
            service_name.to_string(),
        ))
        .with_attribute(opentelemetry::KeyValue::new(
            "service.instance.id",
            instance_id.to_string(),
        ))
        .build()
}

fn build_otlp_trace_exporter(
    endpoint: &str,
    headers: HashMap<String, String>,
    targets: Targets,
) -> Result<OtlpSpanFilter<OtlpSpanExporter>, AppError> {
    let exporter = OtlpSpanExporter::builder()
        .with_http()
        .with_endpoint(normalize_otlp_endpoint(endpoint, "traces"))
        .with_headers(headers)
        .build()
        .map_err(|e| AppError::InvalidInput(e.to_string()))?;
    Ok(OtlpSpanFilter::new(exporter, targets))
}

fn add_otlp_trace_exporter(
    builder: TracerProviderBuilder,
    endpoint: &str,
    headers: HashMap<String, String>,
    targets: Targets,
) -> Result<TracerProviderBuilder, AppError> {
    let trace_exporter = build_otlp_trace_exporter(endpoint, headers, targets)?;
    Ok(builder.with_span_processor(BatchSpanProcessor::builder(trace_exporter).build()))
}

fn build_otlp_logger_provider(
    resource: &Resource,
    endpoint: &str,
    headers: HashMap<String, String>,
) -> Result<SdkLoggerProvider, AppError> {
    let exporter = LogExporter::builder()
        .with_http()
        .with_endpoint(normalize_otlp_endpoint(endpoint, "logs"))
        .with_headers(headers)
        .build()
        .map_err(|e| AppError::InvalidInput(e.to_string()))?;
    Ok(SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_log_processor(BatchLogProcessor::builder(exporter).build())
        .build())
}

fn build_otlp_meter_provider(
    resource: &Resource,
    endpoint: &str,
    headers: HashMap<String, String>,
) -> Result<SdkMeterProvider, AppError> {
    let exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(normalize_otlp_endpoint(endpoint, "metrics"))
        .with_headers(headers)
        .build()
        .map_err(|e| AppError::InvalidInput(e.to_string()))?;
    Ok(SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(
            PeriodicReader::builder(exporter)
                .with_interval(METRICS_INTERVAL)
                .build(),
        )
        .build())
}

/// Per-process context read from the environment at bootstrap.
#[derive(Debug, Default)]
pub struct RunContext {
    /// W3C `traceparent` for linking process spans to a parent distributed trace.
    pub trace_parent: Option<String>,
}

/// Error metadata recorded on the root Coral invocation span.
pub trait RunErrorTelemetry {
    /// Low-cardinality error class for `error.type`.
    fn telemetry_error_type(&self) -> Cow<'_, str>;

    /// Human-readable failure summary for span status and exception message.
    fn telemetry_error_message(&self) -> Cow<'_, str>;

    /// The status the process exits with for this error, for
    /// `process.exit.code`.
    fn telemetry_exit_code(&self) -> u8 {
        1
    }
}

/// Runs `fut` under the root process span configured from `ctx`.
///
/// The returned `Result` is also recorded on the root span as low-cardinality
/// `status` and OTEL status data.
///
/// # Errors
///
/// Returns the same error produced by `fut`.
pub async fn run_with_context<T, E, F>(ctx: &RunContext, fut: F) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: RunErrorTelemetry,
{
    use tracing::Instrument as _;
    let span = build_root_span(ctx.trace_parent.as_deref());
    let result = fut.instrument(span.clone()).await;
    match &result {
        Ok(_) => record_run_success(&span),
        Err(error) => record_run_error(&span, error),
    }
    result
}

/// Builds the root process span, optionally parented to a W3C `traceparent` string.
///
/// Pass the value of `CORAL_TRACE_PARENT` (see [`RunContext::from_env`]) or
/// `None` for an unparented span. Use `.instrument(span).await` to run the
/// process future under this span.
pub fn build_root_span(traceparent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        "coral.runtime",
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        otel.kind = "internal",
        process.executable.name = process_executable_name(),
        process.exit.code = tracing::field::Empty,
        process.pid = i64::from(std::process::id()),
        status = tracing::field::Empty
    );
    coral_telemetry::set_parent_from_trace_headers(&span, traceparent, None);
    span
}

fn record_run_success(span: &tracing::Span) {
    span.record("process.exit.code", 0_i64);
    span.record("status", "ok");
    span.set_status(OtelStatus::Ok);
}

fn record_run_error(span: &tracing::Span, error: &impl RunErrorTelemetry) {
    let error_type = error.telemetry_error_type();
    let message = error.telemetry_error_message();
    span.record("process.exit.code", i64::from(error.telemetry_exit_code()));
    span.record("error.type", error_type.as_ref());
    span.record("exception.message", message.as_ref());
    span.record("status", "error");
    span.set_status(OtelStatus::error(message.into_owned()));
}

fn process_executable_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .or_else(|| {
            std::env::args_os().next().and_then(|arg| {
                std::path::Path::new(&arg)
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "coral".to_string())
}

type TraceLayer = Filtered<
    OpenTelemetryLayer<Registry, SdkTracer>,
    filter::combinator::Or<Targets, Targets, Registry>,
    Registry,
>;

/// The tracer provider and the layer that gives spans their trace context.
///
/// Both exist whatever the export settings, so a request's spans, operational
/// logs and safe errors share one trace identifier. Request spans get context
/// even when `trace_filter` does not export them. The OTLP exporter is
/// attached only when `[otel]` configures an endpoint, and it sends only what
/// `trace_filter` admits. Also returns a trace filter parse error, after which
/// the default filter applies.
fn trace_pipeline(
    config: &TelemetryConfig,
    resource: &Resource,
) -> Result<(SdkTracerProvider, TraceLayer, Option<String>), AppError> {
    let mut builder = SdkTracerProvider::builder().with_resource(resource.clone());
    let (trace_targets, filter_error) = trace_layer_filter(&config.otel.trace_filter);
    if let Some(endpoint) = configured_otlp_endpoint(config) {
        let headers = parse_headers(config.otel.headers.as_deref().unwrap_or_default());
        builder = add_otlp_trace_exporter(builder, endpoint, headers, trace_targets.clone())?;
    }
    let provider = builder.build();
    let layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("coral"))
        .with_level(true)
        .with_filter(trace_targets.or(request_context_targets()));
    Ok((provider, layer, filter_error))
}

pub(crate) fn init_tracing(config: &TelemetryConfig, instance_id: &str) -> Result<(), AppError> {
    INIT.get_or_init(|| try_init_tracing(config, instance_id).map_err(|e| e.to_string()))
        .as_ref()
        .map_err(|e| AppError::InvalidInput(e.clone()))?;
    Ok(())
}

fn try_init_tracing(config: &TelemetryConfig, instance_id: &str) -> Result<(), AppError> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let endpoint = configured_otlp_endpoint(config);
    let (log_filter, log_filter_error) = build_log_filter(config.otel.log_filter.as_deref());

    let resource = telemetry_resource(&config.otel.service_name, instance_id);
    let (provider, otel_trace_layer, trace_filter_error) = trace_pipeline(config, &resource)?;
    if let Ok(mut guard) = PROVIDER.lock() {
        *guard = Some(provider);
    }
    if let Some(endpoint) = endpoint {
        let headers = parse_headers(config.otel.headers.as_deref().unwrap_or_default());
        let logger_provider = build_otlp_logger_provider(&resource, endpoint, headers.clone())?;
        if let Ok(mut guard) = LOGGER_PROVIDER.lock() {
            *guard = Some(logger_provider);
        }
        let meter_provider = build_otlp_meter_provider(&resource, endpoint, headers)?;
        opentelemetry::global::set_meter_provider(meter_provider.clone());
        if let Ok(mut guard) = METER_PROVIDER.lock() {
            *guard = Some(meter_provider);
        }
    }

    let otel_log_layer = LOGGER_PROVIDER.lock().ok().and_then(|guard| {
        guard.as_ref().map(|provider| {
            opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(provider)
                .with_filter(log_filter)
        })
    });

    if let Err(error) = Registry::default()
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .try_init()
    {
        tracing::warn!(
            detail = %error,
            "skipping coral-app tracing subscriber: host process has already installed one"
        );
        return Ok(());
    }
    if let Some(error) = log_filter_error {
        tracing::warn!(
            provided_filter = %config.otel.log_filter.as_deref().unwrap_or(DEFAULT_LOG_FILTER),
            fallback_filter = DEFAULT_LOG_FILTER,
            detail = %error,
            "invalid log_filter; falling back to default filter"
        );
    }
    if let Some(error) = trace_filter_error {
        tracing::warn!(
            provided_filter = %config.otel.trace_filter,
            fallback_filter = DEFAULT_TRACE_FILTER,
            detail = %error,
            "invalid trace_filter; falling back to default filter"
        );
    }

    Ok(())
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
    use opentelemetry::{Key, Value};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{TelemetryConfig, telemetry_resource, trace_pipeline};

    /// With `[otel]` unset, or with a trace filter that exports no request
    /// span, one request's operational logs and safe errors still share the
    /// trace identifier of its request span.
    #[test]
    fn a_request_keeps_one_trace_id_whatever_the_export_settings() {
        for (case, raw) in [
            ("export unset", ""),
            (
                "a filter that exports no request span",
                "[otel]\ntrace_filter = \"coral_search=trace\"\n",
            ),
        ] {
            let config = TelemetryConfig::from_toml(raw).expect("telemetry config");
            let resource = telemetry_resource("coral-test", "instance");
            let (provider, layer, filter_error) =
                trace_pipeline(&config, &resource).expect("trace pipeline");
            assert_eq!(filter_error, None, "{case}");
            let subscriber = tracing_subscriber::Registry::default().with(layer);

            tracing::subscriber::with_default(subscriber, || {
                // One request span per listener, under each one's real target.
                let requests = [
                    tracing::info_span!(target: "coral_mcp::http", "mcp request"),
                    tracing::info_span!(
                        target: "coral_app::auth::authorization_server",
                        "auth request"
                    ),
                    tracing::info_span!(target: "coral_grpc::grpc", "private request"),
                ];
                for request in requests {
                    let _entered = request.enter();
                    let first = coral_telemetry::TraceId::current_or_new();
                    let tool = tracing::info_span!(target: "coral_mcp::server", "tool");
                    assert_eq!(
                        tool.in_scope(coral_telemetry::TraceId::current_or_new),
                        first,
                        "{case}: {request:?}"
                    );
                    assert_eq!(
                        coral_telemetry::TraceId::current_or_new(),
                        first,
                        "{case}: {request:?}"
                    );
                }
            });
            provider.shutdown().expect("provider shutdown");
        }
    }

    /// Operators filter exported telemetry by `service.instance.id` to find
    /// the Coral Instance that produced it.
    #[test]
    fn the_resource_names_the_service_and_the_coral_instance() {
        let resource = telemetry_resource("coral-test", "6f1c2b0e-instance");

        assert_eq!(
            resource.get(&Key::new("service.name")),
            Some(Value::from("coral-test"))
        );
        assert_eq!(
            resource.get(&Key::new("service.instance.id")),
            Some(Value::from("6f1c2b0e-instance"))
        );
    }
}
