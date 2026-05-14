//! Tracing and OpenTelemetry initialization for the local Coral process.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use opentelemetry::Value as OtelValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter as OtlpSpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

pub mod config;
mod local_store;
pub mod metrics;
pub(crate) mod service;

use crate::bootstrap::AppError;
pub use config::TelemetryConfig;
use config::{DEFAULT_LOCAL_TRACE_FILTER, DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER};

static INIT: OnceLock<Result<TracingInitState, String>> = OnceLock::new();
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);
static LOGGER_PROVIDER: Mutex<Option<SdkLoggerProvider>> = Mutex::new(None);
static METER_PROVIDER: Mutex<Option<SdkMeterProvider>> = Mutex::new(None);

const METRICS_INTERVAL: Duration = Duration::from_secs(5);
const LOCAL_TRACE_EXCLUDED_RPC_SERVICES: &[&str] = &["coral.v1.TraceService"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledLocalTraceStore {
    pub(crate) dir: PathBuf,
    pub(crate) retention: Duration,
}

impl InstalledLocalTraceStore {
    fn new(dir: PathBuf, retention: Duration) -> Self {
        Self { dir, retention }
    }
}

#[derive(Debug, Clone, Default)]
struct TracingInitState {
    local_trace_store: Option<InstalledLocalTraceStore>,
}

#[derive(Debug)]
struct TargetFilteringSpanExporter<E> {
    inner: E,
    targets: Targets,
    excluded_rpc_services: &'static [&'static str],
}

impl<E> TargetFilteringSpanExporter<E> {
    fn new(inner: E, targets: Targets) -> Self {
        Self {
            inner,
            targets,
            excluded_rpc_services: &[],
        }
    }

    fn excluding_rpc_services(mut self, services: &'static [&'static str]) -> Self {
        self.excluded_rpc_services = services;
        self
    }
}

impl<E> SpanExporter for TargetFilteringSpanExporter<E>
where
    E: SpanExporter,
{
    async fn export(&self, mut batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        batch.retain(|span| {
            span_matches_targets(span, &self.targets)
                && !span_matches_excluded_rpc_service(span, self.excluded_rpc_services)
        });
        if batch.is_empty() {
            return Ok(());
        }
        self.inner.export(batch).await
    }

    fn shutdown_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&mut self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

fn span_matches_targets(span: &SpanData, targets: &Targets) -> bool {
    let Some(target) = span_string_attribute(span, "target") else {
        return false;
    };
    let Some(level) = span_level_attribute(span) else {
        return false;
    };
    targets.would_enable(&target, &level)
}

fn span_matches_excluded_rpc_service(span: &SpanData, excluded_services: &[&str]) -> bool {
    let Some(service) = span_string_attribute(span, "rpc.service") else {
        return false;
    };
    excluded_services
        .iter()
        .any(|excluded| service == *excluded)
}

fn span_string_attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .and_then(|attribute| match &attribute.value {
            OtelValue::String(value) => Some(value.to_string()),
            _ => None,
        })
}

fn span_level_attribute(span: &SpanData) -> Option<tracing::Level> {
    match span_string_attribute(span, "level")?.as_str() {
        "TRACE" => Some(tracing::Level::TRACE),
        "DEBUG" => Some(tracing::Level::DEBUG),
        "INFO" => Some(tracing::Level::INFO),
        "WARN" => Some(tracing::Level::WARN),
        "ERROR" => Some(tracing::Level::ERROR),
        _ => None,
    }
}

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

fn trace_layer_filter(
    otlp_filter: Option<&str>,
    internal_trace_store_enabled: bool,
) -> (Targets, Option<String>) {
    let Some(otlp_filter) = otlp_filter else {
        return build_trace_targets(DEFAULT_LOCAL_TRACE_FILTER, DEFAULT_LOCAL_TRACE_FILTER);
    };
    let (otlp_targets, error) = build_trace_targets(otlp_filter, DEFAULT_TRACE_FILTER);
    if internal_trace_store_enabled {
        let effective_otlp_filter = if error.is_some() {
            DEFAULT_TRACE_FILTER
        } else {
            otlp_filter
        };
        return build_trace_targets(
            &format!("{effective_otlp_filter},{DEFAULT_LOCAL_TRACE_FILTER}"),
            DEFAULT_LOCAL_TRACE_FILTER,
        );
    }
    (otlp_targets, error)
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

/// Per-invocation context populated from the process environment by the CLI binary.
#[derive(Debug, Default)]
pub struct RunContext {
    /// W3C `traceparent` for linking CLI spans to a parent distributed trace.
    pub trace_parent: Option<String>,
}

/// Error metadata recorded on the root Coral invocation span.
pub trait RunErrorTelemetry {
    /// Low-cardinality error class for `error.type`.
    fn telemetry_error_type(&self) -> Cow<'_, str>;

    /// Human-readable failure summary for span status and exception message.
    fn telemetry_error_message(&self) -> Cow<'_, str>;
}

/// Runs `fut` under a root CLI span configured from `ctx`.
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

/// Builds a root CLI span, optionally parented to a W3C `traceparent` string.
///
/// Pass the value of `CORAL_TRACE_PARENT` (read by the CLI's env module) or
/// `None` for an unparented span. Use `.instrument(span).await` to run the
/// CLI future under this span.
pub fn build_root_span(traceparent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        "coral.cli",
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        otel.kind = "client",
        process.executable.name = process_executable_name(),
        process.exit.code = tracing::field::Empty,
        process.pid = i64::from(std::process::id()),
        status = tracing::field::Empty
    );
    if let Some(tp) = traceparent {
        struct StringMapExtractor<'a>(&'a HashMap<String, String>);
        impl Extractor for StringMapExtractor<'_> {
            fn get(&self, key: &str) -> Option<&str> {
                self.0.get(key).map(String::as_str)
            }
            fn keys(&self) -> Vec<&str> {
                self.0.keys().map(String::as_str).collect()
            }
        }
        let carrier = HashMap::from([("traceparent".to_string(), tp.to_string())]);
        let parent_cx = opentelemetry::global::get_text_map_propagator(|p| {
            p.extract(&StringMapExtractor(&carrier))
        });
        drop(span.set_parent(parent_cx));
    }
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
    span.record("process.exit.code", 1_i64);
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

pub(crate) fn init_tracing(
    config: &TelemetryConfig,
    enable_stderr_logs: bool,
    internal_trace_store_dir: Option<PathBuf>,
) -> Result<Option<InstalledLocalTraceStore>, AppError> {
    let state = INIT
        .get_or_init(|| {
            try_init_tracing(config, enable_stderr_logs, internal_trace_store_dir)
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| AppError::InvalidInput(e.clone()))?;
    Ok(state.local_trace_store.clone())
}

#[expect(
    clippy::too_many_lines,
    reason = "Initialization wires OpenTelemetry traces, logs, metrics, and local export"
)]
fn try_init_tracing(
    config: &TelemetryConfig,
    enable_stderr_logs: bool,
    internal_trace_store_dir: Option<PathBuf>,
) -> Result<TracingInitState, AppError> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let endpoint = config
        .otel
        .endpoint
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let (log_filter, log_filter_error) = build_log_filter(config.otel.log_filter.as_deref());
    let stderr_layer = enable_stderr_logs.then(|| {
        tracing_subscriber::fmt::layer()
            .with_target(true)
            .compact()
            .with_writer(std::io::stderr)
            .with_filter(log_filter.clone())
    });

    let local_trace_store = internal_trace_store_dir
        .filter(|_| config.trace_history.enabled)
        .map(|dir| InstalledLocalTraceStore::new(dir, config.trace_history.retention()));
    let internal_trace_store_enabled = local_trace_store.is_some();
    let should_export_traces = endpoint.is_some() || internal_trace_store_enabled;
    let mut trace_filter_error = None;
    let otel_trace_layer = if should_export_traces {
        let resource = opentelemetry_sdk::Resource::builder()
            .with_attribute(opentelemetry::KeyValue::new(
                "service.name",
                config.otel.service_name.clone(),
            ))
            .build();

        let headers = parse_headers(config.otel.headers.as_deref().unwrap_or_default());
        let mut builder = SdkTracerProvider::builder().with_resource(resource.clone());

        if let Some(ref endpoint) = endpoint {
            let (otlp_trace_targets, error) =
                build_trace_targets(&config.otel.trace_filter, DEFAULT_TRACE_FILTER);
            trace_filter_error = error;
            let trace_exporter = OtlpSpanExporter::builder()
                .with_http()
                .with_endpoint(normalize_otlp_endpoint(endpoint, "traces"))
                .with_headers(headers.clone())
                .build()
                .map_err(|e| AppError::InvalidInput(e.to_string()))?;
            let trace_exporter =
                TargetFilteringSpanExporter::new(trace_exporter, otlp_trace_targets);
            builder = builder.with_span_processor(
                opentelemetry_sdk::trace::BatchSpanProcessor::builder(trace_exporter).build(),
            );

            let log_exporter = LogExporter::builder()
                .with_http()
                .with_endpoint(normalize_otlp_endpoint(endpoint, "logs"))
                .with_headers(headers.clone())
                .build()
                .map_err(|e| AppError::InvalidInput(e.to_string()))?;
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
                .map_err(|e| AppError::InvalidInput(e.to_string()))?;
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

        if let Some(store) = local_trace_store.as_ref() {
            let exporter = local_store::JsonlSpanExporter::new(store.dir.clone(), store.retention)
                .map_err(|e| AppError::InvalidInput(e.to_string()))?;
            let (internal_trace_targets, _) =
                build_trace_targets(DEFAULT_LOCAL_TRACE_FILTER, DEFAULT_LOCAL_TRACE_FILTER);
            let exporter = TargetFilteringSpanExporter::new(exporter, internal_trace_targets)
                .excluding_rpc_services(LOCAL_TRACE_EXCLUDED_RPC_SERVICES);
            builder = builder.with_span_processor(
                opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter).build(),
            );
        }

        let provider = builder.build();
        let tracer = provider.tracer("coral");
        let (trace_targets, layer_filter_error) = trace_layer_filter(
            endpoint
                .as_deref()
                .map(|_| config.otel.trace_filter.as_str()),
            internal_trace_store_enabled,
        );
        if trace_filter_error.is_none() {
            trace_filter_error = layer_filter_error;
        }
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_level(true)
            .with_filter(trace_targets);

        if let Ok(mut guard) = PROVIDER.lock() {
            *guard = Some(provider);
        }

        Some(layer)
    } else {
        None
    };

    if endpoint.is_none() {
        initialize_metrics(None);
    }

    let otel_log_layer = LOGGER_PROVIDER.lock().ok().and_then(|guard| {
        guard.as_ref().map(|provider| {
            opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(provider)
                .with_filter(log_filter)
        })
    });

    if let Err(error) = Registry::default()
        .with(stderr_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .try_init()
    {
        tracing::warn!(
            detail = %error,
            "skipping coral-app tracing subscriber: host process has already installed one"
        );
        return Ok(TracingInitState::default());
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

    Ok(TracingInitState { local_trace_store })
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

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{
        DEFAULT_LOCAL_TRACE_FILTER, DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER,
        LOCAL_TRACE_EXCLUDED_RPC_SERVICES, TargetFilteringSpanExporter, build_log_filter,
        build_trace_targets, normalize_otlp_endpoint, parse_headers, trace_layer_filter,
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
        let (targets, error) = build_trace_targets("coral_app=[", DEFAULT_TRACE_FILTER);
        let (expected, default_error) =
            build_trace_targets(DEFAULT_TRACE_FILTER, DEFAULT_TRACE_FILTER);

        assert_eq!(format!("{targets:?}"), format!("{expected:?}"));
        assert!(error.is_some());
        assert!(default_error.is_none());
    }

    #[test]
    fn default_trace_filter_keeps_http_and_disables_datafusion() {
        let (targets, error) = build_trace_targets(DEFAULT_TRACE_FILTER, DEFAULT_TRACE_FILTER);

        assert!(error.is_none());
        assert!(targets.would_enable("coral_client::grpc", &tracing::Level::TRACE));
        assert!(targets.would_enable("coral_mcp::server", &tracing::Level::TRACE));
        assert!(targets.would_enable("coral_engine::http", &tracing::Level::TRACE));
        assert!(!targets.would_enable("coral_engine::datafusion", &tracing::Level::TRACE));
    }

    #[test]
    fn local_trace_filter_includes_datafusion() {
        let (targets, error) =
            build_trace_targets(DEFAULT_LOCAL_TRACE_FILTER, DEFAULT_LOCAL_TRACE_FILTER);

        assert!(error.is_none());
        assert!(targets.would_enable("coral_client::grpc", &tracing::Level::TRACE));
        assert!(targets.would_enable("coral_mcp::server", &tracing::Level::TRACE));
        assert!(targets.would_enable("coral_engine::http", &tracing::Level::TRACE));
        assert!(targets.would_enable("coral_engine::datafusion", &tracing::Level::TRACE));
    }

    #[test]
    fn otlp_trace_layer_filter_does_not_narrow_trace_history() {
        let (targets, error) = trace_layer_filter(Some(DEFAULT_TRACE_FILTER), true);

        assert!(error.is_none());
        assert!(targets.would_enable("coral_engine::http", &tracing::Level::TRACE));
        assert!(targets.would_enable("coral_engine::datafusion", &tracing::Level::TRACE));
    }

    #[test]
    fn target_filtering_exporter_filters_finished_spans() {
        let memory = InMemorySpanExporter::default();
        let (targets, error) = build_trace_targets(
            "coral_app=info,coral_engine::datafusion=off",
            DEFAULT_TRACE_FILTER,
        );
        assert!(error.is_none());
        let exporter = TargetFilteringSpanExporter::new(memory.clone(), targets);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("filter-test");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_level(true);
        let subscriber = tracing_subscriber::Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let kept = tracing::info_span!(target: "coral_app", "kept");
            let _kept = kept.enter();

            let dropped_debug = tracing::debug_span!(target: "coral_app", "dropped_debug");
            let _dropped_debug = dropped_debug.enter();

            let dropped_datafusion =
                tracing::trace_span!(target: "coral_engine::datafusion", "dropped_datafusion");
            let _dropped_datafusion = dropped_datafusion.enter();
        });
        provider.force_flush().expect("flush spans");

        let mut span_names = memory
            .get_finished_spans()
            .expect("finished spans")
            .into_iter()
            .map(|span| span.name.to_string())
            .collect::<Vec<_>>();
        span_names.sort();

        assert_eq!(span_names, vec!["kept"]);
        provider.shutdown().expect("provider shutdown");
    }

    #[test]
    fn target_filtering_exporter_can_exclude_trace_service_rpc_spans() {
        let memory = InMemorySpanExporter::default();
        let (targets, error) =
            build_trace_targets(DEFAULT_LOCAL_TRACE_FILTER, DEFAULT_LOCAL_TRACE_FILTER);
        assert!(error.is_none());
        let exporter = TargetFilteringSpanExporter::new(memory.clone(), targets)
            .excluding_rpc_services(LOCAL_TRACE_EXCLUDED_RPC_SERVICES);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("filter-test");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_level(true);
        let subscriber = tracing_subscriber::Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let dropped = tracing::info_span!(
                target: "coral_app::transport",
                "trace_service_grpc",
                rpc.service = "coral.v1.TraceService",
            );
            let _dropped = dropped.enter();

            let kept = tracing::info_span!(
                target: "coral_app::transport",
                "query_service_grpc",
                rpc.service = "coral.v1.QueryService",
            );
            let _kept = kept.enter();
        });
        provider.force_flush().expect("flush spans");

        let span_names = memory
            .get_finished_spans()
            .expect("finished spans")
            .into_iter()
            .map(|span| span.name.to_string())
            .collect::<Vec<_>>();

        assert_eq!(span_names, vec!["query_service_grpc"]);
        provider.shutdown().expect("provider shutdown");
    }

    #[test]
    fn invalid_log_filter_falls_back_to_default() {
        let (filter, error) = build_log_filter(Some("coral_app=["));
        let (expected, default_error) = build_log_filter(Some(DEFAULT_LOG_FILTER));

        assert_eq!(format!("{filter:?}"), format!("{expected:?}"));
        assert!(error.is_some());
        assert!(default_error.is_none());
    }

    #[test]
    fn missing_log_filter_uses_default_without_error() {
        let (filter, error) = build_log_filter(None);
        let (expected, default_error) = build_log_filter(Some(DEFAULT_LOG_FILTER));

        assert_eq!(format!("{filter:?}"), format!("{expected:?}"));
        assert!(error.is_none());
        assert!(default_error.is_none());
    }
}
