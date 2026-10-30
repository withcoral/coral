//! Tracing and OpenTelemetry initialization for the local Coral process.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use coral_telemetry::metric::Metrics;
use coral_telemetry::safe;
use coral_telemetry::{OtlpLogFilter, OtlpSpanFilter};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter as OtlpSpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracer, SdkTracerProvider, TracerProviderBuilder};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::{self, FilterExt as _, Filtered, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

pub mod config;

use crate::bootstrap::AppError;
pub use config::TelemetryConfig;
use config::{DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER, TRACE_CONTEXT_FILTER};
static INIT: OnceLock<Result<(), String>> = OnceLock::new();
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);
static LOGGER_PROVIDER: Mutex<Option<SdkLoggerProvider>> = Mutex::new(None);
static METER_PROVIDER: Mutex<Option<SdkMeterProvider>> = Mutex::new(None);

const METRICS_INTERVAL: Duration = Duration::from_secs(5);
/// How many spans, and how many log records, wait for export before Coral
/// drops the next and counts it in `coral.telemetry.dropped`.
const EXPORT_QUEUE_CAPACITY: usize = 2048;
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

fn trace_context_targets() -> Targets {
    TRACE_CONTEXT_FILTER
        .parse()
        .expect("trace context filter must be valid")
}

fn is_span(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.is_span()
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

/// The process's resource: its service name and version, and as
/// `service.instance.id` the id of the Coral Instance whose start initialized
/// telemetry, so every exported span, log and metric names that Coral
/// Instance.
fn telemetry_resource(service_name: &str, service_version: &str, instance_id: &str) -> Resource {
    Resource::builder()
        .with_attribute(opentelemetry::KeyValue::new(
            "service.name",
            service_name.to_string(),
        ))
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            service_version.to_string(),
        ))
        .with_attribute(opentelemetry::KeyValue::new(
            "service.instance.id",
            instance_id.to_string(),
        ))
        .build()
}

fn add_otlp_trace_exporter(
    builder: TracerProviderBuilder,
    endpoint: &str,
    headers: HashMap<String, String>,
    targets: Targets,
    metrics: Metrics,
) -> Result<TracerProviderBuilder, AppError> {
    let exporter = OtlpSpanExporter::builder()
        .with_http()
        .with_endpoint(normalize_otlp_endpoint(endpoint, "traces"))
        .with_headers(headers)
        .build()
        .map_err(|e| AppError::InvalidInput(e.to_string()))?;
    // Filter ahead of the batch queue, so spans that may not leave never fill it.
    let batch = coral_telemetry::batch_span_processor(exporter, EXPORT_QUEUE_CAPACITY, metrics);
    Ok(builder.with_span_processor(OtlpSpanFilter::new(batch, targets)))
}

fn build_otlp_logger_provider(
    resource: &Resource,
    endpoint: &str,
    headers: HashMap<String, String>,
    metrics: Metrics,
) -> Result<SdkLoggerProvider, AppError> {
    let exporter = LogExporter::builder()
        .with_http()
        .with_endpoint(normalize_otlp_endpoint(endpoint, "logs"))
        .with_headers(headers)
        .build()
        .map_err(|e| AppError::InvalidInput(e.to_string()))?;
    Ok(SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_log_processor(OtlpLogFilter::new(coral_telemetry::batch_log_processor(
            exporter,
            EXPORT_QUEUE_CAPACITY,
            metrics,
        )))
        .build())
}

/// The meter provider and the catalog of Coral's metrics built from it.
fn build_otlp_meter_provider(
    resource: &Resource,
    endpoint: &str,
    headers: HashMap<String, String>,
) -> Result<(SdkMeterProvider, Metrics), AppError> {
    let exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(normalize_otlp_endpoint(endpoint, "metrics"))
        .with_headers(headers)
        .build()
        .map_err(|e| AppError::InvalidInput(e.to_string()))?;
    Ok(coral_telemetry::meter_provider(
        SdkMeterProvider::builder().with_resource(resource.clone()),
        exporter,
        METRICS_INTERVAL,
    ))
}

/// Per-process context read from the environment at bootstrap.
#[derive(Debug, Default)]
pub struct RunContext {
    /// W3C `traceparent` for linking process spans to a parent distributed trace.
    pub trace_parent: Option<String>,
}

/// Error metadata recorded on the root Coral invocation span.
pub trait RunErrorTelemetry {
    /// The error's stable kind for `error.type`: a bounded code that never
    /// carries the error's text.
    fn telemetry_error_code(&self) -> &'static str;

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
        otel.kind = "internal",
        process.executable.name = safe::id(&process_executable_name()),
        process.exit.code = tracing::field::Empty,
        process.pid = safe::number(i64::from(std::process::id())),
    );
    coral_telemetry::set_parent_from_trace_headers(&span, traceparent, None);
    span
}

fn record_run_success(span: &tracing::Span) {
    span.record("process.exit.code", 0_i64);
    coral_telemetry::record_success(span);
}

fn record_run_error(span: &tracing::Span, error: &impl RunErrorTelemetry) {
    span.record(
        "process.exit.code",
        safe::number(i64::from(error.telemetry_exit_code())),
    );
    coral_telemetry::record_failure(span, error.telemetry_error_code());
}

/// The file name of the executable this process runs.
struct ExecutableName(String);

/// The operator installed the executable and chose its name.
impl safe::Identifier for ExecutableName {}

impl std::fmt::Display for ExecutableName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn process_executable_name() -> ExecutableName {
    let name = std::env::current_exe()
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
        .unwrap_or_else(|| "coral".to_string());
    ExecutableName(name)
}

type TraceLayerFilter<S> =
    filter::combinator::Or<Targets, filter::combinator::And<Targets, filter::FilterFn, S>, S>;

type TraceLayer =
    Filtered<OpenTelemetryLayer<Registry, SdkTracer>, TraceLayerFilter<Registry>, Registry>;

/// Gives every Coral span trace context, but admits only the events
/// `trace_targets` admits: an event joins whichever span is current and
/// leaves the host with it.
fn trace_layer_filter_for<S>(trace_targets: Targets) -> TraceLayerFilter<S>
where
    S: tracing::Subscriber,
{
    trace_targets.or(trace_context_targets().and(filter::filter_fn(
        is_span as fn(&tracing::Metadata<'_>) -> bool,
    )))
}

/// Samples each root Coral creates at `ratio`, and follows the parent's
/// decision otherwise: a caller's valid `traceparent` for a request, the
/// stored Run root for a Run's phases. An unsampled span still has its
/// trace identifier, so safe errors and logs keep it; it is never recorded
/// or exported.
fn sampler(ratio: f64) -> Sampler {
    Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
}

/// The tracer provider and the layer that gives spans their trace context.
///
/// Both exist whatever the export settings, so a request's spans, operational
/// logs and safe errors share one trace identifier. Every Coral span gets
/// context even when `trace_filter` does not export it, but only
/// `trace_filter` admits events, since an event joins whichever span is
/// current and leaves the host with it. The OTLP exporter is
/// attached only when `[otel]` configures an endpoint, and it sends only what
/// `trace_filter` admits. Also returns a trace filter parse error, after which
/// the default filter applies.
fn trace_pipeline(
    config: &TelemetryConfig,
    resource: &Resource,
    metrics: Option<&Metrics>,
) -> Result<(SdkTracerProvider, TraceLayer, Option<String>), AppError> {
    let mut builder = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(sampler(config.otel.trace_sample_ratio.get()));
    let (trace_targets, filter_error) = trace_layer_filter(&config.otel.trace_filter);
    if let (Some(endpoint), Some(metrics)) = (configured_otlp_endpoint(config), metrics) {
        let headers = parse_headers(config.otel.headers.as_deref().unwrap_or_default());
        builder = add_otlp_trace_exporter(
            builder,
            endpoint,
            headers,
            trace_targets.clone(),
            metrics.clone(),
        )?;
    }
    let provider = builder.build();
    // The span filter reads each span's level; busy and idle time are no
    // semantic convention, so they are not tracked.
    let layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("coral"))
        .with_level(true)
        .with_tracked_inactivity(false)
        .with_filter(trace_layer_filter_for(trace_targets));
    Ok((provider, layer, filter_error))
}

pub(crate) fn init_tracing(
    config: &TelemetryConfig,
    instance_id: &str,
    service_version: &str,
) -> Result<(), AppError> {
    INIT.get_or_init(|| {
        try_init_tracing(config, instance_id, service_version).map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(|e| AppError::InvalidInput(e.clone()))?;
    Ok(())
}

fn try_init_tracing(
    config: &TelemetryConfig,
    instance_id: &str,
    service_version: &str,
) -> Result<(), AppError> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let endpoint = configured_otlp_endpoint(config);
    let (log_filter, log_filter_error) = build_log_filter(config.otel.log_filter.as_deref());

    let resource = telemetry_resource(&config.otel.service_name, service_version, instance_id);
    // Metrics leave only through an endpoint, so without one nothing records them.
    let mut metrics = None;
    if let Some(endpoint) = endpoint {
        let headers = parse_headers(config.otel.headers.as_deref().unwrap_or_default());
        let (meter_provider, catalog) =
            build_otlp_meter_provider(&resource, endpoint, headers.clone())?;
        opentelemetry::global::set_meter_provider(meter_provider.clone());
        if let Ok(mut guard) = METER_PROVIDER.lock() {
            *guard = Some(meter_provider);
        }
        let logger_provider =
            build_otlp_logger_provider(&resource, endpoint, headers, catalog.clone())?;
        if let Ok(mut guard) = LOGGER_PROVIDER.lock() {
            *guard = Some(logger_provider);
        }
        metrics = Some(catalog);
    }
    let (provider, otel_trace_layer, trace_filter_error) =
        trace_pipeline(config, &resource, metrics.as_ref())?;
    if let Ok(mut guard) = PROVIDER.lock() {
        *guard = Some(provider);
    }

    let otel_log_layer = LOGGER_PROVIDER.lock().ok().and_then(|guard| {
        guard.as_ref().map(|provider| {
            opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(provider)
                .with_filter(log_filter)
        })
    });

    if Registry::default()
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .with(metrics.as_ref().map(Metrics::layer))
        .try_init()
        .is_err()
    {
        tracing::warn!(
            error.type = "subscriber_already_installed",
            "skipping coral-app tracing subscriber: host process has already installed one"
        );
        return Ok(());
    }
    if log_filter_error.is_some() {
        tracing::warn!(
            error.type = "invalid_filter",
            "invalid log_filter; falling back to default filter"
        );
    }
    if trace_filter_error.is_some() {
        tracing::warn!(
            error.type = "invalid_filter",
            "invalid trace_filter; falling back to default filter"
        );
    }

    Ok(())
}

/// The kind of an OpenTelemetry SDK failure, for an event's `error.type`.
const fn sdk_error_code(error: &OTelSdkError) -> &'static str {
    match error {
        OTelSdkError::AlreadyShutdown => "already_shutdown",
        OTelSdkError::Timeout(_) => "timeout",
        OTelSdkError::InternalFailure(_) => "internal_failure",
    }
}

/// Flush any pending tracing, log, and metric exports before process exit.
pub fn shutdown_tracing() {
    if let Ok(mut guard) = PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!(
            error.type = safe::kind(sdk_error_code(&error)),
            "OTEL trace provider shutdown failed"
        );
    }
    if let Ok(mut guard) = LOGGER_PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!(
            error.type = safe::kind(sdk_error_code(&error)),
            "OTEL logger provider shutdown failed"
        );
    }
    // Last, so it exports the drops of the others' final exports.
    if let Ok(mut guard) = METER_PROVIDER.lock()
        && let Some(provider) = guard.take()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!(
            error.type = safe::kind(sdk_error_code(&error)),
            "OTEL meter provider shutdown failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::TraceContextExt as _;
    use opentelemetry::{Key, Value};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    use tracing_subscriber::Layer as _;

    use super::{
        TelemetryConfig, telemetry_resource, trace_layer_filter, trace_layer_filter_for,
        trace_pipeline,
    };

    /// With `[otel]` unset, or with a trace filter that exports none of a
    /// request's spans, Coral work anywhere inside the request still reads the
    /// trace identifier of its request span, whichever crate's span is current.
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
            let resource = telemetry_resource("coral-test", "1.2.3+abc1234", "instance");
            let (provider, layer, filter_error) =
                trace_pipeline(&config, &resource, None).expect("trace pipeline");
            assert_eq!(filter_error, None, "{case}");
            let subscriber = tracing_subscriber::Registry::default().with(layer);

            tracing::subscriber::with_default(subscriber, || {
                let request =
                    tracing::info_span!(target: coral_telemetry::REQUEST_TARGET, "request");
                let _entered = request.enter();
                let trace = coral_telemetry::TraceId::current_or_new();
                // A handler span, and one from a crate no filter names.
                let handler = tracing::info_span!(target: "coral_grpc::grpc", "handler");
                let _handler = handler.enter();
                let nested = tracing::info_span!(target: "coral_unnamed::module", "nested");
                assert_eq!(
                    nested.in_scope(coral_telemetry::TraceId::current_or_new),
                    trace,
                    "{case}"
                );
            });
            provider.shutdown().expect("provider shutdown");
        }
    }

    /// Only `trace_filter` admits events, since an event joins the current span
    /// and leaves the host with it.
    #[test]
    fn only_the_trace_filter_admits_events() {
        let capture = coral_telemetry::testing::SpanCapture::default();
        let (trace_targets, _filter_error) = trace_layer_filter(super::DEFAULT_TRACE_FILTER);
        let subscriber = tracing_subscriber::Registry::default().with(
            capture
                .layer()
                .with_filter(trace_layer_filter_for(trace_targets)),
        );
        tracing::subscriber::with_default(subscriber, || {
            let request = tracing::info_span!(target: coral_telemetry::REQUEST_TARGET, "request");
            request.in_scope(|| {
                tracing::error!(target: "coral_unnamed::module", "unnamed");
                tracing::error!(target: "coral_mcp::server", "admitted");
            });
        });
        let spans = capture.finished();
        let request = spans
            .iter()
            .find(|span| span.name == "request")
            .expect("request span");
        let events = request
            .events
            .iter()
            .map(|event| event.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(events, ["admitted"]);
    }

    /// `trace_sample_ratio` decides each root Coral creates: a request
    /// without a caller's context, and every Run root, which decides on its
    /// own whatever its request sampled. A request with a caller's valid
    /// context follows the caller's sampled flag. An unsampled span is never
    /// recorded, so it is never exported, but it keeps its trace identifier
    /// for safe errors and operational logs.
    #[test]
    fn coral_roots_sample_at_the_ratio_and_requests_follow_their_caller() {
        const SAMPLED: &str = "00-00000000000000000000000000000077-0000000000000088-01";
        const UNSAMPLED: &str = "00-00000000000000000000000000000077-0000000000000088-00";
        // (ratio, caller's traceparent, request sampled, Run root sampled)
        let cases = [
            ("", None, true, true),
            ("", Some(UNSAMPLED), false, true),
            ("trace_sample_ratio = 0.0", None, false, false),
            ("trace_sample_ratio = 0.0", Some(SAMPLED), true, false),
            ("trace_sample_ratio = 1.0", Some(UNSAMPLED), false, true),
        ];
        for (ratio, caller, request_sampled, run_sampled) in cases {
            let case = format!("{ratio:?} with caller {caller:?}");
            let config = TelemetryConfig::from_toml(&format!("[otel]\n{ratio}\n"))
                .expect("telemetry config");
            let resource = telemetry_resource("coral-test", "1.2.3+abc1234", "instance");
            let (provider, layer, _filter_error) =
                trace_pipeline(&config, &resource, None).expect("trace pipeline");
            let logs = opentelemetry_sdk::logs::InMemoryLogExporter::default();
            let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                .with_simple_exporter(logs.clone())
                .build();
            let subscriber = tracing_subscriber::Registry::default().with(layer).with(
                opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                    &logger_provider,
                ),
            );
            let request_trace = tracing::subscriber::with_default(subscriber, || {
                let request =
                    tracing::info_span!(target: coral_telemetry::REQUEST_TARGET, "request");
                coral_telemetry::set_parent_from_trace_headers(&request, caller, None);
                let _request = request.enter();
                let context = request.context();
                let span = context.span();
                assert_eq!(span.span_context().is_sampled(), request_sampled, "{case}");
                assert_eq!(span.is_recording(), request_sampled, "{case}");
                let trace_id = coral_telemetry::TraceId::current_or_new();
                assert_eq!(
                    trace_id.as_str(),
                    span.span_context().trace_id().to_string(),
                    "{case}: safe errors keep the request's trace identifier"
                );
                tracing::info!(target: "coral_app::test", "logged");

                let accept = tracing::info_span!(parent: None, "coral.run.accept");
                let run = coral_telemetry::RunTrace::begin(&accept).expect("a Run Trace");
                let accept_context = accept.context();
                assert_eq!(
                    accept_context.span().span_context().is_sampled(),
                    run_sampled,
                    "{case}: the Run root decides on its own"
                );
                assert_ne!(run.trace_id(), trace_id, "{case}");
                trace_id
            });
            logger_provider.force_flush().expect("flush logs");
            let records = logs.get_emitted_logs().expect("emitted logs");
            let logged = records
                .iter()
                .find_map(|log| log.record.trace_context())
                .expect("the log carries trace context");
            assert_eq!(
                logged.trace_id.to_string(),
                request_trace.as_str(),
                "{case}: operational logs keep the request's trace identifier"
            );
            provider.shutdown().expect("provider shutdown");
        }
    }

    /// Operators filter exported telemetry by `service.instance.id` to find
    /// the Coral Instance that produced it, and by `service.version` to find
    /// the build.
    #[test]
    fn the_resource_names_the_service_its_version_and_the_coral_instance() {
        let resource = telemetry_resource("coral-test", "1.2.3+abc1234", "6f1c2b0e-instance");

        assert_eq!(
            resource.get(&Key::new("service.name")),
            Some(Value::from("coral-test"))
        );
        assert_eq!(
            resource.get(&Key::new("service.version")),
            Some(Value::from("1.2.3+abc1234"))
        );
        assert_eq!(
            resource.get(&Key::new("service.instance.id")),
            Some(Value::from("6f1c2b0e-instance"))
        );
    }
}
