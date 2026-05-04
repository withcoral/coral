//! W3C Trace Context propagation for tonic gRPC clients.

use std::sync::OnceLock;

use opentelemetry::propagation::Injector;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

static PROPAGATOR_INIT: OnceLock<()> = OnceLock::new();

/// Installs `TraceContextPropagator` as the process-global text-map
/// propagator the first time this is called.
///
/// `TraceContextInterceptor` injects via the global propagator on every
/// outgoing request. Without this, a client-only process (talking to a
/// remote endpoint or a separate test server, with no local
/// `ServerBuilder::start` to install one) would fall back to the default
/// no-op propagator and silently drop `traceparent` even when the caller
/// has an active span.
pub(crate) fn ensure_global_propagator() {
    PROPAGATOR_INIT.get_or_init(|| {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    });
}

struct MetadataInjector<'a>(&'a mut tonic::metadata::MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
            && let Ok(val) = tonic::metadata::MetadataValue::try_from(&value)
        {
            self.0.insert(key, val);
        }
    }
}

/// tonic client interceptor that injects the current W3C `traceparent` into
/// outgoing gRPC request metadata.
#[derive(Clone)]
pub struct TraceContextInterceptor;

impl tonic::service::Interceptor for TraceContextInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|p| {
            p.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()));
        });
        Ok(request)
    }
}
