//! W3C Trace Context propagation for tonic gRPC clients.

use opentelemetry::propagation::{Injector, TextMapPropagator as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

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
        TraceContextPropagator::new()
            .inject_context(&cx, &mut MetadataInjector(request.metadata_mut()));
        Ok(request)
    }
}
