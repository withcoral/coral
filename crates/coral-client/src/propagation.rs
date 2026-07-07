//! W3C Trace Context propagation for tonic gRPC clients.

use opentelemetry::propagation::Injector;

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

/// tonic client interceptor that injects request-scoped Coral metadata into
/// outgoing gRPC request metadata.
#[derive(Clone)]
pub struct RequestContextInterceptor;

impl tonic::service::Interceptor for RequestContextInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        coral_telemetry::inject_current_context(&mut MetadataInjector(request.metadata_mut()));
        Ok(request)
    }
}
