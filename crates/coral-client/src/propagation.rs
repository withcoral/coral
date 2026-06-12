//! Request metadata propagation for tonic gRPC clients.

use std::sync::{Arc, OnceLock};

use opentelemetry::propagation::Injector;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::error::ClientError;

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

#[derive(Clone, Debug, Default)]
pub(crate) struct StaticClientMetadata {
    entries: Arc<Vec<(MetadataKey<Ascii>, MetadataValue<Ascii>)>>,
}

impl StaticClientMetadata {
    pub(crate) fn try_from_pairs<K, V, I>(metadata: I) -> Result<Self, ClientError>
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        let entries = metadata
            .into_iter()
            .map(|(key, value)| {
                let key = MetadataKey::from_bytes(key.as_ref().as_bytes()).map_err(|error| {
                    ClientError::InvalidMetadata(format!(
                        "metadata key '{}' is invalid: {error}",
                        key.as_ref()
                    ))
                })?;
                let value = MetadataValue::try_from(value.as_ref()).map_err(|error| {
                    ClientError::InvalidMetadata(format!(
                        "metadata value for '{}' is invalid: {error}",
                        key.as_str()
                    ))
                })?;
                Ok::<_, ClientError>((key, value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            entries: Arc::new(entries),
        })
    }
}

/// tonic client interceptor that injects the current W3C `traceparent` and
/// caller-supplied static metadata into outgoing gRPC request metadata.
#[derive(Clone)]
pub struct ClientMetadataInterceptor {
    static_metadata: StaticClientMetadata,
}

impl ClientMetadataInterceptor {
    pub(crate) fn new(static_metadata: StaticClientMetadata) -> Self {
        Self { static_metadata }
    }
}

impl tonic::service::Interceptor for ClientMetadataInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|p| {
            p.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()));
        });
        for (key, value) in self.static_metadata.entries.iter() {
            request.metadata_mut().insert(key.clone(), value.clone());
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use tonic::service::Interceptor as _;

    use super::*;

    #[test]
    fn static_client_metadata_rejects_invalid_keys() {
        let error = StaticClientMetadata::try_from_pairs([("bad key", "value")])
            .expect_err("invalid key should fail");

        assert!(error.to_string().contains("metadata key 'bad key'"));
    }

    #[test]
    fn interceptor_injects_static_metadata() {
        let metadata = StaticClientMetadata::try_from_pairs([("x-coral-cloud-member-id", "saul")])
            .expect("metadata");
        let mut interceptor = ClientMetadataInterceptor::new(metadata);

        let request = interceptor
            .call(tonic::Request::new(()))
            .expect("intercept request");

        assert_eq!(
            request
                .metadata()
                .get("x-coral-cloud-member-id")
                .and_then(|value| value.to_str().ok()),
            Some("saul")
        );
    }
}
