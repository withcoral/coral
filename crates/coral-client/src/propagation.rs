//! W3C Trace Context propagation for tonic gRPC clients.

use std::{future::Future, sync::OnceLock};

use coral_api::CORAL_EPISODE_ID_METADATA_KEY;
use opentelemetry::propagation::Injector;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tonic::metadata::{Ascii, MetadataValue};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

static PROPAGATOR_INIT: OnceLock<()> = OnceLock::new();

tokio::task_local! {
    static EPISODE_ID: Option<MetadataValue<Ascii>>;
}

/// Installs `TraceContextPropagator` as the process-global text-map
/// propagator the first time this is called.
///
/// `RequestContextInterceptor` injects via the global propagator on every
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

/// Runs a future with optional episode attribution for Coral client calls made
/// in the same task.
///
/// This is intentionally best-effort: task-local values do not automatically
/// cross an interior `tokio::spawn`, so a spawned subtask may lose attribution
/// unless the caller scopes it again.
pub async fn with_episode_metadata<F>(
    episode_id: Option<MetadataValue<Ascii>>,
    future: F,
) -> F::Output
where
    F: Future,
{
    EPISODE_ID.scope(episode_id, future).await
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
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|p| {
            p.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()));
        });
        if let Ok(Some(episode_id)) = EPISODE_ID.try_with(Clone::clone) {
            request
                .metadata_mut()
                .insert(CORAL_EPISODE_ID_METADATA_KEY, episode_id);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use coral_api::CORAL_EPISODE_ID_METADATA_KEY;
    use tonic::service::Interceptor as _;

    use super::{RequestContextInterceptor, with_episode_metadata};

    #[tokio::test]
    async fn request_context_interceptor_injects_scoped_episode_id() {
        let episode_id = "ep_scoped".parse().expect("metadata value");

        let request = with_episode_metadata(Some(episode_id), async {
            RequestContextInterceptor
                .call(tonic::Request::new(()))
                .expect("interceptor")
        })
        .await;

        assert_eq!(
            request
                .metadata()
                .get(CORAL_EPISODE_ID_METADATA_KEY)
                .and_then(|value| value.to_str().ok()),
            Some("ep_scoped")
        );
    }

    #[test]
    fn request_context_interceptor_omits_episode_id_without_scope() {
        let request = RequestContextInterceptor
            .call(tonic::Request::new(()))
            .expect("interceptor");

        assert!(
            request
                .metadata()
                .get(CORAL_EPISODE_ID_METADATA_KEY)
                .is_none()
        );
    }
}
