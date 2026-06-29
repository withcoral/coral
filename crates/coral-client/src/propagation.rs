//! W3C Trace Context propagation for tonic gRPC clients.

use std::future::Future;

use coral_api::CORAL_EPISODE_ID_METADATA_KEY;
use opentelemetry::propagation::Injector;
use tonic::metadata::{Ascii, MetadataValue};

tokio::task_local! {
    static EPISODE_ID: Option<MetadataValue<Ascii>>;
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
        coral_telemetry::inject_current_context(&mut MetadataInjector(request.metadata_mut()));
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
