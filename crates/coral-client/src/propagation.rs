//! W3C Trace Context propagation for tonic gRPC clients.

use std::future::Future;

use coral_api::CORAL_TASK_ID_METADATA_KEY;
use opentelemetry::propagation::Injector;
use tonic::metadata::{Ascii, MetadataValue};

tokio::task_local! {
    static TASK_ID: Option<MetadataValue<Ascii>>;
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

/// Runs a future with optional task attribution for Coral client calls made in
/// the same async task.
///
/// This is intentionally best-effort: task-local values do not automatically
/// cross an interior `tokio::spawn`, so a spawned task may lose attribution
/// unless the caller scopes it again.
pub async fn with_task_metadata<F>(task_id: Option<MetadataValue<Ascii>>, future: F) -> F::Output
where
    F: Future,
{
    TASK_ID.scope(task_id, future).await
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
        if let Ok(Some(task_id)) = TASK_ID.try_with(Clone::clone) {
            request
                .metadata_mut()
                .insert(CORAL_TASK_ID_METADATA_KEY, task_id);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use coral_api::CORAL_TASK_ID_METADATA_KEY;
    use tonic::service::Interceptor as _;

    use super::{RequestContextInterceptor, with_task_metadata};

    #[tokio::test]
    async fn request_context_interceptor_injects_scoped_task_id() {
        let task_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("metadata value");

        let request = with_task_metadata(Some(task_id), async {
            RequestContextInterceptor
                .call(tonic::Request::new(()))
                .expect("interceptor")
        })
        .await;

        assert_eq!(
            request
                .metadata()
                .get(CORAL_TASK_ID_METADATA_KEY)
                .and_then(|value| value.to_str().ok()),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn request_context_interceptor_omits_task_id_without_scope() {
        let request = RequestContextInterceptor
            .call(tonic::Request::new(()))
            .expect("interceptor");

        assert!(request.metadata().get(CORAL_TASK_ID_METADATA_KEY).is_none());
    }
}
