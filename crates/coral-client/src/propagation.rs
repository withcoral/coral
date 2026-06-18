//! Request metadata propagation for tonic gRPC clients.

use std::future::Future;
use std::sync::Arc;

use coral_api::CORAL_EPISODE_ID_METADATA_KEY;
use opentelemetry::propagation::Injector;
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};

use crate::error::ClientError;

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
                if is_reserved_static_metadata_key(&key) {
                    return Err(ClientError::InvalidMetadata(format!(
                        "metadata key '{}' is reserved for Coral transport",
                        key.as_str()
                    )));
                }
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

fn is_reserved_static_metadata_key(key: &MetadataKey<Ascii>) -> bool {
    let key = key.as_str();
    matches!(
        key,
        "traceparent" | "tracestate" | "baggage" | CORAL_EPISODE_ID_METADATA_KEY
    ) || key.starts_with("grpc-")
}

/// tonic client interceptor that injects the current W3C `traceparent`, scoped
/// Coral request metadata, and caller-supplied static metadata into outgoing
/// gRPC request metadata.
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
        coral_telemetry::inject_current_context(&mut MetadataInjector(request.metadata_mut()));
        for (key, value) in self.static_metadata.entries.iter() {
            request.metadata_mut().append(key.clone(), value.clone());
        }
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
    use tonic::service::Interceptor as _;

    use super::*;

    #[tokio::test]
    async fn client_metadata_interceptor_injects_scoped_episode_id() {
        let episode_id = "ep_scoped".parse().expect("metadata value");
        let metadata = StaticClientMetadata::default();
        let mut interceptor = ClientMetadataInterceptor::new(metadata);

        let request = with_episode_metadata(Some(episode_id), async {
            interceptor
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
    fn client_metadata_interceptor_omits_episode_id_without_scope() {
        let metadata = StaticClientMetadata::default();
        let mut interceptor = ClientMetadataInterceptor::new(metadata);

        let request = interceptor
            .call(tonic::Request::new(()))
            .expect("interceptor");

        assert!(
            request
                .metadata()
                .get(CORAL_EPISODE_ID_METADATA_KEY)
                .is_none()
        );
    }

    #[test]
    fn static_client_metadata_rejects_invalid_keys() {
        let error = StaticClientMetadata::try_from_pairs([("bad key", "value")])
            .expect_err("invalid key should fail");

        assert!(error.to_string().contains("metadata key 'bad key'"));
    }

    #[test]
    fn static_client_metadata_rejects_reserved_keys() {
        for key in [
            "traceparent",
            "tracestate",
            "baggage",
            "grpc-timeout",
            CORAL_EPISODE_ID_METADATA_KEY,
        ] {
            let error = StaticClientMetadata::try_from_pairs([(key, "value")])
                .expect_err("reserved key should fail");

            assert!(
                error
                    .to_string()
                    .contains(&format!("metadata key '{key}' is reserved")),
                "unexpected error for {key}: {error}"
            );
        }
    }

    #[test]
    fn interceptor_injects_static_metadata() {
        let metadata =
            StaticClientMetadata::try_from_pairs([("x-coral-user-id", "saul")]).expect("metadata");
        let mut interceptor = ClientMetadataInterceptor::new(metadata);

        let request = interceptor
            .call(tonic::Request::new(()))
            .expect("interceptor");

        assert_eq!(
            request
                .metadata()
                .get("x-coral-user-id")
                .and_then(|value| value.to_str().ok()),
            Some("saul")
        );
    }

    #[test]
    fn interceptor_appends_repeated_static_metadata() {
        let metadata = StaticClientMetadata::try_from_pairs([
            ("x-coral-route", "primary"),
            ("x-coral-route", "secondary"),
        ])
        .expect("metadata");
        let mut interceptor = ClientMetadataInterceptor::new(metadata);

        let request = interceptor
            .call(tonic::Request::new(()))
            .expect("interceptor");
        let values = request
            .metadata()
            .get_all("x-coral-route")
            .iter()
            .map(|value| value.to_str().expect("ascii value"))
            .collect::<Vec<_>>();

        assert_eq!(values, vec!["primary", "secondary"]);
    }
}
