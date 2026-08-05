//! Request metadata propagation for tonic gRPC clients.

use std::future::Future;
use std::sync::Arc;

use coral_api::{CORAL_TASK_ID_METADATA_KEY, CORAL_TOOL_INTENT_METADATA_KEY};
use opentelemetry::propagation::Injector;
use tonic::metadata::{Ascii, Binary, MetadataKey, MetadataValue};

use crate::error::ClientError;

pub(crate) const AUTHORIZATION_METADATA_KEY: &str = "authorization";

tokio::task_local! {
    static TASK_ID: Option<MetadataValue<Ascii>>;
    static TOOL_INTENT: Option<MetadataValue<Binary>>;
}

/// A validated bearer credential for an authenticated Coral connection.
///
/// Values may be supplied either as a raw token or with a `Bearer` prefix.
/// The authorization scheme is normalized before it is attached to requests.
#[derive(Clone)]
pub struct BearerToken {
    authorization: String,
}

impl BearerToken {
    /// Validates a bearer token for use as gRPC authorization metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::InvalidBearerToken`] when the token is empty or
    /// cannot be represented as ASCII gRPC metadata.
    pub fn new(token: impl AsRef<str>) -> Result<Self, ClientError> {
        let token = token.as_ref().trim_start();
        let token = match token.split_at_checked(7) {
            Some((prefix, token)) if prefix.eq_ignore_ascii_case("bearer ") => token,
            _ => token,
        }
        .trim_end();
        if token.is_empty() || token.chars().any(char::is_whitespace) {
            return Err(ClientError::InvalidBearerToken(
                "token must be non-empty and contain no whitespace".to_string(),
            ));
        }

        let authorization = format!("Bearer {token}");
        MetadataValue::<Ascii>::try_from(authorization.as_str())
            .map_err(|error| ClientError::InvalidBearerToken(error.to_string()))?;
        Ok(Self { authorization })
    }

    pub(crate) fn authorization(&self) -> &str {
        &self.authorization
    }
}

struct MetadataInjector<'a>(&'a mut tonic::metadata::MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = MetadataKey::from_bytes(key.as_bytes())
            && let Ok(value) = MetadataValue::try_from(&value)
        {
            self.0.insert(key, value);
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
    with_task_context(task_id, None, future).await
}

/// Runs a future with optional task id and tool intent attribution for Coral
/// client calls made in the same async task.
pub async fn with_task_context<F>(
    task_id: Option<MetadataValue<Ascii>>,
    tool_intent: Option<MetadataValue<Binary>>,
    future: F,
) -> F::Output
where
    F: Future,
{
    TASK_ID
        .scope(task_id, TOOL_INTENT.scope(tool_intent, future))
        .await
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
                let mut value = MetadataValue::try_from(value.as_ref()).map_err(|error| {
                    ClientError::InvalidMetadata(format!(
                        "metadata value for '{}' is invalid: {error}",
                        key.as_str()
                    ))
                })?;
                if key == AUTHORIZATION_METADATA_KEY {
                    value.set_sensitive(true);
                }
                Ok::<_, ClientError>((key, value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            entries: Arc::new(entries),
        })
    }

    pub(crate) fn contains_authorization(&self) -> bool {
        self.entries
            .iter()
            .any(|(key, _)| key == AUTHORIZATION_METADATA_KEY)
    }
}

fn is_reserved_static_metadata_key(key: &MetadataKey<Ascii>) -> bool {
    matches!(
        key.as_str(),
        "traceparent"
            | "tracestate"
            | "baggage"
            | CORAL_TASK_ID_METADATA_KEY
            | "content-type"
            | "te"
    ) || key.as_str().starts_with("grpc-")
}

/// tonic client interceptor that injects the current trace context, scoped
/// Coral task attribution, and caller-supplied static metadata.
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
        if let Ok(Some(task_id)) = TASK_ID.try_with(Clone::clone) {
            request
                .metadata_mut()
                .insert(CORAL_TASK_ID_METADATA_KEY, task_id);
        }
        if let Ok(Some(tool_intent)) = TOOL_INTENT.try_with(Clone::clone) {
            request
                .metadata_mut()
                .insert_bin(CORAL_TOOL_INTENT_METADATA_KEY, tool_intent);
        }
        for (key, value) in self.static_metadata.entries.iter() {
            request.metadata_mut().append(key.clone(), value.clone());
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use tonic::service::Interceptor as _;

    use super::*;

    #[tokio::test]
    async fn client_metadata_interceptor_injects_scoped_task_id() {
        let task_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("metadata value");

        let request = with_task_metadata(Some(task_id), async {
            ClientMetadataInterceptor::new(StaticClientMetadata::default())
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

    #[tokio::test]
    async fn client_metadata_interceptor_preserves_utf8_tool_intent() {
        let task_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("metadata value");
        let intent = MetadataValue::<Binary>::from_bytes("Sprawdź odnowienia".as_bytes());

        let request = with_task_context(Some(task_id), Some(intent), async {
            ClientMetadataInterceptor::new(StaticClientMetadata::default())
                .call(tonic::Request::new(()))
                .expect("interceptor")
        })
        .await;

        assert_eq!(
            request
                .metadata()
                .get_bin(CORAL_TOOL_INTENT_METADATA_KEY)
                .expect("tool intent")
                .to_bytes()
                .expect("binary metadata")
                .as_ref(),
            "Sprawdź odnowienia".as_bytes()
        );
    }

    #[test]
    fn client_metadata_interceptor_omits_task_id_without_scope() {
        let request = ClientMetadataInterceptor::new(StaticClientMetadata::default())
            .call(tonic::Request::new(()))
            .expect("interceptor");

        assert!(request.metadata().get(CORAL_TASK_ID_METADATA_KEY).is_none());
    }

    #[test]
    fn bearer_token_normalizes_raw_and_prefixed_values() {
        for (token, expected) in [
            ("secret", "Bearer secret"),
            ("Bearer secret", "Bearer secret"),
            ("bearer secret", "Bearer secret"),
            ("  secret  ", "Bearer secret"),
            ("bearer", "Bearer bearer"),
        ] {
            let bearer = BearerToken::new(token).expect("valid bearer");
            assert_eq!(bearer.authorization(), expected);
        }
    }

    #[test]
    fn bearer_token_rejects_empty_and_invalid_metadata() {
        for token in ["", "  ", "Bearer ", "Bearer \nsecret"] {
            assert!(matches!(
                BearerToken::new(token),
                Err(ClientError::InvalidBearerToken(_))
            ));
        }
    }

    #[test]
    fn static_client_metadata_rejects_invalid_and_reserved_keys() {
        let error = StaticClientMetadata::try_from_pairs([("bad key", "value")])
            .expect_err("invalid key should fail");
        assert!(error.to_string().contains("metadata key 'bad key'"));

        for key in [
            "traceparent",
            "tracestate",
            "baggage",
            CORAL_TASK_ID_METADATA_KEY,
            "content-type",
            "te",
            "grpc-timeout",
        ] {
            let error = StaticClientMetadata::try_from_pairs([(key, "value")])
                .expect_err("reserved key should fail");
            assert!(error.to_string().contains("is reserved"));
        }
    }

    #[tokio::test]
    async fn interceptor_combines_task_and_repeated_static_metadata() {
        let metadata = StaticClientMetadata::try_from_pairs([
            ("x-coral-route", "primary"),
            ("x-coral-route", "secondary"),
            (AUTHORIZATION_METADATA_KEY, "Bearer secret"),
        ])
        .expect("metadata");
        assert!(metadata.contains_authorization());
        let mut interceptor = ClientMetadataInterceptor::new(metadata);
        let task_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("metadata value");

        let request = with_task_metadata(Some(task_id), async {
            interceptor
                .call(tonic::Request::new(()))
                .expect("interceptor")
        })
        .await;
        let routes = request
            .metadata()
            .get_all("x-coral-route")
            .iter()
            .map(|value| value.to_str().expect("ASCII metadata"))
            .collect::<Vec<_>>();

        assert_eq!(routes, vec!["primary", "secondary"]);
        assert_eq!(
            request
                .metadata()
                .get(CORAL_TASK_ID_METADATA_KEY)
                .and_then(|value| value.to_str().ok()),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
        assert!(
            request
                .metadata()
                .get(AUTHORIZATION_METADATA_KEY)
                .expect("authorization")
                .is_sensitive()
        );
    }
}
