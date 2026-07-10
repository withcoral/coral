//! Session authentication shared by served transport surfaces.

use std::fmt;

use tonic::metadata::MetadataMap;

use crate::auth::session::SessionTokenConfig;
use crate::identity::{UserPrincipal, UserPrincipalProvider, UserPrincipalProviderError};

const AUTHORIZATION_METADATA: &str = "authorization";
const UNAUTHENTICATED_MESSAGE: &str = "authentication required";

#[derive(Clone)]
pub(crate) struct SessionUserPrincipalProvider {
    session: SessionTokenConfig,
}

impl SessionUserPrincipalProvider {
    pub(crate) fn new(session: SessionTokenConfig) -> Self {
        Self { session }
    }
}

impl fmt::Debug for SessionUserPrincipalProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionUserPrincipalProvider")
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl UserPrincipalProvider for SessionUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError> {
        let token = strict_bearer(metadata)?;
        let session = self
            .session
            .validate_access_token(token)
            .map_err(|_error| unauthenticated())?;
        Ok(UserPrincipal::for_federated(
            &session.provider,
            &session.subject,
        ))
    }
}

fn strict_bearer(metadata: &MetadataMap) -> Result<&str, UserPrincipalProviderError> {
    let mut values = metadata.get_all(AUTHORIZATION_METADATA).iter();
    let value = values
        .next()
        .filter(|_| values.next().is_none())
        .and_then(|value| value.to_str().ok())
        .ok_or_else(unauthenticated)?;
    let (scheme, token) = value.split_once(' ').ok_or_else(unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(unauthenticated());
    }
    Ok(token)
}

fn unauthenticated() -> UserPrincipalProviderError {
    UserPrincipalProviderError::unauthenticated(UNAUTHENTICATED_MESSAGE)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::SessionUserPrincipalProvider;
    use crate::auth::session::SessionTokenConfig;
    use crate::identity::UserPrincipalProvider as _;

    fn session(key: u8, audience: &str) -> SessionTokenConfig {
        SessionTokenConfig::new(
            Some("https://auth.example"),
            Some(audience),
            [key; 32],
            Duration::from_mins(5),
        )
        .expect("session config")
    }

    fn bearer_metadata(token: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).expect("authorization metadata"),
        );
        metadata
    }

    #[tokio::test]
    async fn valid_session_selects_a_namespaced_user_principal() {
        let config = session(b'k', "https://coral.example/mcp");
        let token = config
            .issue_access_token("oidc", "raw/subject with spaces")
            .expect("session token")
            .access_token;
        let provider = SessionUserPrincipalProvider::new(config);

        let principal = provider
            .principal_for_metadata(&bearer_metadata(&token))
            .await
            .expect("principal");

        assert!(principal.user_id().starts_with("federated-"));
        assert!(!principal.user_id().contains("raw"));
    }

    #[tokio::test]
    async fn malformed_or_invalid_credentials_fail_generically() {
        let config = session(b'k', "https://coral.example/mcp");
        let wrong_key = session(b'x', "https://coral.example/mcp")
            .issue_access_token("oidc", "alice")
            .expect("wrong-key token")
            .access_token;
        let wrong_audience = session(b'k', "https://other.example/mcp")
            .issue_access_token("oidc", "alice")
            .expect("wrong-audience token")
            .access_token;
        let provider = SessionUserPrincipalProvider::new(config);

        let mut duplicate = bearer_metadata(&wrong_key);
        duplicate.append(
            "authorization",
            MetadataValue::try_from("Bearer duplicate").expect("metadata"),
        );
        let mut cases = vec![MetadataMap::new(), duplicate];
        for value in ["Basic token", "Bearer two words"] {
            let mut metadata = MetadataMap::new();
            metadata.insert(
                "authorization",
                MetadataValue::try_from(value).expect("metadata"),
            );
            cases.push(metadata);
        }
        cases.push(bearer_metadata(&wrong_key));
        cases.push(bearer_metadata(&wrong_audience));

        for metadata in cases {
            let error = provider
                .principal_for_metadata(&metadata)
                .await
                .expect_err("credentials must fail");
            assert_eq!(error.client_message(), "authentication required");
        }
    }
}
