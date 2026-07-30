//! Session authentication shared by served transport surfaces.

use std::fmt;

use tonic::metadata::MetadataMap;

use crate::auth::session::SessionTokenVerifier;
use crate::identity::{BearerAuthenticator, Principal, PrincipalProvider, PrincipalProviderError};

const AUTHORIZATION_METADATA: &str = "authorization";
const UNAUTHENTICATED_MESSAGE: &str = "authentication required";

/// Authenticates session tokens minted for one accepted audience.
///
/// The audience is a single value, not an allowlist: a served instance has one
/// resource identifier, and a surface that must accept a different audience gets
/// its own provider instead of widening this one.
#[derive(Clone)]
pub(crate) struct SessionPrincipalProvider {
    verifier: SessionTokenVerifier,
    accepted_audience: String,
}

impl SessionPrincipalProvider {
    pub(crate) fn new(
        verifier: SessionTokenVerifier,
        accepted_audience: impl Into<String>,
    ) -> Self {
        Self {
            verifier,
            accepted_audience: accepted_audience.into(),
        }
    }

    fn principal_for_token(&self, token: &str) -> Result<Principal, PrincipalProviderError> {
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(unauthenticated());
        }
        let session = self
            .verifier
            .validate_access_token(token, &[self.accepted_audience.as_str()])
            .map_err(|_error| unauthenticated())?;
        Ok(Principal::for_federated(&session.subject))
    }
}

impl fmt::Debug for SessionPrincipalProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionPrincipalProvider")
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl PrincipalProvider for SessionPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        self.principal_for_token(strict_bearer(metadata)?)
    }
}

#[tonic::async_trait]
impl BearerAuthenticator for SessionPrincipalProvider {
    async fn principal_for_bearer(&self, token: &str) -> Result<Principal, PrincipalProviderError> {
        self.principal_for_token(token)
    }
}

fn strict_bearer(metadata: &MetadataMap) -> Result<&str, PrincipalProviderError> {
    let mut values = metadata.get_all(AUTHORIZATION_METADATA).iter();
    let value = values
        .next()
        .filter(|_| values.next().is_none())
        .and_then(|value| value.to_str().ok())
        .ok_or_else(unauthenticated)?;
    let (scheme, token) = value.split_once(' ').ok_or_else(unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(unauthenticated());
    }
    Ok(token)
}

fn unauthenticated() -> PrincipalProviderError {
    PrincipalProviderError::unauthenticated(UNAUTHENTICATED_MESSAGE)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::SessionPrincipalProvider;
    use crate::auth::session::{SessionTokenIssuer, test_signing_key};
    use crate::identity::{BearerAuthenticator as _, PrincipalProvider as _};

    const MCP_AUDIENCE: &str = "https://coral.example/mcp";
    const OTHER_AUDIENCE: &str = "https://app.example";
    const CLIENT_ID: &str = "https://client.example/client.json";

    fn session(key: &[u8]) -> SessionTokenIssuer {
        SessionTokenIssuer::new(Some("https://auth.example"), key, Duration::from_mins(5))
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
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let token = config
            .issue_access_token("raw/subject with spaces", CLIENT_ID, MCP_AUDIENCE)
            .expect("session token")
            .access_token;
        let provider = SessionPrincipalProvider::new(config.verifier(), MCP_AUDIENCE);

        let principal = provider
            .principal_for_metadata(&bearer_metadata(&token))
            .await
            .expect("principal");
        assert_eq!(
            provider
                .principal_for_bearer(&token)
                .await
                .expect("bare token principal"),
            principal,
            "the metadata and bare-token entry points must agree"
        );

        assert!(principal.id().as_str().starts_with("federated-"));
        assert!(!principal.id().as_str().contains("raw"));
    }

    #[tokio::test]
    async fn malformed_or_invalid_credentials_fail_generically() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let wrong_signing_key = test_signing_key();
        let wrong_key = session(&wrong_signing_key)
            .issue_access_token("alice", CLIENT_ID, MCP_AUDIENCE)
            .expect("wrong-key token")
            .access_token;
        let wrong_audience = config
            .issue_access_token("alice", CLIENT_ID, "https://other.example/mcp")
            .expect("wrong-audience token")
            .access_token;
        let provider = SessionPrincipalProvider::new(config.verifier(), MCP_AUDIENCE);

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

    #[tokio::test]
    async fn a_provider_accepts_only_its_own_audience() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let provider = SessionPrincipalProvider::new(config.verifier(), MCP_AUDIENCE);

        let accepted = config
            .issue_access_token("alice", CLIENT_ID, MCP_AUDIENCE)
            .expect("token")
            .access_token;
        provider
            .principal_for_metadata(&bearer_metadata(&accepted))
            .await
            .expect("its own audience");

        for audience in [OTHER_AUDIENCE, "https://unapproved.example"] {
            let token = config
                .issue_access_token("alice", CLIENT_ID, audience)
                .expect("token")
                .access_token;
            provider
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .expect_err("any other audience");
            provider
                .principal_for_bearer(&token)
                .await
                .expect_err("any other audience");
        }
    }

    #[tokio::test]
    async fn malformed_bare_tokens_fail_generically() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let provider = SessionPrincipalProvider::new(config.verifier(), MCP_AUDIENCE);

        for token in ["", "two words", "trailing\t"] {
            let error = provider
                .principal_for_bearer(token)
                .await
                .expect_err("malformed token must fail");
            assert_eq!(error.client_message(), "authentication required");
        }
    }
}
