//! Session authentication shared by served transport surfaces.

use std::fmt;
use std::sync::Arc;

use tonic::metadata::MetadataMap;

use crate::auth::session::SessionTokenVerifier;
use crate::identity::{Principal, PrincipalProvider, PrincipalProviderError};

const AUTHORIZATION_METADATA: &str = "authorization";
const UNAUTHENTICATED_MESSAGE: &str = "authentication required";

#[derive(Clone)]
pub(crate) struct SessionPrincipalProvider {
    verifier: SessionTokenVerifier,
    accepted_audiences: Arc<[String]>,
}

impl SessionPrincipalProvider {
    pub(crate) fn new(
        verifier: SessionTokenVerifier,
        accepted_audiences: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            verifier,
            accepted_audiences: accepted_audiences.into_iter().collect(),
        }
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
        let token = strict_bearer(metadata)?;
        let accepted_audiences = self
            .accepted_audiences
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let session = self
            .verifier
            .validate_access_token(token, &accepted_audiences)
            .map_err(|_error| unauthenticated())?;
        Ok(
            Principal::for_federated(&session.provider, &session.subject)
                .with_access_token_attribution(
                    session.token_id,
                    session.audience,
                    session.client_id,
                ),
        )
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
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
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
    use crate::identity::PrincipalProvider as _;

    const MCP_AUDIENCE: &str = "https://coral.example/mcp";
    const BFF_AUDIENCE: &str = "https://app.example";
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
            .issue_access_token("oidc", "raw/subject with spaces", CLIENT_ID, MCP_AUDIENCE)
            .expect("session token")
            .access_token;
        let provider =
            SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

        let principal = provider
            .principal_for_metadata(&bearer_metadata(&token))
            .await
            .expect("principal");

        assert!(principal.id().as_str().starts_with("federated-"));
        assert!(!principal.id().as_str().contains("raw"));
        assert_eq!(principal.audience(), Some(MCP_AUDIENCE));
        assert_eq!(principal.client_id(), Some(CLIENT_ID));
        assert!(
            principal
                .token_id()
                .is_some_and(|token_id| !token_id.is_empty())
        );
    }

    #[tokio::test]
    async fn malformed_or_invalid_credentials_fail_generically() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let wrong_signing_key = test_signing_key();
        let wrong_key = session(&wrong_signing_key)
            .issue_access_token("oidc", "alice", CLIENT_ID, MCP_AUDIENCE)
            .expect("wrong-key token")
            .access_token;
        let wrong_audience = config
            .issue_access_token("oidc", "alice", CLIENT_ID, "https://other.example/mcp")
            .expect("wrong-audience token")
            .access_token;
        let provider =
            SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

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
    async fn private_api_accepts_only_its_explicit_audience_allowlist() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let grpc_provider = SessionPrincipalProvider::new(
            config.verifier(),
            [MCP_AUDIENCE.to_string(), BFF_AUDIENCE.to_string()],
        );
        let mcp_provider =
            SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

        for audience in [MCP_AUDIENCE, BFF_AUDIENCE] {
            let token = config
                .issue_access_token("oidc", "alice", CLIENT_ID, audience)
                .expect("token")
                .access_token;
            grpc_provider
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .expect("allowlisted audience");
            if audience == BFF_AUDIENCE {
                mcp_provider
                    .principal_for_metadata(&bearer_metadata(&token))
                    .await
                    .expect_err("MCP must reject a BFF-audience token");
            }
        }

        let token = config
            .issue_access_token("oidc", "alice", CLIENT_ID, "https://unapproved.example")
            .expect("token")
            .access_token;
        grpc_provider
            .principal_for_metadata(&bearer_metadata(&token))
            .await
            .expect_err("unapproved audience");
    }
}
