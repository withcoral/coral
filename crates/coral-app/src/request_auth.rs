//! Session authentication shared by served transport surfaces.

use std::fmt;
use std::sync::Arc;

use tonic::metadata::MetadataMap;

use crate::auth::session::SessionTokenVerifier;
use crate::identity::{
    BearerAuthenticator, Principal, PrincipalKind, PrincipalProvider, PrincipalProviderError,
};

const AUTHORIZATION_METADATA: &str = "authorization";
const UNAUTHENTICATED_MESSAGE: &str = "authentication required";

/// One accepted audience and the actor kind its tokens authenticate.
///
/// The kind belongs to the surface, not to the person: the same human signs in
/// once and their MCP client and their browser receive tokens for different
/// audiences. A token minted for an agent-only surface therefore authenticates
/// a [`PrincipalKind::Agent`] even though the `user_id` in it is that person's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedAudience {
    resource: String,
    principal_kind: PrincipalKind,
}

impl AcceptedAudience {
    pub(crate) fn new(resource: impl Into<String>, principal_kind: PrincipalKind) -> Self {
        Self {
            resource: resource.into(),
            principal_kind,
        }
    }
}

/// A bare resource identifier names a human-facing surface. An agent-only
/// surface — MCP — has to be tagged with [`AcceptedAudience::new`], because
/// nothing about the identifier itself says which kind reaches it.
///
/// Every audience a served instance passes today is still a bare `String`:
/// `SessionAuthSettings::principal_provider` in `bootstrap::server_config`
/// takes `IntoIterator<Item = String>`, and the CLI's `compose_session_policies`
/// hands it the public audiences verbatim. The MCP surface therefore
/// authenticates a [`PrincipalKind::User`] in a running instance for now —
/// tagging it as an agent is wired by the later stack PR that owns that
/// wiring, which is also where `AcceptedAudience` becomes reachable from
/// outside this crate.
impl From<String> for AcceptedAudience {
    fn from(resource: String) -> Self {
        Self::new(resource, PrincipalKind::User)
    }
}

/// Authenticates session tokens minted for an allowlist of accepted audiences.
///
/// Whether the allowlist holds one entry or several depends on the surface, and
/// the two cases are genuinely different policies. A public surface accepts only
/// tokens minted for itself. The private gRPC API has no resource identity of its
/// own — it is reached through the public surfaces that front it — so it accepts
/// the audience of every one of them, and classifies each caller by the audience
/// the presented token was actually minted for.
#[derive(Clone)]
pub struct SessionPrincipalProvider {
    verifier: SessionTokenVerifier,
    accepted_audiences: Arc<[AcceptedAudience]>,
}

impl SessionPrincipalProvider {
    pub(crate) fn new(
        verifier: SessionTokenVerifier,
        accepted_audiences: impl IntoIterator<Item = impl Into<AcceptedAudience>>,
    ) -> Self {
        Self {
            verifier,
            accepted_audiences: accepted_audiences.into_iter().map(Into::into).collect(),
        }
    }

    fn principal_for_token(&self, token: &str) -> Result<Principal, PrincipalProviderError> {
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(unauthenticated());
        }
        let accepted = self
            .accepted_audiences
            .iter()
            .map(|audience| audience.resource.as_str())
            .collect::<Vec<_>>();
        let session = self
            .verifier
            .validate_access_token(token, &accepted)
            .map_err(|_error| unauthenticated())?;
        // The verifier already matched the audience against this same list, so
        // the lookup is a classification rather than a second admission check.
        // It still fails closed: an unclassifiable caller gets no principal.
        let kind = self
            .accepted_audiences
            .iter()
            .find(|audience| audience.resource == session.audience)
            .ok_or_else(unauthenticated)?
            .principal_kind;
        // The token's subject is Coral's internal `user_id`, so the request
        // principal is that id verbatim — no upstream issuer, subject, or
        // display name enters it, and nothing is derived from one.
        Principal::parse(&session.user_id, kind).map_err(|_error| unauthenticated())
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

    use super::{AcceptedAudience, SessionPrincipalProvider};
    use crate::auth::session::{SessionTokenIssuer, test_signing_key};
    use crate::identity::{BearerAuthenticator as _, PrincipalKind, PrincipalProvider as _};

    const MCP_AUDIENCE: &str = "https://coral.example/mcp";
    const BFF_AUDIENCE: &str = "https://app.example";
    const CLIENT_ID: &str = "https://client.example/client.json";
    const USER_ID: &str = "1f0d2b8a-6d51-4f6e-9a0d-3c8f21b4e7a5";

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
    async fn valid_session_selects_the_internal_user_id_verbatim() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let token = config
            .issue_access_token(USER_ID, CLIENT_ID, BFF_AUDIENCE)
            .expect("session token")
            .access_token;
        let provider = SessionPrincipalProvider::new(config.verifier(), [BFF_AUDIENCE.to_string()]);

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

        assert_eq!(principal.id().as_str(), USER_ID);
        assert_eq!(principal.kind(), PrincipalKind::User);
    }

    /// The audience the token was minted for decides the actor kind, so the
    /// private API — which admits both — classifies each caller by their own
    /// token even though one person is behind both of them.
    #[tokio::test]
    async fn the_mcp_audience_authenticates_an_agent_and_human_surfaces_a_user() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let private_api = SessionPrincipalProvider::new(
            config.verifier(),
            [
                AcceptedAudience::new(MCP_AUDIENCE, PrincipalKind::Agent),
                AcceptedAudience::from(BFF_AUDIENCE.to_string()),
            ],
        );

        for (audience, expected) in [
            (MCP_AUDIENCE, PrincipalKind::Agent),
            (BFF_AUDIENCE, PrincipalKind::User),
        ] {
            let token = config
                .issue_access_token(USER_ID, CLIENT_ID, audience)
                .expect("session token")
                .access_token;
            let principal = private_api
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .unwrap_or_else(|_| panic!("allowlisted audience {audience}"));
            assert_eq!(principal.id().as_str(), USER_ID);
            assert_eq!(
                principal.kind(),
                expected,
                "{audience} must authenticate a {expected:?}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_or_invalid_credentials_fail_generically() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let wrong_signing_key = test_signing_key();
        let wrong_key = session(&wrong_signing_key)
            .issue_access_token(USER_ID, CLIENT_ID, MCP_AUDIENCE)
            .expect("wrong-key token")
            .access_token;
        let wrong_audience = config
            .issue_access_token(USER_ID, CLIENT_ID, "https://other.example/mcp")
            .expect("wrong-audience token")
            .access_token;
        let provider = SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

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

    /// The private gRPC API is reached through the public surfaces that front it
    /// (MCP HTTP today, the UI BFF later), so it admits the audience of each one
    /// — and nothing else.
    #[tokio::test]
    async fn the_private_api_accepts_every_fronting_surface_audience() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let private_api = SessionPrincipalProvider::new(
            config.verifier(),
            [MCP_AUDIENCE.to_string(), BFF_AUDIENCE.to_string()],
        );

        for audience in [MCP_AUDIENCE, BFF_AUDIENCE] {
            let token = config
                .issue_access_token(USER_ID, CLIENT_ID, audience)
                .expect("token")
                .access_token;
            private_api
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .unwrap_or_else(|_| panic!("allowlisted audience {audience}"));
        }

        let unapproved = config
            .issue_access_token(USER_ID, CLIENT_ID, "https://unapproved.example")
            .expect("token")
            .access_token;
        private_api
            .principal_for_metadata(&bearer_metadata(&unapproved))
            .await
            .expect_err("an audience no fronting surface owns");
    }

    /// A public surface owns one resource identifier, so its provider must reject
    /// a token minted for a sibling surface even though the private API takes both.
    #[tokio::test]
    async fn a_public_surface_accepts_only_its_own_audience() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let provider = SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

        let accepted = config
            .issue_access_token(USER_ID, CLIENT_ID, MCP_AUDIENCE)
            .expect("token")
            .access_token;
        provider
            .principal_for_metadata(&bearer_metadata(&accepted))
            .await
            .expect("its own audience");

        for audience in [BFF_AUDIENCE, "https://unapproved.example"] {
            let token = config
                .issue_access_token(USER_ID, CLIENT_ID, audience)
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
        let provider = SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

        for token in ["", "two words", "trailing\t"] {
            let error = provider
                .principal_for_bearer(token)
                .await
                .expect_err("malformed token must fail");
            assert_eq!(error.client_message(), "authentication required");
        }
    }
}
