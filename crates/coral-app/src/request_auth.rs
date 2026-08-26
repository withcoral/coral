//! Session authentication shared by served transport surfaces.

use std::fmt;
use std::sync::Arc;

use tonic::metadata::MetadataMap;

use crate::auth::session::SessionTokenVerifier;
use crate::identity::{BearerAuthenticator, Principal, PrincipalProvider, PrincipalProviderError};
use crate::workspace_mcp_urls::WorkspaceMcpUrls;

const AUTHORIZATION_METADATA: &str = "authorization";
const UNAUTHENTICATED_MESSAGE: &str = "authentication required";

/// Authenticates session tokens minted for an allowlist of accepted audiences.
///
/// Whether the allowlist holds one entry or several depends on the surface, and
/// the two cases are genuinely different policies. A public surface accepts only
/// tokens minted for itself. The private gRPC API has no resource identity of its
/// own — it is reached through the public surfaces that front it — so it accepts
/// the audience of every one of them.
///
/// The audience a token was minted for says which surface the request arrived
/// through, and nothing more: one surface can carry either kind of actor, so it
/// cannot settle what kind the caller is. The token says so itself instead. Its
/// subject names who is calling and its actor-kind claim names what they are,
/// both fixed by the issuer at the moment it knew them — an agent is its own
/// principal with its own identifier, and it arrives here already declared as
/// one rather than being recognised by the surface it came through.
#[derive(Clone)]
pub struct SessionPrincipalProvider {
    verifier: SessionTokenVerifier,
    audiences: AudiencePolicy,
}

/// Which minted audiences a surface's provider admits.
#[derive(Clone)]
enum AudiencePolicy {
    /// An enumerated allowlist, the shape every surface had before
    /// per-workspace MCP resources existed.
    Exact(Arc<[String]>),
    /// An enumerated allowlist plus every canonical per-workspace MCP
    /// resource under one base URL. The family cannot be enumerated — its
    /// members come and go with workspaces — so membership is decided by
    /// parsing the audience against the one URL template.
    ExactPlusWorkspaceFamily {
        exact: Arc<[String]>,
        family: Arc<WorkspaceMcpUrls>,
    },
}

impl AudiencePolicy {
    fn accepts(&self, audience: &str) -> bool {
        match self {
            Self::Exact(exact) => exact.iter().any(|accepted| accepted == audience),
            Self::ExactPlusWorkspaceFamily { exact, family } => {
                exact.iter().any(|accepted| accepted == audience)
                    || family.parse_resource(audience).is_some()
            }
        }
    }

    /// The enumerated allowlist this policy consults, family aside.
    fn exact(&self) -> &[String] {
        match self {
            Self::Exact(exact) | Self::ExactPlusWorkspaceFamily { exact, .. } => exact,
        }
    }

    /// Rejects a malformed accepted-audiences allowlist as a misconfiguration.
    ///
    /// This is the guard [`SessionTokenVerifier::validate_access_token`] applies
    /// to an enumerated list, lifted here so both policies route through the
    /// predicate path yet a whitespace-padded entry still fails loudly rather
    /// than silently admitting a token minted for that exact padded audience.
    /// Every enumerated entry must be non-empty and free of surrounding
    /// whitespace; a pure [`Self::Exact`] policy, which accepts nothing but its
    /// list, must additionally have at least one entry, while the
    /// workspace-family policy has no such minimum — the family covers
    /// acceptance when the exact list is empty.
    fn ensure_well_formed(&self) -> Result<(), ()> {
        let exact = self.exact();
        if matches!(self, Self::Exact(_)) && exact.is_empty() {
            return Err(());
        }
        if exact
            .iter()
            .any(|audience| audience.is_empty() || audience.trim() != audience.as_str())
        {
            return Err(());
        }
        Ok(())
    }
}

impl SessionPrincipalProvider {
    pub(crate) fn new(
        verifier: SessionTokenVerifier,
        accepted_audiences: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            verifier,
            audiences: AudiencePolicy::Exact(accepted_audiences.into_iter().collect()),
        }
    }

    /// Builds a provider admitting `accepted_audiences` plus every
    /// per-workspace MCP resource in `family`.
    pub(crate) fn with_workspace_family(
        verifier: SessionTokenVerifier,
        accepted_audiences: impl IntoIterator<Item = String>,
        family: Arc<WorkspaceMcpUrls>,
    ) -> Self {
        Self {
            verifier,
            audiences: AudiencePolicy::ExactPlusWorkspaceFamily {
                exact: accepted_audiences.into_iter().collect(),
                family,
            },
        }
    }

    fn principal_for_token(&self, token: &str) -> Result<Principal, PrincipalProviderError> {
        // Both policies route through the predicate path; the allowlist's
        // empty-and-whitespace config guard, once specific to the enumerated
        // entry point, now runs here so a misconfigured allowlist fails loudly
        // under either policy rather than only the enumerated one.
        self.audiences
            .ensure_well_formed()
            .map_err(|()| unauthenticated())?;
        self.principal_where(token, &|audience| self.audiences.accepts(audience))
    }

    /// Authenticates a bearer token minted for exactly `audience`.
    ///
    /// This is the per-route check for surfaces whose resource identity varies
    /// by request — a per-workspace MCP URL admits only tokens minted for that
    /// exact URL, so the expected audience arrives with the call instead of
    /// living in the provider's own policy.
    ///
    /// # Errors
    ///
    /// Returns the same generic authentication failure as every other path.
    pub fn principal_for_bearer_with_audience(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<Principal, PrincipalProviderError> {
        self.principal_where(token, &|minted| minted == audience)
    }

    fn principal_where(
        &self,
        token: &str,
        audience_ok: &dyn Fn(&str) -> bool,
    ) -> Result<Principal, PrincipalProviderError> {
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(unauthenticated());
        }
        let session = self
            .verifier
            .validate_access_token_where(token, audience_ok)
            .map_err(|_error| unauthenticated())?;
        // The token's subject is Coral's internal `user_id`, so the request
        // principal is that id verbatim — no upstream issuer, subject, or
        // display name enters it, and nothing is derived from one. The kind
        // comes from the token's own claim for the same reason: the issuer
        // knew what it minted the token for, and no property of the request
        // reconstructs that afterwards.
        Principal::parse(&session.user_id, session.principal_kind)
            .map_err(|_error| unauthenticated())
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
            .issue_access_token_as(USER_ID, CLIENT_ID, BFF_AUDIENCE, PrincipalKind::User)
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

    /// The audience only says which surface a request arrived through, and one
    /// surface can carry either kind of actor, so it settles nothing about the
    /// caller. One person reaching the private API through two surfaces is the
    /// same principal, with the same kind, on both.
    #[tokio::test]
    async fn the_surface_a_token_was_minted_for_does_not_change_the_actor_kind() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let private_api = SessionPrincipalProvider::new(
            config.verifier(),
            [MCP_AUDIENCE.to_string(), BFF_AUDIENCE.to_string()],
        );

        for audience in [MCP_AUDIENCE, BFF_AUDIENCE] {
            let token = config
                .issue_access_token_as(USER_ID, CLIENT_ID, audience, PrincipalKind::User)
                .expect("session token")
                .access_token;
            let principal = private_api
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .unwrap_or_else(|_| panic!("allowlisted audience {audience}"));
            assert_eq!(principal.id().as_str(), USER_ID);
            assert_eq!(
                principal.kind(),
                PrincipalKind::User,
                "{audience} carries a person's user_id, so it authenticates a user"
            );
        }
    }

    /// The kind is the issuer's statement about who it minted for, so the
    /// provider reports it rather than deciding it. Two tokens differing only in
    /// that claim authenticate the same id as different kinds of actor.
    #[tokio::test]
    async fn the_actor_kind_comes_from_the_token_that_declared_it() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let provider = SessionPrincipalProvider::new(config.verifier(), [MCP_AUDIENCE.to_string()]);

        for kind in [PrincipalKind::User, PrincipalKind::Agent] {
            let token = config
                .issue_access_token_as(USER_ID, CLIENT_ID, MCP_AUDIENCE, kind)
                .expect("session token")
                .access_token;
            let principal = provider
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .expect("principal");
            assert_eq!(principal.kind(), kind);
            assert_eq!(
                principal.id().as_str(),
                USER_ID,
                "the kind must not disturb the subject"
            );
        }
    }

    #[tokio::test]
    async fn malformed_or_invalid_credentials_fail_generically() {
        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let wrong_signing_key = test_signing_key();
        let wrong_key = session(&wrong_signing_key)
            .issue_access_token_as(USER_ID, CLIENT_ID, MCP_AUDIENCE, PrincipalKind::User)
            .expect("wrong-key token")
            .access_token;
        let wrong_audience = config
            .issue_access_token_as(
                USER_ID,
                CLIENT_ID,
                "https://other.example/mcp",
                PrincipalKind::User,
            )
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
                .issue_access_token_as(USER_ID, CLIENT_ID, audience, PrincipalKind::User)
                .expect("token")
                .access_token;
            private_api
                .principal_for_metadata(&bearer_metadata(&token))
                .await
                .unwrap_or_else(|_| panic!("allowlisted audience {audience}"));
        }

        let unapproved = config
            .issue_access_token_as(
                USER_ID,
                CLIENT_ID,
                "https://unapproved.example",
                PrincipalKind::User,
            )
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
            .issue_access_token_as(USER_ID, CLIENT_ID, MCP_AUDIENCE, PrincipalKind::User)
            .expect("token")
            .access_token;
        provider
            .principal_for_metadata(&bearer_metadata(&accepted))
            .await
            .expect("its own audience");

        for audience in [BFF_AUDIENCE, "https://unapproved.example"] {
            let token = config
                .issue_access_token_as(USER_ID, CLIENT_ID, audience, PrincipalKind::User)
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

    /// The workspace-family policy admits a token minted for a workspace
    /// resource even when its enumerated exact list is empty — the family
    /// covers acceptance. A whitespace-padded exact entry, by contrast, is a
    /// misconfiguration that fails loudly under this policy just as it does
    /// under a pure allowlist, rather than being silently consulted through the
    /// predicate path.
    #[tokio::test]
    async fn the_workspace_family_policy_guards_its_exact_list() {
        use std::sync::Arc;

        use crate::oauth_resource::CanonicalOauthUrl;
        use crate::workspace_mcp_urls::{McpWorkspaceSegment, WorkspaceMcpUrls};

        let signing_key = test_signing_key();
        let config = session(&signing_key);
        let base = CanonicalOauthUrl::parse("https://coral.example/mcp").expect("canonical base");
        let family = Arc::new(WorkspaceMcpUrls::new(base));
        let workspace_resource =
            family.resource(&McpWorkspaceSegment::parse("team").expect("segment"));
        let token = config
            .issue_access_token_as(USER_ID, CLIENT_ID, &workspace_resource, PrincipalKind::User)
            .expect("workspace token")
            .access_token;

        // Empty exact list, family present: the family admits the workspace token.
        let family_only = super::SessionPrincipalProvider::with_workspace_family(
            config.verifier(),
            Vec::<String>::new(),
            family.clone(),
        );
        family_only
            .principal_for_bearer(&token)
            .await
            .expect("the family admits its own workspace resource");

        // A whitespace-padded exact entry is a misconfiguration: every token is
        // refused, including the otherwise-valid workspace token.
        let padded = super::SessionPrincipalProvider::with_workspace_family(
            config.verifier(),
            [" https://coral.example/other ".to_string()],
            family,
        );
        padded
            .principal_for_bearer(&token)
            .await
            .expect_err("a whitespace-padded allowlist entry must fail loudly");
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
