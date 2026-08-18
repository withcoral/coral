//! Shared validation helpers for app-owned identifiers.

pub(crate) mod spec_document;

use std::fmt;

use crate::bootstrap::AppError;

/// Stable local principal used by single-user local mode.
pub(crate) const LOCAL_PRINCIPAL_ID: &str = "coral:local";

/// Stable, opaque identity shared by every principal kind and authority.
///
/// Providers must supply identifiers from one collision-free namespace. The
/// identifier deliberately does not expose whether the principal is a user,
/// agent, service, or another future actor kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrincipalId(String);

impl PrincipalId {
    /// Parses a canonical principal identifier.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the identifier is empty or contains
    /// whitespace or control characters.
    pub fn parse(value: &str) -> Result<Self, AppError> {
        if value.is_empty()
            || value
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(AppError::InvalidInput(
                "principal id must be non-empty and contain no whitespace or control characters"
                    .to_string(),
            ));
        }
        if value == LOCAL_PRINCIPAL_ID {
            return Err(AppError::InvalidInput(format!(
                "principal id '{LOCAL_PRINCIPAL_ID}' is reserved for local mode"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the canonical principal identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Authenticated category of actor represented by a [`Principal`].
///
/// Kind is available to authorization policy, but does not itself grant a
/// permission or imply a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    /// A human user.
    User,
    /// An autonomous or delegated agent.
    Agent,
}

/// Request-scoped principal selected by the app transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    id: PrincipalId,
    kind: PrincipalKind,
}

impl Principal {
    /// Builds a principal from its canonical identity and authenticated kind.
    #[must_use]
    pub const fn new(id: PrincipalId, kind: PrincipalKind) -> Self {
        Self { id, kind }
    }

    /// Parses and builds a principal with an authenticated kind.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when `id` is not a valid [`PrincipalId`].
    pub fn parse(id: &str, kind: PrincipalKind) -> Result<Self, AppError> {
        PrincipalId::parse(id).map(|id| Self::new(id, kind))
    }

    /// Builds the default local user principal.
    #[must_use]
    pub fn local() -> Self {
        Self {
            id: PrincipalId(LOCAL_PRINCIPAL_ID.to_string()),
            kind: PrincipalKind::User,
        }
    }

    /// Reports whether this is the built-in local principal.
    ///
    /// [`PrincipalId::parse`] refuses `coral:local`, so this is the one way the
    /// identity arises and the one question worth asking about it. Every site
    /// that branches on the built-in principal asks it here, which is also what
    /// makes those sites findable when the local-ownership migration removes
    /// the need for the branch.
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.id.as_str() == LOCAL_PRINCIPAL_ID
    }

    /// Returns the stable principal identity.
    #[must_use]
    pub const fn id(&self) -> &PrincipalId {
        &self.id
    }

    /// Returns the authenticated actor kind.
    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrincipalProviderErrorKind {
    Unauthenticated,
    Unavailable,
    Internal,
}

/// Client-safe failure reported by a request principal provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalProviderError {
    kind: PrincipalProviderErrorKind,
    client_message: String,
}

impl PrincipalProviderError {
    fn new(
        kind: PrincipalProviderErrorKind,
        client_message: impl Into<String>,
        default_message: &str,
    ) -> Self {
        let client_message = client_message.into();
        let client_message = if client_message.trim().is_empty() {
            default_message.to_string()
        } else {
            client_message
        };
        Self {
            kind,
            client_message,
        }
    }

    /// Builds a provider error with a client-safe message.
    #[must_use]
    pub fn unauthenticated(client_message: impl Into<String>) -> Self {
        Self::new(
            PrincipalProviderErrorKind::Unauthenticated,
            client_message,
            "unauthenticated request",
        )
    }

    /// Builds a transient provider failure with a client-safe message.
    #[must_use]
    pub fn unavailable(client_message: impl Into<String>) -> Self {
        Self::new(
            PrincipalProviderErrorKind::Unavailable,
            client_message,
            "principal provider unavailable",
        )
    }

    /// Builds an unexpected provider failure with a client-safe message.
    #[must_use]
    pub fn internal(client_message: impl Into<String>) -> Self {
        Self::new(
            PrincipalProviderErrorKind::Internal,
            client_message,
            "principal provider failed",
        )
    }

    pub(crate) fn kind(&self) -> PrincipalProviderErrorKind {
        self.kind
    }

    /// Returns the client-safe failure message.
    #[must_use]
    pub fn client_message(&self) -> &str {
        &self.client_message
    }
}

impl fmt::Display for PrincipalProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.client_message)
    }
}

impl std::error::Error for PrincipalProviderError {}

/// Server-side provider for request principals.
///
/// The OSS provider always returns [`Principal::local`]. Product runtimes can
/// install a provider that authenticates inbound metadata and returns the
/// corresponding stable principal identity and actor kind. A provider must
/// classify a given [`PrincipalId`] consistently across requests.
#[tonic::async_trait]
pub trait PrincipalProvider: Send + Sync + std::fmt::Debug {
    /// Returns the principal for one inbound gRPC request.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalProviderError`] when transport metadata is
    /// malformed, the provider cannot authenticate the request, or principal
    /// selection fails.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError>;
}

/// Server-side authenticator for a bearer token held outside gRPC metadata.
///
/// A served surface that already parsed the token out of its own transport —
/// the MCP HTTP `Authorization` header, say — authenticates it here instead of
/// re-encoding a gRPC [`tonic::metadata::MetadataMap`] for
/// [`PrincipalProvider`] to take apart again. The two entry points must accept
/// the same tokens, so an implementation is expected to share one verification
/// path between them.
#[tonic::async_trait]
pub trait BearerAuthenticator: Send + Sync + std::fmt::Debug {
    /// Returns the principal a bare bearer token authenticates.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalProviderError`] when the token is malformed, fails
    /// verification, or principal selection fails.
    async fn principal_for_bearer(&self, token: &str) -> Result<Principal, PrincipalProviderError>;
}

/// Default OSS principal provider for local mode.
#[derive(Debug, Default)]
pub struct LocalPrincipalProvider;

#[tonic::async_trait]
impl PrincipalProvider for LocalPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        Ok(Principal::local())
    }
}

pub(crate) fn parse_path_segment(kind: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(format!("missing {kind} name")));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not contain '/' or '\\\\'"
        )));
    }
    if trimmed == "." || trimmed == ".." {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not be '.' or '..'"
        )));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        LOCAL_PRINCIPAL_ID, LocalPrincipalProvider, Principal, PrincipalId, PrincipalKind,
        PrincipalProvider, parse_path_segment,
    };

    #[test]
    fn rejects_empty_names() {
        let error = parse_path_segment("source", "   ").expect_err("empty name should fail");
        assert!(error.to_string().contains("missing source name"));
    }

    #[test]
    fn rejects_path_separators() {
        let error = parse_path_segment("workspace", r"bad\name").expect_err("slash should fail");
        assert!(
            error
                .to_string()
                .contains("workspace name must not contain '/' or '\\\\'")
        );
    }

    #[test]
    fn rejects_dot_segments() {
        let error = parse_path_segment("source", "..").expect_err("dot segment should fail");
        assert!(
            error
                .to_string()
                .contains("source name must not be '.' or '..'")
        );
    }

    #[test]
    fn principal_id_rejects_empty_whitespace_and_control_characters() {
        for invalid in [
            "",
            " saul",
            "saul ",
            "alice bob",
            "alice\tbob",
            "alice\nbob",
            "alice\0bob",
        ] {
            let error = PrincipalId::parse(invalid).expect_err("invalid principal id");

            assert!(error.to_string().contains("principal id must be non-empty"));
        }
    }

    #[test]
    fn principal_id_rejects_reserved_local_identity() {
        PrincipalId::parse(LOCAL_PRINCIPAL_ID).expect_err("local identity must stay app-owned");
    }

    #[test]
    fn principal_preserves_canonical_opaque_id_and_explicit_kind() {
        let id = PrincipalId::parse("product:principal/saul").expect("valid principal id");
        let principal = Principal::new(id.clone(), PrincipalKind::Agent);

        assert_eq!(principal.id(), &id);
        assert_eq!(principal.id().as_str(), "product:principal/saul");
        assert_eq!(principal.kind(), PrincipalKind::Agent);
    }

    #[tokio::test]
    async fn local_provider_returns_local_principal() {
        let principal = LocalPrincipalProvider
            .principal_for_metadata(&tonic::metadata::MetadataMap::new())
            .await
            .expect("local principal");

        assert_eq!(principal, Principal::local());
        assert_eq!(principal.kind(), PrincipalKind::User);
    }
}
