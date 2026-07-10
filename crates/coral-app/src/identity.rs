//! Shared validation helpers for app-owned identifiers.

use std::fmt;

use crate::bootstrap::AppError;

/// Stable local user used by single-user local mode.
pub(crate) const LOCAL_MEMBER_ID: &str = "local";

/// Request-scoped user principal selected by the app transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrincipal {
    user_id: String,
}

impl UserPrincipal {
    /// Builds the default single-user local principal.
    #[must_use]
    pub fn local() -> Self {
        Self {
            user_id: LOCAL_MEMBER_ID.to_string(),
        }
    }

    /// Builds a principal for a validated user id.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the user id is empty, contains whitespace,
    /// contains path separators, or aliases the reserved local single-user
    /// sentinel.
    pub fn for_user(user_id: &str) -> Result<Self, AppError> {
        if user_id.chars().any(char::is_whitespace) {
            return Err(AppError::InvalidInput(
                "user id must not contain whitespace".to_string(),
            ));
        }
        let user_id = parse_path_segment("user", user_id)?;
        if user_id == LOCAL_MEMBER_ID {
            return Err(AppError::InvalidInput(format!(
                "user id '{LOCAL_MEMBER_ID}' is reserved for single-user local mode"
            )));
        }
        Ok(Self { user_id })
    }

    pub(crate) fn for_federated(provider: &str, subject: &str) -> Self {
        let mut identity = Vec::with_capacity(provider.len() + subject.len() + 32);
        identity.extend_from_slice(b"coral-federated-user-v1\0");
        identity.extend_from_slice(&(provider.len() as u64).to_be_bytes());
        identity.extend_from_slice(provider.as_bytes());
        identity.extend_from_slice(&(subject.len() as u64).to_be_bytes());
        identity.extend_from_slice(subject.as_bytes());
        Self {
            user_id: format!("federated-{}", crate::hash::sha256_hex(&identity)),
        }
    }

    /// Returns the validated user id.
    #[must_use]
    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserPrincipalProviderErrorKind {
    Unauthenticated,
    Unavailable,
    Internal,
}

/// Client-safe failure reported by a request principal provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrincipalProviderError {
    kind: UserPrincipalProviderErrorKind,
    client_message: String,
}

impl UserPrincipalProviderError {
    fn new(
        kind: UserPrincipalProviderErrorKind,
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
            UserPrincipalProviderErrorKind::Unauthenticated,
            client_message,
            "unauthenticated request",
        )
    }

    /// Builds a transient provider failure with a client-safe message.
    #[must_use]
    pub fn unavailable(client_message: impl Into<String>) -> Self {
        Self::new(
            UserPrincipalProviderErrorKind::Unavailable,
            client_message,
            "user principal provider unavailable",
        )
    }

    /// Builds an unexpected provider failure with a client-safe message.
    #[must_use]
    pub fn internal(client_message: impl Into<String>) -> Self {
        Self::new(
            UserPrincipalProviderErrorKind::Internal,
            client_message,
            "user principal provider failed",
        )
    }

    pub(crate) fn kind(&self) -> UserPrincipalProviderErrorKind {
        self.kind
    }

    /// Returns the client-safe failure message.
    #[must_use]
    pub fn client_message(&self) -> &str {
        &self.client_message
    }
}

impl fmt::Display for UserPrincipalProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.client_message)
    }
}

impl std::error::Error for UserPrincipalProviderError {}

/// Server-side provider for request user principals.
///
/// The OSS provider always returns [`UserPrincipal::local`]. Product runtimes
/// can install a provider that authenticates inbound metadata and returns the
/// corresponding user principal.
#[tonic::async_trait]
pub trait UserPrincipalProvider: Send + Sync + std::fmt::Debug {
    /// Returns the user principal for one inbound gRPC request.
    ///
    /// # Errors
    ///
    /// Returns [`UserPrincipalProviderError`] when transport metadata is
    /// malformed, the provider cannot authenticate the request, or principal
    /// selection fails.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError>;
}

/// Default OSS principal provider for single-user local mode.
#[derive(Debug, Default)]
pub struct SingleUserPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for SingleUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError> {
        Ok(UserPrincipal::local())
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
        SingleUserPrincipalProvider, UserPrincipal, UserPrincipalProvider, parse_path_segment,
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
    fn user_principal_rejects_whitespace_anywhere() {
        for invalid in [" saul", "saul ", "alice bob", "alice\tbob", "alice\nbob"] {
            let error = UserPrincipal::for_user(invalid).expect_err("whitespace should fail");

            assert!(
                error
                    .to_string()
                    .contains("user id must not contain whitespace")
            );
        }
    }

    #[test]
    fn user_principal_rejects_path_segments_and_reserved_local_id() {
        for invalid in ["team/saul", r"team\saul", ".", "..", "local"] {
            UserPrincipal::for_user(invalid).expect_err("invalid user id should fail");
        }
    }

    #[test]
    fn user_principal_preserves_valid_id() {
        let principal = UserPrincipal::for_user("saul").expect("valid user");

        assert_eq!(principal.user_id(), "saul");
    }

    #[test]
    fn federated_principal_is_stable_and_namespaces_provider_and_subject() {
        let principal = UserPrincipal::for_federated("oidc", "alice");
        assert_eq!(principal, UserPrincipal::for_federated("oidc", "alice"));
        assert_ne!(principal, UserPrincipal::for_federated("saml", "alice"));
        assert_ne!(principal, UserPrincipal::for_federated("oidc", "bob"));
        assert_ne!(
            UserPrincipal::for_federated("ab", "c"),
            UserPrincipal::for_federated("a", "bc")
        );
        UserPrincipal::for_user(principal.user_id()).expect("generated id is path-safe");
    }

    #[tokio::test]
    async fn single_user_provider_returns_local_principal() {
        let principal = SingleUserPrincipalProvider
            .principal_for_metadata(&tonic::metadata::MetadataMap::new())
            .await
            .expect("local principal");

        assert_eq!(principal, UserPrincipal::local());
    }
}
