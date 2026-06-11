//! Shared validation helpers for app-owned identifiers.

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
    /// Returns [`AppError`] if the user id is empty, contains path separators,
    /// or aliases the reserved local single-user sentinel.
    pub fn for_user(user_id: &str) -> Result<Self, AppError> {
        let user_id = parse_path_segment("user", user_id)?;
        if user_id == LOCAL_MEMBER_ID {
            return Err(AppError::InvalidInput(format!(
                "user id '{LOCAL_MEMBER_ID}' is reserved for single-user local mode"
            )));
        }
        Ok(Self { user_id })
    }

    /// Returns the validated user id.
    #[must_use]
    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

impl Default for UserPrincipal {
    fn default() -> Self {
        Self::local()
    }
}

/// Errors raised while authenticating or selecting a request user principal.
#[derive(Debug, thiserror::Error)]
pub enum UserPrincipalError {
    /// The request did not present valid authentication.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// The request presented malformed identity metadata.
    #[error("invalid user principal metadata: {0}")]
    InvalidInput(String),
    /// The principal provider failed unexpectedly.
    #[error("user principal provider failed: {0}")]
    Internal(String),
}

impl UserPrincipalError {
    /// Builds an unauthenticated principal error.
    #[must_use]
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::Unauthenticated(message.into())
    }

    /// Builds an invalid-input principal error.
    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    /// Builds an internal principal provider error.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

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
    /// Returns [`UserPrincipalError`] when transport metadata is malformed or
    /// the provider cannot authenticate the request.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalError>;
}

/// Default OSS principal provider for single-user local mode.
#[derive(Debug, Default)]
pub struct SingleUserPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for SingleUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalError> {
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
    use super::parse_path_segment;

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
}
