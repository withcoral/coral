//! Product authorization seam for management-plane mutations.

use std::fmt;

use crate::identity::UserPrincipal;

/// Error returned when a product runtime rejects a management-plane mutation.
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    /// The authenticated principal is not allowed to perform the operation.
    #[error("{0}")]
    Forbidden(String),
    /// Authorization failed unexpectedly.
    #[error("{0}")]
    Internal(String),
}

impl AuthorizationError {
    /// Builds a permission-denied authorization error.
    #[must_use]
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }

    /// Builds an internal authorization error.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

/// Source mutation operation being authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMutationKind {
    /// Install a source from Coral's bundled source catalog.
    CreateBundled,
    /// Install a bundled source while retrieving OAuth credentials.
    CreateBundledWithOAuth,
    /// Import a source spec.
    Import,
    /// Import a source spec while retrieving OAuth credentials.
    ImportWithOAuth,
    /// Remove an installed source from a workspace.
    Delete,
}

/// Product-provided authorization policy for management-plane mutations.
///
/// OSS Coral installs [`AllowAllManagementAuthorizer`] by default to preserve
/// local single-user behavior. Product runtimes can replace it to gate source
/// and identity-spec mutations while reusing the shared gRPC service surface.
#[tonic::async_trait]
pub trait ManagementAuthorizer: fmt::Debug + Send + Sync + 'static {
    /// Authorizes creating or deleting global identity specs.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationError`] when the principal is not allowed to
    /// mutate identity specs.
    async fn authorize_identity_spec_mutation(
        &self,
        principal: &UserPrincipal,
    ) -> Result<(), AuthorizationError>;

    /// Authorizes creating, importing, or deleting a workspace source through
    /// the shared OSS source mutation APIs.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationError`] when the principal is not allowed to
    /// mutate sources in the workspace.
    async fn authorize_source_mutation(
        &self,
        principal: &UserPrincipal,
        workspace_id: &str,
        kind: SourceMutationKind,
    ) -> Result<(), AuthorizationError>;
}

/// Default OSS authorizer for local single-user usage.
#[derive(Debug, Default)]
pub struct AllowAllManagementAuthorizer;

#[tonic::async_trait]
impl ManagementAuthorizer for AllowAllManagementAuthorizer {
    async fn authorize_identity_spec_mutation(
        &self,
        _principal: &UserPrincipal,
    ) -> Result<(), AuthorizationError> {
        Ok(())
    }

    async fn authorize_source_mutation(
        &self,
        _principal: &UserPrincipal,
        _workspace_id: &str,
        _kind: SourceMutationKind,
    ) -> Result<(), AuthorizationError> {
        Ok(())
    }
}

pub(crate) fn authorization_status(error: AuthorizationError) -> tonic::Status {
    match error {
        AuthorizationError::Forbidden(message) => tonic::Status::permission_denied(message),
        AuthorizationError::Internal(message) => tonic::Status::internal(message),
    }
}
