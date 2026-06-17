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

/// Product-provided authorization policy for workspace data-plane reads.
///
/// OSS Coral installs [`AllowAllWorkspaceReadAuthorizer`] by default to
/// preserve local single-user behavior. Product runtimes can replace it to gate
/// SQL query and catalog reads for multi-user workspace control planes.
#[tonic::async_trait]
pub trait WorkspaceReadAuthorizer: fmt::Debug + Send + Sync + 'static {
    /// Authorizes reading query-visible data from a workspace.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationError`] when the principal is not allowed to read
    /// from the workspace.
    async fn authorize_workspace_read(
        &self,
        principal: &UserPrincipal,
        workspace_id: &str,
    ) -> Result<(), AuthorizationError>;

    /// Returns whether reads without recoverable workspace metadata are allowed.
    ///
    /// Product runtimes should keep the default `false` so trace records that
    /// cannot be mapped to a workspace fail closed.
    fn allows_unscoped_workspace_reads(&self) -> bool {
        false
    }

    /// Returns whether every workspace read is allowed without filtering.
    ///
    /// Product runtimes should keep the default `false`; the default OSS
    /// allow-all authorizer overrides this to preserve local single-user trace
    /// pagination behavior.
    fn allows_all_workspace_reads(&self) -> bool {
        false
    }
}

/// Default OSS authorizer for local single-user usage.
#[derive(Debug, Default)]
pub struct AllowAllManagementAuthorizer;

/// Default OSS workspace read authorizer for local single-user usage.
#[derive(Debug, Default)]
pub struct AllowAllWorkspaceReadAuthorizer;

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

#[tonic::async_trait]
impl WorkspaceReadAuthorizer for AllowAllWorkspaceReadAuthorizer {
    async fn authorize_workspace_read(
        &self,
        _principal: &UserPrincipal,
        _workspace_id: &str,
    ) -> Result<(), AuthorizationError> {
        Ok(())
    }

    fn allows_unscoped_workspace_reads(&self) -> bool {
        true
    }

    fn allows_all_workspace_reads(&self) -> bool {
        true
    }
}

pub(crate) fn authorization_status(error: AuthorizationError) -> tonic::Status {
    match error {
        AuthorizationError::Forbidden(message) => tonic::Status::permission_denied(message),
        AuthorizationError::Internal(message) => tonic::Status::internal(message),
    }
}
