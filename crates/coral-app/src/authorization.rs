//! Product authorization seam for management-plane mutations.

use std::fmt;

use crate::identity::UserPrincipal;

/// Error returned when a product runtime rejects a management-plane mutation.
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    /// The authenticated principal is not allowed to perform the operation.
    #[error("{0}")]
    Forbidden(String),
    /// The product authorization policy failed before it could make a decision.
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

/// Upsert/delete management-resource mutation operation being authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceMutationKind {
    /// Install, create, or replace the resource.
    Upsert,
    /// Remove the resource.
    Delete,
}

/// Workspace source mutation operation being authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceSourceMutationKind {
    /// Install a source from Coral's bundled source catalog.
    CreateBundled,
    /// Install a bundled source while retrieving OAuth credentials.
    CreateBundledWithOAuth,
    /// Install a source from an authored or imported source spec.
    CreateFromSourceSpec,
    /// Remove an installed source from a workspace.
    Delete,
}

/// Management-plane mutation exposed by Coral's shared service surface.
///
/// Source specs, workspace sources, and workspace identities are separate
/// resources: importing or updating an authored manifest mutates a source spec,
/// adding that spec to a workspace mutates the workspace's installed source
/// catalog, and changing workspace-owned identity material mutates a workspace
/// identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementMutation<'a> {
    /// Create, replace, or delete a global identity spec.
    IdentitySpec {
        /// Identity-spec operation being authorized.
        kind: ResourceMutationKind,
    },
    /// Create, replace, or delete an authored source spec.
    SourceSpec {
        /// Source-spec operation being authorized.
        kind: ResourceMutationKind,
    },
    /// Create, replace, or delete workspace-owned identity material.
    WorkspaceIdentity {
        /// Workspace whose identity catalog will change.
        workspace_id: &'a str,
        /// Workspace identity operation being authorized.
        kind: ResourceMutationKind,
    },
    /// Create or delete a workspace source.
    WorkspaceSource {
        /// Workspace whose installed source catalog will change.
        workspace_id: &'a str,
        /// Workspace source operation being authorized.
        kind: WorkspaceSourceMutationKind,
    },
}

/// Product-provided authorization policy for management-plane mutations.
///
/// OSS Coral installs [`AllowAllManagementAuthorizer`] by default to preserve
/// local single-user behavior. Product runtimes can replace it to gate the
/// management mutations exposed through the shared gRPC service surface.
#[tonic::async_trait]
pub trait ManagementAuthorizer: fmt::Debug + Send + Sync + 'static {
    /// Authorizes one management-plane mutation.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationError`] when the principal is not allowed to
    /// perform the mutation.
    async fn authorize_management_mutation(
        &self,
        principal: &UserPrincipal,
        mutation: ManagementMutation<'_>,
    ) -> Result<(), AuthorizationError>;
}

/// Default OSS authorizer for local single-user usage.
#[derive(Debug, Default)]
pub struct AllowAllManagementAuthorizer;

#[tonic::async_trait]
impl ManagementAuthorizer for AllowAllManagementAuthorizer {
    async fn authorize_management_mutation(
        &self,
        _principal: &UserPrincipal,
        _mutation: ManagementMutation<'_>,
    ) -> Result<(), AuthorizationError> {
        Ok(())
    }
}

pub(crate) fn authorization_status(error: AuthorizationError) -> tonic::Status {
    match error {
        AuthorizationError::Forbidden(message) => {
            tonic::Status::permission_denied(bounded_status_message(message))
        }
        AuthorizationError::Internal(message) => {
            tonic::Status::internal(bounded_status_message(message))
        }
    }
}

const MAX_AUTHORIZATION_STATUS_MESSAGE_BYTES: usize = 512;

fn bounded_status_message(mut message: String) -> String {
    const SUFFIX: &str = "...";

    if message.len() <= MAX_AUTHORIZATION_STATUS_MESSAGE_BYTES {
        return message;
    }

    let mut end = MAX_AUTHORIZATION_STATUS_MESSAGE_BYTES - SUFFIX.len();
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message.push_str(SUFFIX);
    message
}

#[cfg(test)]
mod tests {
    use super::{AuthorizationError, MAX_AUTHORIZATION_STATUS_MESSAGE_BYTES, authorization_status};

    #[test]
    fn authorization_status_bounds_product_messages() {
        let status = authorization_status(AuthorizationError::forbidden("ø".repeat(1024)));

        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().len() <= MAX_AUTHORIZATION_STATUS_MESSAGE_BYTES);
        assert!(status.message().ends_with("..."));
    }
}
