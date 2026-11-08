use coral_auth::{PrincipalAuthentication, WorkspaceFixedBy};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::McpEndpoint;

/// Bounded authentication failures; no token, claim, or verifier material.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum AuthenticationError {
    /// Credentials are missing, invalid, expired, or disabled.
    #[error("authentication required")]
    Unauthenticated,
    /// Current authentication evidence could not be loaded.
    #[error("authentication unavailable")]
    Unavailable,
    /// No credential reached the Organization MCP Endpoint where no sign-in
    /// can pick a Workspace: auth-disabled local mode.
    #[error("Use the workspace MCP URL from Coral.")]
    WorkspaceUrlRequired,
}

/// Public OAuth discovery information, derived only from configured URLs.
#[derive(Clone, Debug)]
pub struct OauthProtectedResource {
    /// Exact MCP resource and token audience.
    pub resource: String,
    /// Public authorization-server issuer.
    pub authorization_server: String,
    /// Public protected-resource metadata URL.
    pub metadata_url: String,
}

/// Authentication Adapter seam, invoked before every MCP transport request.
/// Implementations validate credentials, never client metadata or MCP sessions.
#[async_trait::async_trait]
pub trait WorkspaceAuthenticator: Send + Sync {
    /// Resolves the presented credential at `endpoint` and returns the
    /// Workspace the request acts in: the one a Workspace URL names, or the
    /// one the credential fixes at the Organization MCP Endpoint.
    async fn authenticate(
        &self,
        endpoint: &McpEndpoint,
        bearer: Option<&str>,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError>;
    /// Optional OAuth discovery. Unknown Workspaces reveal no resource existence.
    fn oauth_resource(&self, _endpoint: &McpEndpoint) -> Option<OauthProtectedResource> {
        None
    }
}

/// Auth-disabled local authentication: [`PrincipalAuthentication::local`] at an MCP endpoint.
pub(crate) struct LocalAuthentication(pub PrincipalAuthentication);
#[async_trait::async_trait]
impl WorkspaceAuthenticator for LocalAuthentication {
    async fn authenticate(
        &self,
        endpoint: &McpEndpoint,
        bearer: Option<&str>,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        let fixed_by = match endpoint {
            McpEndpoint::Workspace(workspace) => WorkspaceFixedBy::Url(workspace),
            McpEndpoint::Organization => WorkspaceFixedBy::Credential,
        };
        Ok(self
            .0
            .authenticate(fixed_by, bearer, &endpoint.path())
            .await?)
    }
}

impl From<coral_auth::AuthenticationError> for AuthenticationError {
    fn from(error: coral_auth::AuthenticationError) -> Self {
        match error {
            coral_auth::AuthenticationError::Unauthenticated => Self::Unauthenticated,
            coral_auth::AuthenticationError::Unavailable => Self::Unavailable,
            coral_auth::AuthenticationError::WorkspaceUrlRequired => Self::WorkspaceUrlRequired,
        }
    }
}
