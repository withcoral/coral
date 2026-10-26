use coral_workspace::{
    AuthenticatedPrincipal, WorkspaceId, WorkspaceModule, local_organization_owner,
};

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

pub(crate) struct LocalAuthentication(pub WorkspaceModule);
#[async_trait::async_trait]
impl WorkspaceAuthenticator for LocalAuthentication {
    /// No credential is the implicit local User, who has no sign-in to pick a
    /// Workspace with, so at `/mcp` only an Agent credential works.
    async fn authenticate(
        &self,
        endpoint: &McpEndpoint,
        bearer: Option<&str>,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        match (endpoint, bearer) {
            (McpEndpoint::Workspace(workspace), None) => Ok((
                AuthenticatedPrincipal::user(local_organization_owner()),
                workspace.clone(),
            )),
            (McpEndpoint::Organization, None) => Err(AuthenticationError::WorkspaceUrlRequired),
            (McpEndpoint::Workspace(workspace), Some(credential)) => Ok((
                self.0
                    .authenticate_agent(workspace, credential, now()?)
                    .await?,
                workspace.clone(),
            )),
            (McpEndpoint::Organization, Some(credential)) => Ok(self
                .0
                .authenticate_agent_credential(credential, now()?)
                .await?),
        }
    }
}

pub(crate) fn now() -> Result<i64, AuthenticationError> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| AuthenticationError::Unavailable)?
        .as_nanos();
    i64::try_from(nanos).map_err(|_error| AuthenticationError::Unavailable)
}

impl From<coral_workspace::Error> for AuthenticationError {
    fn from(error: coral_workspace::Error) -> Self {
        match error {
            coral_workspace::Error::InvalidAgentCredential
            | coral_workspace::Error::AgentNotFound
            | coral_workspace::Error::AgentDisabled
            | coral_workspace::Error::AgentRegistrationExpired
            | coral_workspace::Error::PermissionDenied(_) => AuthenticationError::Unauthenticated,
            _ => AuthenticationError::Unavailable,
        }
    }
}
