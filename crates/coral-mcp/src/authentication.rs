use coral_workspace::{
    AuthenticatedPrincipal, WorkspaceId, WorkspaceModule, local_organization_owner,
};

/// Bounded authentication failures; no token, claim, or verifier material.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum AuthenticationError {
    /// Credentials are missing, invalid, expired, or disabled.
    #[error("authentication required")]
    Unauthenticated,
    /// Current authentication evidence could not be loaded.
    #[error("authentication unavailable")]
    Unavailable,
}

/// Public OAuth discovery information, derived only from configured URLs.
#[derive(Clone, Debug)]
pub struct OauthProtectedResource {
    /// Exact Workspace MCP resource and token audience.
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
    /// Resolves the presented credential at one exact Workspace URL.
    async fn authenticate(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError>;
    /// Optional OAuth discovery. Unknown Workspaces reveal no resource existence.
    fn oauth_resource(&self, _workspace: &WorkspaceId) -> Option<OauthProtectedResource> {
        None
    }
}

pub(crate) struct LocalAuthentication(pub WorkspaceModule);
#[async_trait::async_trait]
impl WorkspaceAuthenticator for LocalAuthentication {
    async fn authenticate(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        match bearer {
            None => Ok(AuthenticatedPrincipal::user(local_organization_owner())),
            Some(credential) => self
                .0
                .authenticate_agent(workspace, credential, now()?)
                .await
                .map_err(AuthenticationError::from),
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
