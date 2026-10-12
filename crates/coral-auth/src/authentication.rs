use coral_workspace::{
    AuthenticatedPrincipal, OrganizationModule, PrincipalKind, WorkspaceId, WorkspaceModule,
};

use crate::SessionTokenIssuer;
use crate::session::SessionTokenVerifier;

/// Bounded authentication failures, without token, claim, or verifier material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthenticationError {
    /// Credentials are missing, invalid, expired, or disabled.
    #[error("authentication required")]
    Unauthenticated,
    /// Current authentication evidence could not be loaded.
    #[error("authentication unavailable")]
    Unavailable,
}

/// Transport-neutral credential verification into current canonical Principal evidence.
///
/// User tokens must name the exact audience supplied by the trusted Adapter.
/// Agent credentials instead authenticate their Registration in the exact Workspace.
/// Neither result bypasses domain authorization. Each call reloads current authority;
/// no session, client metadata, or retained role can substitute for that lookup.
#[derive(Clone)]
pub struct PrincipalAuthentication {
    sessions: SessionTokenVerifier,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
}

impl PrincipalAuthentication {
    /// Builds authentication from the issuer's verification material and authority Modules.
    /// The authentication Module retains no signing key.
    #[must_use]
    pub fn new(
        issuer: &SessionTokenIssuer,
        organization: OrganizationModule,
        workspaces: WorkspaceModule,
    ) -> Self {
        Self {
            sessions: issuer.verifier(),
            organization,
            workspaces,
        }
    }

    /// Authenticates one request without provisioning a User or granting resource access.
    ///
    /// `expected_audience` is selected by the Adapter, never from unverified claims.
    /// Workspace URL construction and private method reachability remain caller-owned.
    /// Missing credentials do not select the implicit local Owner.
    ///
    /// # Errors
    /// Returns a bounded rejection or authority-store unavailability.
    pub async fn authenticate(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
        expected_audience: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.authenticate_request(workspace, bearer, expected_audience, false)
            .await
    }

    /// Verifies a credential whose signed scope must fix this exact Workspace.
    /// Agent Registrations already carry that immutable scope.
    ///
    /// # Errors
    /// Rejects unscoped or differently scoped tokens as well as invalid credentials.
    pub async fn authenticate_scoped(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
        expected_audience: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.authenticate_request(workspace, bearer, expected_audience, true)
            .await
    }

    /// Authenticates an unscoped human management request (including Workspace creation).
    /// # Errors
    /// Rejects Agents, scoped MCP credentials, wrong audiences, and disabled Users.
    pub async fn authenticate_user(
        &self,
        bearer: Option<&str>,
        audience: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        let session = self
            .sessions
            .validate_access_token(
                bearer.ok_or(AuthenticationError::Unauthenticated)?,
                &[audience],
            )
            .map_err(|_error| AuthenticationError::Unauthenticated)?;
        if session.principal_kind != PrincipalKind::User || session.workspace_id.is_some() {
            return Err(AuthenticationError::Unauthenticated);
        }
        let user_id = crate::external_user_id(&session.user_id)
            .map_err(|()| AuthenticationError::Unauthenticated)?;
        self.organization
            .resolve_user_authority(&user_id)
            .await
            .map(AuthenticatedPrincipal::user)
            .map_err(AuthenticationError::from)
    }

    async fn authenticate_request(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
        expected_audience: &str,
        require_scope: bool,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        let credential = bearer.ok_or(AuthenticationError::Unauthenticated)?;
        if credential.starts_with("coral_agent_") {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_error| AuthenticationError::Unavailable)?
                .as_nanos();
            let now = i64::try_from(nanos).map_err(|_error| AuthenticationError::Unavailable)?;
            return self
                .workspaces
                .authenticate_agent(workspace, credential, now)
                .await
                .map_err(AuthenticationError::from);
        }
        let session = self
            .sessions
            .validate_access_token(credential, &[expected_audience])
            .map_err(|_error| AuthenticationError::Unauthenticated)?;
        if (require_scope && session.workspace_id.is_none())
            || session
                .workspace_id
                .as_deref()
                .is_some_and(|scope| scope != workspace.to_string())
            || session.principal_kind != PrincipalKind::User
        {
            return Err(AuthenticationError::Unauthenticated);
        }
        let user_id = crate::external_user_id(&session.user_id)
            .map_err(|()| AuthenticationError::Unauthenticated)?;
        self.organization
            .resolve_user_authority(&user_id)
            .await
            .map(AuthenticatedPrincipal::user)
            .map_err(AuthenticationError::from)
    }
}

impl From<coral_workspace::Error> for AuthenticationError {
    fn from(error: coral_workspace::Error) -> Self {
        match error {
            coral_workspace::Error::InvalidAgentCredential
            | coral_workspace::Error::AgentNotFound
            | coral_workspace::Error::AgentDisabled
            | coral_workspace::Error::AgentRegistrationExpired
            | coral_workspace::Error::PermissionDenied(_) => Self::Unauthenticated,
            _ => Self::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests;
