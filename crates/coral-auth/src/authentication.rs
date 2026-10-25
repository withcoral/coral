use coral_workspace::{
    AuthenticatedPrincipal, OrganizationModule, PrincipalKind, WorkspaceId, WorkspaceModule,
};

use crate::SessionTokenIssuer;
use crate::session::{SessionTokenVerifier, ValidatedSession};

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

    /// Verifies a credential at the Organization MCP Endpoint, which names no
    /// Workspace, and returns the Workspace the credential fixes: a User token's
    /// signed `workspace_id`, or an Agent Registration's Workspace.
    ///
    /// # Errors
    /// Rejects unscoped tokens, wrong audiences, Agent-kind tokens, invalid
    /// Agent credentials, and disabled Users.
    pub async fn authenticate_credential(
        &self,
        bearer: Option<&str>,
        expected_audience: &str,
    ) -> Result<CredentialWorkspace, AuthenticationError> {
        let credential = bearer.ok_or(AuthenticationError::Unauthenticated)?;
        if credential.starts_with("coral_agent_") {
            let principal = self
                .workspaces
                .authenticate_agent_credential(credential, now_unix_nanos()?)
                .await?;
            let workspace = principal
                .agent_workspace_id()
                .ok_or(AuthenticationError::Unavailable)?
                .clone();
            return Ok(CredentialWorkspace {
                principal,
                workspace,
            });
        }
        let session = self
            .sessions
            .validate_access_token(credential, &[expected_audience])
            .map_err(|_error| AuthenticationError::Unauthenticated)?;
        let workspace = session
            .workspace_id
            .as_deref()
            .and_then(|scope| WorkspaceId::parse(scope).ok())
            .ok_or(AuthenticationError::Unauthenticated)?;
        let principal = self.user(&session).await?;
        Ok(CredentialWorkspace {
            principal,
            workspace,
        })
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
            return self
                .workspaces
                .authenticate_agent(workspace, credential, now_unix_nanos()?)
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
        {
            return Err(AuthenticationError::Unauthenticated);
        }
        self.user(&session).await
    }

    /// The current authority of the User a validated User-kind token names.
    async fn user(
        &self,
        session: &ValidatedSession,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        if session.principal_kind != PrincipalKind::User {
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

/// A Principal authenticated at the Organization MCP Endpoint, with the
/// Workspace its credential fixes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialWorkspace {
    principal: AuthenticatedPrincipal,
    workspace: WorkspaceId,
}

impl CredentialWorkspace {
    /// Returns the authenticated Principal.
    #[must_use]
    pub const fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Returns the Workspace the credential fixes.
    #[must_use]
    pub const fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }

    /// Splits the Principal and its Workspace.
    #[must_use]
    pub fn into_parts(self) -> (AuthenticatedPrincipal, WorkspaceId) {
        (self.principal, self.workspace)
    }
}

fn now_unix_nanos() -> Result<i64, AuthenticationError> {
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
            | coral_workspace::Error::PermissionDenied(_) => Self::Unauthenticated,
            _ => Self::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests;
