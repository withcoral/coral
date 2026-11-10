use coral_workspace::{
    AuthenticatedPrincipal, OrganizationModule, PrincipalKind, WorkspaceId, WorkspaceModule,
    local_organization_owner,
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
    /// No credential reached a request whose credential must fix the
    /// Workspace, and no sign-in can pick one: auth-disabled local mode only.
    #[error("workspace URL required")]
    WorkspaceUrlRequired,
}

/// Transport-neutral credential verification into current canonical Principal evidence.
///
/// User tokens must name the exact audience supplied by the trusted Adapter.
/// Agent credentials instead authenticate their Registration in the exact Workspace.
/// Neither result bypasses domain authorization. Each call reloads current authority;
/// no session, client metadata, or retained role can substitute for that lookup.
///
/// An auth-disabled deployment ([`Self::local`]) has no User sign-in: a missing
/// credential is the implicit local Owner, and every presented credential must
/// authenticate an Agent Registration. A credential never falls back to the Owner.
#[derive(Clone)]
pub struct PrincipalAuthentication {
    mode: Mode,
    workspaces: WorkspaceModule,
}

#[derive(Clone)]
enum Mode {
    /// Users sign in; their tokens are verified and their authority reloaded.
    SignedIn(SignIn),
    /// Auth-disabled: a missing credential is the implicit local Owner.
    Local,
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
            mode: Mode::SignedIn(SignIn {
                sessions: issuer.verifier(),
                organization,
            }),
            workspaces,
        }
    }

    /// Builds authentication for an auth-disabled deployment, where no User signs in.
    ///
    /// Composition must bind every listener that serves it to loopback only.
    #[must_use]
    pub const fn local(workspaces: WorkspaceModule) -> Self {
        Self {
            mode: Mode::Local,
            workspaces,
        }
    }

    /// Whether a User signs in: `false` for [`Self::local`].
    #[must_use]
    pub const fn signs_in(&self) -> bool {
        matches!(self.mode, Mode::SignedIn(_))
    }

    /// Authenticates one request without provisioning a User or granting
    /// resource access, and returns the Workspace it acts in.
    ///
    /// `expected_audience` is selected by the Adapter, never from unverified claims;
    /// local mode verifies no User token, so it reads none.
    /// Workspace URL construction and private method reachability remain caller-owned.
    /// With sign-in, missing credentials do not select the implicit local Owner.
    ///
    /// # Errors
    /// Returns a bounded rejection or authority-store unavailability. Where the
    /// credential must fix the Workspace, unscoped tokens are rejected too, and a
    /// missing credential in local mode is [`AuthenticationError::WorkspaceUrlRequired`].
    pub async fn authenticate(
        &self,
        fixed_by: WorkspaceFixedBy<'_>,
        bearer: Option<&str>,
        expected_audience: &str,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        self.authenticate_request(fixed_by, bearer, expected_audience, false)
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
        self.authenticate_request(
            WorkspaceFixedBy::Url(workspace),
            bearer,
            expected_audience,
            true,
        )
        .await
        .map(|(principal, _workspace)| principal)
    }

    /// Authenticates an unscoped human management request (including Workspace creation).
    /// It never returns an Agent. In local mode only a missing credential, the
    /// implicit local Owner, is accepted.
    /// # Errors
    /// Rejects Agents, scoped MCP credentials, wrong audiences, and disabled Users.
    pub async fn authenticate_user(
        &self,
        bearer: Option<&str>,
        audience: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        let Mode::SignedIn(sign_in) = &self.mode else {
            return match bearer {
                None => Ok(local_owner()),
                Some(_) => Err(AuthenticationError::Unauthenticated),
            };
        };
        let session = sign_in.session(
            bearer.ok_or(AuthenticationError::Unauthenticated)?,
            audience,
        )?;
        if session.workspace_id.is_some() {
            return Err(AuthenticationError::Unauthenticated);
        }
        sign_in.user(&session).await
    }

    async fn authenticate_request(
        &self,
        fixed_by: WorkspaceFixedBy<'_>,
        bearer: Option<&str>,
        expected_audience: &str,
        require_scope: bool,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        let Some(credential) = bearer else {
            return match (&self.mode, fixed_by) {
                (Mode::SignedIn(_), _) => Err(AuthenticationError::Unauthenticated),
                (Mode::Local, WorkspaceFixedBy::Url(workspace)) => {
                    Ok((local_owner(), workspace.clone()))
                }
                (Mode::Local, WorkspaceFixedBy::Credential) => {
                    Err(AuthenticationError::WorkspaceUrlRequired)
                }
            };
        };
        // Without sign-in, every credential names an Agent Registration.
        let sign_in = match &self.mode {
            Mode::SignedIn(sign_in) if !credential.starts_with("coral_agent_") => sign_in,
            Mode::SignedIn(_) | Mode::Local => return self.agent(fixed_by, credential).await,
        };
        let session = sign_in.session(credential, expected_audience)?;
        let scope = session.workspace_id.as_deref();
        let workspace = match fixed_by {
            WorkspaceFixedBy::Url(workspace) => {
                if (require_scope && scope.is_none())
                    || scope.is_some_and(|scope| scope != workspace.to_string())
                {
                    return Err(AuthenticationError::Unauthenticated);
                }
                workspace.clone()
            }
            WorkspaceFixedBy::Credential => scope
                .and_then(|scope| WorkspaceId::parse(scope).ok())
                .ok_or(AuthenticationError::Unauthenticated)?,
        };
        Ok((sign_in.user(&session).await?, workspace))
    }

    /// The Agent whose Registration `credential` authenticates, in the Workspace it fixes.
    async fn agent(
        &self,
        fixed_by: WorkspaceFixedBy<'_>,
        credential: &str,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        let now = now_unix_nanos()?;
        Ok(match fixed_by {
            WorkspaceFixedBy::Url(workspace) => (
                self.workspaces
                    .authenticate_agent(workspace, credential, now)
                    .await?,
                workspace.clone(),
            ),
            WorkspaceFixedBy::Credential => {
                self.workspaces
                    .authenticate_agent_credential(credential, now)
                    .await?
            }
        })
    }
}

/// User sign-in: token verification and the current authority of the User it names.
#[derive(Clone)]
struct SignIn {
    sessions: SessionTokenVerifier,
    organization: OrganizationModule,
}

impl SignIn {
    fn session(
        &self,
        credential: &str,
        audience: &str,
    ) -> Result<ValidatedSession, AuthenticationError> {
        self.sessions
            .validate_access_token(credential, &[audience])
            .map_err(|_error| AuthenticationError::Unauthenticated)
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

fn local_owner() -> AuthenticatedPrincipal {
    AuthenticatedPrincipal::user(local_organization_owner())
}

/// What fixes the Workspace a request acts in.
#[derive(Clone, Copy, Debug)]
pub enum WorkspaceFixedBy<'a> {
    /// The request URL names the Workspace. A User token may be unscoped or
    /// scoped to it; an Agent Registration must belong to it.
    Url(&'a WorkspaceId),
    /// Nothing in the request names one, so the credential must: a User
    /// token's signed `workspace_id`, or an Agent Registration's Workspace.
    Credential,
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
