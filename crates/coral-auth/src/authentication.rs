use std::sync::Arc;

use coral_db::{Clock, UnixNanos};
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
///
/// Each call reads the clock it was built with once. That instant dates every
/// expiry the call checks, and the call returns it beside the Principal, so
/// that the Adapter dates what the request decides next by the same reading.
#[derive(Clone)]
pub struct PrincipalAuthentication {
    mode: Mode,
    workspaces: WorkspaceModule,
    clock: Arc<dyn Clock>,
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
    ///
    /// `clock` is the deployment's one clock: the one that dates the tokens
    /// `issuer` issues, or a token looks expired or not valid yet.
    #[must_use]
    pub fn new(
        issuer: &SessionTokenIssuer,
        organization: OrganizationModule,
        workspaces: WorkspaceModule,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            mode: Mode::SignedIn(SignIn {
                sessions: issuer.verifier(),
                organization,
            }),
            workspaces,
            clock,
        }
    }

    /// Builds authentication for an auth-disabled deployment, where no User signs in.
    ///
    /// Composition must bind every listener that serves it to loopback only.
    /// `clock` is the deployment's one clock.
    #[must_use]
    pub const fn local(workspaces: WorkspaceModule, clock: Arc<dyn Clock>) -> Self {
        Self {
            mode: Mode::Local,
            workspaces,
            clock,
        }
    }

    /// Whether a User signs in: `false` for [`Self::local`].
    #[must_use]
    pub const fn signs_in(&self) -> bool {
        matches!(self.mode, Mode::SignedIn(_))
    }

    /// Authenticates one request without provisioning a User or granting
    /// resource access, and returns the Workspace it acts in and the instant
    /// the call read.
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
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId, UnixNanos), AuthenticationError> {
        self.authenticate_request(fixed_by, bearer, expected_audience, false)
            .await
    }

    /// Verifies a credential whose signed scope must fix this exact Workspace,
    /// and returns the instant the call read beside the Principal.
    /// Agent Registrations already carry that immutable scope.
    ///
    /// # Errors
    /// Rejects unscoped or differently scoped tokens as well as invalid credentials.
    pub async fn authenticate_scoped(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
        expected_audience: &str,
    ) -> Result<(AuthenticatedPrincipal, UnixNanos), AuthenticationError> {
        self.authenticate_request(
            WorkspaceFixedBy::Url(workspace),
            bearer,
            expected_audience,
            true,
        )
        .await
        .map(|(principal, _workspace, now)| (principal, now))
    }

    /// Authenticates an unscoped human management request (including Workspace creation).
    /// It never returns an Agent. In local mode only a missing credential, the
    /// implicit local Owner, is accepted. The instant the call read is
    /// returned beside the Principal.
    /// # Errors
    /// Rejects Agents, scoped MCP credentials, wrong audiences, and disabled Users.
    pub async fn authenticate_user(
        &self,
        bearer: Option<&str>,
        audience: &str,
    ) -> Result<(AuthenticatedPrincipal, UnixNanos), AuthenticationError> {
        let now = self.clock.now();
        let Mode::SignedIn(sign_in) = &self.mode else {
            return match bearer {
                None => Ok((local_owner(), now)),
                Some(_) => Err(AuthenticationError::Unauthenticated),
            };
        };
        let session = sign_in.session(
            bearer.ok_or(AuthenticationError::Unauthenticated)?,
            audience,
            now,
        )?;
        if session.workspace_id.is_some() {
            return Err(AuthenticationError::Unauthenticated);
        }
        Ok((sign_in.user(&session).await?, now))
    }

    async fn authenticate_request(
        &self,
        fixed_by: WorkspaceFixedBy<'_>,
        bearer: Option<&str>,
        expected_audience: &str,
        require_scope: bool,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId, UnixNanos), AuthenticationError> {
        let now = self.clock.now();
        let Some(credential) = bearer else {
            return match (&self.mode, fixed_by) {
                (Mode::SignedIn(_), _) => Err(AuthenticationError::Unauthenticated),
                (Mode::Local, WorkspaceFixedBy::Url(workspace)) => {
                    Ok((local_owner(), workspace.clone(), now))
                }
                (Mode::Local, WorkspaceFixedBy::Credential) => {
                    Err(AuthenticationError::WorkspaceUrlRequired)
                }
            };
        };
        // Without sign-in, every credential names an Agent Registration.
        let sign_in = match &self.mode {
            Mode::SignedIn(sign_in) if !credential.starts_with("coral_agent_") => sign_in,
            Mode::SignedIn(_) | Mode::Local => {
                let (agent, workspace) = self.agent(fixed_by, credential, now).await?;
                return Ok((agent, workspace, now));
            }
        };
        let session = sign_in.session(credential, expected_audience, now)?;
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
        Ok((sign_in.user(&session).await?, workspace, now))
    }

    /// The Agent whose Registration `credential` authenticates at `now`, in
    /// the Workspace it fixes.
    async fn agent(
        &self,
        fixed_by: WorkspaceFixedBy<'_>,
        credential: &str,
        now: UnixNanos,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        let now = now.get();
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
        now: UnixNanos,
    ) -> Result<ValidatedSession, AuthenticationError> {
        self.sessions
            .validate_access_token(credential, &[audience], now)
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

impl From<coral_workspace::Error> for AuthenticationError {
    fn from(error: coral_workspace::Error) -> Self {
        use coral_workspace::AuthorityRefusal as Refusal;
        match error.refusal() {
            Some(
                Refusal::InvalidAgentCredential
                | Refusal::AgentNotFound
                | Refusal::AgentDisabled
                | Refusal::AgentRegistrationExpired
                | Refusal::PermissionDenied,
            ) => Self::Unauthenticated,
            // Neither is an answer about the credential.
            Some(Refusal::WorkspaceNotFound | Refusal::WorkspaceArchived) | None => {
                Self::Unavailable
            }
        }
    }
}

#[cfg(test)]
mod tests;
