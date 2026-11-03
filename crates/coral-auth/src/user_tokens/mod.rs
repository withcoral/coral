//! User access tokens issued at Coral's token endpoint, and the Refresh Grants
//! that renew them without another interactive sign-in.
//!
//! # Interface
//!
//! - [`UserTokens::redeem_code`] spends an authorization code and issues an
//!   access token for the target the Adapter resolved. When the client
//!   declared the `refresh_token` grant, it also starts a Refresh Grant and
//!   returns its first refresh token.
//! - [`UserTokens::refresh`] exchanges a current refresh token for a new
//!   access token and its single-use successor.
//! - [`UserTokens::revoke`] ends the Refresh Grant a current or consumed
//!   refresh token belongs to.
//! - [`UserTokens::start_web_session`] takes a Reef Refresh Grant from its
//!   client and holds it behind an opaque session handle;
//!   [`UserTokens::web_session_token`] mints that session's Reef access
//!   tokens; [`UserTokens::end_web_session`] ends the session and its grant.
//! - [`retire_for_access_removal_in_transaction`] ends the Refresh Grants an
//!   access removal takes away, in the removal's transaction.
//!
//! # Invariants
//!
//! A Refresh Grant fixes the User, client, canonical resource, private
//! audience, and Workspace of its sign-in; a refresh can never change them.
//! Its absolute expiry is fixed at the sign-in, and rotation never extends it.
//! Each refresh token is spent at most once: presenting a consumed token, or
//! two refreshes racing on one token, ends the whole grant, including the
//! newest token. A presentation with the wrong client, resource, or audience
//! changes nothing. Refresh rechecks current enabled Organization Membership
//! and, for a Workspace-scoped grant, current Workspace inspection authority,
//! holding the rows that removal and disablement change. Losing either
//! refuses the refresh; losing a Membership deletes the grants it supported,
//! so rejoining revives none. Revocation ends only refresh: access tokens
//! already issued keep their own expiry and live authority checks.
//!
//! Only the SHA-256 digest of a refresh token is stored. Nothing is returned
//! before its writes commit, and a storage failure is reported as
//! [`TokenRefusal::Unavailable`], never as a rejected credential.

mod repository;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod web_session_tests;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coral_db::{Database, Transaction};
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, OrganizationModule, PreparedAccessRemoval,
    PrincipalKind, UserId, WorkspaceId, WorkspaceModule,
};
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

use crate::authorization_codes::{AuthorizationCodes, AuthorizationGrant, CodeRedemption};
use crate::{IssuedAccessToken, PrivateAudience, SessionTokenIssuer};

/// How long a Refresh Grant can refresh when the operator sets no lifetime.
pub const DEFAULT_REFRESH_GRANT_LIFETIME: Duration = Duration::from_hours(24 * 30);

/// The most Refresh Grants one User holds through one client. Starting
/// another ends that client's oldest, never another client's.
const MAX_GRANTS_PER_CLIENT: u64 = 64;

/// How long a Web Session's access token lasts at most. Each serves one
/// request, so few remain valid once the session ends.
const WEB_SESSION_TOKEN_LIFETIME: Duration = Duration::from_mins(5);

/// The most times one Refresh Grant rotates. A client refreshing more often
/// than this over the grant's lifetime has to sign in again.
const MAX_ROTATIONS: i64 = 10_000;

/// The most expired grants one cleanup pass deletes, so no sign-in pays for
/// a backlog.
const PURGE_BATCH: u64 = 128;

const REFRESH_TOKEN_PREFIX: &str = "coral_rt_";
const WEB_SESSION_PREFIX: &str = "coral_ws_";

/// What the access tokens from a sign-in are for, as the Adapter resolved it
/// from the token request's resource and audience.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessTarget {
    /// The approved resource itself, with no Workspace scope.
    Resource,
    /// One Workspace's public MCP URL. Its tokens name no Workspace, since
    /// the URL fixes it, but the grant answers to that Workspace's authority.
    WorkspaceResource(WorkspaceId),
    /// The approved resource, scoped to the Workspace picked at sign-in.
    PickedWorkspace,
    /// The private MCP surface of one Workspace.
    PrivateMcp(WorkspaceId),
    /// Reef, with no Workspace scope.
    Reef,
}

/// The bindings a refresh presents beside its refresh token.
#[derive(Clone, Copy, Debug)]
pub struct RefreshRequest<'a> {
    /// The client the grant was issued to.
    pub client_id: &'a str,
    /// The canonical resource, when the request names one.
    pub resource: Option<&'a str>,
    /// The private audience, when the request names one.
    pub audience: Option<PrivateAudience>,
}

/// One refresh token, shown only to the client it was issued to.
pub struct RefreshToken(Zeroizing<String>);

/// A fresh high-entropy secret named by `prefix`, and the digest stored for it.
fn generate_secret(prefix: &str) -> Result<(Zeroizing<String>, String), TokenRefusal> {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    SystemRandom::new()
        .fill(bytes.as_mut())
        .map_err(|_error| TokenRefusal::Issuance)?;
    let secret = Zeroizing::new(format!(
        "{prefix}{}",
        URL_SAFE_NO_PAD.encode(bytes.as_ref())
    ));
    let digest = digest(&secret).ok_or(TokenRefusal::Issuance)?;
    Ok((secret, digest))
}

impl RefreshToken {
    fn generate() -> Result<(Self, String), TokenRefusal> {
        let (token, digest) = generate_secret(REFRESH_TOKEN_PREFIX)?;
        Ok((Self(token), digest))
    }

    /// The token, for the token response alone.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RefreshToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RefreshToken(<redacted>)")
    }
}

/// One Web Session handle, shown only to the client that started it. It is
/// a bearer secret: whoever holds it gets that session's access tokens.
pub struct WebSessionHandle(Zeroizing<String>);

impl WebSessionHandle {
    /// The handle, for the client's own protected session storage alone.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for WebSessionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WebSessionHandle(<redacted>)")
    }
}

/// A Web Session that just started.
#[derive(Debug)]
pub struct StartedWebSession {
    /// The session's handle.
    pub handle: WebSessionHandle,
    /// When the session ends, as Unix nanoseconds; never extended.
    pub expires_at_unix_nanos: i64,
}

/// An access token a Web Session minted.
pub struct WebSessionToken {
    /// The new access token. It never outlives its session.
    pub access_token: IssuedAccessToken,
    /// When the session ends, as Unix nanoseconds.
    pub session_expires_at_unix_nanos: i64,
}

/// What a successful token request returns.
pub struct IssuedTokens {
    /// The new access token.
    pub access_token: IssuedAccessToken,
    /// The next refresh token, when the grant can refresh.
    pub refresh_token: Option<RefreshToken>,
}

/// Why a token request issued nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TokenRefusal {
    /// The code or refresh token is unknown, spent, expired, revoked, bound
    /// to another client or target, or its User lost the authority it needs.
    #[error("the grant is invalid")]
    InvalidGrant,
    /// The database did not accept or answer a statement.
    #[error("token storage is unavailable")]
    Unavailable,
    /// The access or refresh token could not be generated.
    #[error("token issuance failed")]
    Issuance,
}

impl TokenRefusal {
    /// The refusal's stable kind, for an event's `error.type`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidGrant => "invalid_grant",
            Self::Unavailable => "unavailable",
            Self::Issuance => "issuance",
        }
    }
}

/// Issues User access tokens and keeps the Refresh Grants that renew them.
#[derive(Clone)]
pub struct UserTokens {
    database: Arc<Database>,
    codes: AuthorizationCodes,
    issuer: SessionTokenIssuer,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    refresh_grant_lifetime: Duration,
}

impl UserTokens {
    /// Issues tokens signed by `issuer` over the deployment `database`.
    #[must_use]
    pub fn new(database: Arc<Database>, issuer: SessionTokenIssuer) -> Self {
        Self {
            codes: AuthorizationCodes::new(database.clone()),
            organization: OrganizationModule::new(database.clone()),
            workspaces: WorkspaceModule::new(database.clone()),
            database,
            issuer,
            refresh_grant_lifetime: DEFAULT_REFRESH_GRANT_LIFETIME,
        }
    }

    /// Sets how long a Refresh Grant can refresh after its sign-in.
    #[must_use]
    pub const fn with_refresh_grant_lifetime(mut self, lifetime: Duration) -> Self {
        self.refresh_grant_lifetime = lifetime;
        self
    }

    /// Spends `code` when `redemption` repeats its bindings, and issues an
    /// access token for `target` at host time `now_unix_nanos`.
    ///
    /// A code whose client declared the `refresh_token` grant also starts a
    /// Refresh Grant when the User holds the authority a refresh would need;
    /// otherwise it issues the access token alone, as for any other client.
    ///
    /// # Errors
    ///
    /// [`TokenRefusal::InvalidGrant`] for a code that cannot be spent or a
    /// target the code does not support.
    pub async fn redeem_code(
        &self,
        code: &str,
        redemption: CodeRedemption<'_>,
        target: AccessTarget,
        now_unix_nanos: i64,
    ) -> Result<IssuedTokens, TokenRefusal> {
        let grant = self
            .codes
            .redeem(code, redemption, now_unix_nanos)
            .await
            .map_err(|_error| TokenRefusal::Unavailable)?
            .ok_or(TokenRefusal::InvalidGrant)?;
        let scope = Scope::for_code(target, &grant)?;
        if !grant.refresh_eligible {
            return Ok(IssuedTokens {
                access_token: self.mint(&scope)?,
                refresh_token: None,
            });
        }
        let expires_at = i64::try_from(self.refresh_grant_lifetime.as_nanos())
            .ok()
            .and_then(|lifetime| now_unix_nanos.checked_add(lifetime))
            .ok_or(TokenRefusal::Issuance)?;
        let mut tx = self.database.begin().await.map_err(unavailable)?;
        match self.hold_authority(&mut tx, &scope, now_unix_nanos).await {
            Ok(()) => {}
            Err(TokenRefusal::InvalidGrant) => {
                rollback(tx).await;
                return Ok(IssuedTokens {
                    access_token: self.mint(&scope)?,
                    refresh_token: None,
                });
            }
            Err(refusal) => {
                rollback(tx).await;
                return Err(refusal);
            }
        }
        repository::evict_oldest(
            &mut tx,
            &scope.user_id,
            &scope.client_id,
            MAX_GRANTS_PER_CLIENT - 1,
        )
        .await?;
        let grant_id = uuid::Uuid::new_v4().to_string();
        repository::insert_grant(&mut tx, &grant_id, &scope, now_unix_nanos, expires_at).await?;
        let (refresh_token, digest) = RefreshToken::generate()?;
        repository::insert_token(&mut tx, &digest, &grant_id).await?;
        let access_token = self.mint(&scope)?;
        tx.commit().await.map_err(unavailable)?;
        self.purge_expired(now_unix_nanos).await;
        Ok(IssuedTokens {
            access_token,
            refresh_token: Some(refresh_token),
        })
    }

    /// Deletes a bounded batch of grants that can no longer refresh, in a
    /// transaction of its own. A failed pass is retried by the next sign-in.
    async fn purge_expired(&self, now_unix_nanos: i64) {
        let Ok(mut tx) = self.database.begin().await else {
            return;
        };
        if repository::purge_expired(&mut tx, now_unix_nanos, PURGE_BATCH)
            .await
            .is_ok()
        {
            let _result = tx.commit().await;
        }
    }

    /// Exchanges `refresh_token` for a new access token and its successor at
    /// host time `now_unix_nanos`.
    ///
    /// # Errors
    ///
    /// [`TokenRefusal::InvalidGrant`] for an unknown, expired, or revoked
    /// token, one `request` does not match, or a User who lost the authority
    /// the grant needs. A consumed token ends its grant before the refusal.
    pub async fn refresh(
        &self,
        refresh_token: &str,
        request: RefreshRequest<'_>,
        now_unix_nanos: i64,
    ) -> Result<IssuedTokens, TokenRefusal> {
        let (digest, presented) = self
            .current_refresh_token(
                refresh_token,
                |scope| scope.accepts(request),
                now_unix_nanos,
            )
            .await?;
        let (scope, grant_id) = (&presented.scope, presented.grant_id.as_str());
        let mut tx = self.spend(&digest, &presented, now_unix_nanos).await?;
        if !repository::record_rotation(&mut tx, grant_id, MAX_ROTATIONS).await? {
            repository::delete_grant(&mut tx, grant_id).await?;
            tx.commit().await.map_err(unavailable)?;
            return Err(TokenRefusal::InvalidGrant);
        }
        let (successor, successor_digest) = RefreshToken::generate()?;
        repository::insert_token(&mut tx, &successor_digest, grant_id).await?;
        let access_token = match self.mint(scope) {
            Ok(token) => token,
            Err(refusal) => {
                rollback(tx).await;
                return Err(refusal);
            }
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(IssuedTokens {
            access_token,
            refresh_token: Some(successor),
        })
    }

    /// Ends the Refresh Grant `refresh_token` belongs to, current or
    /// consumed, when `client_id` is the client it was issued to.
    ///
    /// An unknown token, or one issued to another client, changes nothing
    /// and is not reported, so revocation discloses no token's existence.
    ///
    /// # Errors
    ///
    /// [`TokenRefusal::Unavailable`] when the database does not answer.
    pub async fn revoke(&self, refresh_token: &str, client_id: &str) -> Result<(), TokenRefusal> {
        let Some(digest) = digest(refresh_token) else {
            return Ok(());
        };
        match repository::find(&self.database, &digest).await? {
            Some(presented) if presented.scope.client_id == client_id => {
                self.end_grant(&presented.grant_id).await
            }
            _ => Ok(()),
        }
    }

    /// Takes the Reef Refresh Grant `refresh_token` belongs to from
    /// `client_id` and holds it in a new Web Session that lasts `max_age`,
    /// never past the grant's own expiry.
    ///
    /// The refresh token is spent: presenting it again is a replay that ends
    /// the grant and its session. The client then keeps only the handle.
    ///
    /// # Errors
    ///
    /// [`TokenRefusal::InvalidGrant`] for a token that could not refresh, one
    /// issued to another client, or one whose grant is not for Reef. Only a
    /// Reef grant may back a Web Session, so an MCP grant never reaches
    /// management access.
    pub async fn start_web_session(
        &self,
        refresh_token: &str,
        client_id: &str,
        max_age: Duration,
        now_unix_nanos: i64,
    ) -> Result<StartedWebSession, TokenRefusal> {
        let reef_grant = |scope: &Scope| {
            scope.client_id == client_id && scope.private_audience == Some(PrivateAudience::Reef)
        };
        let (digest, presented) = self
            .current_refresh_token(refresh_token, reef_grant, now_unix_nanos)
            .await?;
        let grant_id = presented.grant_id.as_str();
        let expires_at = i64::try_from(max_age.as_nanos())
            .ok()
            .and_then(|max_age| now_unix_nanos.checked_add(max_age))
            .unwrap_or(i64::MAX)
            .min(presented.expires_at_unix_nanos);
        if expires_at <= now_unix_nanos {
            return Err(TokenRefusal::InvalidGrant);
        }
        let mut tx = self.spend(&digest, &presented, now_unix_nanos).await?;
        let (handle, session_digest) = generate_secret(WEB_SESSION_PREFIX)?;
        repository::insert_session(&mut tx, &session_digest, grant_id, expires_at).await?;
        tx.commit().await.map_err(unavailable)?;
        self.purge_expired_sessions(now_unix_nanos).await;
        Ok(StartedWebSession {
            handle: WebSessionHandle(handle),
            expires_at_unix_nanos: expires_at,
        })
    }

    /// Mints a Reef access token for the Web Session `handle` names, under
    /// the User's current authority, at host time `now_unix_nanos`.
    ///
    /// Minting writes nothing, so any number of requests and processes can
    /// resolve one session at once. Each token serves one request, so it
    /// lasts [`WEB_SESSION_TOKEN_LIFETIME`] at most, and never past its
    /// session.
    ///
    /// # Errors
    ///
    /// [`TokenRefusal::InvalidGrant`] for an unknown, ended, or expired
    /// session, another client's session, or a User who is no longer an
    /// enabled Organization Member.
    pub async fn web_session_token(
        &self,
        handle: &str,
        client_id: &str,
        now_unix_nanos: i64,
    ) -> Result<WebSessionToken, TokenRefusal> {
        let digest = digest(handle).ok_or(TokenRefusal::InvalidGrant)?;
        let session = repository::find_session(&self.database, &digest)
            .await?
            .ok_or(TokenRefusal::InvalidGrant)?;
        let scope = &session.scope;
        if scope.client_id != client_id || session.expires_at_unix_nanos <= now_unix_nanos {
            return Err(TokenRefusal::InvalidGrant);
        }
        self.organization
            .resolve_user_authority(&scope.user_id)
            .await
            .map_err(|error| refusal(&error))?;
        // Tokens carry whole seconds. A session in its last second can carry
        // no token that ends with it, so it is already over.
        let session_end = u64::try_from(session.expires_at_unix_nanos / 1_000_000_000)
            .map_err(|_error| TokenRefusal::Issuance)?;
        if i64::try_from(session_end).unwrap_or(i64::MAX) <= now_unix_nanos / 1_000_000_000 {
            return Err(TokenRefusal::InvalidGrant);
        }
        let audience = scope
            .private_audience
            .ok_or(TokenRefusal::Unavailable)?
            .as_str();
        let access_token = self
            .issuer
            .issue_bounded_access_token(
                scope.user_id.as_str(),
                &scope.client_id,
                audience,
                WEB_SESSION_TOKEN_LIFETIME,
                session_end,
            )
            .map_err(|_text| TokenRefusal::Issuance)?;
        Ok(WebSessionToken {
            access_token,
            session_expires_at_unix_nanos: session.expires_at_unix_nanos,
        })
    }

    /// Ends the Web Session `handle` names, with its grant, when `client_id`
    /// started it. An unknown or ended session, or another client's, changes
    /// nothing and is not reported.
    ///
    /// # Errors
    ///
    /// [`TokenRefusal::Unavailable`] when the database does not answer.
    pub async fn end_web_session(&self, handle: &str, client_id: &str) -> Result<(), TokenRefusal> {
        let Some(digest) = digest(handle) else {
            return Ok(());
        };
        match repository::find_session(&self.database, &digest).await? {
            Some(session) if session.scope.client_id == client_id => {
                self.end_grant(&session.grant_id).await
            }
            _ => Ok(()),
        }
    }

    /// Deletes a bounded batch of grants whose Web Session expired.
    async fn purge_expired_sessions(&self, now_unix_nanos: i64) {
        let Ok(mut tx) = self.database.begin().await else {
            return;
        };
        if repository::purge_expired_sessions(&mut tx, now_unix_nanos, PURGE_BATCH)
            .await
            .is_ok()
        {
            let _result = tx.commit().await;
        }
    }

    /// Finds the unexpired grant a presented refresh token belongs to, when
    /// `binds` accepts its scope. A token that does not bind changes nothing;
    /// a consumed one that does is a replay, and its grant ends before the
    /// refusal. Returns the token's digest and what was found.
    ///
    /// Anyone can present a token, and on SQLite every writer queues behind
    /// `BEGIN`. Only a replay that must end its grant opens a write here.
    async fn current_refresh_token(
        &self,
        refresh_token: &str,
        binds: impl FnOnce(&Scope) -> bool,
        now_unix_nanos: i64,
    ) -> Result<(String, repository::PresentedToken), TokenRefusal> {
        let digest = digest(refresh_token).ok_or(TokenRefusal::InvalidGrant)?;
        let presented = repository::find(&self.database, &digest)
            .await?
            .ok_or(TokenRefusal::InvalidGrant)?;
        if !binds(&presented.scope) || presented.expires_at_unix_nanos <= now_unix_nanos {
            return Err(TokenRefusal::InvalidGrant);
        }
        if presented.consumed {
            self.end_grant(&presented.grant_id).await?;
            return Err(TokenRefusal::InvalidGrant);
        }
        Ok((digest, presented))
    }

    /// Spends a current refresh token under its grant's current authority,
    /// and returns the write transaction for the caller to finish.
    ///
    /// The grant row is held after the authority rows, in the order access
    /// removal takes them, so a revocation or removal racing the spend either
    /// ends the grant first or ends what the spend goes on to write. A spend
    /// that loses the race for its token is a replay: the grant ends, and that
    /// commits before the refusal.
    async fn spend(
        &self,
        digest: &str,
        presented: &repository::PresentedToken,
        now_unix_nanos: i64,
    ) -> Result<Transaction<'_>, TokenRefusal> {
        let grant_id = presented.grant_id.as_str();
        let mut tx = self.database.begin().await.map_err(unavailable)?;
        if let Err(refusal) = self
            .hold_authority(&mut tx, &presented.scope, now_unix_nanos)
            .await
        {
            rollback(tx).await;
            return Err(refusal);
        }
        if !repository::hold_live_grant(&mut tx, grant_id, now_unix_nanos).await? {
            rollback(tx).await;
            return Err(TokenRefusal::InvalidGrant);
        }
        if !repository::consume(&mut tx, digest, now_unix_nanos).await? {
            repository::delete_grant(&mut tx, grant_id).await?;
            tx.commit().await.map_err(unavailable)?;
            return Err(TokenRefusal::InvalidGrant);
        }
        Ok(tx)
    }

    async fn end_grant(&self, grant_id: &str) -> Result<(), TokenRefusal> {
        let mut tx = self.database.begin().await.map_err(unavailable)?;
        repository::delete_grant(&mut tx, grant_id).await?;
        tx.commit().await.map_err(unavailable)
    }

    /// Holds the authority a grant for `scope` needs until the commit.
    async fn hold_authority(
        &self,
        tx: &mut Transaction<'_>,
        scope: &Scope,
        now_unix_nanos: i64,
    ) -> Result<(), TokenRefusal> {
        let user = self
            .organization
            .hold_member_in_transaction(tx, &scope.user_id)
            .await
            .map_err(|error| refusal(&error))?;
        if let Some(workspace) = scope.workspace.authority() {
            self.workspaces
                .check_principal_inspection_in_transaction(
                    tx,
                    &AuthenticatedPrincipal::user(user),
                    workspace,
                    now_unix_nanos,
                )
                .await
                .map_err(|error| refusal(&error))?;
        }
        Ok(())
    }

    fn mint(&self, scope: &Scope) -> Result<IssuedAccessToken, TokenRefusal> {
        let audience = match scope.private_audience {
            Some(audience) => audience.as_str(),
            None => scope.resource.as_str(),
        };
        let user_id = scope.user_id.as_str();
        match scope.workspace.in_token() {
            Some(workspace) => self.issuer.issue_workspace_access_token(
                user_id,
                &scope.client_id,
                audience,
                workspace,
            ),
            None => self
                .issuer
                .issue_access_token(user_id, &scope.client_id, audience),
        }
        .map_err(|_text| TokenRefusal::Issuance)
    }
}

/// Ends the Refresh Grants that an access removal takes away, in its
/// transaction: a departing Organization Member's every grant, and a User's
/// grants scoped to each Workspace where their access ends.
///
/// # Errors
///
/// Rejects evidence from another transaction; any failure requires rollback.
pub async fn retire_for_access_removal_in_transaction(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    prepared: &PreparedAccessRemoval,
) -> Result<(), crate::Error> {
    prepared.verify_transaction(tx, context)?;
    if let Some(user) = prepared.departing_organization_member() {
        repository::delete_user_grants(tx, user).await?;
        return Ok(());
    }
    for lost in prepared.lost_access() {
        if lost.principal().kind() == PrincipalKind::User {
            let user = UserId::parse(lost.principal().id().as_str())?;
            repository::delete_workspace_grants(tx, lost.workspace_id(), &user).await?;
        }
    }
    Ok(())
}

/// Everything a grant fixes about the access tokens it issues.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Scope {
    user_id: UserId,
    client_id: String,
    resource: String,
    private_audience: Option<PrivateAudience>,
    workspace: WorkspaceBinding,
}

/// The Workspace a grant answers to, if any, and whether its tokens name it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum WorkspaceBinding {
    /// No Workspace: Reef, or a registered resource.
    None,
    /// A Workspace's public MCP URL fixes the Workspace, so tokens name none.
    ByUrl(WorkspaceId),
    /// Each token names the Workspace.
    InToken(WorkspaceId),
}

impl WorkspaceBinding {
    /// The Workspace whose authority the grant answers to.
    const fn authority(&self) -> Option<&WorkspaceId> {
        match self {
            Self::None => None,
            Self::ByUrl(workspace) | Self::InToken(workspace) => Some(workspace),
        }
    }

    /// The Workspace each access token names.
    const fn in_token(&self) -> Option<&WorkspaceId> {
        match self {
            Self::InToken(workspace) => Some(workspace),
            Self::None | Self::ByUrl(_) => None,
        }
    }
}

impl Scope {
    fn for_code(target: AccessTarget, grant: &AuthorizationGrant) -> Result<Self, TokenRefusal> {
        let (private_audience, workspace) = match target {
            AccessTarget::Resource => (None, WorkspaceBinding::None),
            AccessTarget::WorkspaceResource(workspace) => {
                (None, WorkspaceBinding::ByUrl(workspace))
            }
            // Only the Workspace picker issues a code for the Organization
            // MCP Endpoint, so a code naming no Workspace is not one.
            AccessTarget::PickedWorkspace => (
                None,
                WorkspaceBinding::InToken(
                    grant
                        .workspace_id
                        .clone()
                        .ok_or(TokenRefusal::InvalidGrant)?,
                ),
            ),
            AccessTarget::PrivateMcp(workspace) => (
                Some(PrivateAudience::Mcp),
                WorkspaceBinding::InToken(workspace),
            ),
            AccessTarget::Reef => (Some(PrivateAudience::Reef), WorkspaceBinding::None),
        };
        Ok(Self {
            user_id: UserId::parse(&grant.user_id).map_err(|_error| TokenRefusal::InvalidGrant)?,
            client_id: grant.client_id.clone(),
            resource: grant.resource.clone(),
            private_audience,
            workspace,
        })
    }

    /// Whether a refresh presenting `request` is bound to this grant. An
    /// omitted resource or audience keeps the grant's own; a supplied one
    /// must equal it, so a refresh can never retarget the grant.
    fn accepts(&self, request: RefreshRequest<'_>) -> bool {
        self.client_id == request.client_id
            && request
                .resource
                .is_none_or(|resource| resource == self.resource)
            && request
                .audience
                .is_none_or(|audience| self.private_audience == Some(audience))
    }
}

fn digest(secret: &str) -> Option<String> {
    (!secret.is_empty()).then(|| URL_SAFE_NO_PAD.encode(Sha256::digest(secret.as_bytes())))
}

/// An authority decision becomes a refusal unless storage failed to answer.
fn refusal(error: &coral_workspace::Error) -> TokenRefusal {
    match error {
        coral_workspace::Error::Database(_)
        | coral_workspace::Error::CorruptData(_)
        | coral_workspace::Error::EventFields(_) => TokenRefusal::Unavailable,
        _ => TokenRefusal::InvalidGrant,
    }
}

fn unavailable(_error: coral_db::Error) -> TokenRefusal {
    TokenRefusal::Unavailable
}

/// Rolls back a transaction that issued nothing. A rollback that fails ends
/// the transaction when its connection closes, so the refusal stands.
async fn rollback(tx: Transaction<'_>) {
    let _result = tx.rollback().await;
}
