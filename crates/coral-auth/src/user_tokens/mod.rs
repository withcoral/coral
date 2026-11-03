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

/// The most Refresh Grants one User holds. Starting another ends the oldest.
const MAX_GRANTS_PER_USER: u64 = 64;

/// The most times one Refresh Grant rotates. A client refreshing more often
/// than this over the grant's lifetime has to sign in again.
const MAX_ROTATIONS: i64 = 10_000;

/// The most expired grants one cleanup pass deletes, so no sign-in pays for
/// a backlog.
const PURGE_BATCH: u64 = 128;

const REFRESH_TOKEN_PREFIX: &str = "coral_rt_";

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

impl RefreshToken {
    fn generate() -> Result<(Self, String), TokenRefusal> {
        let mut bytes = Zeroizing::new([0_u8; 32]);
        SystemRandom::new()
            .fill(bytes.as_mut())
            .map_err(|_error| TokenRefusal::Issuance)?;
        let token = Zeroizing::new(format!(
            "{REFRESH_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(bytes.as_ref())
        ));
        let digest = digest(&token).ok_or(TokenRefusal::Issuance)?;
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
        repository::evict_oldest(&mut tx, &scope.user_id, MAX_GRANTS_PER_USER - 1).await?;
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
        let digest = digest(refresh_token).ok_or(TokenRefusal::InvalidGrant)?;
        // Anyone can present a token, and on SQLite every writer queues behind
        // `BEGIN`. Only a token that can refresh, or a replay that must end
        // its grant, opens a write transaction.
        let presented = repository::find(&self.database, &digest)
            .await?
            .ok_or(TokenRefusal::InvalidGrant)?;
        let scope = &presented.scope;
        if !scope.accepts(request) || presented.expires_at_unix_nanos <= now_unix_nanos {
            return Err(TokenRefusal::InvalidGrant);
        }
        let grant_id = presented.grant_id.as_str();
        if presented.consumed {
            self.end_grant(grant_id).await?;
            return Err(TokenRefusal::InvalidGrant);
        }
        let mut tx = self.database.begin().await.map_err(unavailable)?;
        if let Err(refusal) = self.hold_authority(&mut tx, scope, now_unix_nanos).await {
            rollback(tx).await;
            return Err(refusal);
        }
        // The grant row is held after the authority rows, in the order access
        // removal takes them, so a revocation or removal racing this refresh
        // either ends the grant first or ends the successor too.
        if !repository::hold_live_grant(&mut tx, grant_id, now_unix_nanos).await? {
            rollback(tx).await;
            return Err(TokenRefusal::InvalidGrant);
        }
        // A refresh that loses the race for one token is a replay of it.
        if !repository::consume(&mut tx, &digest, now_unix_nanos).await?
            || !repository::record_rotation(&mut tx, grant_id, MAX_ROTATIONS).await?
        {
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
        if let Some(workspace) = &scope.workspace_id {
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
        match scope.workspace_id.as_ref().filter(|_| scope.scoped_tokens) {
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
    /// The Workspace whose authority the grant answers to.
    workspace_id: Option<WorkspaceId>,
    /// Whether each access token names that Workspace.
    scoped_tokens: bool,
}

impl Scope {
    fn for_code(target: AccessTarget, grant: &AuthorizationGrant) -> Result<Self, TokenRefusal> {
        let scoped = |workspace| (Some(workspace), true);
        let (private_audience, (workspace_id, scoped_tokens)) = match target {
            AccessTarget::Resource => (None, (None, false)),
            AccessTarget::WorkspaceResource(workspace) => (None, (Some(workspace), false)),
            // Only the Workspace picker issues a code for the Organization
            // MCP Endpoint, so a code naming no Workspace is not one.
            AccessTarget::PickedWorkspace => (
                None,
                scoped(
                    grant
                        .workspace_id
                        .clone()
                        .ok_or(TokenRefusal::InvalidGrant)?,
                ),
            ),
            AccessTarget::PrivateMcp(workspace) => (Some(PrivateAudience::Mcp), scoped(workspace)),
            AccessTarget::Reef => (Some(PrivateAudience::Reef), (None, false)),
        };
        Ok(Self {
            user_id: UserId::parse(&grant.user_id).map_err(|_error| TokenRefusal::InvalidGrant)?,
            client_id: grant.client_id.clone(),
            resource: grant.resource.clone(),
            private_audience,
            workspace_id,
            scoped_tokens,
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
