//! Coral-owned Organization Membership, role, and disablement.
//!
//! Login provisioning is host-only: the authentication Adapter
//! selects Member or the explicitly configured bootstrap Owner. Administration
//! requires current Owner authority and the version the caller read. Both use
//! the same record read by Workspace authorization and held by the Policy
//! Enforcement Point. Removal retains version continuity; login can rejoin
//! only removed Membership. No protocol work, remote synchronization, or destructive cleanup
//! lives here.

#[path = "organization_repository.rs"]
mod repository;

pub(crate) use repository::{current_member_ids, enabled_member_total};

use std::collections::HashSet;
use std::sync::Arc;

use coral_db::{Database, Transaction, UnixNanos, Version};

use crate::list_repository::Users;
use crate::{AuthenticatedUser, ChangeContext, Error, IfMatch, OrganizationRole, UserId};

pub(super) async fn hold_administration(tx: &mut Transaction<'_>) -> Result<(), Error> {
    repository::hold_administration(tx).await
}

/// Whether a User's Organization Membership is current and enabled. A removed
/// or unknown User is not enabled.
pub(super) async fn is_enabled(tx: &mut Transaction<'_>, user: &UserId) -> Result<bool, Error> {
    Ok(repository::find_membership(tx, user)
        .await?
        .is_some_and(|membership| membership.enabled))
}

/// The Users of one list for whom [`is_enabled`] holds, read in one statement.
pub(super) async fn enabled_among(
    tx: &mut Transaction<'_>,
    users: Users<'_>,
) -> Result<HashSet<UserId>, Error> {
    Ok(repository::find_listed_memberships(tx, users)
        .await?
        .into_iter()
        .filter(|membership| membership.enabled)
        .map(|membership| membership.user_id)
        .collect())
}

/// Current Coral-owned role and state of one Organization Membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationMembership {
    user_id: UserId,
    role: OrganizationRole,
    enabled: bool,
    version: Version,
}
impl OrganizationMembership {
    /// Stable User identity.
    #[must_use]
    pub const fn user_id(&self) -> &UserId {
        &self.user_id
    }
    /// Current role, retained even while disabled.
    #[must_use]
    pub const fn role(&self) -> OrganizationRole {
        self.role
    }
    /// Whether this User may authenticate and exercise authority.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
    /// The version an administrative update must have read: the count of
    /// accepted changes to this Membership.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }
    fn authenticated(&self) -> AuthenticatedUser {
        if self.enabled {
            AuthenticatedUser::organization_member(self.user_id.clone(), self.role)
        } else {
            AuthenticatedUser::without_organization_membership(self.user_id.clone())
        }
    }
}

/// Accepted Organization change, without before/after values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationLifecycleAction {
    /// First eligible login provisions Membership.
    Created,
    /// An Owner changes the Organization role.
    RoleUpdated,
    /// An Owner disables or restores a User.
    StateUpdated,
    /// Membership and its current private work were removed.
    Removed,
}
impl OrganizationLifecycleAction {
    /// Stable administrative action spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "organization_membership.created",
            Self::RoleUpdated => "organization_membership.role_updated",
            Self::StateUpdated => "organization_membership.state_updated",
            Self::Removed => "organization_membership.removed",
        }
    }
    /// Exact changed field paths, never their values.
    #[must_use]
    pub const fn changed_field_paths(self) -> &'static [&'static str] {
        match self {
            Self::Created | Self::Removed => &["role", "enabled"],
            Self::RoleUpdated => &["role"],
            Self::StateUpdated => &["enabled"],
        }
    }
}

/// Safe immutable evidence of one accepted Organization Membership change.
#[derive(Debug, Clone)]
pub struct OrganizationLifecycleEvent {
    /// Stable event identity.
    pub id: uuid::Uuid,
    /// Accountable User, never credential material.
    pub actor_user_id: UserId,
    /// Changed Membership's User.
    pub target_user_id: UserId,
    /// Action and changed field paths; contains no changed field values.
    pub action: OrganizationLifecycleAction,
    /// Only accepted changes produce evidence.
    pub outcome: crate::LifecycleOutcome,
    /// Event time, not a concurrency control.
    pub occurred_at: UnixNanos,
    /// Safe trace correlation.
    pub trace_id: crate::TraceId,
}

/// Organization authority owner, independent of authentication protocols.
#[derive(Clone)]
pub struct OrganizationModule {
    database: Arc<Database>,
}
impl OrganizationModule {
    /// Composes authority over the domain database.
    #[must_use]
    pub const fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Provisions initial or rejoined Membership without changing current Membership.
    ///
    /// This host-only seam is not a self-service role setter. The Adapter must
    /// select Member unless this exact verified identity is the configured
    /// bootstrap Owner. A repeated login cannot promote or re-enable a User,
    /// and a rejoin keeps the disablement recorded before the removal.
    /// # Errors
    /// Returns invalid input or persistence failure.
    pub async fn provision_login(
        &self,
        context: &ChangeContext,
    ) -> Result<OrganizationMembership, Error> {
        let mut tx = self.database.begin().await?;
        let membership = Self::provision_login_in_transaction(&mut tx, context).await?;
        tx.commit().await?;
        Ok(membership)
    }

    /// Joins login-directory provisioning so identity and Membership commit atomically.
    /// The caller supplies verified identity and the host-selected initial role.
    /// # Errors
    /// Returns invalid input or persistence failure; never commits independently.
    pub async fn provision_login_in_transaction(
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
    ) -> Result<OrganizationMembership, Error> {
        repository::hold_administration(tx).await?;
        let role = context
            .actor()
            .organization_role()
            .ok_or(Error::PermissionDenied(
                crate::Denial::OrganizationMembershipRequired,
            ))?;
        let user_id = context.actor().user_id();
        let mut created = repository::insert_membership_if_absent(tx, user_id, role).await?;
        if !created && let Some(removed) = repository::removed_version(tx, user_id).await? {
            repository::rejoin_membership(tx, user_id, role, next_version(removed)?.get()).await?;
            created = true;
        }
        if created {
            repository::record_event(
                tx,
                context,
                context.actor().user_id(),
                OrganizationLifecycleAction::Created,
            )
            .await?;
        }
        read_membership(tx, context.actor().user_id()).await
    }

    /// Resolves an already verified User identity to current enabled authority.
    /// This does not verify credentials; authentication must happen first.
    /// # Errors
    /// Missing or disabled Users fail closed.
    pub async fn resolve_user_authority(
        &self,
        user_id: &UserId,
    ) -> Result<AuthenticatedUser, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let user = enabled_user(&mut tx, user_id).await?;
        tx.commit().await?;
        Ok(user)
    }

    /// Checks current Organization Owner authority without granting resource visibility.
    /// # Errors
    /// Denies missing, disabled, and non-Owner Membership.
    pub async fn check_owner(&self, actor: &AuthenticatedUser) -> Result<(), Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        require_owner(&mut tx, actor).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Checks Owner inspection authority in the caller's read snapshot.
    /// # Errors
    /// Denies missing, disabled, and non-Owner Membership; never writes or commits.
    pub async fn check_owner_inspection_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
    ) -> Result<(), Error> {
        require_owner(tx, actor).await
    }

    /// Checks current enabled Organization Membership of any role in the
    /// caller's read snapshot and returns that role, so a domain Module can bound
    /// Member visibility without granting Owner authority.
    /// # Errors
    /// Denies missing and disabled Membership; never writes or commits.
    pub async fn check_inspection_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
    ) -> Result<OrganizationRole, Error> {
        current_user(tx, actor.user_id())
            .await?
            .organization_role()
            .ok_or(Error::PermissionDenied(
                crate::Denial::OrganizationMembershipRequired,
            ))
    }

    /// Holds current enabled Organization Membership of any role through the
    /// caller's commit, so removal and disablement wait for the decision.
    /// # Errors
    /// Denies missing and disabled Membership; never commits independently.
    pub async fn hold_member_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        user_id: &UserId,
    ) -> Result<AuthenticatedUser, Error> {
        let current = hold_user(tx, user_id).await?;
        if current.organization_role().is_none() {
            return Err(Error::PermissionDenied(
                crate::Denial::OrganizationMembershipRequired,
            ));
        }
        Ok(current)
    }

    /// Holds current Owner authority through the caller's administrative commit.
    /// # Errors
    /// Denies missing, disabled, and non-Owner Membership; never commits independently.
    pub async fn check_owner_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
    ) -> Result<(), Error> {
        let current = hold_user(tx, actor.user_id()).await?;
        require_owner_role(&current)
    }

    /// Changes the role of the Organization Membership that the caller read
    /// at `if_match`, under current Organization Owner authority.
    /// # Errors
    /// Denies a Membership that changed since with
    /// [`Error::OrganizationAuthorityConflict`], absent authority, and loss of
    /// the last enabled Owner.
    pub async fn set_role(
        &self,
        context: &ChangeContext,
        user_id: &UserId,
        if_match: &IfMatch,
        role: OrganizationRole,
    ) -> Result<OrganizationMembership, Error> {
        self.update(context, user_id, if_match, Some(role), None)
            .await
    }

    /// Disables or explicitly restores the User whose Organization Membership
    /// the caller read at `if_match`, without changing retained work.
    /// # Errors
    /// Denies a Membership that changed since with
    /// [`Error::OrganizationAuthorityConflict`], absent authority, and loss of
    /// the last enabled Owner.
    pub async fn set_enabled(
        &self,
        context: &ChangeContext,
        user_id: &UserId,
        if_match: &IfMatch,
        enabled: bool,
    ) -> Result<OrganizationMembership, Error> {
        self.update(context, user_id, if_match, None, Some(enabled))
            .await
    }

    async fn update(
        &self,
        context: &ChangeContext,
        user_id: &UserId,
        if_match: &IfMatch,
        role: Option<OrganizationRole>,
        enabled: Option<bool>,
    ) -> Result<OrganizationMembership, Error> {
        let mut tx = self.database.begin().await?;
        // All Organization administrators take this hold before User row holds.
        repository::hold_administration(&mut tx).await?;
        require_owner(&mut tx, context.actor()).await?;
        hold_target(&mut tx, context.actor(), user_id).await?;
        let mut membership = read_membership(&mut tx, user_id)
            .await
            .map_err(absent_target)?;
        if !if_match.admits(membership.version) {
            return Err(Error::OrganizationAuthorityConflict);
        }
        let next_role = role.unwrap_or(membership.role);
        let next_enabled = enabled.unwrap_or(membership.enabled);
        if membership.role == next_role && membership.enabled == next_enabled {
            return Ok(membership);
        }
        if membership.enabled
            && membership.role == OrganizationRole::Owner
            && (!next_enabled || next_role != OrganizationRole::Owner)
        {
            let count = repository::enabled_owner_count(&mut tx).await?;
            if count <= 1 {
                return Err(Error::LastOrganizationOwner);
            }
        }
        // The User is held, so the row is at the version just read.
        let next_version = next_version(membership.version)?;
        repository::update_membership(
            &mut tx,
            user_id,
            next_role,
            next_enabled,
            next_version.get(),
        )
        .await?;
        repository::record_event(
            &mut tx,
            context,
            user_id,
            if role.is_some() {
                OrganizationLifecycleAction::RoleUpdated
            } else {
                OrganizationLifecycleAction::StateUpdated
            },
        )
        .await?;
        tx.commit().await?;
        membership.role = next_role;
        membership.enabled = next_enabled;
        membership.version = next_version;
        Ok(membership)
    }

    /// Inspects the requesting User's current Membership and own safe profile.
    /// This does not read the Organization directory or grant management authority.
    /// # Errors
    /// Denies absent or disabled Membership and persistence failures.
    pub async fn inspect_current_membership(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<(OrganizationMembership, crate::UserProfile), Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let membership = read_membership(&mut tx, actor.user_id()).await?;
        if !membership.enabled() {
            return Err(Error::PermissionDenied(
                crate::Denial::OrganizationMembershipRequired,
            ));
        }
        let profile = crate::profile::read_profile(&mut tx, actor.user_id()).await?;
        tx.commit().await?;
        Ok((membership, profile))
    }

    /// Lists current Membership and safe profile evidence for removal review.
    /// Disabled Memberships remain visible; removed Memberships do not.
    /// # Errors
    /// Denies absent, disabled, or non-Owner authority before reading the directory.
    pub async fn list_memberships(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<(OrganizationMembership, crate::UserProfile)>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        require_owner(&mut tx, actor).await?;
        let memberships = repository::current_memberships(&mut tx).await?;
        let profiles =
            crate::profile::read_listed_profiles(&mut tx, Users::OrganizationMembers).await?;
        tx.commit().await?;
        Ok(memberships
            .into_iter()
            .map(|membership| {
                let profile = profiles.of(membership.user_id());
                (membership, profile)
            })
            .collect())
    }

    /// Lists safe Organization administration evidence for a current Owner.
    /// # Errors
    /// Denies absent/disabled/non-Owner authority or persistence failures.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<OrganizationLifecycleEvent>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        require_owner(&mut tx, actor).await?;
        let events = repository::list_lifecycle_events(&mut tx).await?;
        tx.commit().await?;
        Ok(events)
    }
}

/// The version an accepted change leaves an Organization Membership at.
fn next_version(version: Version) -> Result<Version, Error> {
    version
        .next()
        .ok_or_else(|| Error::CorruptData("Organization authority revision overflowed".into()))
}

async fn require_owner(tx: &mut Transaction<'_>, actor: &AuthenticatedUser) -> Result<(), Error> {
    require_owner_role(&current_user(tx, actor.user_id()).await?)
}

fn require_owner_role(actor: &AuthenticatedUser) -> Result<(), Error> {
    if actor.organization_role() != Some(OrganizationRole::Owner) {
        return Err(Error::PermissionDenied(
            crate::Denial::OrganizationOwnerRequired,
        ));
    }
    Ok(())
}

/// Read-only current authority in the caller's snapshot; no retained-role fallback.
pub(crate) async fn current_user(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<AuthenticatedUser, Error> {
    Ok(read_membership(tx, user_id).await?.authenticated())
}

/// [`current_user`], denied when the User's Organization Membership is disabled.
pub(crate) async fn enabled_user(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<AuthenticatedUser, Error> {
    let user = current_user(tx, user_id).await?;
    if user.organization_role().is_none() {
        return Err(Error::PermissionDenied(
            crate::Denial::OrganizationMembershipRequired,
        ));
    }
    Ok(user)
}

async fn read_membership(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<OrganizationMembership, Error> {
    repository::find_membership(tx, user_id)
        .await?
        .ok_or(Error::PermissionDenied(
            crate::Denial::NoOrganizationMembership,
        ))
}

/// Reads the current User holding the row changed by role/disablement
/// administration shared until commit, so that administration waits for the
/// decisions reading it.
pub(crate) async fn share_user(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<AuthenticatedUser, Error> {
    user_locked(tx, user_id, coral_db::RowLock::Shared).await
}

/// Reads the current User, holding its row as `lock` says.
pub(crate) async fn user_locked(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
    lock: coral_db::RowLock,
) -> Result<AuthenticatedUser, Error> {
    repository::find_membership_locked(tx, user_id, lock)
        .await?
        .ok_or(Error::PermissionDenied(
            crate::Denial::NoOrganizationMembership,
        ))
        .map(|membership| membership.authenticated())
}

/// Holds the same row changed by role/disablement administration until
/// commit, and reads the current User from the held row.
pub(crate) async fn hold_user(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<AuthenticatedUser, Error> {
    repository::hold_membership(tx, user_id)
        .await?
        .ok_or(Error::PermissionDenied(
            crate::Denial::NoOrganizationMembership,
        ))
        .map(|membership| membership.authenticated())
}

/// Holds a User that a request names, and says whether there was a row to
/// hold. The caller's own missing Membership is a denial, as in
/// [`hold_user`]. Another User with no row is not reported here: the caller
/// reports the absence after the held check of its own authority, so a
/// caller whose authority lapsed learns nothing about the User it named.
///
/// A target with no row is absent for the whole request. Nothing serializes
/// a later read of that User with a change to it, unless the caller holds
/// the Organization administration guard.
pub(crate) async fn hold_target(
    tx: &mut Transaction<'_>,
    actor: &AuthenticatedUser,
    user_id: &UserId,
) -> Result<bool, Error> {
    if actor.user_id() == user_id {
        hold_user(tx, user_id).await.map(|_held| true)
    } else {
        repository::hold_user_row(tx, user_id).await
    }
}

/// Holds the caller and the User it names, in one stable order so that two
/// requests that name each other cannot deadlock. Says whether the named
/// User had a row to hold, as [`hold_target`] does.
pub(crate) async fn hold_actor_and_target(
    tx: &mut Transaction<'_>,
    actor: &AuthenticatedUser,
    target: &UserId,
) -> Result<bool, Error> {
    let mut users = vec![actor.user_id(), target];
    users.sort();
    users.dedup();
    let mut target_held = true;
    for user in users {
        let held = hold_target(tx, actor, user).await?;
        if user == target {
            target_held = held;
        }
    }
    Ok(target_held)
}

/// Requires that the User a request names has an enabled Organization
/// Membership. `held` is what [`hold_actor_and_target`] returned: with no
/// row to hold, a User created since is not held either, so the target is
/// absent for this request.
pub(crate) async fn require_enabled_target(
    tx: &mut Transaction<'_>,
    held: bool,
    target: &UserId,
) -> Result<(), Error> {
    if !held {
        return Err(Error::OrganizationMembershipNotFound);
    }
    let user = current_user(tx, target).await.map_err(absent_target)?;
    if user.organization_role().is_none() {
        return Err(Error::MemberTargetNotEnabled);
    }
    Ok(())
}

/// A User that a request names and that has no Membership is absent. That
/// is no denial of the caller.
pub(crate) fn absent_target(error: Error) -> Error {
    match error {
        Error::PermissionDenied(crate::Denial::NoOrganizationMembership) => {
            Error::OrganizationMembershipNotFound
        }
        other => other,
    }
}

pub(crate) async fn prepare_removal(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    user: &UserId,
    if_match: &IfMatch,
    transfer_to: Option<&UserId>,
) -> Result<(), Error> {
    // The caller already holds the Organization administration guard. Check
    // authority before exposing target-dependent absence or version errors.
    require_owner(tx, context.actor()).await?;
    // The transfer target joins the ordered holds, so it cannot be disabled
    // between this check and the Owner Membership write.
    let mut users = vec![context.actor().user_id(), user];
    users.extend(transfer_to);
    users.sort();
    users.dedup();
    for held in users {
        // An unknown transfer target is reported by the caller's eligibility check.
        if Some(held) == transfer_to {
            repository::hold_user(tx, held).await?;
        } else {
            hold_target(tx, context.actor(), held).await?;
        }
    }
    require_owner(tx, context.actor()).await?;
    let membership = read_membership(tx, user).await.map_err(absent_target)?;
    if !if_match.admits(membership.version) {
        return Err(Error::OrganizationAuthorityConflict);
    }
    if membership.enabled
        && membership.role == OrganizationRole::Owner
        && repository::enabled_owner_count(tx).await? <= 1
    {
        return Err(Error::LastOrganizationOwner);
    }
    Ok(())
}

pub(crate) async fn apply_removal(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    user: &UserId,
) -> Result<(), Error> {
    let membership = read_membership(tx, user).await.map_err(absent_target)?;
    repository::remove_membership(tx, user, next_version(membership.version)?.get()).await?;
    repository::record_event(tx, context, user, OrganizationLifecycleAction::Removed).await
}
