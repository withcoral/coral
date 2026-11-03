//! Coral-owned Organization Membership, role, and disablement.
//!
//! Login provisioning is host-only: the authentication Adapter
//! selects Member or the explicitly configured bootstrap Owner. Administration
//! requires current Owner authority and an exact revision. Both use the same
//! record read by Workspace authorization and held by the Policy Enforcement
//! Point. Removal retains revision continuity; login can rejoin only removed
//! Membership. No protocol work, remote synchronization, or destructive cleanup
//! lives here.

#[path = "organization_repository.rs"]
mod repository;

use std::sync::Arc;

use coral_db::{Database, Transaction};

use crate::{AuthenticatedUser, ChangeContext, Error, OrganizationRole, UserId};

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

/// Current Coral-owned role and state of one Organization Membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationMembership {
    user_id: UserId,
    role: OrganizationRole,
    enabled: bool,
    revision: u64,
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
    /// Exact revision required for an administrative update.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
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
    pub occurred_at_unix_nanos: i64,
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
    /// bootstrap Owner. A repeated login cannot promote or re-enable a User.
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
                "login requires Organization Membership",
            ))?;
        let user_id = context.actor().user_id();
        let mut created = repository::insert_membership_if_absent(tx, user_id, role).await?;
        if !created && let Some(revision) = repository::removed_revision(tx, user_id).await? {
            repository::rejoin_membership(tx, user_id, role, next_revision(revision)?).await?;
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
        let user = current_user(&mut tx, user_id).await?;
        if user.organization_role().is_none() {
            return Err(Error::PermissionDenied("User is disabled"));
        }
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
                "current Organization Membership is required",
            ))
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

    /// Changes a User's role under current Organization Owner authority.
    /// # Errors
    /// Denies stale revisions, absent authority, and loss of the last enabled Owner.
    pub async fn set_role(
        &self,
        context: &ChangeContext,
        user_id: &UserId,
        revision: u64,
        role: OrganizationRole,
    ) -> Result<OrganizationMembership, Error> {
        self.update(context, user_id, revision, Some(role), None)
            .await
    }

    /// Disables or explicitly restores a User without changing retained work.
    /// # Errors
    /// Denies stale revisions, absent authority, and loss of the last enabled Owner.
    pub async fn set_enabled(
        &self,
        context: &ChangeContext,
        user_id: &UserId,
        revision: u64,
        enabled: bool,
    ) -> Result<OrganizationMembership, Error> {
        self.update(context, user_id, revision, None, Some(enabled))
            .await
    }

    async fn update(
        &self,
        context: &ChangeContext,
        user_id: &UserId,
        revision: u64,
        role: Option<OrganizationRole>,
        enabled: Option<bool>,
    ) -> Result<OrganizationMembership, Error> {
        let mut tx = self.database.begin().await?;
        // All Organization administrators take this hold before User row holds.
        repository::hold_administration(&mut tx).await?;
        require_owner(&mut tx, context.actor()).await?;
        hold_user(&mut tx, user_id).await?;
        let mut membership = read_membership(&mut tx, user_id).await?;
        if membership.revision != revision {
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
                return Err(Error::PermissionDenied(
                    "the last enabled Organization Owner must remain",
                ));
            }
        }
        let next_revision = next_revision(revision)?;
        repository::update_membership(&mut tx, user_id, next_role, next_enabled, next_revision)
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
        membership.revision = next_revision.cast_unsigned();
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
                "current Organization Membership is required",
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
        let mut memberships = Vec::new();
        for user in repository::current_members(&mut tx).await? {
            memberships.push((
                read_membership(&mut tx, &user).await?,
                crate::profile::read_profile(&mut tx, &user).await?,
            ));
        }
        tx.commit().await?;
        Ok(memberships)
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

fn next_revision(revision: u64) -> Result<i64, Error> {
    i64::try_from(revision)
        .ok()
        .and_then(|revision| revision.checked_add(1))
        .ok_or_else(|| Error::CorruptData("Organization authority revision overflowed".into()))
}

async fn require_owner(tx: &mut Transaction<'_>, actor: &AuthenticatedUser) -> Result<(), Error> {
    require_owner_role(&current_user(tx, actor.user_id()).await?)
}

fn require_owner_role(actor: &AuthenticatedUser) -> Result<(), Error> {
    if actor.organization_role() != Some(OrganizationRole::Owner) {
        return Err(Error::PermissionDenied(
            "Organization Owner authority is required",
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

async fn read_membership(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<OrganizationMembership, Error> {
    repository::find_membership(tx, user_id)
        .await?
        .ok_or(Error::PermissionDenied(
            "current Organization authority is unavailable",
        ))
}

/// Reads the current User holding the row changed by role/disablement
/// administration shared until commit, so that administration waits for the
/// decisions reading it.
pub(crate) async fn share_user(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<AuthenticatedUser, Error> {
    repository::find_membership_locked(tx, user_id, coral_db::RowLock::Shared)
        .await?
        .ok_or(Error::PermissionDenied(
            "current Organization authority is unavailable",
        ))
        .map(|membership| membership.authenticated())
}

/// Holds the same row changed by role/disablement administration until commit.
pub(crate) async fn hold_user(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<AuthenticatedUser, Error> {
    repository::hold_user(tx, user_id).await?;
    current_user(tx, user_id).await
}

pub(crate) async fn prepare_removal(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    user: &UserId,
    revision: u64,
    transfer_to: Option<&UserId>,
) -> Result<(), Error> {
    // The caller already holds the Organization administration guard. Check
    // authority before exposing target-dependent absence or revision errors.
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
            hold_user(tx, held).await?;
        }
    }
    require_owner(tx, context.actor()).await?;
    let membership = read_membership(tx, user).await?;
    if membership.revision != revision {
        return Err(Error::OrganizationAuthorityConflict);
    }
    if membership.enabled
        && membership.role == OrganizationRole::Owner
        && repository::enabled_owner_count(tx).await? <= 1
    {
        return Err(Error::PermissionDenied(
            "the last enabled Organization Owner must remain",
        ));
    }
    Ok(())
}

pub(crate) async fn apply_removal(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    user: &UserId,
) -> Result<(), Error> {
    let membership = read_membership(tx, user).await?;
    repository::remove_membership(tx, user, next_revision(membership.revision)?).await?;
    repository::record_event(tx, context, user, OrganizationLifecycleAction::Removed).await
}
