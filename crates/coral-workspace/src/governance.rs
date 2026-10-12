//! Organization Owner governance over Workspaces the Owner is not a Member of.
//! It lists Workspaces and assigns designated Owners; it grants no authority
//! inside a Workspace.
use coral_db::Transaction;

use crate::{
    AuthenticatedUser, AuthoritySource, ChangeContext, Error, EventDetail, LifecycleAction,
    LifecycleTargetKind, OrganizationRole, ResourceMetadata, UserId, UserProfile, Workspace,
    WorkspaceId, WorkspaceModule, WorkspaceRole, WorkspaceState,
};

/// One designated Owner on record.
#[derive(Debug, Clone)]
pub struct WorkspaceOwner {
    /// Safe name and email.
    pub profile: UserProfile,
    /// Whether the Owner's Organization Membership is enabled. A disabled Owner
    /// is still the Owner on record.
    pub enabled: bool,
}

/// One Workspace as an Organization Owner sees it without a Membership.
#[derive(Debug, Clone)]
pub struct OrganizationWorkspace {
    /// Identity, name, and lifecycle state.
    pub workspace: Workspace,
    /// Concurrency metadata for archive and restore.
    pub metadata: ResourceMetadata,
    /// Designated Owners on record.
    pub owners: Vec<WorkspaceOwner>,
}

pub(super) async fn owners(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
) -> Result<Vec<WorkspaceOwner>, Error> {
    let mut owners = Vec::new();
    for membership in crate::repository::list_memberships(&mut *tx, workspace).await? {
        if membership.role() == WorkspaceRole::Owner {
            owners.push(WorkspaceOwner {
                profile: crate::profile::read_profile(tx, membership.user_id()).await?,
                enabled: crate::organization::is_enabled(tx, membership.user_id()).await?,
            });
        }
    }
    Ok(owners)
}

/// Makes one User a designated Owner under Organization Owner authority. The
/// caller holds the Workspace and has checked the target's eligibility.
pub(super) async fn make_owner(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    target: &UserId,
) -> Result<(), Error> {
    let previous = crate::repository::membership_role_in_transaction(tx, workspace, target).await?;
    if previous == Some(WorkspaceRole::Owner) {
        return Ok(());
    }
    crate::repository::upsert_membership(
        tx,
        workspace,
        target,
        WorkspaceRole::Owner,
        context.occurred_at_unix_nanos(),
    )
    .await?;
    crate::record_event(
        tx,
        context,
        workspace,
        EventDetail {
            authority_source: AuthoritySource::OrganizationOwner,
            action: if previous.is_some() {
                LifecycleAction::MembershipRoleUpdated
            } else {
                LifecycleAction::MembershipCreated
            },
            target_kind: LifecycleTargetKind::WorkspaceMembership,
            target_id: target.as_str(),
            changed_field_paths: &["role"],
        },
    )
    .await
}

impl WorkspaceModule {
    /// Lists every Workspace in the Organization with its designated Owners.
    /// This grants no access to anything inside a Workspace.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority.
    pub async fn list_organization_workspaces(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<OrganizationWorkspace>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        require_organization_owner(
            &crate::organization::current_user(&mut tx, actor.user_id()).await?,
        )?;
        let mut result = Vec::new();
        for workspace in crate::repository::all_workspaces(&mut tx).await? {
            result.push(organization_workspace(&mut tx, workspace).await?);
        }
        tx.commit().await?;
        Ok(result)
    }

    /// Inspects one Workspace as an Organization Owner sees it.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority.
    pub async fn inspect_organization_workspace(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<OrganizationWorkspace, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        require_organization_owner(
            &crate::organization::current_user(&mut tx, actor.user_id()).await?,
        )?;
        let found = crate::repository::find_workspace(&mut tx, workspace)
            .await?
            .ok_or(Error::WorkspaceNotFound)?;
        let view = organization_workspace(&mut tx, found).await?;
        tx.commit().await?;
        Ok(view)
    }

    /// Lists the Events that Organization Owner governance authorized in any
    /// Workspace: Owner assignment, archive, restore, and ownership transfer.
    /// Every other Workspace Event needs a Membership there.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority.
    pub async fn list_governance_events(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<crate::AdministrativeLifecycleEvent>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        require_organization_owner(
            &crate::organization::current_user(&mut tx, actor.user_id()).await?,
        )?;
        let events = crate::repository::list_governance_events(&mut tx).await?;
        tx.commit().await?;
        Ok(events)
    }

    /// Archives a Workspace as an Organization Owner, with or without a Membership.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority and an active Workspace.
    /// A rejected request changes nothing.
    pub async fn archive_as_organization_owner(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
    ) -> Result<Workspace, Error> {
        self.change_lifecycle(
            context,
            workspace,
            crate::AuthorityAction::GovernArchive,
            WorkspaceState::Archived,
            LifecycleAction::WorkspaceArchived,
        )
        .await
    }

    /// Restores a Workspace as an Organization Owner, with or without a Membership.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority and an archived Workspace.
    /// A rejected request changes nothing.
    pub async fn restore_as_organization_owner(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
    ) -> Result<Workspace, Error> {
        self.change_lifecycle(
            context,
            workspace,
            crate::AuthorityAction::GovernRestore,
            WorkspaceState::Active,
            LifecycleAction::WorkspaceRestored,
        )
        .await
    }

    /// Makes one User a designated Owner of an active Workspace. A current
    /// Member is promoted; a current Owner is left unchanged with no Event.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority, an active Workspace, and
    /// an enabled Organization Membership for the target. A rejected assignment
    /// changes nothing.
    pub async fn assign_owner(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        target: &UserId,
    ) -> Result<(), Error> {
        let mut tx = self.database.begin().await?;
        // Same order as access removal: Organization guard, Users, Workspace.
        crate::organization::hold_administration(&mut tx).await?;
        require_organization_owner(
            &crate::organization::hold_user(&mut tx, context.actor().user_id()).await?,
        )?;
        if !crate::repository::hold_workspace(&mut tx, workspace).await? {
            return Err(Error::WorkspaceNotFound);
        }
        let state = crate::repository::find_workspace(&mut tx, workspace)
            .await?
            .ok_or(Error::WorkspaceNotFound)?
            .state();
        if state == WorkspaceState::Archived {
            return Err(Error::WorkspaceArchived);
        }
        // Disablement takes the Organization guard first, so the target cannot
        // be disabled between this check and the write.
        if !crate::organization::is_enabled(&mut tx, target).await? {
            return Err(Error::OwnerTargetNotEnabled);
        }
        crate::management::claim_request(&mut tx, context).await?;
        make_owner(&mut tx, context, workspace, target).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn organization_workspace(
    tx: &mut Transaction<'_>,
    workspace: Workspace,
) -> Result<OrganizationWorkspace, Error> {
    let metadata = crate::management_repository::metadata(
        tx,
        workspace.id(),
        LifecycleTargetKind::Workspace,
        &workspace.id().to_string(),
    )
    .await?;
    let owners = owners(tx, workspace.id()).await?;
    Ok(OrganizationWorkspace {
        workspace,
        metadata,
        owners,
    })
}

pub(super) fn require_organization_owner(actor: &AuthenticatedUser) -> Result<(), Error> {
    if actor.organization_role() != Some(OrganizationRole::Owner) {
        return Err(Error::PermissionDenied(
            "Organization Owner authority is required",
        ));
    }
    Ok(())
}
