//! Safe management inspection and optimistic concurrency. All metadata changes
//! join the owning mutation transaction; no retained Event is used as a version.
use crate::management_repository::{self as repository, metadata};
use coral_db::Transaction;

use crate::{
    AuthenticatedUser, ChangeContext, Error, LifecycleTargetKind, WorkspaceAction,
    WorkspaceEvidence, WorkspaceId, WorkspaceModule,
};

/// Non-secret configuration concurrency and timestamps.
#[derive(Debug, Clone)]
pub struct ResourceMetadata {
    /// Opaque current version, bound to one resource.
    pub etag: String,
    /// Initial creation timestamp.
    pub created_at: i64,
    /// Latest accepted configuration timestamp.
    pub updated_at: i64,
}

pub(super) fn require_owner(evidence: &WorkspaceEvidence) -> Result<(), Error> {
    if evidence.effective_role() != crate::WorkspaceRole::Owner {
        return Err(Error::PermissionDenied(
            "Workspace Owner authority is required",
        ));
    }
    Ok(())
}

/// Coherent, secret-free Agent administration projection.
#[derive(Debug, Clone)]
pub struct AgentInspection {
    /// Current Agent.
    pub agent: crate::Agent,
    /// Agent concurrency metadata.
    pub agent_metadata: ResourceMetadata,
    /// Current singleton Registration.
    pub registration: crate::AgentRegistration,
    /// Registration concurrency metadata.
    pub registration_metadata: ResourceMetadata,
    /// Workspace lifecycle from the same authorized snapshot.
    pub workspace_state: crate::WorkspaceState,
    /// Authorized creator attribution, even after departure or disablement.
    pub creator_profile: crate::UserProfile,
}

/// Independent blockers of Coral access, not client connectivity or Tool readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentAccessBlocker {
    /// Restore the Workspace before configuring or authenticating.
    WorkspaceArchived,
    /// Explicit Agent lifecycle disablement.
    AgentDisabled,
    /// Independent Registration suspension.
    ApiSuspended,
    /// Registration's exclusive authentication deadline has passed.
    Expired,
}
impl AgentInspection {
    /// All current blockers; fixing one never implies the others are cleared.
    #[must_use]
    pub fn access_blockers(&self, now_unix_nanos: i64) -> Vec<AgentAccessBlocker> {
        let mut blockers = Vec::new();
        if self.workspace_state == crate::WorkspaceState::Archived {
            blockers.push(AgentAccessBlocker::WorkspaceArchived);
        }
        if self.agent.state() == crate::AgentState::Disabled {
            blockers.push(AgentAccessBlocker::AgentDisabled);
        }
        if !self.registration.is_enabled() {
            blockers.push(AgentAccessBlocker::ApiSuspended);
        }
        if self
            .registration
            .expires_at_unix_nanos()
            .is_some_and(|expires| expires <= now_unix_nanos)
        {
            blockers.push(AgentAccessBlocker::Expired);
        }
        blockers
    }
}

pub(super) async fn record_change(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    kind: LifecycleTargetKind,
    id: &str,
) -> Result<(), Error> {
    repository::upsert_metadata(
        tx,
        workspace,
        kind,
        id,
        &uuid::Uuid::new_v4().to_string(),
        context.occurred_at_unix_nanos(),
    )
    .await
}

pub(super) async fn check_etag(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    kind: LifecycleTargetKind,
    id: &str,
) -> Result<(), Error> {
    if let Some(expected) = context.expected_etag()
        && metadata(tx, workspace, kind, id).await?.etag != expected
    {
        return Err(Error::EtagMismatch);
    }
    Ok(())
}

pub(super) async fn claim_request(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
) -> Result<(), Error> {
    if let Some(id) = context.request_id()
        && !repository::insert_request(tx, context.actor().user_id(), id).await?
    {
        return Err(Error::RequestAlreadyApplied);
    }
    Ok(())
}

impl WorkspaceModule {
    /// Inspects current Agents and etags in one Owner-authorized snapshot.
    /// # Errors
    /// Denies non-Owners; archived Workspaces remain inspectable.
    pub async fn inspect_agents(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<AgentInspection>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        require_owner(&evidence)?;
        let mut result = Vec::new();
        for agent in crate::agent_repository::list(&mut tx, workspace).await? {
            result.push(agent_inspection(&mut tx, agent, evidence.workspace().state()).await?);
        }
        tx.commit().await?;
        Ok(result)
    }

    /// Inspects an exact Agent and its singleton Registration coherently.
    /// # Errors
    /// Requires Owner authority; conceals cross-Workspace targets.
    pub async fn inspect_agent(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        agent: &crate::PrincipalId,
    ) -> Result<AgentInspection, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let agent = self
            .agent_for_inspection_in_transaction(&mut tx, &evidence, agent)
            .await?;
        let view = agent_inspection(&mut tx, agent, evidence.workspace().state()).await?;
        tx.commit().await?;
        Ok(view)
    }

    /// Lists designated Memberships and current etags in one Owner-authorized snapshot.
    /// The flag reports whether the Member's Organization Membership is enabled.
    /// # Errors
    /// Denies non-Owners and conceals inaccessible Workspaces.
    pub async fn inspect_memberships(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<
        Vec<(
            crate::WorkspaceMembership,
            ResourceMetadata,
            crate::UserProfile,
            bool,
        )>,
        Error,
    > {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        require_owner(&evidence)?;
        let mut result = Vec::new();
        for member in crate::repository::list_memberships(&mut tx, workspace).await? {
            let meta = metadata(
                &mut tx,
                workspace,
                LifecycleTargetKind::WorkspaceMembership,
                member.user_id().as_str(),
            )
            .await?;
            let profile = crate::profile::read_profile(&mut tx, member.user_id()).await?;
            let enabled = crate::organization::is_enabled(&mut tx, member.user_id()).await?;
            result.push((member, meta, profile, enabled));
        }
        tx.commit().await?;
        Ok(result)
    }

    /// Inspects an exact Membership; ordinary Members see only themselves.
    /// The flag reports whether the Member's Organization Membership is enabled.
    /// # Errors
    /// Conceals absent and other Users' Memberships from non-Owners alike.
    pub async fn inspect_membership(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        user: &crate::UserId,
    ) -> Result<(crate::WorkspaceMembership, ResourceMetadata, bool), Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        if evidence.effective_role() != crate::WorkspaceRole::Owner && actor.user_id() != user {
            return Err(Error::MembershipNotFound);
        }
        let role = crate::repository::membership_role_in_transaction(&mut tx, workspace, user)
            .await?
            .ok_or(Error::MembershipNotFound)?;
        let meta = metadata(
            &mut tx,
            workspace,
            LifecycleTargetKind::WorkspaceMembership,
            user.as_str(),
        )
        .await?;
        let enabled = crate::organization::is_enabled(&mut tx, user).await?;
        tx.commit().await?;
        Ok((
            crate::WorkspaceMembership::new(workspace.clone(), user.clone(), role),
            meta,
            enabled,
        ))
    }

    /// Safe designated-Owner directory, not a Membership list or private-work grant.
    /// A disabled Owner is still the Owner on record and is listed as disabled.
    /// # Errors
    /// Conceals inaccessible Workspaces.
    pub async fn list_owners(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<crate::WorkspaceOwner>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let owners = crate::governance::owners(&mut tx, workspace).await?;
        tx.commit().await?;
        Ok(owners)
    }

    /// Inspects one Workspace and its concurrency metadata in one authority snapshot.
    /// # Errors
    /// Conceals absent or inaccessible Workspaces.
    pub async fn inspect_workspace(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<(WorkspaceEvidence, ResourceMetadata), Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let meta = metadata(
            &mut tx,
            workspace,
            LifecycleTargetKind::Workspace,
            &workspace.to_string(),
        )
        .await?;
        tx.commit().await?;
        Ok((evidence, meta))
    }
}

async fn agent_inspection(
    tx: &mut Transaction<'_>,
    agent: crate::Agent,
    workspace_state: crate::WorkspaceState,
) -> Result<AgentInspection, Error> {
    let workspace = agent.workspace_id();
    let registration =
        crate::agent_repository::registration(&mut *tx, workspace, agent.principal().id()).await?;
    let agent_metadata = metadata(
        tx,
        workspace,
        LifecycleTargetKind::Agent,
        agent.principal().id().as_str(),
    )
    .await?;
    let registration_metadata = metadata(
        tx,
        workspace,
        LifecycleTargetKind::AgentRegistration,
        &registration.id().to_string(),
    )
    .await?;
    let creator_profile = crate::profile::read_profile(tx, agent.created_by()).await?;
    Ok(AgentInspection {
        agent,
        agent_metadata,
        registration,
        registration_metadata,
        workspace_state,
        creator_profile,
    })
}
