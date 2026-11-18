//! Safe management inspection and optimistic concurrency. All metadata changes
//! join the owning mutation transaction; no retained Event is used as a version.
use std::collections::HashMap;

use crate::list_repository::{Resources, Users};
use crate::management_repository::{self as repository, listed_metadata, metadata};
use coral_db::{Transaction, UnixNanos};

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
    pub created_at: UnixNanos,
    /// Latest accepted configuration timestamp.
    pub updated_at: UnixNanos,
}

pub(super) fn require_owner(evidence: &WorkspaceEvidence) -> Result<(), Error> {
    if evidence.effective_role() != crate::WorkspaceRole::Owner {
        return Err(Error::PermissionDenied(
            crate::Denial::WorkspaceOwnerRequired,
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

/// The Workspaces where one User holds a Membership, as that User sees them.
#[derive(Debug, Clone)]
pub struct WorkspaceInspections {
    /// Each Workspace with the User's role there and its concurrency
    /// metadata, in [listing order](crate::Workspace::listing_key).
    pub workspaces: Vec<(WorkspaceEvidence, ResourceMetadata)>,
    /// The User's own safe profile.
    pub profile: crate::UserProfile,
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
    pub fn access_blockers(&self, now: UnixNanos) -> Vec<AgentAccessBlocker> {
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
            .expires_at()
            .is_some_and(|expires| expires <= now)
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
        context.occurred_at(),
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
            .check_owner_inspection_in_transaction(&mut tx, actor, workspace)
            .await?;
        let agents = crate::agent_repository::list(&mut tx, workspace).await?;
        let mut registrations = crate::agent_repository::list_registrations(&mut tx, workspace)
            .await?
            .into_iter()
            .map(|registration| (registration.agent_id().clone(), registration))
            .collect::<HashMap<_, _>>();
        let agent_metadata = listed_metadata(&mut tx, Resources::Agents(workspace)).await?;
        let registration_metadata =
            listed_metadata(&mut tx, Resources::AgentRegistrations(workspace)).await?;
        let creators =
            crate::profile::read_listed_profiles(&mut tx, Users::AgentCreators(workspace)).await?;
        tx.commit().await?;
        agents
            .into_iter()
            .map(|agent| {
                let registration = registrations
                    .remove(agent.principal().id())
                    .ok_or(Error::AgentNotFound)?;
                Ok(AgentInspection {
                    agent_metadata: agent_metadata.of(agent.principal().id().as_str())?,
                    registration_metadata: registration_metadata
                        .of(&registration.id().to_string())?,
                    registration,
                    workspace_state: evidence.workspace().state(),
                    creator_profile: creators.of(agent.created_by()),
                    agent,
                })
            })
            .collect()
    }

    /// Checks that the User may inspect what only a Workspace Owner sees, in
    /// the caller's read snapshot.
    pub(super) async fn check_owner_inspection_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<WorkspaceEvidence, Error> {
        let evidence = self
            .check_in_transaction(tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        require_owner(&evidence)?;
        Ok(evidence)
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
        self.check_owner_inspection_in_transaction(&mut tx, actor, workspace)
            .await?;
        let members = crate::repository::list_memberships(&mut tx, workspace).await?;
        let member_metadata = listed_metadata(&mut tx, Resources::Memberships(workspace)).await?;
        let profiles =
            crate::profile::read_listed_profiles(&mut tx, Users::Members(workspace)).await?;
        let enabled =
            crate::organization::enabled_among(&mut tx, Users::Members(workspace)).await?;
        tx.commit().await?;
        members
            .into_iter()
            .map(|member| {
                let meta = member_metadata.of(member.user_id().as_str())?;
                let profile = profiles.of(member.user_id());
                let enabled = enabled.contains(member.user_id());
                Ok((member, meta, profile, enabled))
            })
            .collect()
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

    /// Inspects every Workspace where the User holds a Membership, as
    /// [`Self::inspect_workspace`] inspects one, with the User's own profile,
    /// in one authority snapshot.
    /// # Errors
    /// Returns a denial when the User no longer has Organization Membership.
    pub async fn inspect_workspaces(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<WorkspaceInspections, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let actor = crate::organization::enabled_user(&mut tx, actor.user_id()).await?;
        let listed = crate::repository::list_workspaces(&mut tx, actor.user_id()).await?;
        let workspace_metadata =
            listed_metadata(&mut tx, Resources::WorkspacesOf(actor.user_id())).await?;
        let profile = crate::profile::read_profile(&mut tx, actor.user_id()).await?;
        tx.commit().await?;
        let workspaces = listed
            .into_iter()
            .map(|(workspace, role)| {
                let meta = workspace_metadata.of(&workspace.id().to_string())?;
                // The rule that decides each Workspace is the one that decides
                // a single inspection: the User's Membership there.
                let evidence = crate::authorize(
                    &actor,
                    (workspace, Some(role)),
                    crate::AuthorityAction::Public(WorkspaceAction::Inspect),
                )?;
                Ok((evidence, meta))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(WorkspaceInspections {
            workspaces,
            profile,
        })
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
