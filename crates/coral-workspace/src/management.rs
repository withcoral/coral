//! Safe management inspection and optimistic concurrency. All metadata changes
//! join the owning mutation transaction; no retained Event is used as a version.
//!
//! Each resource's row keeps its version, which every accepted change raises
//! by one, and its times. An inspection shows them, and a change is checked
//! against the version on the row it changes.
use std::collections::HashMap;

use crate::list_repository::Users;
use crate::management_repository as repository;
use coral_db::{Transaction, UnixNanos, Version};

use crate::{
    AuthenticatedUser, Error, IfMatch, RequestId, UserId, WorkspaceAction, WorkspaceEvidence,
    WorkspaceId, WorkspaceModule,
};

/// Non-secret configuration concurrency and timestamps, as the resource's own
/// row holds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceMetadata {
    /// The count of accepted changes to the resource. Its text is the etag
    /// an Adapter shows.
    pub version: Version,
    /// Initial creation timestamp. An Agent or an Agent Registration that a
    /// Coral from before the row kept its times created has none.
    pub created_at: Option<UnixNanos>,
    /// Latest accepted configuration timestamp, absent as the creation
    /// timestamp is until the resource next changes.
    pub updated_at: Option<UnixNanos>,
}

impl ResourceMetadata {
    /// The metadata of a resource that was created at `created_at` and has
    /// not changed since.
    pub(crate) const fn created(created_at: UnixNanos) -> Self {
        Self {
            version: Version::FIRST,
            created_at: Some(created_at),
            updated_at: Some(created_at),
        }
    }

    /// The metadata that one accepted change at `changed_at` leaves: the
    /// next version, and that time as the latest.
    pub(crate) fn changed(self, changed_at: UnixNanos) -> Result<Self, Error> {
        let version = self
            .version
            .next()
            .ok_or_else(|| Error::CorruptData("resource version overflowed".to_string()))?;
        Ok(Self {
            version,
            created_at: self.created_at,
            updated_at: Some(changed_at),
        })
    }
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

/// Checks the etag the caller read against the version that the resource's
/// row holds now. The caller holds the resource's Workspace and has read the
/// row under that hold.
///
/// A UUID etag is one that Coral showed before each row kept its version. No
/// version meets it: the caller reads the resource again.
pub(super) fn check(if_match: &IfMatch, current: Version) -> Result<(), Error> {
    if if_match.admits(current) {
        Ok(())
    } else {
        Err(Error::EtagMismatch)
    }
}

/// Claims `request` for `user`, so that the action it names runs once: the
/// claim commits with the action, and a second claim of the same identity by
/// the same User is refused.
pub(super) async fn claim_request(
    tx: &mut Transaction<'_>,
    user: &UserId,
    request: &RequestId,
) -> Result<(), Error> {
    if repository::insert_request(tx, user, request).await? {
        Ok(())
    } else {
        Err(Error::RequestAlreadyApplied)
    }
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
            .map(|registration| (registration.0.agent_id().clone(), registration))
            .collect::<HashMap<_, _>>();
        let creators =
            crate::profile::read_listed_profiles(&mut tx, Users::AgentCreators(workspace)).await?;
        tx.commit().await?;
        agents
            .into_iter()
            .map(|(agent, agent_metadata)| {
                let (registration, registration_metadata) = registrations
                    .remove(agent.principal().id())
                    .ok_or(Error::AgentNotFound)?;
                Ok(AgentInspection {
                    agent_metadata,
                    registration_metadata,
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
        crate::agent::require_agent_inspector(&evidence)?;
        let view =
            agent_inspection(&mut tx, workspace, agent, evidence.workspace().state()).await?;
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
        let profiles =
            crate::profile::read_listed_profiles(&mut tx, Users::Members(workspace)).await?;
        let enabled =
            crate::organization::enabled_among(&mut tx, Users::Members(workspace)).await?;
        tx.commit().await?;
        Ok(members
            .into_iter()
            .map(|(member, meta)| {
                let profile = profiles.of(member.user_id());
                let enabled = enabled.contains(member.user_id());
                (member, meta, profile, enabled)
            })
            .collect())
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
        let (role, meta) = crate::repository::membership_in_transaction(&mut tx, workspace, user)
            .await?
            .ok_or(Error::MembershipNotFound)?;
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
        tx.commit().await?;
        let meta = evidence.workspace().metadata();
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
        let profile = crate::profile::read_profile(&mut tx, actor.user_id()).await?;
        tx.commit().await?;
        let workspaces = listed
            .into_iter()
            .map(|(workspace, role)| {
                let meta = workspace.metadata();
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

/// One Agent of the Workspace that is not deleted, with its Registration and
/// what each one's row holds, in the caller's snapshot.
async fn agent_inspection(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    agent: &crate::PrincipalId,
    workspace_state: crate::WorkspaceState,
) -> Result<AgentInspection, Error> {
    let (agent, agent_metadata) =
        crate::agent_repository::find_with_metadata(&mut *tx, workspace, agent).await?;
    let (registration, registration_metadata) =
        crate::agent_repository::registration_with_metadata(
            &mut *tx,
            workspace,
            agent.principal().id(),
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
