//! The rows a list reads about, named as a set. A list reads what it shows
//! about its rows (profiles, Organization Memberships, concurrency metadata)
//! from tables other than its own. It names the set of its rows here, and the
//! repository of each such table selects that set with a subquery: the list
//! then sends one statement for each table, however many rows it has.
use sea_query::SelectStatement;

use crate::{LifecycleTargetKind, UserId, WorkspaceId, WorkspaceRole};

/// The Users a list reads profiles or Organization Memberships for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Users<'a> {
    /// The Users with a Membership in one Workspace.
    Members(&'a WorkspaceId),
    /// The designated Owners of one Workspace.
    Owners(&'a WorkspaceId),
    /// The designated Owners of every Workspace.
    EveryOwner,
    /// The Users who created the Agents of one Workspace that are not deleted.
    AgentCreators(&'a WorkspaceId),
    /// The Users with a current Organization Membership.
    OrganizationMembers,
}

impl Users<'_> {
    /// Selects the User ids of the set, in one column.
    pub(crate) fn ids(self) -> SelectStatement {
        match self {
            Self::Members(workspace) => {
                crate::repository::membership_user_ids(Some(workspace), None)
            }
            Self::Owners(workspace) => {
                crate::repository::membership_user_ids(Some(workspace), Some(WorkspaceRole::Owner))
            }
            Self::EveryOwner => {
                crate::repository::membership_user_ids(None, Some(WorkspaceRole::Owner))
            }
            Self::AgentCreators(workspace) => crate::agent_repository::creator_ids(workspace),
            Self::OrganizationMembers => crate::organization::current_member_ids(),
        }
    }
}

/// The resources a list reads concurrency metadata for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Resources<'a> {
    /// The Memberships of one Workspace.
    Memberships(&'a WorkspaceId),
    /// The Agents of one Workspace that are not deleted.
    Agents(&'a WorkspaceId),
    /// The Registrations of the Agents of one Workspace.
    AgentRegistrations(&'a WorkspaceId),
    /// The Workspaces where one User holds a Membership.
    WorkspacesOf(&'a UserId),
    /// Every Workspace in the Organization.
    EveryWorkspace,
}

/// Where the resources of one set are, and the ids that select them.
pub(crate) enum ResourceScope<'a> {
    /// Resources of one Workspace, with the statement that selects their ids.
    Within(&'a WorkspaceId, SelectStatement),
    /// Workspaces themselves, with the statement that selects their ids.
    Workspaces(SelectStatement),
}

impl<'a> Resources<'a> {
    /// The kind every resource of the set has.
    pub(crate) const fn kind(self) -> LifecycleTargetKind {
        match self {
            Self::Memberships(_) => LifecycleTargetKind::WorkspaceMembership,
            Self::Agents(_) => LifecycleTargetKind::Agent,
            Self::AgentRegistrations(_) => LifecycleTargetKind::AgentRegistration,
            Self::WorkspacesOf(_) | Self::EveryWorkspace => LifecycleTargetKind::Workspace,
        }
    }

    /// Selects the ids of the set, in one column, inside their Workspace.
    pub(crate) fn scope(self) -> ResourceScope<'a> {
        match self {
            Self::Memberships(workspace) => ResourceScope::Within(
                workspace,
                crate::repository::membership_user_ids(Some(workspace), None),
            ),
            Self::Agents(workspace) => {
                ResourceScope::Within(workspace, crate::agent_repository::agent_ids(workspace))
            }
            Self::AgentRegistrations(workspace) => ResourceScope::Within(
                workspace,
                crate::agent_repository::registration_ids(workspace),
            ),
            Self::WorkspacesOf(user) => {
                ResourceScope::Workspaces(crate::repository::membership_workspace_ids_of(user))
            }
            Self::EveryWorkspace => {
                ResourceScope::Workspaces(crate::repository::every_workspace_id())
            }
        }
    }
}
