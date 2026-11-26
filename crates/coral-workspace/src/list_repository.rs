//! The rows a list reads about, named as a set. A list reads what it shows
//! about its rows (profiles, Organization Memberships) from tables other than
//! its own. It names the set of its rows here, and the repository of each
//! such table selects that set with a subquery: the list then sends one
//! statement for each table, however many rows it has. A row's concurrency
//! metadata is on the row itself, so the list's own statement reads it.
use sea_query::SelectStatement;

use crate::{WorkspaceId, WorkspaceRole};

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
