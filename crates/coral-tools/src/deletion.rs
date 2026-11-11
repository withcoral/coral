use coral_catalog::AuthoredToolKind;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, PrincipalKind, WorkspaceId, WorkspaceModule};

use crate::Error;
use crate::authoring::now_unix_nanos;
use crate::private_tool::delete_current_with_event;
use crate::repository;
use crate::types::{PrivateToolChangeContext, PrivateToolId, PrivateToolPath};

/// Deletion of current Private Tools: the authenticated Principal's own, and
/// for a Workspace Owner a Workspace-owned Agent's.
///
/// It starts no compiler, so management surfaces can delete without the
/// authoring cost of [`crate::PrivateToolModule`]. It stages each deletion
/// in the caller's transaction and commits none. The caller removes the
/// deleted Private Tool's exact rule in the same transaction, through
/// `coral_policy::remove_authored_tool_rules_in_transaction`, and sends its
/// identity to `SearchIndex::sync_authored_tool` after the commit:
/// coral-policy and coral-search depend on this crate, so neither call can
/// come from here.
#[derive(Clone)]
pub struct PrivateToolDeletion {
    workspaces: WorkspaceModule,
}

impl PrivateToolDeletion {
    /// Builds deletion over the application's Workspace authority.
    #[must_use]
    pub const fn new(workspaces: WorkspaceModule) -> Self {
        Self { workspaces }
    }

    /// Stages the deletion of one Workspace-owned Agent Private Tool as a
    /// Workspace Owner, with its lifecycle Event, in the caller's
    /// transaction.
    ///
    /// The accountable User remains the lifecycle actor and never authors or
    /// executes as the Agent.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace. A caller
    /// without it and an absent path get the same [`Error::NotFound`].
    /// Failure requires rollback.
    pub async fn delete_agent_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        let user = context
            .actor()
            .authenticated_user()
            .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        self.workspaces
            .check_configuration_in_transaction(transaction, user, workspace_id)
            .await
            .map_err(conceal_agent_private_tool_error)?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let tool = repository::find_current_in_transaction(transaction, workspace_id, &agent, path)
            .await?
            .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        delete_current_with_event(transaction, context, workspace_id, &tool).await
    }

    /// Stages the deletion of the actor's own Private Tool at `path`, with
    /// its lifecycle Event, in the caller's transaction.
    ///
    /// # Errors
    ///
    /// Requires the owning Principal and an active Workspace. Absence or other
    /// ownership is concealed as [`Error::NotFound`]. Failure requires rollback.
    pub async fn delete_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        let authority = self
            .workspaces
            .check_authoring_in_transaction(
                transaction,
                context.actor(),
                workspace_id,
                now_unix_nanos()?,
            )
            .await?;
        let tool = repository::find_current_in_transaction(
            transaction,
            workspace_id,
            context.actor().principal(),
            path,
        )
        .await?
        .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        authority.require_current_at(now_unix_nanos()?)?;
        delete_current_with_event(transaction, context, workspace_id, &tool).await
    }
}

fn conceal_agent_private_tool_error(error: coral_workspace::Error) -> Error {
    match error {
        coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_) => {
            Error::NotFound(AuthoredToolKind::Private)
        }
        other => Error::Workspace(other),
    }
}
