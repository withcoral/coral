use std::sync::Arc;

use coral_catalog::AuthoredToolKind;
use coral_db::{Database, Transaction};
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
/// authoring cost of [`crate::PrivateToolModule`].
#[derive(Clone)]
pub struct PrivateToolDeletion {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
}

impl PrivateToolDeletion {
    /// Builds deletion over the application's shared database and Workspace authority.
    #[must_use]
    pub const fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
        }
    }

    /// Atomically deletes one current path and records its lifecycle Event.
    ///
    /// # Errors
    ///
    /// Requires the owning Principal and an active Workspace. Absence or other
    /// ownership is concealed as [`Error::NotFound`].
    pub async fn delete(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        let mut transaction = self.database.begin().await?;
        let id = self
            .delete_in_transaction(&mut transaction, context, workspace_id, path)
            .await?;
        transaction.commit().await?;
        Ok(id)
    }

    /// Atomically deletes one Workspace-owned Agent Private Tool as a Workspace Owner.
    ///
    /// The accountable User remains the lifecycle actor and never authors or
    /// executes as the Agent.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace. A caller
    /// without it and an absent path get the same [`Error::NotFound`].
    pub async fn delete_agent(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        let user = context
            .actor()
            .authenticated_user()
            .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        let mut transaction = self.database.begin().await?;
        self.workspaces
            .check_configuration_in_transaction(&mut transaction, user, workspace_id)
            .await
            .map_err(conceal_agent_private_tool_error)?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let tool =
            repository::find_current_in_transaction(&mut transaction, workspace_id, &agent, path)
                .await?
                .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        let deleted_id =
            delete_current_with_event(&mut transaction, context, workspace_id, &tool).await?;
        transaction.commit().await?;
        Ok(deleted_id)
    }

    /// Stages deletion and lifecycle evidence in the caller's transaction.
    ///
    /// # Errors
    /// Rechecks authority and exact current ownership. Failure requires rollback.
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
