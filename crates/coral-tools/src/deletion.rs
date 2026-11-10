use std::sync::Arc;

use coral_catalog::AuthoredToolKind;
use coral_db::{Database, Transaction};
use coral_workspace::{WorkspaceId, WorkspaceModule};

use crate::Error;
use crate::authoring::now_unix_nanos;
use crate::private_tool::delete_current_with_event;
use crate::repository;
use crate::types::{PrivateToolChangeContext, PrivateToolId, PrivateToolPath};

/// Deletion of the authenticated Principal's own current Private Tools.
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
