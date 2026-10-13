use std::sync::Arc;

use coral_db::{Database, Transaction};
use coral_workspace::{WorkspaceId, WorkspaceModule};

use crate::Error;
use crate::repository;
use crate::saved_function::{delete_current_with_event, now_unix_nanos};
use crate::types::{SavedFunctionChangeContext, SavedFunctionId, SavedFunctionPath};

/// Deletion of the authenticated Principal's own current Saved Functions.
///
/// It starts no compiler, so management surfaces can delete without the
/// authoring cost of [`crate::SavedFunctionModule`].
#[derive(Clone)]
pub struct SavedFunctionDeletion {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
}

impl SavedFunctionDeletion {
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
    /// ownership is concealed as [`Error::SavedFunctionNotFound`].
    pub async fn delete(
        &self,
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunctionId, Error> {
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
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunctionId, Error> {
        let authority = self
            .workspaces
            .check_authoring_in_transaction(
                transaction,
                context.actor(),
                workspace_id,
                now_unix_nanos()?,
            )
            .await?;
        let function = repository::find_current_in_transaction(
            transaction,
            workspace_id,
            context.actor().principal(),
            path,
        )
        .await?
        .ok_or(Error::SavedFunctionNotFound)?;
        authority.require_current_at(now_unix_nanos()?)?;
        delete_current_with_event(transaction, context, workspace_id, &function).await
    }
}
