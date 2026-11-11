//! The Private Tool workflow deletes a Private Tool for a management
//! surface: the caller's own, or an Agent's for a Workspace Owner.
//!
//! coral-tools owns the deletion and its rules. It cannot call policy or
//! search, which depend on it, so this workflow owns the transaction that
//! deletes the Private Tool together with its exact rule, and hands search
//! the Private Tool's id after the commit (ADR 0017). A notification never
//! fails the workflow; the reconcile task converges what a lost or failed
//! notification missed. `coral.removeCustomTool` composes the same for a Program,
//! in coral-run.
use std::sync::Arc;

use coral_db::Database;
use coral_search::SearchIndex;
use coral_tools::{
    Error, PrivateToolChangeContext, PrivateToolDeletion, PrivateToolId, PrivateToolPath,
};
use coral_workspace::{PrincipalId, WorkspaceId};

/// Deletes Private Tools with their exact rules, and tells search after
/// each.
#[derive(Clone)]
pub struct PrivateToolWorkflow {
    database: Arc<Database>,
    deletion: PrivateToolDeletion,
    index: SearchIndex,
}

impl PrivateToolWorkflow {
    /// Composes the workflow over the database a delete's transaction opens
    /// on, the Private Tool deletion, and the process's one search index.
    #[must_use]
    pub const fn new(
        database: Arc<Database>,
        deletion: PrivateToolDeletion,
        index: SearchIndex,
    ) -> Self {
        Self {
            database,
            deletion,
            index,
        }
    }

    /// Deletes the Private Tool at `path` together with its exact rule, and
    /// returns its identity: the actor's own or, with `agent`, that
    /// Workspace-owned Agent's as a Workspace Owner.
    ///
    /// # Errors
    ///
    /// Returns the errors of
    /// [`PrivateToolDeletion::delete_in_transaction`] or, with `agent`, of
    /// [`PrivateToolDeletion::delete_agent_in_transaction`]. Failure
    /// changes nothing.
    pub async fn delete(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        agent: Option<&PrincipalId>,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        let mut transaction = self.database.begin().await?;
        let deleted = match agent {
            Some(agent) => {
                self.deletion
                    .delete_agent_in_transaction(
                        &mut transaction,
                        context,
                        workspace_id,
                        agent,
                        path,
                    )
                    .await?
            }
            None => {
                self.deletion
                    .delete_in_transaction(&mut transaction, context, workspace_id, path)
                    .await?
            }
        };
        crate::tool_rules::remove_in_transaction(&mut transaction, workspace_id, deleted.clone())
            .await?;
        transaction.commit().await?;
        self.index
            .sync_authored_tool(workspace_id.clone(), deleted.clone());
        Ok(deleted)
    }
}
