//! The Shared Tool workflow publishes and removes Shared Tools, and tells
//! Universal Search after each commit.
//!
//! coral-tools owns a publish's one transaction. It cannot call search or
//! policy, which depend on it, so this workflow composes them around it: it
//! types a publish from the Tools its publishing Owner can discover (ADR
//! 0020), it owns the transaction that removes a Shared Tool together with
//! its exact rule, and it hands search the Shared Tool's id after each
//! commit (ADR 0017). A notification never fails the workflow; the
//! reconcile task converges what a lost or failed notification missed.
use std::sync::Arc;

use coral_db::Database;
use coral_search::{CallerToolDeclarations, SearchIndex, UniversalSearchModule};
use coral_tools::{
    Error, SharedToolId, SharedToolModule, SharedToolPath, SharedToolPublication, SharedToolPublish,
};
use coral_workspace::{AuthenticatedPrincipal, ChangeContext, WorkspaceId};

/// Publishes and removes Shared Tools, and tells search after each.
#[derive(Clone)]
pub struct SharedToolWorkflow {
    database: Arc<Database>,
    shared_tools: SharedToolModule,
    search: UniversalSearchModule,
    index: SearchIndex,
}

impl SharedToolWorkflow {
    /// Composes the workflow over the database a removal's transaction
    /// opens on, the Shared Tool module, the discovery that types a
    /// publish, and the process's one search index.
    #[must_use]
    pub const fn new(
        database: Arc<Database>,
        shared_tools: SharedToolModule,
        search: UniversalSearchModule,
        index: SearchIndex,
    ) -> Self {
        Self {
            database,
            shared_tools,
            search,
            index,
        }
    }

    /// Publishes the exact content the Owner reviewed as the next revision
    /// of a Shared Tool, typed from the Tools that Owner can discover; or,
    /// for a [`SharedToolPublish::review`], returns its review and writes
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`SharedToolModule::publish`].
    pub async fn publish(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: SharedToolPublish,
    ) -> Result<SharedToolPublication, Error> {
        let author = AuthenticatedPrincipal::user(context.actor().clone());
        let declarations = CallerToolDeclarations::new(&self.search, &author, workspace_id);
        let publication = self
            .shared_tools
            .publish(context, workspace_id, publish, Some(&declarations))
            .await?;
        if let Some(published) = publication.published() {
            self.index
                .sync_authored_tool(workspace_id.clone(), published.id().clone());
        }
        Ok(publication)
    }

    /// Removes the live Shared Tool at `path` together with its exact rule,
    /// and returns its identity.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`SharedToolModule::remove`]. Failure changes
    /// nothing.
    pub async fn remove(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        path: &SharedToolPath,
        expected_revision: u64,
    ) -> Result<SharedToolId, Error> {
        let mut transaction = self.database.begin().await?;
        let removed = self
            .shared_tools
            .remove_in_transaction(
                &mut transaction,
                context,
                workspace_id,
                path,
                expected_revision,
            )
            .await?;
        crate::tool_rules::remove_in_transaction(&mut transaction, workspace_id, removed.clone())
            .await?;
        transaction.commit().await?;
        self.index
            .sync_authored_tool(workspace_id.clone(), removed.clone());
        Ok(removed)
    }
}
