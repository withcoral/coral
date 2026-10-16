//! The Workspace workflow owns the one transaction that creates a Workspace
//! together with its Built-in Provider.
//!
//! Workspace writes the Workspace and the creator's Owner Membership; Provider
//! then writes "Coral" at `coral.workspace` with the creator as its actor.
//! The workflow commits only when both succeed, so a failure leaves neither.
//!
//! Per ADR 0006, the workflow notifies Universal Search after the commit. The
//! reconcile task converges what a lost notification missed.
use std::sync::Arc;

use coral_db::Database;
use coral_provider::ProviderModule;
use coral_search::SearchIndex;
use coral_workspace::{ChangeContext, Workspace, WorkspaceDisplayName, WorkspaceModule};

use crate::Error;

/// Creates Workspaces, each with its Built-in Provider, one transaction each.
#[derive(Clone)]
pub struct WorkspaceWorkflow {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    providers: ProviderModule,
    search: SearchIndex,
}

impl WorkspaceWorkflow {
    /// Composes the workflow over the initialized domain database and the
    /// process's one search index.
    #[must_use]
    pub const fn new(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        providers: ProviderModule,
        search: SearchIndex,
    ) -> Self {
        Self {
            database,
            workspaces,
            providers,
            search,
        }
    }

    /// Creates an active Workspace, the creator's Owner Membership, and the
    /// Workspace's Built-in Provider.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`WorkspaceModule::create_in_transaction`] and
    /// [`ProviderModule::install_builtin_in_transaction`]. Failure writes nothing.
    pub async fn create_workspace(
        &self,
        context: &ChangeContext,
        display_name: WorkspaceDisplayName,
    ) -> Result<Workspace, Error> {
        let mut transaction = self.database.begin().await?;
        let workspace = self
            .workspaces
            .create_in_transaction(&mut transaction, context, display_name)
            .await?;
        let provider = self
            .providers
            .install_builtin_in_transaction(&mut transaction, context, workspace.id())
            .await?;
        transaction.commit().await?;
        self.search
            .index_active_generation(workspace.id().clone(), provider.id().clone());
        Ok(workspace)
    }
}
