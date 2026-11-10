//! The Workspace workflow owns the one transaction that creates a Workspace
//! together with the Providers Coral defines in it.
//!
//! Workspace writes the Workspace and the creator's Owner Membership; Provider
//! then writes its Built-in, Private and Shared Providers with the creator as
//! their actor. The workflow commits only when all succeed, so a failure
//! leaves none.
//!
//! Per ADR 0017, the workflow notifies Universal Search after the commit,
//! through the mapping every Provider change shares. The reconcile task
//! converges what a lost notification missed.
use std::sync::Arc;

use coral_db::Database;
use coral_provider::ProviderModule;
use coral_search::SearchIndex;
use coral_workspace::{ChangeContext, Workspace, WorkspaceDisplayName, WorkspaceModule};

use crate::Error;

/// Creates Workspaces, each with the Providers Coral defines, one transaction
/// each.
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
    /// Workspace's Built-in, Private and Shared Providers.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`WorkspaceModule::create_in_transaction`] and
    /// [`ProviderModule::install_every_coral_defined_in_transaction`]. Failure
    /// writes nothing.
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
        let installed = self
            .providers
            .install_every_coral_defined_in_transaction(&mut transaction, context, workspace.id())
            .await?;
        transaction.commit().await?;
        for provider in installed {
            crate::providers::notify(&self.search, provider);
        }
        Ok(workspace)
    }
}
