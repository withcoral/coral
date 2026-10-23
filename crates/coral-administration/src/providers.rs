//! The Provider workflow owns the one transaction for publication, removal,
//! rename, and a Built-in Provider catalog update.
//!
//! Provider prepares a publication outside any transaction, then writes its
//! terminal result in the transaction this workflow begins. The workflow commits
//! after every success and never after an error: some Provider errors come after
//! earlier writes. Removal and rename need no prepare step.
//!
//! Per ADR 0011, the workflow that commits notifies Universal Search, after the
//! commit. A notification never fails the workflow; the reconcile task converges
//! what a lost or failed notification missed.
use std::sync::Arc;

use coral_db::Database;
use coral_provider::{
    ActiveGeneration, Error, MaterializedPublication, PreparedPublication, Provider,
    ProviderDisplayName, ProviderId, ProviderModule, ProviderPublication,
    ProviderRegenerationPublication, ProviderRegenerationRequest, PublicationEvidence, Published,
    WorkspaceProviderPublicationRequest, WorkspaceProviderPublisher,
};
use coral_search::SearchIndex;
use coral_workspace::{ChangeContext, WorkspaceId};

/// Publishes, removes, and renames Providers, one transaction each.
#[derive(Clone)]
pub struct ProviderWorkflow {
    database: Arc<Database>,
    providers: ProviderModule,
    search: SearchIndex,
}

impl ProviderWorkflow {
    /// Composes the workflow over the initialized domain database and the
    /// process's one search index.
    #[must_use]
    pub const fn new(
        database: Arc<Database>,
        providers: ProviderModule,
        search: SearchIndex,
    ) -> Self {
        Self {
            database,
            providers,
            search,
        }
    }

    /// Publishes one Workspace-owned Provider.
    ///
    /// An exact replay of a committed request returns the same terminal result.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::prepare_publication`] and
    /// [`ProviderModule::publish_in_transaction`]. Safe structural and domain
    /// failures are successful calls whose returned outcome is `Failed`.
    pub async fn publish_workspace_provider(
        &self,
        context: &ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<ProviderPublication, Error> {
        match Box::pin(self.providers.prepare_publication(context, request)).await? {
            PreparedPublication::Replayed(publication) => Ok(publication),
            PreparedPublication::Materialized(publication) => {
                Box::pin(self.publish(context, *publication)).await
            }
        }
    }

    /// Activates one replacement Provider Generation.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::prepare_regeneration`] and
    /// [`ProviderModule::publish_in_transaction`].
    pub async fn regenerate_provider(
        &self,
        context: &ChangeContext,
        request: ProviderRegenerationRequest,
    ) -> Result<ProviderRegenerationPublication, Error> {
        match Box::pin(self.providers.prepare_regeneration(context, request)).await? {
            PreparedPublication::Replayed(publication) => Ok(publication),
            PreparedPublication::Materialized(publication) => {
                Box::pin(self.publish(context, *publication)).await
            }
        }
    }

    /// Removes one Provider's current configuration.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::delete_provider_in_transaction`].
    pub async fn delete_provider(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        self.providers
            .delete_provider_in_transaction(&mut transaction, context, owner, provider)
            .await?;
        transaction.commit().await?;
        // Removal keeps the Provider's Generations, so no cascade removes its rows.
        self.search.remove_provider(owner.clone(), provider.clone());
        Ok(())
    }

    /// Renames one Provider.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::rename_provider_in_transaction`].
    pub async fn rename_provider(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let mut transaction = self.database.begin().await?;
        let provider = self
            .providers
            .rename_provider_in_transaction(&mut transaction, context, workspace, id, display_name)
            .await?;
        transaction.commit().await?;
        // The rows carry the display name; a rename keeps the Generation.
        self.search
            .index_active_generation(workspace.clone(), id.clone());
        Ok(provider)
    }

    /// Installs the Built-in Provider, with the catalog this process serves,
    /// in every Workspace that has none, each in its own transaction, then
    /// tells Universal Search. Bootstrap runs it before the process takes
    /// requests. A Workspace whose install fails waits for the next boot, and
    /// the others still install.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::workspaces_without_builtin`].
    pub async fn install_missing_builtins(&self, occurred_at_unix_nanos: i64) -> Result<(), Error> {
        for workspace_id in self.providers.workspaces_without_builtin().await? {
            if let Err(error) = self
                .install_missing_builtin(&workspace_id, occurred_at_unix_nanos)
                .await
            {
                tracing::warn!(
                    workspace_id = %workspace_id,
                    error = %error,
                    "failed to install a Built-in Provider"
                );
            }
        }
        Ok(())
    }

    async fn install_missing_builtin(
        &self,
        workspace_id: &WorkspaceId,
        occurred_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let installed = self
            .providers
            .install_missing_builtin_in_transaction(
                &mut transaction,
                workspace_id,
                occurred_at_unix_nanos,
            )
            .await?;
        transaction.commit().await?;
        if let Some(provider) = installed {
            self.search
                .index_active_generation(workspace_id.clone(), provider.id().clone());
        }
        Ok(())
    }

    /// Updates every Workspace's Built-in Provider that pins another catalog
    /// to the one this process serves, each in its own transaction, then tells
    /// Universal Search. Bootstrap runs it before the process takes requests.
    /// A Workspace whose update fails keeps its catalog until the next boot,
    /// and the others still update.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::outdated_builtin_generations`].
    pub async fn update_builtin_catalogs(&self, occurred_at_unix_nanos: i64) -> Result<(), Error> {
        for outdated in self.providers.outdated_builtin_generations().await? {
            if let Err(error) = self
                .update_builtin_catalog(&outdated, occurred_at_unix_nanos)
                .await
            {
                tracing::warn!(
                    workspace_id = %outdated.workspace_id(),
                    provider_id = %outdated.provider_id(),
                    error = %error,
                    "failed to update a Built-in Provider's catalog"
                );
            }
        }
        Ok(())
    }

    async fn update_builtin_catalog(
        &self,
        outdated: &ActiveGeneration,
        occurred_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let replaced = self
            .providers
            .update_builtin_catalog_in_transaction(
                &mut transaction,
                outdated,
                occurred_at_unix_nanos,
            )
            .await?;
        transaction.commit().await?;
        if replaced {
            self.search.index_active_generation(
                outdated.workspace_id().clone(),
                outdated.provider_id().clone(),
            );
            self.search
                .remove_generation(outdated.generation_id().clone());
        }
        Ok(())
    }

    async fn publish<T>(
        &self,
        context: &ChangeContext,
        publication: MaterializedPublication<T>,
    ) -> Result<T, Error> {
        let mut transaction = self.database.begin().await?;
        let Published {
            publication,
            evidence,
        } = self
            .providers
            .publish_in_transaction(&mut transaction, context, publication)
            .await?;
        transaction.commit().await?;
        // A replay, a reuse of the active Generation, or a failed outcome
        // changed nothing to index.
        if let PublicationEvidence::Activated(activated) = evidence {
            // Search prepares the Generation's entries from storage, so
            // publication never waits for them (ADR 0017).
            self.search.index_generation(
                activated.workspace().clone(),
                activated.provider().clone(),
                activated.generation().clone(),
            );
            if let Some(replaced) = activated.replaced() {
                self.search.remove_generation(replaced.clone());
            }
        }
        Ok(publication)
    }
}

/// A Built-in Operation publishes through this workflow, so its Publication
/// and its search notification are the ones Reef's gives.
#[async_trait::async_trait]
impl WorkspaceProviderPublisher for ProviderWorkflow {
    async fn publish_workspace_provider(
        &self,
        context: &ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<ProviderPublication, Error> {
        Self::publish_workspace_provider(self, context, request).await
    }

    async fn regenerate_workspace_provider(
        &self,
        context: &ChangeContext,
        request: ProviderRegenerationRequest,
    ) -> Result<ProviderRegenerationPublication, Error> {
        Self::regenerate_provider(self, context, request).await
    }

    async fn rename_provider(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        display_name: ProviderDisplayName,
    ) -> Result<Provider, Error> {
        Self::rename_provider(self, context, workspace, provider, display_name).await
    }
}
