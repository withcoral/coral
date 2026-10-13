//! The Provider workflow owns the one transaction for publication, removal, and rename.
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
    ActivatedGeneration, EntryMaterial, Error, MaterializedPublication, PreparedPublication,
    Provider, ProviderDisplayName, ProviderId, ProviderModule, ProviderPublication,
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
    /// Returns the errors of [`ProviderModule::update_provider_in_transaction`].
    pub async fn update_provider(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let mut transaction = self.database.begin().await?;
        let provider = self
            .providers
            .update_provider_in_transaction(&mut transaction, context, workspace, id, display_name)
            .await?;
        transaction.commit().await?;
        // The rows carry the display name; a rename keeps the Generation.
        self.search
            .index_active_generation(workspace.clone(), id.clone());
        Ok(provider)
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
            entries: material,
        } = self
            .providers
            .publish_in_transaction(&mut transaction, context, publication)
            .await?;
        transaction.commit().await?;
        // A replay, a reuse of the active Generation, or a failed outcome
        // changed nothing to index.
        if let PublicationEvidence::Activated(activated) = evidence {
            self.index_activated(&activated, material);
        }
        Ok(publication)
    }

    fn index_activated(&self, activated: &ActivatedGeneration, material: EntryMaterial) {
        let workspace = material.workspace_id().clone();
        match material.into_entries(activated) {
            Ok(entries) => {
                self.search.index_generation(Arc::new(entries));
                if let Some(replaced) = activated.replaced() {
                    self.search.remove_generation(replaced.clone());
                }
            }
            // The reconcile task indexes the stored Generation instead.
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    workspace_id = %workspace,
                    provider_id = %activated.provider(),
                    generation_id = %activated.generation(),
                    error = %error,
                    "failed to prepare a published Generation's search entries"
                );
            }
        }
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
}
