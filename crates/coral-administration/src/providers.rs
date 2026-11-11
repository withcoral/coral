//! The Provider workflow runs each Provider change and then tells Universal
//! Search what the change committed.
//!
//! Provider owns each change's one transaction and returns the
//! [`SearchChange`] it committed. Per ADR 0017, the workflow hands it to
//! search after the commit. A notification never fails the workflow; the
//! reconcile task converges what a lost or failed notification missed.
use coral_provider::{
    Committed, CoralDefinedProvider, Error, Provider, ProviderDisplayName, ProviderId,
    ProviderModule, ProviderPublication, ProviderRegenerationPublication,
    ProviderRegenerationRequest, ProviderRenamer, SearchChange,
    WorkspaceProviderPublicationRequest,
};
use coral_search::SearchIndex;
use coral_telemetry::safe;
use coral_workspace::{ChangeContext, WorkspaceId};

/// Publishes, removes, and renames Providers, and tells search after each.
#[derive(Clone)]
pub struct ProviderWorkflow {
    providers: ProviderModule,
    search: SearchIndex,
}

impl ProviderWorkflow {
    /// Composes the workflow over the Provider module and the process's one
    /// search index.
    #[must_use]
    pub const fn new(providers: ProviderModule, search: SearchIndex) -> Self {
        Self { providers, search }
    }

    /// Publishes one Workspace-owned Provider.
    ///
    /// An exact replay of a committed request returns the same terminal result.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::publish_workspace_provider`].
    /// Safe structural and domain failures are successful calls whose
    /// returned outcome is `Failed`.
    pub async fn publish_workspace_provider(
        &self,
        context: &ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<ProviderPublication, Error> {
        let committed =
            Box::pin(self.providers.publish_workspace_provider(context, request)).await?;
        Ok(self.notify(committed))
    }

    /// Activates one replacement Provider Generation.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::regenerate_provider`].
    pub async fn regenerate_provider(
        &self,
        context: &ChangeContext,
        request: ProviderRegenerationRequest,
    ) -> Result<ProviderRegenerationPublication, Error> {
        let committed = Box::pin(self.providers.regenerate_provider(context, request)).await?;
        Ok(self.notify(committed))
    }

    /// Removes one Provider's current configuration.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::delete_provider`].
    pub async fn delete_provider(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<(), Error> {
        let committed = self
            .providers
            .delete_provider(context, owner, provider)
            .await?;
        self.notify(committed);
        Ok(())
    }

    /// Renames one Provider.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::rename_provider`].
    pub async fn rename_provider(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let committed = self
            .providers
            .rename_provider(context, workspace, id, display_name)
            .await?;
        Ok(self.notify(committed))
    }

    /// Installs each Provider Coral defines in every Workspace that has
    /// none, each in its own transaction, then tells Universal Search: the
    /// Built-in Provider, with the catalog this process serves, the Private
    /// Provider and the Shared Provider. Bootstrap runs it before the process
    /// takes requests. A Workspace whose install fails waits for the next
    /// boot, and the others still install.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ProviderModule::install_missing_coral_defined`].
    pub async fn install_missing_coral_providers(
        &self,
        occurred_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        for missing in self
            .providers
            .install_missing_coral_defined(occurred_at_unix_nanos)
            .await?
        {
            let workspace_id = missing.workspace_id().clone();
            let provider = missing.provider();
            match missing.into_installed() {
                Ok(committed) => {
                    self.notify(committed);
                }
                Err(error) => warn_install_failed(provider, &workspace_id, &error),
            }
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
            match self
                .providers
                .update_builtin_catalog(&outdated, occurred_at_unix_nanos)
                .await
            {
                Ok(committed) => {
                    self.notify(committed);
                }
                Err(error) => tracing::warn!(
                    coral.workspace.id = safe::id(&outdated.workspace_id()),
                    coral.provider.id = safe::id(&outdated.provider_id()),
                    error.type = safe::kind(error.code()),
                    "failed to update a Built-in Provider's catalog"
                ),
            }
        }
        Ok(())
    }

    fn notify<T>(&self, committed: Committed<T>) -> T {
        notify(&self.search, committed)
    }
}

/// Records that a boot could not install `provider` in the Workspace, which
/// waits for the next boot.
fn warn_install_failed(provider: CoralDefinedProvider, workspace_id: &WorkspaceId, error: &Error) {
    match provider {
        CoralDefinedProvider::Builtin => tracing::warn!(
            coral.workspace.id = safe::id(workspace_id),
            error.type = safe::kind(error.code()),
            "failed to install a Built-in Provider"
        ),
        CoralDefinedProvider::Private => tracing::warn!(
            coral.workspace.id = safe::id(workspace_id),
            error.type = safe::kind(error.code()),
            "failed to install a Private Provider"
        ),
        CoralDefinedProvider::Shared => tracing::warn!(
            coral.workspace.id = safe::id(workspace_id),
            error.type = safe::kind(error.code()),
            "failed to install a Shared Provider"
        ),
    }
}

/// Hands `search` what a Provider change committed, and returns the change's
/// result. Every workflow that commits a Provider change notifies through
/// this one mapping, after its commit.
pub(crate) fn notify<T>(search: &SearchIndex, committed: Committed<T>) -> T {
    let (value, change) = committed.into_parts();
    match change {
        SearchChange::Unchanged => {}
        // Search prepares the Generation's entries from storage, so the
        // change never waits for them (ADR 0017).
        SearchChange::Activated(activated) => {
            search.index_generation(
                activated.workspace().clone(),
                activated.provider().clone(),
                activated.generation().clone(),
            );
            if let Some(replaced) = activated.replaced() {
                search.remove_generation(replaced.clone());
            }
        }
        SearchChange::Refreshed {
            workspace,
            provider,
        } => search.index_active_generation(workspace, provider),
        SearchChange::Removed {
            workspace,
            provider,
        } => search.remove_provider(workspace, provider),
    }
    value
}

/// A Built-in Tool renames through this workflow, so its rename and its search
/// notification are the ones Reef's gives.
#[async_trait::async_trait]
impl ProviderRenamer for ProviderWorkflow {
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
