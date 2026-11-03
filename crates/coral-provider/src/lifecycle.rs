//! Destructive Provider lifecycle never repairs authority-bearing references.
use crate::change::{Committed, ProviderChange, SearchChange};
use crate::{Error, LifecycleAction, ProviderId, ProviderModule, account, repository};
use coral_workspace::ChangeContext;
use coral_workspace::WorkspaceId;

impl ProviderModule {
    /// Irreversibly removes current configuration, retaining Publication and
    /// Invocation provenance, in its own transaction. Retires the Provider's
    /// unfinished Update Setups, Accounts and pending authorizations with it.
    ///
    /// # Errors
    /// Requires current owner configuration authority and the exact Provider etag.
    pub async fn delete_provider(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<Committed<()>, Error> {
        let mut tx = self.database.begin().await?;
        let held = self
            .hold_for_change(&mut tx, context, owner, provider, ProviderChange::Delete)
            .await?;
        account::delete_workspace_provider_current_in_transaction(
            &mut tx,
            context,
            owner,
            provider,
            held.authority,
        )
        .await?;
        account::oauth_client_secret_repository::delete(
            &mut tx,
            owner,
            &account::oauth_client_secret_repository::Target::Provider(provider),
        )
        .await?;
        repository::delete_provider(&mut tx, owner, provider).await?;
        held.record(
            &mut tx,
            context,
            LifecycleAction::ProviderDeleted,
            &["provider"],
        )
        .await?;
        tx.commit().await?;
        Ok(Committed::new(
            (),
            SearchChange::Removed {
                workspace: owner.clone(),
                provider: provider.clone(),
            },
        ))
    }
}
