//! Destructive Provider lifecycle never repairs authority-bearing references.
use crate::{
    Error, LifecycleAction, LifecycleActor, LifecycleOutcome, ProviderId, ProviderModule, account,
    repository,
};
use coral_db::Transaction;
use coral_workspace::ChangeContext;
use coral_workspace::WorkspaceId;

impl ProviderModule {
    /// Irreversibly removes current configuration, retaining Publication and Invocation provenance.
    /// Retires its unfinished Update Setups, Accounts and pending authorizations in the caller's
    /// transaction.
    ///
    /// # Errors
    /// Requires current owner configuration authority and the exact Provider etag. Never commit
    /// the transaction after an error.
    pub async fn delete_provider_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<(), Error> {
        let actor = context.actor();
        let authority_source = self
            .check_owner_configuration_in_transaction(tx, actor, owner)
            .await?;
        let current = crate::change::hold_for_change(
            tx,
            owner,
            provider,
            crate::change::ProviderChange::Delete,
        )
        .await?
        .provider;
        current.check_etag(context)?;
        let workspace = owner;
        {
            account::delete_workspace_provider_current_in_transaction(
                tx,
                context,
                workspace,
                provider,
                authority_source,
            )
            .await?;
        }
        account::oauth_client_secret_repository::delete(
            tx,
            owner,
            &account::oauth_client_secret_repository::Target::Provider(provider),
        )
        .await?;
        repository::delete_provider(tx, owner, provider).await?;
        repository::insert_lifecycle_event(
            tx,
            repository::NewLifecycleEvent {
                workspace_id: Some(owner),
                actor: LifecycleActor::User {
                    user_id: actor.user_id().clone(),
                    authority_source,
                },
                action: LifecycleAction::ProviderDeleted,
                target_id: &provider.to_string(),
                outcome: LifecycleOutcome::Accepted,
                changed_field_paths: &["provider"],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok(())
    }
}

/// Provider-rule tests remove a Provider without the Provider workflow and its notifications.
#[cfg(any(test, feature = "test-fixtures"))]
impl ProviderModule {
    /// Removes one Provider in its own transaction.
    ///
    /// # Errors
    /// Returns the errors of [`Self::delete_provider_in_transaction`].
    pub async fn delete_provider(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<(), Error> {
        let mut tx = self.database.begin().await?;
        self.delete_provider_in_transaction(&mut tx, context, owner, provider)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
