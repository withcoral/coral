//! The Built-in Provider: "Coral admin" at the Call Root `coral.admin`.
//!
//! Every Workspace gets one when it is created, with its own Generation of the
//! Coral-defined catalog. It has no Setup, no Publication and no Accounts.
use coral_db::Transaction;
use coral_workspace::{ChangeContext, WorkspaceId};

use crate::repository as provider_repository;
use crate::{
    CredentialRoute, Error, LifecycleAction, LifecycleActor, LifecycleOutcome, Provider,
    ProviderCallRoot, ProviderDisplayName, ProviderId, ProviderModule, ProviderState, access,
};

impl ProviderModule {
    /// Creates the Workspace's Built-in Provider in the caller's transaction,
    /// with the acting Owner as its recorded actor.
    ///
    /// Workspace creation calls this in the transaction that creates the
    /// Workspace. It joins that transaction and never commits.
    ///
    /// # Errors
    ///
    /// Returns an authority failure unless the actor is a Workspace Owner,
    /// [`Error::ProviderCallRootConflict`] when the Workspace already has a
    /// Built-in Provider, or a catalog or persistence failure. An error may
    /// follow writes; do not commit the transaction after one.
    pub async fn install_builtin_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
    ) -> Result<Provider, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(transaction, context.actor(), workspace_id)
            .await?;
        let access = match access::prepare(
            transaction,
            workspace_id,
            ProviderId::generate(),
            ProviderCallRoot::Builtin,
            CredentialRoute::None,
        )
        .await?
        {
            access::PreparedProviderAccess::Ready(access) => access,
            access::PreparedProviderAccess::CallRootConflict => {
                return Err(Error::ProviderCallRootConflict);
            }
        };
        let generation = coral_catalog::publish_builtin_generation_in_transaction(
            transaction,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        let provider = Provider::new(
            access,
            ProviderDisplayName::parse(coral_catalog::BUILTIN_PROVIDER_DISPLAY_NAME)?,
            ProviderState::Active,
        );
        provider_repository::insert_provider(
            transaction,
            &provider,
            &generation,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        provider_repository::insert_lifecycle_event(
            transaction,
            provider_repository::NewLifecycleEvent {
                workspace_id: Some(workspace_id),
                actor: LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: authority,
                },
                action: LifecycleAction::BuiltinProviderInstalled,
                target_id: &provider.id().to_string(),
                outcome: LifecycleOutcome::Succeeded,
                changed_field_paths: &[
                    "provider.call_root",
                    "provider.display_name",
                    "provider.credential_route",
                    "provider.state",
                    "provider.active_generation",
                    "catalog",
                ],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok(provider)
    }
}
