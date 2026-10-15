//! The Built-in Provider: "Coral" at the Call Root `coral.admin`.
//!
//! Every Workspace gets one when it is created, with its own Generation of the
//! Coral-defined catalog. It has no Setup, no Publication and no Accounts.
//! When Coral boots, it updates every Workspace's Built-in Provider to the
//! catalog it serves.
use coral_db::Transaction;
use coral_workspace::{ChangeContext, WorkspaceId};

use crate::repository as provider_repository;
use crate::{
    ActivatedGeneration, CredentialRoute, Error, LifecycleAction, LifecycleActor, LifecycleOutcome,
    Provider, ProviderCallRoot, ProviderDisplayName, ProviderId, ProviderModule, ProviderState,
    access,
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
    /// Built-in Provider, or a persistence failure. An error may follow writes;
    /// do not commit the transaction after one.
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
            &self.builtin_catalog,
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

impl ProviderModule {
    /// Lists the Built-in Providers, across Workspaces, whose Generation pins a
    /// catalog other than the one this process serves.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure, or corrupt data for a Generation that
    /// pins no well-formed catalog identity.
    pub async fn outdated_builtin_providers(
        &self,
    ) -> Result<Vec<(WorkspaceId, ProviderId)>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let mut outdated = Vec::new();
        for active in provider_repository::builtin_active_generations(&mut transaction).await? {
            if !coral_catalog::builtin_generation_pins_in_transaction(
                &mut transaction,
                &self.builtin_catalog,
                active.generation_id(),
            )
            .await?
            {
                outdated.push((active.workspace_id().clone(), active.provider_id().clone()));
            }
        }
        transaction.rollback().await?;
        Ok(outdated)
    }

    /// Updates one Workspace's Built-in Provider to the catalog this process
    /// serves, in the caller's transaction, with Coral as the recorded actor.
    /// The Provider keeps its state, in an active or an archived Workspace.
    /// `None` means the Provider is gone or already pins this catalog, and
    /// nothing changes.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data failure. An error may follow
    /// writes; do not commit after one.
    pub async fn update_builtin_catalog_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
        occurred_at_unix_nanos: i64,
    ) -> Result<Option<ActivatedGeneration>, Error> {
        // Processes that boot together each try; the first one to hold the
        // row updates it, and the others then find this catalog pinned.
        if !provider_repository::hold_provider_in_transaction(
            transaction,
            workspace_id,
            provider_id,
        )
        .await?
        {
            return Ok(None);
        }
        let Some(stored) =
            provider_repository::provider_in_transaction(transaction, workspace_id, provider_id)
                .await?
        else {
            return Ok(None);
        };
        if *stored.provider.call_root() != ProviderCallRoot::Builtin
            || coral_catalog::builtin_generation_pins_in_transaction(
                transaction,
                &self.builtin_catalog,
                &stored.generation_id,
            )
            .await?
        {
            return Ok(None);
        }
        let generation = coral_catalog::publish_builtin_generation_in_transaction(
            transaction,
            &self.builtin_catalog,
            occurred_at_unix_nanos,
        )
        .await?;
        if !provider_repository::activate_builtin_generation(
            transaction,
            workspace_id,
            provider_id,
            &stored.generation_id,
            &generation,
        )
        .await?
        {
            return Err(Error::EtagMismatch);
        }
        provider_repository::insert_lifecycle_event(
            transaction,
            provider_repository::NewLifecycleEvent {
                workspace_id: Some(workspace_id),
                actor: LifecycleActor::Coral,
                action: LifecycleAction::ProviderUpdated,
                target_id: &provider_id.to_string(),
                outcome: LifecycleOutcome::Accepted,
                changed_field_paths: &["provider.active_generation", "catalog"],
                occurred_at_unix_nanos,
                trace_id: &coral_telemetry::TraceId::current_or_new(),
            },
        )
        .await?;
        Ok(Some(ActivatedGeneration::new(
            provider_id.clone(),
            generation,
            Some(stored.generation_id),
        )))
    }
}
