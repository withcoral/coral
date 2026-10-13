//! The Built-in Provider: "Coral" at the Call Root `coral.admin`.
//!
//! Every Workspace gets one when it is created, with its own Generation of the
//! Coral-defined catalog. It has no Setup, no Publication and no Accounts.
//! When the Coral that serves a Workspace defines a different catalog, an
//! Owner can update the Workspace's Built-in Provider to it.
use coral_catalog::BuiltinCatalogChanges;
use coral_db::Transaction;
use coral_workspace::{AuthenticatedUser, ChangeContext, WorkspaceId};

use crate::change::{self, ProviderChange};
use crate::repository as provider_repository;
use crate::{
    ActivatedGeneration, CredentialRoute, Error, LifecycleAction, LifecycleActor, LifecycleOutcome,
    Provider, ProviderCallRoot, ProviderDisplayName, ProviderId, ProviderModule, ProviderState,
    access, builtin_repository,
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
    /// Compares the Workspace's Built-in Provider catalog with the one this
    /// process serves. `None` means the Workspace already has it.
    ///
    /// # Errors
    ///
    /// Returns an authority failure unless the actor is a Workspace Owner,
    /// [`Error::NotBuiltinProvider`] for another Provider, or a persistence
    /// failure.
    pub async fn inspect_builtin_catalog_update(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
    ) -> Result<Option<BuiltinCatalogChanges>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut transaction, actor, workspace_id)
            .await?;
        let stored = provider_repository::provider_in_transaction(
            &mut transaction,
            workspace_id,
            provider_id,
        )
        .await?
        .ok_or(Error::ProviderNotFound)?;
        if stored.provider.call_root() != &ProviderCallRoot::Builtin {
            return Err(Error::NotBuiltinProvider);
        }
        let changes = coral_catalog::builtin_catalog_changes_in_transaction(
            &mut transaction,
            &self.builtin_catalog,
            &stored.generation_id,
        )
        .await?;
        transaction.rollback().await?;
        Ok(changes)
    }

    /// Updates the Workspace's Built-in Provider to the catalog this process
    /// serves, in the caller's transaction, with the acting Owner recorded.
    /// The Provider keeps its state. `None` means the Workspace already has
    /// this catalog, and nothing changes.
    ///
    /// # Errors
    ///
    /// Returns an authority failure unless the actor is an Owner of an active
    /// Workspace, [`Error::EtagMismatch`] for a stale etag,
    /// [`Error::NotBuiltinProvider`] for another Provider, or a persistence
    /// failure. An error may follow writes; do not commit after one.
    pub async fn update_builtin_catalog_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
    ) -> Result<Option<ActivatedGeneration>, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(transaction, context.actor(), workspace_id)
            .await?;
        let stored = change::hold_for_change(
            transaction,
            workspace_id,
            provider_id,
            ProviderChange::UpdateBuiltinCatalog,
        )
        .await?;
        stored.provider.check_etag(context)?;
        if coral_catalog::builtin_catalog_changes_in_transaction(
            transaction,
            &self.builtin_catalog,
            &stored.generation_id,
        )
        .await?
        .is_none()
        {
            return Ok(None);
        }
        let generation = coral_catalog::publish_builtin_generation_in_transaction(
            transaction,
            &self.builtin_catalog,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        if !builtin_repository::activate_builtin_generation(
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
                actor: LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: authority,
                },
                action: LifecycleAction::ProviderUpdated,
                target_id: &provider_id.to_string(),
                outcome: LifecycleOutcome::Accepted,
                changed_field_paths: &["provider.active_generation", "catalog"],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
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
