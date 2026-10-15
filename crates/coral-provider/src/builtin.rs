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
    ActiveGeneration, CredentialRoute, Error, LifecycleAction, LifecycleActor, LifecycleOutcome,
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

/// What one boot's Built-in catalog update did, Built-in Provider by
/// Built-in Provider.
#[derive(Debug)]
pub struct BuiltinCatalogUpdates {
    /// Each Generation an update replaced, with its Workspace and Provider.
    pub replaced: Vec<ActiveGeneration>,
    /// Each Built-in Provider whose update failed. It keeps its catalog until
    /// the next boot.
    pub failed: Vec<(ActiveGeneration, Error)>,
}

impl ProviderModule {
    /// Updates every Workspace's Built-in Provider whose Generation pins a
    /// catalog other than the one this process serves, each in its own
    /// transaction, with Coral as the recorded actor. The Provider keeps its
    /// state, in an active or an archived Workspace. Bootstrap runs it before
    /// the process takes requests. A failed update leaves the others to run.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure, or corrupt data for a Generation that
    /// pins no well-formed catalog identity, while it lists the Built-in
    /// Providers. A failed update of one Provider is in
    /// [`BuiltinCatalogUpdates::failed`] instead.
    pub async fn update_builtin_catalogs(
        &self,
        occurred_at_unix_nanos: i64,
    ) -> Result<BuiltinCatalogUpdates, Error> {
        let mut updates = BuiltinCatalogUpdates {
            replaced: Vec::new(),
            failed: Vec::new(),
        };
        for outdated in self.outdated_builtin_generations().await? {
            match self
                .update_builtin_catalog(&outdated, occurred_at_unix_nanos)
                .await
            {
                Ok(true) => updates.replaced.push(outdated),
                Ok(false) => {}
                Err(error) => updates.failed.push((outdated, error)),
            }
        }
        Ok(updates)
    }

    /// Lists the Built-in Providers' active Generations, across Workspaces,
    /// that pin a catalog other than the one this process serves.
    async fn outdated_builtin_generations(&self) -> Result<Vec<ActiveGeneration>, Error> {
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
                outdated.push(active);
            }
        }
        transaction.rollback().await?;
        Ok(outdated)
    }

    /// Replaces `outdated` with a Generation of the catalog this process
    /// serves, in its own transaction. `false` means the Provider is gone or
    /// another process replaced `outdated` first, and nothing changes.
    async fn update_builtin_catalog(
        &self,
        outdated: &ActiveGeneration,
        occurred_at_unix_nanos: i64,
    ) -> Result<bool, Error> {
        let workspace_id = outdated.workspace_id();
        let provider_id = outdated.provider_id();
        let mut transaction = self.database.begin().await?;
        // Processes that boot together each try; the first one to hold the
        // row updates it, and the others then find another Generation active.
        if !provider_repository::hold_provider_in_transaction(
            &mut transaction,
            workspace_id,
            provider_id,
        )
        .await?
        {
            return Ok(false);
        }
        let Some(stored) = provider_repository::provider_in_transaction(
            &mut transaction,
            workspace_id,
            provider_id,
        )
        .await?
        else {
            return Ok(false);
        };
        if stored.generation_id != *outdated.generation_id() {
            return Ok(false);
        }
        let generation = coral_catalog::publish_builtin_generation_in_transaction(
            &mut transaction,
            &self.builtin_catalog,
            occurred_at_unix_nanos,
        )
        .await?;
        if !provider_repository::activate_builtin_generation(
            &mut transaction,
            workspace_id,
            provider_id,
            outdated.generation_id(),
            &generation,
        )
        .await?
        {
            return Err(Error::EtagMismatch);
        }
        provider_repository::insert_lifecycle_event(
            &mut transaction,
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
        transaction.commit().await?;
        Ok(true)
    }
}
