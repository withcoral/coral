//! The Built-in Provider: "Coral" at the Call Root `coral.workspace`.
//!
//! Every Workspace gets one when it is created, with its own Generation of the
//! Coral-defined catalog. It has no Setup, no Publication and no Accounts.
//! When Coral boots, it installs one in each Workspace that has none, such as
//! a Workspace created before Coral had it, and updates every Workspace's
//! Built-in Provider to the catalog it serves.
use coral_catalog::ProviderGenerationId;
use coral_db::Transaction;
use coral_workspace::{ChangeContext, TraceId, WorkspaceId};

use crate::change::{Committed, SearchChange};
use crate::repository as provider_repository;
use crate::{
    ActivatedGeneration, ActiveGeneration, CredentialRoute, Error, LifecycleAction, LifecycleActor,
    LifecycleOutcome, Provider, ProviderAccess, ProviderCallRoot, ProviderDisplayName, ProviderId,
    ProviderModule, ProviderState, access,
};

impl ProviderModule {
    /// Creates the Workspace's Built-in Provider in the caller's transaction,
    /// with the acting Owner as its recorded actor.
    ///
    /// Workspace creation calls this in the transaction that creates the
    /// Workspace. It joins that transaction and never commits, so the caller
    /// hands the returned [`Committed`]'s search change to Universal Search
    /// after its own commit.
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
    ) -> Result<Committed<Provider>, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(transaction, context.actor(), workspace_id)
            .await?;
        let Some(access) = prepare_builtin_access(transaction, workspace_id).await? else {
            return Err(Error::ProviderCallRootConflict);
        };
        self.insert_builtin(
            transaction,
            access,
            LifecycleActor::User {
                user_id: context.actor().user_id().clone(),
                authority_source: authority,
            },
            context.occurred_at_unix_nanos(),
            context.trace_id(),
        )
        .await
        .map(|(provider, generation)| first_generation(provider, generation))
    }

    /// Lists the Workspaces, in any state, that have no Built-in Provider,
    /// such as one created before Coral had it.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data failure.
    pub async fn workspaces_without_builtin(&self) -> Result<Vec<WorkspaceId>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let installed = provider_repository::builtin_active_generations(&mut transaction)
            .await?
            .into_iter()
            .map(|active| active.workspace_id().clone())
            .collect::<std::collections::HashSet<_>>();
        let missing = self
            .workspaces
            .workspace_ids_in_transaction(&mut transaction)
            .await?
            .into_iter()
            .filter(|workspace_id| !installed.contains(workspace_id))
            .collect();
        transaction.rollback().await?;
        Ok(missing)
    }

    /// Creates the Built-in Provider of a Workspace that has none, in its own
    /// transaction, with Coral as the recorded actor. The Workspace may be in
    /// any state. `None` means the Workspace is gone or another process
    /// installed it first, and nothing changes.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure.
    pub async fn install_missing_builtin(
        &self,
        workspace_id: &WorkspaceId,
        occurred_at_unix_nanos: i64,
    ) -> Result<Committed<Option<Provider>>, Error> {
        let mut transaction = self.database.begin().await?;
        // Processes that boot together each try; the first one to hold the
        // Workspace installs it, and the others then find it installed.
        if !self
            .workspaces
            .hold_in_transaction(&mut transaction, workspace_id)
            .await?
        {
            transaction.rollback().await?;
            return Ok(Committed::unchanged(None));
        }
        let Some(access) = prepare_builtin_access(&mut transaction, workspace_id).await? else {
            transaction.rollback().await?;
            return Ok(Committed::unchanged(None));
        };
        let (provider, generation) = self
            .insert_builtin(
                &mut transaction,
                access,
                LifecycleActor::Coral,
                occurred_at_unix_nanos,
                &TraceId::current_or_new(),
            )
            .await?;
        transaction.commit().await?;
        let committed = first_generation(provider, generation);
        Ok(Committed {
            value: Some(committed.value),
            search: committed.search,
        })
    }

    /// Writes the Built-in Provider with a Generation of the catalog this
    /// process serves, and the event that records who installed it.
    async fn insert_builtin(
        &self,
        transaction: &mut Transaction<'_>,
        access: ProviderAccess,
        actor: LifecycleActor,
        occurred_at_unix_nanos: i64,
        trace_id: &TraceId,
    ) -> Result<(Provider, ProviderGenerationId), Error> {
        let workspace_id = access.workspace_id().clone();
        let generation = coral_catalog::publish_builtin_generation_in_transaction(
            transaction,
            &self.builtin_catalog,
            occurred_at_unix_nanos,
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
            occurred_at_unix_nanos,
        )
        .await?;
        provider_repository::insert_lifecycle_event(
            transaction,
            provider_repository::NewLifecycleEvent {
                workspace_id: Some(&workspace_id),
                actor,
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
                occurred_at_unix_nanos,
                trace_id,
            },
        )
        .await?;
        Ok((provider, generation))
    }
}

/// A new Built-in Provider, whose first Generation search must index.
fn first_generation(provider: Provider, generation: ProviderGenerationId) -> Committed<Provider> {
    Committed {
        search: SearchChange::Activated(ActivatedGeneration::new(
            provider.workspace_id().clone(),
            provider.id().clone(),
            generation,
            None,
        )),
        value: provider,
    }
}

/// The Built-in Provider's access, or `None` when the Workspace already has one.
async fn prepare_builtin_access(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Option<ProviderAccess>, Error> {
    Ok(
        match access::prepare(
            transaction,
            workspace_id,
            ProviderId::generate(),
            ProviderCallRoot::Builtin,
            CredentialRoute::None,
        )
        .await?
        {
            access::PreparedProviderAccess::Ready(access) => Some(access),
            access::PreparedProviderAccess::CallRootConflict => None,
        },
    )
}

impl ProviderModule {
    /// Lists the Built-in Providers' active Generations, across Workspaces and
    /// in any state, that pin a catalog other than the one this process serves.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure, or corrupt data for a Generation that
    /// pins no well-formed catalog identity.
    pub async fn outdated_builtin_generations(&self) -> Result<Vec<ActiveGeneration>, Error> {
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
    /// serves, in its own transaction, with Coral as the recorded actor. The
    /// Provider keeps its state, in an active or an archived Workspace.
    /// `false` means the Provider is gone or another process replaced
    /// `outdated` first, and nothing changes.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data failure.
    pub async fn update_builtin_catalog(
        &self,
        outdated: &ActiveGeneration,
        occurred_at_unix_nanos: i64,
    ) -> Result<Committed<bool>, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(generation) = self
            .replace_builtin_generation(&mut transaction, outdated, occurred_at_unix_nanos)
            .await?
        else {
            transaction.rollback().await?;
            return Ok(Committed::unchanged(false));
        };
        transaction.commit().await?;
        Ok(Committed {
            value: true,
            search: SearchChange::Activated(ActivatedGeneration::new(
                outdated.workspace_id().clone(),
                outdated.provider_id().clone(),
                generation,
                Some(outdated.generation_id().clone()),
            )),
        })
    }

    /// The Generation that replaced `outdated`, or `None` when another
    /// process replaced it first or the Provider is gone.
    async fn replace_builtin_generation(
        &self,
        transaction: &mut Transaction<'_>,
        outdated: &ActiveGeneration,
        occurred_at_unix_nanos: i64,
    ) -> Result<Option<ProviderGenerationId>, Error> {
        let workspace_id = outdated.workspace_id();
        let provider_id = outdated.provider_id();
        // Processes that boot together each try; the first one to hold the
        // row replaces the Generation, and the others then find another active.
        let Some(stored) = provider_repository::current_provider(
            transaction,
            workspace_id,
            provider_repository::ProviderKey::Id(provider_id),
            provider_repository::ProviderHold::Exclusive,
        )
        .await?
        else {
            return Ok(None);
        };
        if stored.generation_id != *outdated.generation_id() {
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
            outdated.generation_id(),
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
        Ok(Some(generation))
    }
}
