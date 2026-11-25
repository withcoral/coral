//! The Providers Coral defines in every Workspace: the Built-in Provider,
//! "Coral" at the Call Root `coral`, the Private Provider, "Private" at
//! `coral.private`, and the Shared Provider, "Shared" at `coral.shared`.
//!
//! Every Workspace gets one of each when it is created, each with its own
//! Generation. It has no Setup, no Publication and no Accounts. The Built-in
//! Provider's Generation holds the Coral-defined catalog. The Private and
//! Shared Providers' hold no Tool: their Tools are each Principal's Private
//! Tools and the Workspace's Shared Tools, which coral-tools keeps. When
//! Coral boots, it installs each in every Workspace that has none, such as a
//! Workspace created before Coral had it, and updates every Workspace's
//! Built-in Provider to the catalog it serves.
use coral_catalog::{AuthoredToolKind, ProviderGenerationId};
use coral_db::{Transaction, UnixNanos};
use coral_workspace::{ChangeContext, TraceId, WorkspaceId};

use crate::change::{Committed, SearchChange};
use crate::repository as provider_repository;
use crate::{
    ActivatedGeneration, ActiveGeneration, CredentialRoute, Error, LifecycleAction, LifecycleActor,
    LifecycleOutcome, Provider, ProviderAccess, ProviderCallRoot, ProviderDisplayName, ProviderId,
    ProviderModule, ProviderState, access,
};

/// A Provider that Coral defines and installs in every Workspace.
///
/// Each kind is one variant, so a rule that depends on the kind matches on
/// it, and a new kind is installed wherever [`Self::ALL`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoralDefinedProvider {
    /// "Coral" at `coral`, whose Generation holds the Coral-defined catalog.
    Builtin,
    /// "Private" at `coral.private`, whose Tools are each Principal's own
    /// Private Tools.
    Private,
    /// "Shared" at `coral.shared`, whose Tools are the Workspace's Shared
    /// Tools.
    Shared,
}

impl CoralDefinedProvider {
    /// Every Provider Coral defines, in the order Coral installs them.
    pub const ALL: [Self; 3] = [Self::Builtin, Self::Private, Self::Shared];

    /// The Call Root of this Provider in every Workspace.
    #[must_use]
    pub const fn call_root(self) -> ProviderCallRoot {
        match self {
            Self::Builtin => ProviderCallRoot::Builtin,
            Self::Private => ProviderCallRoot::Private,
            Self::Shared => ProviderCallRoot::Shared,
        }
    }

    /// The lifecycle action that records its install.
    const fn installed(self) -> LifecycleAction {
        match self {
            Self::Builtin => LifecycleAction::BuiltinProviderInstalled,
            Self::Private => LifecycleAction::PrivateProviderInstalled,
            Self::Shared => LifecycleAction::SharedProviderInstalled,
        }
    }
}

/// One Coral-defined Provider that a boot found a Workspace without, and
/// what installing it did.
#[derive(Debug)]
pub struct MissingCoralDefinedProvider {
    workspace_id: WorkspaceId,
    provider: CoralDefinedProvider,
    installed: Result<Committed<Option<Provider>>, Error>,
}

impl MissingCoralDefinedProvider {
    /// Returns the Workspace that had none.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns which Provider it lacked.
    #[must_use]
    pub const fn provider(&self) -> CoralDefinedProvider {
        self.provider
    }

    /// The outcome of the install, which ran in its own transaction: the
    /// Provider beside the change Universal Search must read, or `None` when
    /// the Workspace is gone or another process installed it first.
    ///
    /// # Errors
    ///
    /// Returns the persistence failure that left this Workspace without the
    /// Provider until the next boot.
    pub fn into_installed(self) -> Result<Committed<Option<Provider>>, Error> {
        self.installed
    }
}

impl ProviderModule {
    /// Creates every Provider Coral defines in a new Workspace, in the
    /// caller's transaction, with the acting Owner as each one's recorded
    /// actor.
    ///
    /// Workspace creation calls this in the transaction that creates the
    /// Workspace. It joins that transaction and never commits, so the caller
    /// hands each returned [`Committed`]'s search change to Universal Search
    /// after its own commit.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::install_coral_defined_in_transaction`].
    /// An error may follow writes; do not commit the transaction after one.
    pub async fn install_every_coral_defined_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Committed<Provider>>, Error> {
        let mut installed = Vec::with_capacity(CoralDefinedProvider::ALL.len());
        for provider in CoralDefinedProvider::ALL {
            installed.push(
                self.install_coral_defined_in_transaction(
                    transaction,
                    context,
                    workspace_id,
                    provider,
                )
                .await?,
            );
        }
        Ok(installed)
    }

    /// Creates one Provider Coral defines in the Workspace, in the caller's
    /// transaction, with the acting Owner as its recorded actor. It joins
    /// that transaction and never commits.
    ///
    /// The Built-in Provider's search change names its first Generation.
    /// The Private and Shared Providers' Generations declare no Tool, so
    /// theirs is [`SearchChange::Unchanged`].
    ///
    /// # Errors
    ///
    /// Returns an authority failure unless the actor is a Workspace Owner,
    /// [`Error::ProviderCallRootConflict`] when the Workspace already has
    /// this Provider, or a persistence failure. An error may follow writes;
    /// do not commit the transaction after one.
    pub async fn install_coral_defined_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        provider: CoralDefinedProvider,
    ) -> Result<Committed<Provider>, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(transaction, context.actor(), workspace_id)
            .await?;
        let Some(access) = prepare_access(transaction, workspace_id, provider).await? else {
            return Err(Error::ProviderCallRootConflict);
        };
        self.insert(
            transaction,
            access,
            provider,
            LifecycleActor::User {
                user_id: context.actor().user_id().clone(),
                authority_source: authority,
            },
            context.occurred_at(),
            context.trace_id(),
        )
        .await
    }

    /// Installs each Provider Coral defines in every Workspace, in any
    /// state, that has none, such as a Workspace created before Coral had
    /// it. Each install runs in its own transaction, with Coral as the
    /// recorded actor, so one that fails leaves the others installed.
    ///
    /// The caller hands each installed Provider's search change to Universal
    /// Search.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data failure when it cannot read
    /// which Workspaces lack a Provider. Nothing is installed then.
    pub async fn install_missing_coral_defined(
        &self,
        occurred_at: UnixNanos,
    ) -> Result<Vec<MissingCoralDefinedProvider>, Error> {
        let mut installs = Vec::new();
        for (workspace_id, provider) in self.workspaces_without_coral_defined().await? {
            let installed = self
                .install_missing(&workspace_id, occurred_at, provider)
                .await;
            installs.push(MissingCoralDefinedProvider {
                workspace_id,
                provider,
                installed,
            });
        }
        Ok(installs)
    }

    /// Each Workspace, in any state, with each Provider Coral defines that
    /// it lacks, from one read snapshot.
    async fn workspaces_without_coral_defined(
        &self,
    ) -> Result<Vec<(WorkspaceId, CoralDefinedProvider)>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let workspaces = self
            .workspaces
            .workspace_ids_in_transaction(&mut transaction)
            .await?;
        let mut missing = Vec::new();
        for provider in CoralDefinedProvider::ALL {
            let installed = provider_repository::root_active_generations(
                &mut transaction,
                &provider.call_root(),
            )
            .await?
            .into_iter()
            .map(|active| active.workspace_id().clone())
            .collect::<std::collections::HashSet<_>>();
            missing.extend(
                workspaces
                    .iter()
                    .filter(|workspace_id| !installed.contains(*workspace_id))
                    .map(|workspace_id| (workspace_id.clone(), provider)),
            );
        }
        transaction.rollback().await?;
        Ok(missing)
    }

    /// Creates `provider` in a Workspace that has none, in its own
    /// transaction, with Coral as the recorded actor. `None` means the
    /// Workspace is gone or another process installed it first, and nothing
    /// changes.
    async fn install_missing(
        &self,
        workspace_id: &WorkspaceId,
        occurred_at: UnixNanos,
        provider: CoralDefinedProvider,
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
        let Some(access) = prepare_access(&mut transaction, workspace_id, provider).await? else {
            transaction.rollback().await?;
            return Ok(Committed::unchanged(None));
        };
        let installed = self
            .insert(
                &mut transaction,
                access,
                provider,
                LifecycleActor::Coral,
                occurred_at,
                &TraceId::current_or_new(),
            )
            .await?;
        transaction.commit().await?;
        let (installed, search) = installed.into_parts();
        Ok(Committed::new(Some(installed), search))
    }

    /// Writes the Provider with its Generation and the event that records
    /// who installed it, and names what Universal Search must read once the
    /// caller commits: the Built-in Provider's first Generation. The Private
    /// and Shared Providers' Generations declare no Tool, so search has
    /// nothing of theirs to index.
    async fn insert(
        &self,
        transaction: &mut Transaction<'_>,
        access: ProviderAccess,
        kind: CoralDefinedProvider,
        actor: LifecycleActor,
        occurred_at: UnixNanos,
        trace_id: &TraceId,
    ) -> Result<Committed<Provider>, Error> {
        let workspace_id = access.workspace_id().clone();
        let (generation, display_name) = match kind {
            CoralDefinedProvider::Builtin => (
                coral_catalog::publish_builtin_generation_in_transaction(
                    transaction,
                    &self.builtin_catalog,
                    occurred_at,
                )
                .await?,
                coral_catalog::BUILTIN_PROVIDER_DISPLAY_NAME,
            ),
            CoralDefinedProvider::Private => {
                authored_generation(transaction, AuthoredToolKind::Private, occurred_at).await?
            }
            CoralDefinedProvider::Shared => {
                authored_generation(transaction, AuthoredToolKind::Shared, occurred_at).await?
            }
        };
        let provider = Provider::new(
            access,
            ProviderDisplayName::parse(display_name)?,
            ProviderState::Active,
        );
        provider_repository::insert_provider(transaction, &provider, &generation, occurred_at)
            .await?;
        provider_repository::insert_lifecycle_event(
            transaction,
            provider_repository::NewLifecycleEvent {
                workspace_id: Some(&workspace_id),
                actor,
                action: kind.installed(),
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
                occurred_at,
                trace_id,
            },
        )
        .await?;
        let search = match kind {
            CoralDefinedProvider::Builtin => SearchChange::Activated(ActivatedGeneration::new(
                provider.workspace_id().clone(),
                provider.id().clone(),
                generation,
                None,
            )),
            CoralDefinedProvider::Private | CoralDefinedProvider::Shared => SearchChange::Unchanged,
        };
        Ok(Committed::new(provider, search))
    }
}

/// The first Generation of the Workspace's Provider for Tools of `kind`,
/// which declares no Tool, and that Provider's display name.
async fn authored_generation(
    transaction: &mut Transaction<'_>,
    kind: AuthoredToolKind,
    occurred_at: UnixNanos,
) -> Result<(ProviderGenerationId, &'static str), Error> {
    let generation =
        coral_catalog::publish_authored_generation_in_transaction(transaction, kind, occurred_at)
            .await?;
    Ok((generation, kind.provider_display_name()))
}

/// The new Provider's access, or `None` when the Workspace already has one
/// at its Call Root.
async fn prepare_access(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider: CoralDefinedProvider,
) -> Result<Option<ProviderAccess>, Error> {
    Ok(
        match access::prepare(
            transaction,
            workspace_id,
            ProviderId::generate(),
            provider.call_root(),
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
    /// Lists the Built-in Providers' active Generations, of `workspace` or
    /// across Workspaces, in any state, that pin a catalog other than the one
    /// this process serves now: the Sub-agent's while the Organization has a
    /// model, if this process has a Sub-agent.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure, or corrupt data for a Generation that
    /// pins no well-formed catalog identity.
    pub async fn outdated_builtin_generations(
        &self,
        workspace: Option<&WorkspaceId>,
    ) -> Result<Vec<ActiveGeneration>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let served = self
            .builtin_catalog_in_transaction(&mut transaction)
            .await?
            .clone();
        let installed = match workspace {
            Some(workspace) => {
                provider_repository::workspace_builtin_generation(&mut transaction, workspace)
                    .await?
                    .into_iter()
                    .collect()
            }
            None => provider_repository::builtin_active_generations(&mut transaction).await?,
        };
        let mut outdated = Vec::new();
        for active in installed {
            if !coral_catalog::builtin_generation_pins_in_transaction(
                &mut transaction,
                &served,
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

    /// The Built-in catalog that this process serves now. A Workspace is
    /// installed with the base catalog, and the update that gives it the
    /// Sub-agent's Tools writes their rule in the same transaction.
    async fn builtin_catalog_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
    ) -> Result<&coral_catalog::BuiltinCatalog, Error> {
        match &self.sub_agent_catalog {
            Some(catalog) if sub_agent_model_is_set(transaction).await? => Ok(catalog),
            _ => Ok(&self.builtin_catalog),
        }
    }

    /// Whether this process now gives a Workspace the Built-in catalog that
    /// holds the Sub-agent's Tools. A Workspace's own can lag behind.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure.
    pub async fn serves_sub_agent_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
    ) -> Result<bool, Error> {
        Ok(self.sub_agent_catalog.is_some() && sub_agent_model_is_set(transaction).await?)
    }

    /// [`Self::update_builtin_catalog_in_transaction`] in a transaction of its
    /// own, for a test that writes no rule.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data failure.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn update_builtin_catalog(
        &self,
        outdated: &ActiveGeneration,
        occurred_at: UnixNanos,
    ) -> Result<Committed<bool>, Error> {
        let mut transaction = self.database.begin().await?;
        let updated = self
            .update_builtin_catalog_in_transaction(&mut transaction, outdated, occurred_at)
            .await?;
        transaction.commit().await?;
        Ok(updated.map_or_else(
            || Committed::unchanged(false),
            |committed| {
                let ((), search) = committed.into_parts();
                Committed::new(true, search)
            },
        ))
    }

    /// Replaces `outdated` with a Generation of the catalog this process
    /// serves now, with Coral as the recorded actor. The Provider keeps its
    /// state, in an active or an archived Workspace. `None` means the
    /// Provider is gone or another process replaced `outdated` first, and
    /// nothing changes.
    ///
    /// The caller owns the transaction. It writes the Sub-agent's rule there
    /// when the catalog holds its Tools, and hands the search change to
    /// Universal Search after the commit.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-data failure. Do not commit the
    /// transaction after one.
    pub async fn update_builtin_catalog_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        outdated: &ActiveGeneration,
        occurred_at: UnixNanos,
    ) -> Result<Option<Committed<()>>, Error> {
        let Some(generation) = self
            .replace_builtin_generation(transaction, outdated, occurred_at)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Committed::new(
            (),
            SearchChange::Activated(ActivatedGeneration::new(
                outdated.workspace_id().clone(),
                outdated.provider_id().clone(),
                generation,
                Some(outdated.generation_id().clone()),
            )),
        )))
    }

    /// The Generation that replaced `outdated`, or `None` when another
    /// process replaced it first or the Provider is gone.
    async fn replace_builtin_generation(
        &self,
        transaction: &mut Transaction<'_>,
        outdated: &ActiveGeneration,
        occurred_at: UnixNanos,
    ) -> Result<Option<ProviderGenerationId>, Error> {
        let workspace_id = outdated.workspace_id();
        let provider_id = outdated.provider_id();
        // Processes that boot together each try; the first one to hold the
        // row replaces the Generation, and the others then find another active.
        let Some(stored) = provider_repository::current_provider(
            transaction,
            workspace_id,
            provider_repository::ProviderKey::Id(provider_id),
            coral_db::RowLock::Exclusive,
        )
        .await?
        else {
            return Ok(None);
        };
        if stored.generation_id != *outdated.generation_id() {
            return Ok(None);
        }
        let served = self
            .builtin_catalog_in_transaction(transaction)
            .await?
            .clone();
        let generation = coral_catalog::publish_builtin_generation_in_transaction(
            transaction,
            &served,
            occurred_at,
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
                occurred_at,
                trace_id: &coral_telemetry::TraceId::current_or_new(),
            },
        )
        .await?;
        Ok(Some(generation))
    }
}

async fn sub_agent_model_is_set(transaction: &mut Transaction<'_>) -> Result<bool, Error> {
    let model =
        coral_agent_model::current_id_in_transaction(transaction, coral_db::RowLock::None).await?;
    Ok(model.is_some())
}

/// Every Workspace's Built-in Provider, in any state. The caller owns the
/// transaction and the authority to read across Workspaces.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn builtin_providers_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<(WorkspaceId, ProviderId)>, Error> {
    let active = provider_repository::builtin_active_generations(transaction).await?;
    Ok(active
        .into_iter()
        .map(|active| (active.workspace_id().clone(), active.provider_id().clone()))
        .collect())
}

/// The Workspace's Built-in Provider. The caller owns the transaction and the
/// authority to read it.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn builtin_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Option<ProviderId>, Error> {
    let active =
        provider_repository::workspace_builtin_generation(transaction, workspace_id).await?;
    Ok(active.map(|active| active.provider_id().clone()))
}
