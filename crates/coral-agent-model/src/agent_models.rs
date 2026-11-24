//! The model, and the Organization Owner's changes to it.

use std::sync::{Arc, OnceLock};

use coral_credentials::{CredentialVault, Secret};
use coral_db::{Database, RowLock, Transaction, UnixNanos, Version};
use coral_outbound::DestinationPolicy;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, IfMatch, OrganizationModule, UserId, WorkspaceAction,
    WorkspaceId, WorkspaceModule,
};
use uuid::Uuid;

use crate::error::Error;
use crate::repository::{self, StoredModel};
use crate::settings::{EnteredSettings, ModelSettings};
use crate::tester::{ModelTester, TestFailure, TestKey};

/// The model that the Organization's Sub-agents use. It never holds the API
/// key.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentModel {
    pub(crate) id: Uuid,
    pub(crate) version: Version,
    pub(crate) settings: ModelSettings,
    pub(crate) has_api_key: bool,
    pub(crate) changed_by: UserId,
    pub(crate) changed_at: UnixNanos,
}

impl AgentModel {
    /// Names where the model's requests go. See the crate's invariants.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// The etag of the model as it was read: a deterministic, non-authorizing
    /// UUID derived from the id and the version, as a Provider's is. So an
    /// etag read before a removal admits no model set after it.
    #[must_use]
    pub fn etag(&self) -> Uuid {
        Uuid::new_v5(&self.id, self.version.to_string().as_bytes())
    }

    /// What the Owner set, but the key.
    #[must_use]
    pub const fn settings(&self) -> &ModelSettings {
        &self.settings
    }

    /// Whether an API key is stored.
    #[must_use]
    pub const fn has_api_key(&self) -> bool {
        self.has_api_key
    }

    /// Who last changed the model.
    #[must_use]
    pub const fn changed_by(&self) -> &UserId {
        &self.changed_by
    }

    /// When the model last changed.
    #[must_use]
    pub const fn changed_at(&self) -> UnixNanos {
        self.changed_at
    }
}

/// The model as a Turn uses it: its settings, and its API key opened.
#[derive(Debug)]
pub struct ActiveModel {
    /// Names where the model's requests go.
    pub id: Uuid,
    /// What the Owner set, but the key.
    pub settings: ModelSettings,
    /// The API key, when one is stored.
    pub api_key: Option<Secret>,
}

/// What a change does to the stored API key.
#[derive(Debug)]
pub enum ApiKeyChange {
    /// Leaves it, unless the base URL changes, which drops it.
    Keep,
    /// Stores this one.
    Set(Secret),
    /// Removes it.
    Clear,
}

/// A model after a change.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelChange {
    /// The model as it is stored now.
    pub model: AgentModel,
    /// Whether the change issued a new id, which no Workspace agreed to.
    pub moved: bool,
}

/// Reads and changes the model, for an Organization Owner.
#[derive(Clone)]
pub struct AgentModels {
    inner: Arc<Inner>,
}

struct Inner {
    database: Arc<Database>,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    vault: CredentialVault,
    policy: DestinationPolicy,
    /// Set once, when the Sub-agent is built.
    tester: OnceLock<Arc<dyn ModelTester>>,
}

impl AgentModels {
    /// Composes the module. `policy` is the operator's, which a saved base
    /// URL must meet.
    #[must_use]
    pub fn new(database: Arc<Database>, vault: CredentialVault, policy: DestinationPolicy) -> Self {
        Self {
            inner: Arc::new(Inner {
                organization: OrganizationModule::new(Arc::clone(&database)),
                workspaces: WorkspaceModule::new(Arc::clone(&database)),
                database,
                vault,
                policy,
                tester: OnceLock::new(),
            }),
        }
    }

    /// The model, or `None` while the Organization has none.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner.
    pub async fn read(&self, actor: &AuthenticatedUser) -> Result<Option<AgentModel>, Error> {
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        self.inner
            .organization
            .check_owner_inspection_in_transaction(&mut tx, actor)
            .await?;
        let stored = repository::find(&mut tx, RowLock::None).await?;
        tx.commit().await?;
        Ok(stored.map(|stored| stored.model))
    }

    /// Gives the module what tests a model. The Sub-agent's factory calls it
    /// once; the first tester stays.
    pub fn set_tester(&self, tester: Arc<dyn ModelTester>) {
        self.inner.tester.get_or_init(|| tester);
    }

    /// Asks a model for one reply with the settings as entered, saved or not.
    /// The outer error says why no request was sent, the inner one why the
    /// model did not answer. It changes nothing.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, before it
    /// checks the settings. Returns [`Error::Invalid`] with the setting to
    /// correct, and [`Error::NoTester`] in a process with no Sub-agent.
    pub async fn test(
        &self,
        actor: &AuthenticatedUser,
        entered: &EnteredSettings,
        key: TestKey,
    ) -> Result<Result<(), TestFailure>, Error> {
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        self.inner
            .organization
            .check_owner_inspection_in_transaction(&mut tx, actor)
            .await?;
        let settings = ModelSettings::parse(self.inner.policy, entered)?;
        let key = match key {
            TestKey::Entered(key) => Some(key),
            TestKey::Absent => None,
            // The stored key is only ever sent to the base URL that it was
            // saved with.
            TestKey::Stored => self
                .active_in_transaction(&mut tx)
                .await?
                .filter(|active| active.settings.base_url == settings.base_url)
                .and_then(|active| active.api_key),
        };
        tx.commit().await?;
        let tester = self.inner.tester.get().ok_or(Error::NoTester)?;
        Ok(tester.test(&settings, key).await)
    }

    /// The model, for a Workspace Owner to see what they agree to by turning
    /// `coral.agent` on. It shows them no key, and nothing else of the
    /// Organization.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not the Workspace's Owner, and returns
    /// [`Error::WorkspaceArchived`] for an archived one, or [`Error::NotSet`]
    /// while the Organization has no model.
    pub async fn read_for_workspace(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<AgentModel, Error> {
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        let configure = WorkspaceAction::Configure;
        self.inner
            .workspaces
            .check_in_transaction(&mut tx, actor, workspace_id, configure)
            .await?;
        let stored = repository::find(&mut tx, RowLock::None).await?;
        tx.commit().await?;
        stored.map(|stored| stored.model).ok_or(Error::NotSet)
    }

    /// The model as a Turn uses it, or `None` while the Organization has none.
    /// The caller owns the transaction, and decides whether the Workspace may
    /// use the model.
    ///
    /// # Errors
    ///
    /// Returns a persistence failure, [`Error::CorruptData`] for a row that is
    /// not what Coral writes, or [`Error::Sealing`] for a key that does not
    /// open for the model it is stored with.
    pub async fn active_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
    ) -> Result<Option<ActiveModel>, Error> {
        let Some(stored) = repository::find(tx, RowLock::None).await? else {
            return Ok(None);
        };
        let model = stored.model;
        let record = key_record(model.id, &model.settings.base_url);
        let open = |sealed: String| {
            let opened = self.inner.vault.open_sealed(&record, &sealed);
            let opened = opened.map_err(|_cryptography| Error::Sealing)?;
            Secret::new(opened.to_string()).map_err(|_invalid| Error::Sealing)
        };
        Ok(Some(ActiveModel {
            id: model.id,
            settings: model.settings,
            api_key: stored.sealed_api_key.map(open).transpose()?,
        }))
    }

    /// Sets the model of an Organization that has none. The caller owns the
    /// transaction, and turns the Sub-agent off everywhere in it.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, before it
    /// checks the settings. Returns [`Error::Invalid`] with the setting to
    /// correct, and [`Error::AlreadySet`] when there is a model. Do not commit
    /// the transaction after an error.
    pub async fn set_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        entered: &EnteredSettings,
        api_key: Option<Secret>,
    ) -> Result<AgentModel, Error> {
        self.hold_owner(tx, context).await?;
        let settings = ModelSettings::parse(self.inner.policy, entered)?;
        let stored = self.sealed(
            context,
            Uuid::new_v4(),
            Version::FIRST,
            settings,
            api_key.as_ref(),
        )?;
        if !repository::insert(tx, &stored).await? {
            return Err(Error::AlreadySet);
        }
        Ok(stored.model)
    }

    /// Changes the model that the caller read at `if_match`. A new base URL
    /// issues a new id and drops the key that was stored. The caller owns the
    /// transaction, and turns the Sub-agent off everywhere in it when
    /// [`ModelChange::moved`] says so.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, before it
    /// checks the settings. Returns [`Error::Invalid`] with the setting to
    /// correct, [`Error::NotSet`] when there is no model and
    /// [`Error::EtagMismatch`] when it changed since. Do not commit the
    /// transaction after an error.
    pub async fn change_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        if_match: &IfMatch,
        entered: &EnteredSettings,
        api_key: ApiKeyChange,
    ) -> Result<ModelChange, Error> {
        self.hold_owner(tx, context).await?;
        let settings = ModelSettings::parse(self.inner.policy, entered)?;
        let current = held(tx, if_match).await?;
        let moved = settings.base_url != current.model.settings.base_url;
        let (id, version) = if moved {
            (Uuid::new_v4(), Version::FIRST)
        } else {
            let next = current.model.version.next();
            let next = next.ok_or(Error::CorruptData("a model's version cannot rise"))?;
            (current.model.id, next)
        };
        let mut next = match &api_key {
            ApiKeyChange::Set(key) => self.sealed(context, id, version, settings, Some(key))?,
            ApiKeyChange::Keep | ApiKeyChange::Clear => {
                self.sealed(context, id, version, settings, None)?
            }
        };
        // The stored key is sealed for the id and base URL it was saved with,
        // so it stays only where both do.
        if matches!(api_key, ApiKeyChange::Keep) && !moved {
            next.model.has_api_key = current.sealed_api_key.is_some();
            next.sealed_api_key.clone_from(&current.sealed_api_key);
        }
        repository::replace(tx, &current.model, &next).await?;
        Ok(ModelChange {
            model: next.model,
            moved,
        })
    }

    /// Removes the model that the caller read at `if_match`, and its key. The
    /// caller owns the transaction, and turns the Sub-agent off everywhere in
    /// it.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, and returns
    /// [`Error::NotSet`] when there is no model and [`Error::EtagMismatch`]
    /// when it changed since. Do not commit the transaction after an error.
    pub async fn remove_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        if_match: &IfMatch,
    ) -> Result<(), Error> {
        self.hold_owner(tx, context).await?;
        let current = held(tx, if_match).await?;
        repository::delete(tx, &current.model).await
    }

    /// Holds the actor's Organization Owner authority through the commit.
    async fn hold_owner(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
    ) -> Result<(), Error> {
        let held = self
            .inner
            .organization
            .check_owner_in_transaction(tx, context.actor());
        Ok(held.await?)
    }

    /// The row of a model, with `api_key` sealed for its id and base URL.
    fn sealed(
        &self,
        context: &ChangeContext,
        id: Uuid,
        version: Version,
        settings: ModelSettings,
        api_key: Option<&Secret>,
    ) -> Result<StoredModel, Error> {
        let record = key_record(id, &settings.base_url);
        let sealed_api_key = api_key
            .map(|key| self.inner.vault.seal(&record, key.expose()))
            .transpose()
            .map_err(|_cryptography| Error::Sealing)?;
        Ok(StoredModel {
            model: AgentModel {
                id,
                version,
                settings,
                has_api_key: sealed_api_key.is_some(),
                changed_by: context.actor().user_id().clone(),
                changed_at: context.occurred_at(),
            },
            sealed_api_key,
        })
    }
}

/// The model at `if_match`, with its row held until the change commits.
async fn held(tx: &mut Transaction<'_>, if_match: &IfMatch) -> Result<StoredModel, Error> {
    let current = repository::find(tx, RowLock::Exclusive)
        .await?
        .ok_or(Error::NotSet)?;
    if if_match.admits_token(&current.model.etag()) {
        Ok(current)
    } else {
        Err(Error::EtagMismatch)
    }
}

/// The id of the Organization's model, or `None` while it has none, holding
/// the model's row as `lock` says. It authorizes nobody: the caller decides.
///
/// # Errors
///
/// Returns a persistence failure.
pub async fn current_id_in_transaction(
    tx: &mut Transaction<'_>,
    lock: RowLock,
) -> Result<Option<Uuid>, coral_db::Error> {
    repository::find_id(tx, lock).await
}

/// The record a model's API key is sealed for: its id and its base URL.
pub(crate) fn key_record(id: Uuid, base_url: &str) -> String {
    format!("sub-agent-model:{id}:{base_url}")
}
