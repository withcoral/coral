//! The model, and the Organization Owner's changes to it.

use std::sync::Arc;

use coral_credentials::{CredentialVault, Secret};
use coral_db::{Database, RowLock, Transaction, UnixNanos, Version};
use coral_outbound::DestinationPolicy;
use coral_workspace::{AuthenticatedUser, ChangeContext, IfMatch, OrganizationModule, UserId};
use uuid::Uuid;

use crate::error::Error;
use crate::repository::{self, StoredModel};
use crate::settings::{EnteredSettings, ModelSettings};

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

/// Reads and changes the model, for an Organization Owner.
#[derive(Clone)]
pub struct AgentModels {
    inner: Arc<Inner>,
}

struct Inner {
    database: Arc<Database>,
    organization: OrganizationModule,
    vault: CredentialVault,
    policy: DestinationPolicy,
}

impl AgentModels {
    /// Composes the module. `policy` is the operator's, which a saved base
    /// URL must meet.
    #[must_use]
    pub fn new(database: Arc<Database>, vault: CredentialVault, policy: DestinationPolicy) -> Self {
        Self {
            inner: Arc::new(Inner {
                organization: OrganizationModule::new(Arc::clone(&database)),
                database,
                vault,
                policy,
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

    /// Sets the model of an Organization that has none.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, before it
    /// checks the settings. Returns [`Error::Invalid`] with the setting to
    /// correct, and [`Error::AlreadySet`] when there is a model. All change
    /// nothing.
    pub async fn set(
        &self,
        context: &ChangeContext,
        entered: &EnteredSettings,
        api_key: Option<Secret>,
    ) -> Result<AgentModel, Error> {
        let mut tx = self.begin_change(context).await?;
        let settings = ModelSettings::parse(self.inner.policy, entered)?;
        let stored = self.sealed(
            context,
            Uuid::new_v4(),
            Version::FIRST,
            settings,
            api_key.as_ref(),
        )?;
        if !repository::insert(&mut tx, &stored).await? {
            return Err(Error::AlreadySet);
        }
        tx.commit().await?;
        Ok(stored.model)
    }

    /// Changes the model that the caller read at `if_match`. A new base URL
    /// issues a new id and drops the key that was stored.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, before it
    /// checks the settings. Returns [`Error::Invalid`] with the setting to
    /// correct, [`Error::NotSet`] when there is no model and
    /// [`Error::EtagMismatch`] when it changed since. All change nothing.
    pub async fn change(
        &self,
        context: &ChangeContext,
        if_match: &IfMatch,
        entered: &EnteredSettings,
        api_key: ApiKeyChange,
    ) -> Result<AgentModel, Error> {
        let mut tx = self.begin_change(context).await?;
        let settings = ModelSettings::parse(self.inner.policy, entered)?;
        let current = held(&mut tx, if_match).await?;
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
        repository::replace(&mut tx, &current.model, &next).await?;
        tx.commit().await?;
        Ok(next.model)
    }

    /// Removes the model that the caller read at `if_match`, and its key.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, and returns
    /// [`Error::NotSet`] when there is no model and [`Error::EtagMismatch`]
    /// when it changed since. All change nothing.
    pub async fn remove(&self, context: &ChangeContext, if_match: &IfMatch) -> Result<(), Error> {
        let mut tx = self.begin_change(context).await?;
        let current = held(&mut tx, if_match).await?;
        repository::delete(&mut tx, &current.model).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Opens a change that holds the actor's Organization Owner authority.
    async fn begin_change(&self, context: &ChangeContext) -> Result<Transaction<'_>, Error> {
        let mut tx = self.inner.database.begin().await?;
        self.inner
            .organization
            .check_owner_in_transaction(&mut tx, context.actor())
            .await?;
        Ok(tx)
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

/// The record a model's API key is sealed for: its id and its base URL.
pub(crate) fn key_record(id: Uuid, base_url: &str) -> String {
    format!("sub-agent-model:{id}:{base_url}")
}
