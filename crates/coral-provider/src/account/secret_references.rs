//! Credential Fields a Secret Store holds (ADR 0036). A Workspace Owner names a
//! store granted to the Workspace, and Coral reads the value once to check it.
//! Each dispatch reads it again, outside every lock, and nothing keeps a copy.
//! Provider setup does the same for its shared Account's fields, and Publication
//! copies the references.
use std::collections::BTreeMap;

use coral_credentials::{CredentialField, CredentialValues, Secret, SecretReference};
use coral_db::Transaction;
use coral_secret_store::Error as StoreError;
use coral_telemetry::metric::{self, SecretStoreRead};
use coral_telemetry::safe;
use coral_workspace::{ChangeContext, WorkspaceId};

use super::{
    AccountModule, Authorization, AuthorizationId, CredentialMaterial, check_values, expire,
    load_authorization,
};
use crate::{Error, ProviderSetupId};
use coral_catalog::ProviderCredentialContract;

/// The name of a store this deployment defines.
struct DefinedStoreName<'a>(&'a str);

impl std::fmt::Display for DefinedStoreName<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Only a read from a store found in the registry makes one, so it names a store
/// the operator defined.
impl safe::Identifier for DefinedStoreName<'_> {}

/// Names the store a read went to, which the read metric leaves out.
fn log_read(
    store: &DefinedStoreName<'_>,
    kind: &'static str,
    read: SecretStoreRead,
    elapsed: std::time::Duration,
) {
    if matches!(read, SecretStoreRead::Read) {
        tracing::info!(
            coral.secret_store.name = safe::id(store),
            coral.secret_store.kind = safe::kind(kind),
            coral.secret_store.read_ms = safe::number(elapsed.as_millis()),
            "read a value from a Secret Store"
        );
    } else {
        tracing::warn!(
            coral.secret_store.name = safe::id(store),
            coral.secret_store.kind = safe::kind(kind),
            coral.secret_store.read_ms = safe::number(elapsed.as_millis()),
            error.type = safe::kind(read.as_str()),
            "failed to read a value from a Secret Store"
        );
    }
}

impl AccountModule {
    /// Completes secret entry when each Credential Field is either a typed value
    /// or a Secret Reference. A reference is checked by reading it outside any
    /// transaction; the value is discarded, and only a passing check saves.
    /// # Errors
    /// Rejects a reference on anything but a shared Account, a store not granted
    /// to the Workspace, and a reference the store cannot read, naming the cause.
    /// A rejected submission leaves the Account and its Authorization unchanged.
    pub async fn submit_fields(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        fields: BTreeMap<String, CredentialField>,
    ) -> Result<Authorization, Error> {
        let (values, references) = split_fields(fields);
        if references.is_empty() {
            let values = CredentialValues::new(values).map_err(|_invalid| Error::AccountInput)?;
            return self.submit_secret(context, owner, id, values).await;
        }
        if let Some(contract) = self
            .admit_references(context, owner, id, &references)
            .await?
        {
            self.check_read_once(&contract, &values, &references)
                .await?;
        }
        self.complete(
            context,
            owner,
            id,
            CredentialMaterial::Fields { values, references },
        )
        .await
    }

    /// Saves Credential Fields for one Setup, for the shared Account Publication
    /// would make. A reference is read once to check it, outside any transaction,
    /// and the value is discarded. Discovery and OAuth methods take no reference.
    /// # Errors
    /// The refusals of [`Self::submit_fields`], and [`Error::SecretReferenceNotAllowed`]
    /// while the Setup awaits discovery. A refusal leaves the Setup unchanged.
    pub async fn authorize_setup_fields(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        configuration_revision: i64,
        fields: BTreeMap<String, CredentialField>,
    ) -> Result<super::setup::SetupAuthentication, Error> {
        let (values, references) = split_fields(fields);
        if references.is_empty() {
            let values = CredentialValues::new(values).map_err(|_invalid| Error::AccountInput)?;
            return self
                .authorize_setup(context, owner, id, configuration_revision, values)
                .await;
        }
        let contract = self
            .admit_setup_fields(context, owner, id, configuration_revision, &references)
            .await?;
        self.check_read_once(&contract, &values, &references)
            .await?;
        self.save_setup_material(
            context,
            owner,
            id,
            configuration_revision,
            CredentialMaterial::Fields { values, references },
        )
        .await
    }

    /// The contract to check a Setup's references against, before any store read.
    async fn admit_setup_fields(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        configuration_revision: i64,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<ProviderCredentialContract, Error> {
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        let evidence = self
            .providers
            .workspaces
            .check_in_transaction(
                &mut tx,
                context.actor(),
                owner,
                coral_workspace::WorkspaceAction::Configure,
            )
            .await?;
        crate::require_owner(&evidence)?;
        let loaded = crate::started_setup(&mut tx, context.actor(), owner, id).await?;
        if loaded.setup.state() == crate::ProviderSetupState::AwaitingAuthorization
            && loaded.setup.input_kind() == coral_catalog::ProviderSetupInputKind::Mcp
        {
            return Err(Error::SecretReferenceNotAllowed);
        }
        crate::require_ready(&loaded.setup)?;
        if loaded.configuration.is_none() {
            return Err(Error::ProviderSetupNotReady);
        }
        if loaded.configuration_revision != configuration_revision {
            return Err(Error::EtagMismatch);
        }
        let contract =
            crate::configuration::effective_contract(self.providers.outbound_policy, &loaded)?;
        self.admit_setup_references(&mut tx, owner, &contract, references)
            .await?;
        tx.commit().await?;
        Ok(contract)
    }

    /// Reads each reference once and checks the values fit the method. The values
    /// are discarded here, and a failed read names its cause.
    async fn check_read_once(
        &self,
        contract: &ProviderCredentialContract,
        values: &BTreeMap<String, Secret>,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<(), Error> {
        // A field the method does not name is refused before any store is read.
        super::check_field_names(
            contract,
            values.keys().chain(references.keys()).map(String::as_str),
        )?;
        let mut checked = BTreeMap::new();
        for (name, value) in values {
            checked.insert(name.clone(), Secret::new(value.expose().to_owned())?);
        }
        for (name, reference) in references {
            checked.insert(name.clone(), self.read_reference(reference).await?);
        }
        let checked = CredentialValues::new(checked).map_err(|_invalid| Error::AccountInput)?;
        check_values(contract, &checked)
    }

    /// The contract to check references against, or `None` when the Authorization
    /// has ended, so completion reports its settled state and nothing is read.
    async fn admit_references(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<Option<ProviderCredentialContract>, Error> {
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .scope_authority(&mut tx, context.actor(), owner)
            .await?;
        let mut row = load_authorization(&mut tx, owner, id).await?;
        self.check_authorization_actor(&mut tx, context.actor(), owner, &row)
            .await?;
        expire(&mut tx, context, owner, authority, &mut row).await?;
        if !row.state.is_open() {
            tx.commit().await?;
            return Ok(None);
        }
        self.admit_references_in_transaction(&mut tx, owner, &row, references)
            .await?;
        tx.commit().await?;
        Ok(Some(row.contract))
    }

    /// A reference fits only a shared Account, on a method with Credential Fields,
    /// and names a store config defines and the Workspace holds a grant for.
    pub(super) async fn admit_references_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        row: &super::LoadedAuthorization,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<(), Error> {
        // OAuth names no Credential Fields, so it never takes a reference.
        if row.contract.authentication().credential_fields().is_empty() {
            return Err(Error::AccountInput);
        }
        if row.intent.caller.is_some() {
            return Err(Error::SecretReferenceNotAllowed);
        }
        self.admit_stores(tx, owner, references).await
    }

    /// The same check for a Setup, which decides its Account's scope at Publication.
    pub(super) async fn admit_setup_references(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        contract: &ProviderCredentialContract,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<(), Error> {
        if contract.authentication().credential_fields().is_empty() {
            return Err(Error::AccountInput);
        }
        self.admit_stores(tx, owner, references).await
    }

    async fn admit_stores(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<(), Error> {
        for reference in references.values() {
            if !self.store_usable(tx, owner, reference).await? {
                return Err(Error::SecretStoreNotGranted);
            }
        }
        Ok(())
    }

    /// Whether config defines the reference's store and the Workspace holds a grant.
    pub(super) async fn store_usable(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        reference: &SecretReference,
    ) -> Result<bool, Error> {
        Ok(self.secret_stores.get(reference.store()).is_some()
            && self
                .providers
                .workspaces
                .secret_store_granted_in_transaction(tx, owner, reference.store())
                .await?)
    }

    /// Reads one value and records the read, with no path, key or value. A store
    /// config no longer defines leaves no usable Account.
    pub(super) async fn read_reference(
        &self,
        reference: &SecretReference,
    ) -> Result<Secret, Error> {
        let name = reference.store();
        let store = self.secret_stores.get(name).ok_or(Error::AccountRequired)?;
        let kind = self
            .secret_stores
            .kind(name)
            .ok_or(Error::AccountRequired)?;
        let started = std::time::Instant::now();
        let result = store.read(reference).await;
        let elapsed = started.elapsed();
        let read = result
            .as_ref()
            .map_or_else(|error| outcome(*error), |_value| SecretStoreRead::Read);
        metric::secret_store_read(kind.telemetry_name(), read, elapsed);
        log_read(
            &DefinedStoreName(name),
            kind.telemetry_name(),
            read,
            elapsed,
        );
        result.map_err(Error::SecretStoreRead)
    }

    /// The values to send for a store-backed Account, read outside every lock.
    pub(super) async fn read_fields(
        &self,
        contract: &ProviderCredentialContract,
        fields: BTreeMap<String, CredentialField>,
    ) -> Result<CredentialValues, Error> {
        let mut values = BTreeMap::new();
        for (name, field) in fields {
            let value = match field {
                CredentialField::Value(secret) => secret,
                CredentialField::Reference(reference) => self.read_reference(&reference).await?,
            };
            values.insert(name, value);
        }
        // Each read is what the service would see, so one that no longer fits the
        // method leaves no usable Account, as a stored value would.
        let values = CredentialValues::new(values).map_err(|_invalid| Error::AccountRequired)?;
        check_values(contract, &values).map_err(|_invalid| Error::AccountRequired)?;
        Ok(values)
    }
}

/// Typed values and references, by field name.
fn split_fields(
    fields: BTreeMap<String, CredentialField>,
) -> (BTreeMap<String, Secret>, BTreeMap<String, SecretReference>) {
    let mut values = BTreeMap::new();
    let mut references = BTreeMap::new();
    for (name, field) in fields {
        match field {
            CredentialField::Value(secret) => {
                values.insert(name, secret);
            }
            CredentialField::Reference(reference) => {
                references.insert(name, reference);
            }
        }
    }
    (values, references)
}

const fn outcome(error: StoreError) -> SecretStoreRead {
    match error {
        StoreError::Unreachable => SecretStoreRead::Unreachable,
        StoreError::EntryNotFound => SecretStoreRead::EntryNotFound,
        StoreError::KeyNotFound => SecretStoreRead::KeyNotFound,
        StoreError::PermissionDenied => SecretStoreRead::PermissionDenied,
        StoreError::MalformedResponse => SecretStoreRead::MalformedResponse,
        StoreError::NeedsEnterprise => SecretStoreRead::NeedsEnterprise,
        StoreError::KindMissing => SecretStoreRead::KindMissing,
    }
}
