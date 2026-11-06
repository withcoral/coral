//! Credential Fields a Secret Store holds (ADR 0032). A Workspace Owner names a
//! store granted to the Workspace, and Coral reads the value once to check it.
//! Each dispatch reads it again, outside every lock, and nothing keeps a copy.
use std::collections::BTreeMap;

use coral_credentials::{CredentialField, CredentialValues, Secret, SecretReference};
use coral_db::Transaction;
use coral_secret_store::Error as StoreError;
use coral_telemetry::metric::{self, SecretStoreRead};
use coral_workspace::{ChangeContext, WorkspaceId};

use super::{
    AccountModule, Authorization, AuthorizationId, CredentialMaterial, check_values, expire,
    load_authorization,
};
use crate::Error;
use coral_catalog::ProviderCredentialContract;

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
        if references.is_empty() {
            let values = CredentialValues::new(values).map_err(|_invalid| Error::AccountInput)?;
            return self.submit_secret(context, owner, id, values).await;
        }
        if let Some(contract) = self
            .admit_references(context, owner, id, &references)
            .await?
        {
            let mut checked = BTreeMap::new();
            for (name, value) in &values {
                checked.insert(name.clone(), Secret::new(value.expose().to_owned())?);
            }
            for (name, reference) in &references {
                checked.insert(name.clone(), self.read_reference(reference).await?);
            }
            let checked = CredentialValues::new(checked).map_err(|_invalid| Error::AccountInput)?;
            check_values(&contract, &checked)?;
        }
        self.complete(
            context,
            owner,
            id,
            CredentialMaterial::Fields { values, references },
        )
        .await
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
        metric::secret_store_read(
            name,
            kind.as_str(),
            result
                .as_ref()
                .map_or_else(|error| outcome(*error), |_value| SecretStoreRead::Read),
            started.elapsed(),
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

const fn outcome(error: StoreError) -> SecretStoreRead {
    match error {
        StoreError::Unreachable => SecretStoreRead::Unreachable,
        StoreError::EntryNotFound => SecretStoreRead::EntryNotFound,
        StoreError::KeyNotFound => SecretStoreRead::KeyNotFound,
        StoreError::PermissionDenied => SecretStoreRead::PermissionDenied,
        StoreError::MalformedResponse => SecretStoreRead::MalformedResponse,
    }
}
