// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! Secret Store kinds that need the Enterprise Edition (ADR 0036).
//!
//! [`KvV2Kind`] builds `HashiCorp` Vault and `OpenBao` KV version 2 stores. Each
//! store asks the Edition on every read: while it is Community, the read fails
//! with [`Error::NeedsEnterprise`] before anything is sent, and nothing is
//! deleted.

use std::sync::Arc;

use coral_edition::EditionCheck;
use coral_secret_store::{
    BuildEnvironment, Error, NamedVariables, ReferenceForm, Secret, SecretReference, SecretStore,
    SecretStoreFactory, SecretStoreKind, SettingError, StoreSettings, StoreStatus, env_var_name,
};
use serde::Deserialize;

pub mod kv_v2;

use kv_v2::{AppRole, KvV2Settings, KvV2Store, MountPath, StoreAddress};

/// The `kv-v2` store kind.
pub struct KvV2Kind {
    kind: SecretStoreKind,
    edition: EditionCheck,
}

impl KvV2Kind {
    /// Builds stores that work only while `edition` says Enterprise.
    ///
    /// # Panics
    /// Never: the kind's name is a valid token.
    #[must_use]
    pub fn new(edition: EditionCheck) -> Self {
        Self {
            kind: SecretStoreKind::new("kv-v2", "Vault KV v2").expect("a valid kind name"),
            edition,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSettings {
    address: String,
    mount: String,
    auth: RawAuth,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuth {
    method: RawAuthMethod,
    /// Where the auth method is mounted; `approle` by default.
    mount: Option<String>,
    role_id: String,
    secret_id_env: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawAuthMethod {
    Approle,
}

/// One store's checked settings. Holds no secret: the secret ID is read only
/// when the store is built.
struct Definition {
    address: StoreAddress,
    mount: MountPath,
    auth_mount: MountPath,
    role_id: String,
    secret_id_env: String,
}

fn definition(settings: &StoreSettings) -> Result<Definition, SettingError> {
    let raw: RawSettings = settings.decode()?;
    let address = StoreAddress::parse(&raw.address)
        .map_err(|error| SettingError::new("address", error.to_string()))?;
    let mount = MountPath::parse(&raw.mount)
        .map_err(|error| SettingError::new("mount", error.to_string()))?;
    let auth_mount = MountPath::parse(raw.auth.mount.as_deref().unwrap_or("approle"))
        .map_err(|error| SettingError::new("auth.mount", error.to_string()))?;
    let role_id = raw.auth.role_id.trim();
    if role_id.is_empty() || role_id.chars().any(char::is_control) {
        return Err(SettingError::new(
            "auth.role_id",
            "must not be empty or hold control characters",
        ));
    }
    let secret_id_env = env_var_name("auth.secret_id_env", &raw.auth.secret_id_env)?;
    let RawAuthMethod::Approle = raw.auth.method;
    Ok(Definition {
        address,
        mount,
        auth_mount,
        role_id: role_id.to_owned(),
        secret_id_env,
    })
}

impl SecretStoreFactory for KvV2Kind {
    fn kind(&self) -> &SecretStoreKind {
        &self.kind
    }

    fn validate(
        &self,
        settings: &StoreSettings,
        _named: &NamedVariables,
    ) -> Result<(), SettingError> {
        definition(settings).map(drop)
    }

    fn build(
        &self,
        settings: &StoreSettings,
        environment: &BuildEnvironment<'_>,
    ) -> Result<Arc<dyn SecretStore>, SettingError> {
        let store = definition(settings)?;
        let secret_id = environment.secret("auth.secret_id_env", &store.secret_id_env)?;
        let client = coral_outbound::Client::new(environment.policy, kv_v2::REQUEST_TIMEOUT)
            .map_err(|_error| SettingError::new("", "secret store client initialization failed"))?;
        Ok(Arc::new(EnterpriseOnly {
            edition: self.edition.clone(),
            store: KvV2Store::new(
                client,
                KvV2Settings {
                    address: store.address,
                    mount: store.mount,
                    auth: AppRole {
                        mount: store.auth_mount,
                        role_id: store.role_id,
                        secret_id,
                    },
                },
            ),
        }))
    }
}

/// A store that reads only while the Edition is Enterprise.
struct EnterpriseOnly<S> {
    edition: EditionCheck,
    store: S,
}

#[async_trait::async_trait]
impl<S: SecretStore> SecretStore for EnterpriseOnly<S> {
    fn reference_form(&self) -> ReferenceForm {
        self.store.reference_form()
    }

    async fn read(&self, reference: &SecretReference) -> Result<Secret, Error> {
        if !self.edition.is_enterprise() {
            return Err(Error::NeedsEnterprise);
        }
        self.store.read(reference).await
    }

    fn status(&self) -> StoreStatus {
        if self.edition.is_enterprise() {
            self.store.status()
        } else {
            StoreStatus::NeedsEnterprise
        }
    }
}

#[cfg(test)]
mod tests;
