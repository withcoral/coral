// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! Secret Store kinds that need the Enterprise Edition (ADR 0036).
//!
//! [`KvV2Kind`] builds `HashiCorp` Vault and `OpenBao` KV version 2 stores, and
//! [`AwsSecretsManagerKind`] AWS Secrets Manager stores. Each store asks the
//! Edition on every read: while it is Community, the read fails with
//! [`Error::NeedsEnterprise`] before anything is sent, and nothing is deleted.

use std::sync::Arc;

use coral_edition::EditionCheck;
use coral_secret_store::{
    BuildEnvironment, Error, NamedVariables, ReferenceForm, Secret, SecretReference, SecretStore,
    SecretStoreFactory, SecretStoreKind, SecretStoreName, SettingError, StoreSettings, StoreStatus,
    env_var_name,
};
use serde::Deserialize;

pub mod aws_secrets_manager;
pub mod kv_v2;
mod store_http;

use aws_secrets_manager::{SecretsManagerSettings, SecretsManagerStore};
use kv_v2::{AppRole, KvV2Settings, KvV2Store, MountPath, StoreAddress};

/// The Enterprise Feature every kind here is part of: reading credentials
/// from a secrets system outside Coral. The `env` kind every build has is
/// not part of it.
pub const EXTERNAL_SECRET_STORES: coral_edition::EnterpriseFeature =
    coral_edition::EnterpriseFeature::new(
        coral_edition::EnterpriseFeatureId::new("external-secret-stores"),
        "External secret stores",
    );

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
        _name: &SecretStoreName,
        settings: &StoreSettings,
        _named: &NamedVariables,
    ) -> Result<(), SettingError> {
        definition(settings).map(drop)
    }

    fn build(
        &self,
        _name: &SecretStoreName,
        settings: &StoreSettings,
        environment: &BuildEnvironment,
    ) -> Result<Arc<dyn SecretStore>, SettingError> {
        let store = definition(settings)?;
        let secret_id = environment.secret("auth.secret_id_env", &store.secret_id_env)?;
        let client = store_http::client(environment.policy)?;
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

/// The `aws_secrets_manager` store kind.
pub struct AwsSecretsManagerKind {
    kind: SecretStoreKind,
    edition: EditionCheck,
}

impl AwsSecretsManagerKind {
    /// Builds stores that work only while `edition` says Enterprise.
    ///
    /// # Panics
    /// Never: the kind's name is a valid token.
    #[must_use]
    pub fn new(edition: EditionCheck) -> Self {
        Self {
            kind: SecretStoreKind::new("aws_secrets_manager", "AWS Secrets Manager")
                .expect("a valid kind name"),
            edition,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAwsSettings {
    account_id: String,
    region: String,
    profile: Option<String>,
    role_arn: Option<String>,
    endpoint: Option<String>,
}

fn aws_definition(
    name: &SecretStoreName,
    settings: &StoreSettings,
    aws: &coral_aws::Environment,
) -> Result<SecretsManagerSettings, SettingError> {
    let raw: RawAwsSettings = settings.decode()?;
    let aws_error =
        |error: coral_aws::SettingsError| SettingError::new(error.field(), error.to_string());
    let checked = coral_aws::Settings::new(
        &raw.account_id,
        &raw.region,
        raw.profile.as_deref(),
        raw.role_arn.as_deref(),
    )
    .map_err(aws_error)?;
    let endpoint = raw
        .endpoint
        .as_deref()
        .map(StoreAddress::parse)
        .transpose()
        .map_err(|error| SettingError::new("endpoint", error.to_string()))?;
    let identity = coral_aws::AwsIdentity::new(checked, name.as_str(), aws).map_err(aws_error)?;
    Ok(SecretsManagerSettings {
        region: raw.region,
        endpoint,
        identity,
    })
}

impl SecretStoreFactory for AwsSecretsManagerKind {
    fn kind(&self) -> &SecretStoreKind {
        &self.kind
    }

    /// Builds the identity too, which reads nothing and sends nothing, so
    /// that a name too long for a role session fails here.
    fn validate(
        &self,
        name: &SecretStoreName,
        settings: &StoreSettings,
        _named: &NamedVariables,
    ) -> Result<(), SettingError> {
        aws_definition(name, settings, &coral_aws::Environment::default()).map(drop)
    }

    fn build(
        &self,
        name: &SecretStoreName,
        settings: &StoreSettings,
        environment: &BuildEnvironment,
    ) -> Result<Arc<dyn SecretStore>, SettingError> {
        Ok(Arc::new(EnterpriseOnly {
            edition: self.edition.clone(),
            store: SecretsManagerStore::new(
                store_http::client(environment.policy)?,
                aws_definition(name, settings, &environment.aws)?,
                Arc::clone(&environment.clock),
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
