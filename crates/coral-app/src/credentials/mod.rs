//! Internal credential-set identity and lifecycle helpers.

pub(crate) mod config;
pub(crate) mod oauth;
mod store;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use coral_spec::{ManifestInputKind, ManifestInputSpec};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::workspaces::WorkspaceName;

use self::oauth::{OAuthCredentialService, RefreshOAuthCredentialRequest};

pub(crate) use store::{CredentialStore, CredentialsError};

/// Opaque credential material captured for best-effort rollback.
#[derive(Clone)]
pub(crate) struct CredentialMaterialSnapshot {
    storage: CredentialStorageKind,
    material: Option<Vec<u8>>,
}

impl CredentialMaterialSnapshot {
    fn new(storage: CredentialStorageKind, material: Option<Vec<u8>>) -> Self {
        Self { storage, material }
    }

    fn storage(&self) -> CredentialStorageKind {
        self.storage
    }

    fn material(&self) -> Option<&[u8]> {
        self.material.as_deref()
    }
}

/// Durable credential material storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CredentialStorageKind {
    File,
    Keychain,
}

impl CredentialStorageKind {
    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Keychain => "keychain",
        }
    }
}

impl fmt::Display for CredentialStorageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_config_value().fmt(f)
    }
}

/// Configured storage preference for newly installed sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CredentialStoragePreference {
    #[default]
    Auto,
    File,
    Keychain,
}

/// Result of replacing credential material.
pub(crate) struct CredentialWriteOutcome {
    pub(crate) visible_keys: Vec<String>,
    pub(crate) storage: CredentialStorageKind,
}

pub(crate) const CORAL_INTERNAL_KEY_PREFIX: &str = "__coral";
pub(crate) const OAUTH_INTERNAL_KEY_PREFIX: &str = "__coral_oauth.";

pub(crate) fn is_internal_material_key(key: &str) -> bool {
    key.starts_with(CORAL_INTERNAL_KEY_PREFIX)
}

/// App-owned identity for one durable credential set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CredentialSetId(String);

impl CredentialSetId {
    /// Build the source-backed credential-set id used for today's compatibility
    /// model.
    #[must_use]
    pub(crate) fn for_source(source_name: &SourceName) -> Self {
        Self(format!("source.{}", source_name.as_str()))
    }

    pub(crate) fn source_name(&self) -> Result<SourceName, AppError> {
        let Some(source_name) = self.0.strip_prefix("source.") else {
            return Err(AppError::FailedPrecondition(format!(
                "credential set '{}' is not source-backed",
                self.0
            )));
        };
        SourceName::parse(source_name)
    }
}

impl fmt::Display for CredentialSetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// App-level credential-set lifecycle operations.
#[derive(Clone)]
pub(crate) struct CredentialManager {
    store: CredentialStore,
    oauth_credential_service: OAuthCredentialService,
    // Per credential set locks guard the persisted-material read/refresh/write
    // sequence. Concurrent loads of the same expired credential can otherwise
    // spend the same rotating refresh token before either write is persisted.
    provider_refresh_locks: ProviderRefreshLocks,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CredentialRefreshLockKey {
    workspace_name: WorkspaceName,
    credential_set_id: CredentialSetId,
}

type ProviderRefreshLock = Arc<tokio::sync::Mutex<()>>;
type ProviderRefreshLocks =
    Arc<tokio::sync::Mutex<BTreeMap<CredentialRefreshLockKey, ProviderRefreshLock>>>;

impl CredentialManager {
    pub(crate) fn new(store: CredentialStore) -> Self {
        Self {
            store,
            oauth_credential_service: OAuthCredentialService::new(),
            provider_refresh_locks: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn replace_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        secrets: &BTreeMap<String, String>,
    ) -> Result<CredentialWriteOutcome, AppError> {
        self.store
            .replace_material(workspace_name, credential_set_id, storage, secrets)?;
        Ok(CredentialWriteOutcome {
            visible_keys: visible_material_keys(secrets),
            storage,
        })
    }

    pub(crate) fn read_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
    ) -> Result<BTreeMap<String, String>, AppError> {
        self.store
            .read_material(workspace_name, credential_set_id, storage)
    }

    /// Read persisted credential material for the declared inputs, refreshing
    /// provider-managed credentials before returning when needed.
    pub(crate) async fn read_material_for_inputs(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        inputs: &[ManifestInputSpec],
    ) -> Result<BTreeMap<String, String>, AppError> {
        if !has_oauth_credential_inputs(inputs) {
            return self.read_material(workspace_name, credential_set_id, storage);
        }

        let refresh_lock = self
            .provider_refresh_lock(workspace_name, credential_set_id)
            .await;
        let _refresh_guard = refresh_lock.lock().await;
        let mut material = self.read_material(workspace_name, credential_set_id, storage)?;
        self.refresh_and_persist_oauth_material(
            workspace_name,
            credential_set_id,
            storage,
            inputs,
            &mut material,
        )
        .await?;
        Ok(material)
    }

    pub(crate) fn snapshot_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
    ) -> Result<CredentialMaterialSnapshot, AppError> {
        self.store
            .snapshot_material(workspace_name, credential_set_id, storage)
    }

    pub(crate) fn restore_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), AppError> {
        self.store
            .restore_material(workspace_name, credential_set_id, snapshot)
    }

    pub(crate) fn remove_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
    ) -> Result<(), AppError> {
        self.store
            .remove_material(workspace_name, credential_set_id, storage)
    }

    pub(crate) fn default_write_storage(&self) -> Result<CredentialStorageKind, AppError> {
        self.store.default_write_storage().map_err(Into::into)
    }

    async fn refresh_and_persist_oauth_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        inputs: &[ManifestInputSpec],
        material: &mut BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        for input in inputs {
            if input.kind != ManifestInputKind::Secret {
                continue;
            }
            let Some(credential) = input.credential.as_ref() else {
                continue;
            };
            let Some(oauth) = credential
                .methods
                .iter()
                .find_map(|method| method.oauth.as_ref())
            else {
                continue;
            };
            let before_refresh = material.clone();
            if self
                .oauth_credential_service
                .refresh_if_needed(
                    RefreshOAuthCredentialRequest::for_source_input(&input.key, oauth),
                    material,
                )
                .await?
            {
                *material = self.persist_refreshed_oauth_material(
                    workspace_name,
                    credential_set_id,
                    storage,
                    &input.key,
                    &before_refresh,
                    material,
                )?;
            }
        }
        Ok(())
    }

    fn persist_refreshed_oauth_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        input_key: &str,
        before_refresh: &BTreeMap<String, String>,
        refreshed_material: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, AppError> {
        self.store.update_material(
            workspace_name,
            credential_set_id,
            storage,
            |mut current_material| {
                if provider_input_material_matches(&current_material, before_refresh, input_key) {
                    replace_provider_input_material(
                        &mut current_material,
                        refreshed_material,
                        input_key,
                    );
                } else {
                    tracing::debug!(
                        source_secret = input_key,
                        "skipping OAuth refresh persistence because credential material changed"
                    );
                }
                let next_material = current_material;
                Ok((next_material.clone(), next_material))
            },
        )
    }

    async fn provider_refresh_lock(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> ProviderRefreshLock {
        let key = CredentialRefreshLockKey {
            workspace_name: workspace_name.clone(),
            credential_set_id: credential_set_id.clone(),
        };
        let mut locks = self.provider_refresh_locks.lock().await;
        Arc::clone(
            locks
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }
}

fn has_oauth_credential_inputs(inputs: &[ManifestInputSpec]) -> bool {
    inputs.iter().any(|input| {
        input.kind == ManifestInputKind::Secret
            && input.credential.as_ref().is_some_and(|credential| {
                credential
                    .methods
                    .iter()
                    .any(|method| method.oauth.is_some())
            })
    })
}

fn provider_input_material_matches(
    current: &BTreeMap<String, String>,
    expected: &BTreeMap<String, String>,
    input_key: &str,
) -> bool {
    provider_input_material_keys(current, expected, input_key)
        .into_iter()
        .all(|key| current.get(&key) == expected.get(&key))
}

fn replace_provider_input_material(
    current: &mut BTreeMap<String, String>,
    refreshed: &BTreeMap<String, String>,
    input_key: &str,
) {
    current.retain(|key, _| !provider_input_material_key(key, input_key));
    current.extend(
        refreshed
            .iter()
            .filter(|(key, _)| provider_input_material_key(key, input_key))
            .map(|(key, value)| (key.clone(), value.clone())),
    );
}

fn provider_input_material_keys(
    left: &BTreeMap<String, String>,
    right: &BTreeMap<String, String>,
    input_key: &str,
) -> BTreeSet<String> {
    left.keys()
        .chain(right.keys())
        .filter(|key| provider_input_material_key(key, input_key))
        .cloned()
        .collect()
}

fn provider_input_material_key(key: &str, input_key: &str) -> bool {
    key == input_key || self::oauth::material_key_belongs_to_input(key, input_key)
}

fn visible_material_keys(material: &BTreeMap<String, String>) -> Vec<String> {
    material
        .keys()
        .filter(|key| !is_internal_material_key(key))
        .cloned()
        .collect()
}
