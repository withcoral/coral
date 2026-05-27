//! Internal credential-set identity and lifecycle helpers.

pub(crate) mod config;
pub(crate) mod oauth;
mod store;

use std::collections::BTreeMap;
use std::fmt;

use coral_spec::{ManifestInputKind, ManifestInputSpec};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::storage::fs::FileLock;
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
}

impl CredentialManager {
    pub(crate) fn new(store: CredentialStore) -> Self {
        Self {
            store,
            oauth_credential_service: OAuthCredentialService::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn replace_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        secrets: &BTreeMap<String, String>,
    ) -> Result<CredentialWriteOutcome, AppError> {
        self.material_guard(workspace_name, credential_set_id)?
            .replace_material(storage, secrets)
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

        let _refresh_file_lock = self
            .credential_refresh_lock(workspace_name, credential_set_id)
            .await?;
        let mut material = self.read_material(workspace_name, credential_set_id, storage)?;
        self.refresh_and_persist_oauth_material(
            workspace_name,
            credential_set_id,
            storage,
            inputs,
            &mut material,
        )
        .await
        .map_err(credential_refresh_error)?;
        Ok(material)
    }

    pub(crate) fn default_write_storage(&self) -> Result<CredentialStorageKind, AppError> {
        self.store.default_write_storage().map_err(Into::into)
    }

    pub(crate) fn material_guard<'a>(
        &'a self,
        workspace_name: &'a WorkspaceName,
        credential_set_id: &'a CredentialSetId,
    ) -> Result<CredentialMaterialGuard<'a>, AppError> {
        Ok(CredentialMaterialGuard {
            manager: self,
            workspace_name,
            credential_set_id,
            _refresh_file_lock: self
                .store
                .credential_refresh_lock(workspace_name, credential_set_id)?,
        })
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
        refreshed_material: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, AppError> {
        self.store.update_material(
            workspace_name,
            credential_set_id,
            storage,
            |mut current_material| {
                replace_provider_input_material(
                    &mut current_material,
                    refreshed_material,
                    input_key,
                );
                let next_material = current_material;
                Ok((next_material.clone(), next_material))
            },
        )
    }

    async fn credential_refresh_lock(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<FileLock, AppError> {
        let store = self.store.clone();
        let workspace_name = workspace_name.clone();
        let credential_set_id = credential_set_id.clone();
        // Acquiring the file lock can block behind another refresh; keep that
        // wait off the async runtime worker so the lock holder can keep making
        // progress toward releasing it.
        tokio::task::spawn_blocking(move || {
            store.credential_refresh_lock(&workspace_name, &credential_set_id)
        })
        .await?
    }
}

pub(crate) struct CredentialMaterialGuard<'a> {
    manager: &'a CredentialManager,
    workspace_name: &'a WorkspaceName,
    credential_set_id: &'a CredentialSetId,
    _refresh_file_lock: FileLock,
}

impl CredentialMaterialGuard<'_> {
    #[cfg(test)]
    pub(crate) fn replace_material(
        &self,
        storage: CredentialStorageKind,
        secrets: &BTreeMap<String, String>,
    ) -> Result<CredentialWriteOutcome, AppError> {
        self.manager.store.replace_material(
            self.workspace_name,
            self.credential_set_id,
            storage,
            secrets,
        )?;
        Ok(CredentialWriteOutcome {
            visible_keys: visible_material_keys(secrets),
            storage,
        })
    }

    pub(crate) fn update_material_or_empty_on_parse<F>(
        &self,
        storage: CredentialStorageKind,
        update: F,
    ) -> Result<CredentialWriteOutcome, AppError>
    where
        F: Fn(BTreeMap<String, String>) -> Result<BTreeMap<String, String>, AppError>,
    {
        self.manager
            .store
            .update_material(
                self.workspace_name,
                self.credential_set_id,
                storage,
                |material| {
                    let updated = update(material)?;
                    let visible_keys = visible_material_keys(&updated);
                    Ok((
                        updated,
                        CredentialWriteOutcome {
                            visible_keys,
                            storage,
                        },
                    ))
                },
            )
            .or_else(|error| match error {
                AppError::Credentials(CredentialsError::Parse(_))
                    if storage == CredentialStorageKind::File =>
                {
                    let updated = update(BTreeMap::new())?;
                    let visible_keys = visible_material_keys(&updated);
                    self.manager.store.replace_material(
                        self.workspace_name,
                        self.credential_set_id,
                        storage,
                        &updated,
                    )?;
                    Ok(CredentialWriteOutcome {
                        visible_keys,
                        storage,
                    })
                }
                other => Err(other),
            })
    }

    pub(crate) fn snapshot_material(
        &self,
        storage: CredentialStorageKind,
    ) -> Result<CredentialMaterialSnapshot, AppError> {
        self.manager
            .store
            .snapshot_material(self.workspace_name, self.credential_set_id, storage)
    }

    pub(crate) fn restore_material(
        &self,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), AppError> {
        self.manager
            .store
            .restore_material(self.workspace_name, self.credential_set_id, snapshot)
    }

    pub(crate) fn remove_material(&self, storage: CredentialStorageKind) -> Result<(), AppError> {
        self.manager
            .store
            .remove_material(self.workspace_name, self.credential_set_id, storage)
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

fn credential_refresh_error(error: AppError) -> AppError {
    match error {
        AppError::FailedPrecondition(message) => AppError::CredentialRefresh(message),
        other => AppError::CredentialRefresh(other.to_string()),
    }
}
