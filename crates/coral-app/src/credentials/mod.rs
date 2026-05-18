//! Internal credential-set identity and lifecycle helpers.

pub(crate) mod oauth;
mod store;

use std::collections::BTreeMap;
use std::fmt;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::workspaces::WorkspaceName;

pub(crate) use store::{CredentialStore, CredentialsError};

/// Opaque credential material captured for best-effort rollback.
#[derive(Clone)]
pub(crate) struct CredentialMaterialSnapshot(Option<Vec<u8>>);

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

    pub(super) fn source_name(&self) -> Result<SourceName, AppError> {
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
}

impl CredentialManager {
    pub(crate) fn new(store: CredentialStore) -> Self {
        Self { store }
    }

    pub(crate) fn replace_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        secrets: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, AppError> {
        self.store
            .replace_material(workspace_name, credential_set_id, secrets)?;
        Ok(visible_material_keys(secrets))
    }

    pub(crate) fn read_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<BTreeMap<String, String>, AppError> {
        self.store.read_material(workspace_name, credential_set_id)
    }

    pub(crate) fn snapshot_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<CredentialMaterialSnapshot, AppError> {
        self.store
            .snapshot_material(workspace_name, credential_set_id)
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
    ) -> Result<(), AppError> {
        self.store
            .remove_material(workspace_name, credential_set_id)
    }
}

fn visible_material_keys(material: &BTreeMap<String, String>) -> Vec<String> {
    material
        .keys()
        .filter(|key| !is_internal_material_key(key))
        .cloned()
        .collect()
}
