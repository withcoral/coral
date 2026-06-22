//! Storage seam types and traits for provider identity instances.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;

use coral_spec::IdentitySpecType;
use serde::{Deserialize, Serialize};
use tracing::info_span;

use crate::bootstrap::AppError;
use crate::credentials::{
    parse_env_file, remove_file_if_exists_unlocked, render_env_file, write_file_unlocked,
};
use crate::identity::{UserPrincipal, parse_path_segment};
use crate::identity_specs::{
    IdentitySpecManager, IdentitySpecRecord, identity_spec_fingerprint, validate_identity_spec_name,
};
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};

const IDENTITY_INSTANCE_DOCUMENT_VERSION: u32 = 1;
const FIXED_TOKEN_MATERIAL_KEY: &str = "TOKEN";

/// One stored provider identity instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityInstanceRecord {
    /// Stable identity name used by source identity bindings.
    pub name: IdentityInstanceName,
    /// Installed identity spec used to instantiate this identity.
    pub identity_spec: String,
    /// Fingerprint of the identity spec at creation time.
    pub identity_spec_fingerprint: Option<String>,
    /// Provider or issuer copied from the identity spec.
    pub issuer: String,
    /// Identity type copied from the identity spec.
    pub identity_type: String,
    /// Safe provider metadata, never credential material.
    pub metadata: BTreeMap<String, String>,
}

/// Validated storage name for one provider identity instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityInstanceName(String);

impl IdentityInstanceName {
    /// Builds an identity name from a storage-safe string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the name is empty or contains path separators.
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity", &value.into()).map(Self)
    }

    /// Returns the storage name string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityInstanceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdentityInstanceName {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdentityInstanceName {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Opaque durable owner key for stored provider-facing identity material.
///
/// OSS Coral constructs this from the request user principal. Other runtimes can
/// use their own stable owner keys without adding their ownership model to OSS
/// identity management.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityOwnerKey(String);

impl IdentityOwnerKey {
    /// Builds an owner key from a storage-safe string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the key is empty or contains path separators.
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity owner", &value.into()).map(Self)
    }

    pub(crate) fn for_user_principal(principal: &UserPrincipal) -> Result<Self, AppError> {
        Self::new(principal.user_id())
    }

    /// Returns the storage key string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityOwnerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdentityOwnerKey {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdentityOwnerKey {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Request to create or replace a fixed-token identity.
#[derive(Debug, Clone)]
pub struct CreateFixedTokenIdentityCommand {
    /// Stable identity name used by source identity bindings.
    pub name: String,
    /// Installed fixed-token identity spec name.
    pub identity_spec: String,
    /// Bearer token to store.
    pub token: String,
}

/// Locked access to credential material for one provider identity instance.
#[tonic::async_trait]
pub trait IdentityInstanceMaterialGuard: Send {
    /// Reads the identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when material cannot be read or decoded.
    async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError>;

    /// Replaces the identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when material cannot be written.
    async fn write_material(&self, material: &BTreeMap<String, String>) -> Result<(), AppError>;
}

/// Durable storage backend for provider identity instances.
#[tonic::async_trait]
pub trait IdentityInstanceStore: Send + Sync + std::fmt::Debug + 'static {
    /// Lists identities owned by one opaque owner key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<IdentityInstanceRecord>, AppError>;

    /// Loads one identity owned by one opaque owner key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &IdentityInstanceName,
    ) -> Result<Option<IdentityInstanceRecord>, AppError>;

    /// Replaces one identity and its credential material atomically.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn replace_identity(
        &self,
        owner: &IdentityOwnerKey,
        record: &IdentityInstanceRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError>;

    /// Deletes one identity and its credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &IdentityInstanceName,
    ) -> Result<bool, AppError>;

    /// Returns locked access to one identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the material lock cannot be acquired.
    async fn material_guard(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &IdentityInstanceName,
    ) -> Result<Box<dyn IdentityInstanceMaterialGuard>, AppError>;
}

/// Manages provider-facing identity instances keyed by an opaque owner.
#[derive(Debug, Clone)]
pub(crate) struct IdentityInstanceManager {
    identity_specs: IdentitySpecManager,
    store: Arc<dyn IdentityInstanceStore>,
}

/// Product-facing handle for stored provider identity management.
///
/// The handle intentionally deals only in opaque owner keys. OSS callers keep
/// using user-owned gRPC methods, while other runtimes can map their owner
/// metadata into stable keys.
#[derive(Debug, Clone)]
pub struct IdentityManagementHandle {
    manager: IdentityInstanceManager,
}

impl IdentityManagementHandle {
    /// Creates or replaces a fixed-token identity under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when validation or storage fails.
    pub async fn create_fixed_token_identity(
        &self,
        owner: &IdentityOwnerKey,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<IdentityInstanceRecord, AppError> {
        self.manager
            .create_fixed_token_identity(owner, command)
            .await
    }

    /// Lists identities stored under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    pub async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<IdentityInstanceRecord>, AppError> {
        self.manager.list_identities(owner).await
    }

    /// Deletes an identity stored under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    pub async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<bool, AppError> {
        self.manager.delete_identity(owner, identity_name).await
    }
}

impl IdentityInstanceManager {
    pub(crate) fn new(layout: AppStateLayout, identity_specs: IdentitySpecManager) -> Self {
        Self::new_with_store(
            identity_specs.clone(),
            Arc::new(FileIdentityInstanceStore::new(layout, identity_specs)),
        )
    }

    pub(crate) fn new_with_store(
        identity_specs: IdentitySpecManager,
        store: Arc<dyn IdentityInstanceStore>,
    ) -> Self {
        Self {
            identity_specs,
            store,
        }
    }

    pub(crate) fn handle(&self) -> IdentityManagementHandle {
        IdentityManagementHandle {
            manager: self.clone(),
        }
    }

    fn prepare_identity_creation(
        &self,
        owner: &IdentityOwnerKey,
        name: &str,
        identity_spec: &str,
        expected: IdentitySpecType,
    ) -> Result<(IdentityOwnerKey, IdentityInstanceName, IdentitySpecRecord), AppError> {
        self.identity_specs.ensure_dsl_v4_enabled()?;
        let name = validate_identity_name(name)?;
        let identity_spec_name = validate_identity_spec_name(identity_spec)?;
        let spec = self.identity_specs.get_identity_spec(&identity_spec_name)?;
        if spec.manifest.identity_type != expected {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{identity_spec_name}' has type '{}'; expected {}",
                spec.manifest.identity_type.label(),
                expected.label()
            )));
        }
        Ok((owner.clone(), name, spec))
    }

    async fn create_fixed_token_identity(
        &self,
        owner: &IdentityOwnerKey,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<IdentityInstanceRecord, AppError> {
        let span = info_span!("coral.app.identities.create_fixed_token");
        let _guard = span.enter();
        let (owner, name, spec) = self.prepare_identity_creation(
            owner,
            &command.name,
            &command.identity_spec,
            IdentitySpecType::FixedToken,
        )?;
        if command.token.is_empty() {
            return Err(AppError::InvalidInput(
                "fixed token identity token must not be empty".to_string(),
            ));
        }
        let record = IdentityInstanceRecord {
            name,
            identity_spec: spec.manifest.name.clone(),
            identity_spec_fingerprint: Some(identity_spec_fingerprint(&spec.manifest)?),
            issuer: spec.manifest.issuer,
            identity_type: spec.manifest.identity_type.label().to_string(),
            metadata: BTreeMap::new(),
        };
        let material = BTreeMap::from([(FIXED_TOKEN_MATERIAL_KEY.to_string(), command.token)]);
        self.store
            .replace_identity(&owner, &record, &material)
            .await?;
        Ok(record)
    }

    pub(crate) async fn create_user_owned_fixed_token_identity(
        &self,
        principal: &UserPrincipal,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<IdentityInstanceRecord, AppError> {
        let owner = IdentityOwnerKey::for_user_principal(principal)?;
        self.create_fixed_token_identity(&owner, command).await
    }

    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<IdentityInstanceRecord>, AppError> {
        self.store.list_identities(owner).await
    }

    pub(crate) async fn list_user_owned_identities(
        &self,
        principal: &UserPrincipal,
    ) -> Result<Vec<IdentityInstanceRecord>, AppError> {
        let span = info_span!("coral.app.identities.list_user_owned");
        let _guard = span.enter();
        let owner = IdentityOwnerKey::for_user_principal(principal)?;
        self.list_identities(&owner).await
    }

    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<bool, AppError> {
        let identity_name = validate_identity_name(identity_name)?;
        self.store.delete_identity(owner, &identity_name).await
    }
}

#[derive(Debug, Clone)]
struct FileIdentityInstanceStore {
    layout: AppStateLayout,
    identity_specs: IdentitySpecManager,
}

impl FileIdentityInstanceStore {
    fn new(layout: AppStateLayout, identity_specs: IdentitySpecManager) -> Self {
        Self {
            layout,
            identity_specs,
        }
    }

    fn load_identity_unlocked(
        &self,
        owner_key: &str,
        name: &str,
    ) -> Result<IdentityInstanceRecord, AppError> {
        let owner_key = validate_identity_owner_key(owner_key)?;
        let name = validate_identity_name(name)?;
        let path = self
            .layout
            .user_owned_identity_manifest_file(&owner_key, name.as_str());
        if !path.exists() {
            return Err(AppError::IdentityNotFound(name.to_string()));
        }
        let raw = fs::read_to_string(&path)?;
        let document: IdentityInstanceDocument = serde_yaml::from_str(&raw)?;
        document.into_record(&name)
    }

    fn material_lock_for_layout(
        layout: &AppStateLayout,
        owner_key: &str,
        identity_name: &IdentityInstanceName,
    ) -> Result<FileLock, AppError> {
        FileLock::exclusive(
            &layout.user_owned_identity_refresh_lock_file(owner_key, identity_name.as_str()),
        )
        .map_err(Into::into)
    }
}

#[tonic::async_trait]
impl IdentityInstanceStore for FileIdentityInstanceStore {
    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<IdentityInstanceRecord>, AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let _lock = FileLock::shared(store.layout.state_lock())?;
            let root = store.layout.user_owned_identities_root(&owner_key);
            if !root.exists() {
                return Ok(Vec::new());
            }
            let mut records = Vec::new();
            for entry in fs::read_dir(root)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let Some(name) = entry.file_name().to_str().map(ToString::to_string) else {
                    continue;
                };
                records.push(store.load_identity_unlocked(&owner_key, &name)?);
            }
            records.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(records)
        })
        .await?
    }

    async fn load_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &IdentityInstanceName,
    ) -> Result<Option<IdentityInstanceRecord>, AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let identity_name = identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let _lock = FileLock::shared(store.layout.state_lock())?;
            match store.load_identity_unlocked(&owner_key, identity_name.as_str()) {
                Ok(record) => Ok(Some(record)),
                Err(AppError::IdentityNotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await?
    }

    async fn replace_identity(
        &self,
        owner: &IdentityOwnerKey,
        record: &IdentityInstanceRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let record = record.clone();
        let material = material.clone();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let _material_lock =
                Self::material_lock_for_layout(&store.layout, &owner_key, &record.name)?;
            let _state_lock = FileLock::exclusive(store.layout.state_lock())?;
            let manifest_path = store
                .layout
                .user_owned_identity_manifest_file(&owner_key, record.name.as_str());
            let material_path = store
                .layout
                .user_owned_identity_material_file(&owner_key, record.name.as_str());
            if let Some(parent) = manifest_path.parent() {
                storage_fs::ensure_private_dir(parent)?;
            }
            validate_identity_spec_reference_unlocked(&store.identity_specs, &record)?;
            write_files_transactionally(&[&manifest_path, &material_path], || {
                let document = IdentityInstanceDocument::from_record(&record);
                write_file_unlocked(&manifest_path, serde_yaml::to_string(&document)?.as_bytes())?;
                write_file_unlocked(&material_path, render_env_file(&material).as_bytes())?;
                Ok(())
            })
        })
        .await?
    }

    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &IdentityInstanceName,
    ) -> Result<bool, AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let identity_name = identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let _material_lock =
                Self::material_lock_for_layout(&store.layout, &owner_key, &identity_name)?;
            let _state_lock = FileLock::exclusive(store.layout.state_lock())?;
            let manifest_path = store
                .layout
                .user_owned_identity_manifest_file(&owner_key, identity_name.as_str());
            let material_path = store
                .layout
                .user_owned_identity_material_file(&owner_key, identity_name.as_str());
            if !manifest_path.exists() {
                return Ok(false);
            }
            remove_file_if_exists_unlocked(&material_path)?;
            remove_file_if_exists_unlocked(&manifest_path)?;
            let identity_dir = store
                .layout
                .user_owned_identity_dir(&owner_key, identity_name.as_str());
            if identity_dir.exists() {
                match fs::remove_dir(&identity_dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(true)
        })
        .await?
    }

    async fn material_guard(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &IdentityInstanceName,
    ) -> Result<Box<dyn IdentityInstanceMaterialGuard>, AppError> {
        let layout = self.layout.clone();
        let owner_key = owner.as_str().to_string();
        let identity_name = identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let lock = Self::material_lock_for_layout(&layout, &owner_key, &identity_name)?;
            Ok(Box::new(FileIdentityInstanceMaterialGuard {
                layout,
                owner_key,
                identity_name,
                _lock: lock,
            }) as Box<dyn IdentityInstanceMaterialGuard>)
        })
        .await?
    }
}

struct FileIdentityInstanceMaterialGuard {
    layout: AppStateLayout,
    owner_key: String,
    identity_name: IdentityInstanceName,
    _lock: FileLock,
}

#[tonic::async_trait]
impl IdentityInstanceMaterialGuard for FileIdentityInstanceMaterialGuard {
    async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError> {
        let path = self
            .layout
            .user_owned_identity_material_file(&self.owner_key, self.identity_name.as_str());
        let identity_name = self.identity_name.to_string();
        tokio::task::spawn_blocking(move || match fs::read_to_string(path) {
            Ok(raw) => parse_env_file(&raw).map_err(Into::into),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(AppError::FailedPrecondition(format!(
                    "identity '{identity_name}' is missing credential material"
                )))
            }
            Err(error) => Err(error.into()),
        })
        .await?
    }

    async fn write_material(&self, material: &BTreeMap<String, String>) -> Result<(), AppError> {
        let layout = self.layout.clone();
        let owner_key = self.owner_key.clone();
        let identity_name = self.identity_name.clone();
        let material = material.clone();
        tokio::task::spawn_blocking(move || {
            let path = layout.user_owned_identity_material_file(&owner_key, identity_name.as_str());
            let _state_lock = FileLock::exclusive(layout.state_lock())?;
            let manifest_path =
                layout.user_owned_identity_manifest_file(&owner_key, identity_name.as_str());
            if !manifest_path.exists() {
                return Err(AppError::IdentityNotFound(identity_name.to_string()));
            }
            write_file_unlocked(&path, render_env_file(&material).as_bytes()).map_err(Into::into)
        })
        .await?
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityInstanceDocument {
    version: u32,
    name: String,
    identity_spec: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity_spec_fingerprint: Option<String>,
    issuer: String,
    identity_type: String,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl IdentityInstanceDocument {
    fn from_record(record: &IdentityInstanceRecord) -> Self {
        Self {
            version: IDENTITY_INSTANCE_DOCUMENT_VERSION,
            name: record.name.to_string(),
            identity_spec: record.identity_spec.clone(),
            identity_spec_fingerprint: record.identity_spec_fingerprint.clone(),
            issuer: record.issuer.clone(),
            identity_type: record.identity_type.clone(),
            metadata: record.metadata.clone(),
        }
    }

    fn into_record(
        self,
        expected_name: &IdentityInstanceName,
    ) -> Result<IdentityInstanceRecord, AppError> {
        if self.version != IDENTITY_INSTANCE_DOCUMENT_VERSION {
            return Err(AppError::FailedPrecondition(format!(
                "identity '{}' has unsupported document version {}; expected {}",
                self.name, self.version, IDENTITY_INSTANCE_DOCUMENT_VERSION
            )));
        }
        let name = validate_identity_name(&self.name)?;
        if &name != expected_name {
            return Err(AppError::FailedPrecondition(format!(
                "identity file for '{expected_name}' contains identity '{name}'"
            )));
        }
        Ok(IdentityInstanceRecord {
            name,
            identity_spec: validate_identity_spec_name(&self.identity_spec)?,
            identity_spec_fingerprint: self.identity_spec_fingerprint,
            issuer: self.issuer,
            identity_type: self.identity_type,
            metadata: self.metadata,
        })
    }
}

fn validate_identity_owner_key(owner: &str) -> Result<String, AppError> {
    parse_path_segment("identity owner", owner)
}

fn validate_identity_name(name: &str) -> Result<IdentityInstanceName, AppError> {
    IdentityInstanceName::new(name)
}

fn validate_identity_spec_reference_unlocked(
    identity_specs: &IdentitySpecManager,
    record: &IdentityInstanceRecord,
) -> Result<(), AppError> {
    let spec = identity_specs
        .load_identity_spec_manifest_unlocked_for_state_lock(&record.identity_spec)?;
    let fingerprint = identity_spec_fingerprint(&spec.manifest)?;
    if spec.manifest.issuer != record.issuer
        || spec.manifest.identity_type.label() != record.identity_type.as_str()
        || record.identity_spec_fingerprint.as_deref() != Some(fingerprint.as_str())
    {
        return Err(AppError::FailedPrecondition(format!(
            "identity '{}' references identity spec '{}' that changed before the identity could be stored",
            record.name, record.identity_spec
        )));
    }
    Ok(())
}

/// Snapshots `paths`, runs `write`, and restores every file to its prior
/// contents if `write` fails, so a partial write never leaves files mutated.
fn write_files_transactionally(
    paths: &[&std::path::Path],
    write: impl FnOnce() -> Result<(), AppError>,
) -> Result<(), AppError> {
    let snapshots = paths
        .iter()
        .map(|&path| snapshot_file_unlocked(path))
        .collect::<Result<Vec<_>, _>>()?;
    let result = write();
    if result.is_err() {
        for (&path, snapshot) in paths.iter().zip(snapshots) {
            restore_file_unlocked(path, snapshot)?;
        }
    }
    result
}

fn snapshot_file_unlocked(path: &std::path::Path) -> Result<Option<Vec<u8>>, AppError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn restore_file_unlocked(
    path: &std::path::Path,
    snapshot: Option<Vec<u8>>,
) -> Result<(), AppError> {
    match snapshot {
        Some(bytes) => write_file_unlocked(path, &bytes).map_err(Into::into),
        None => remove_file_if_exists_unlocked(path).map_err(Into::into),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::{Features, dsl_v4_features};
    use tempfile::TempDir;

    #[test]
    fn owner_key_rejects_storage_unsafe_values() {
        IdentityOwnerKey::new(" ").unwrap_err();
        IdentityOwnerKey::new("a/b").unwrap_err();
        IdentityOwnerKey::new("a\\b").unwrap_err();
        IdentityOwnerKey::new("..").unwrap_err();
    }

    #[test]
    fn owner_key_round_trips_storage_key() {
        let owner = IdentityOwnerKey::new("member-123").expect("owner key");

        assert_eq!(owner.as_str(), "member-123");
        assert_eq!(owner.to_string(), "member-123");
        assert_eq!(
            "member-123".parse::<IdentityOwnerKey>().expect("parse"),
            owner
        );
    }

    #[test]
    fn identity_name_rejects_storage_unsafe_values() {
        IdentityInstanceName::new(" ").unwrap_err();
        IdentityInstanceName::new("a/b").unwrap_err();
        IdentityInstanceName::new("a\\b").unwrap_err();
        IdentityInstanceName::new("..").unwrap_err();
    }

    #[test]
    fn identity_name_round_trips_storage_name() {
        let identity_name = IdentityInstanceName::new("github-primary").expect("identity name");

        assert_eq!(identity_name.as_str(), "github-primary");
        assert_eq!(identity_name.to_string(), "github-primary");
        assert_eq!(
            "github-primary"
                .parse::<IdentityInstanceName>()
                .expect("parse"),
            identity_name
        );
    }

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout directories");
        layout
    }

    fn manager() -> (TempDir, IdentityInstanceManager, IdentitySpecManager) {
        manager_with_features(dsl_v4_features())
    }

    fn manager_with_features(
        features: Features,
    ) -> (TempDir, IdentityInstanceManager, IdentitySpecManager) {
        let temp = TempDir::new().expect("tempdir");
        let layout = test_layout(&temp);
        let identity_specs =
            IdentitySpecManager::new_with_usage_providers(layout.clone(), features, Vec::new());
        (
            temp,
            IdentityInstanceManager::new(layout, identity_specs.clone()),
            identity_specs,
        )
    }

    fn manager_with_github_pat_spec() -> (TempDir, IdentityInstanceManager, IdentitySpecManager) {
        let (temp, manager, identity_specs) = manager();
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml())
            .expect("add identity spec");
        (temp, manager, identity_specs)
    }

    fn fixed_identity_spec_yaml() -> String {
        fixed_identity_spec_yaml_with_issuer("github")
    }

    fn fixed_identity_spec_yaml_with_issuer(issuer: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: github_pat
version: 0.1.0
issuer: {issuer}
type: fixed_token
audience:
  host: github.com
"
        )
    }

    fn github_local_command(token: &str) -> CreateFixedTokenIdentityCommand {
        CreateFixedTokenIdentityCommand {
            name: "github_local".to_string(),
            identity_spec: "github_pat".to_string(),
            token: token.to_string(),
        }
    }

    fn github_local_name() -> IdentityInstanceName {
        IdentityInstanceName::new("github_local").expect("github_local identity name")
    }

    fn local_owner() -> IdentityOwnerKey {
        IdentityOwnerKey::new("local").expect("local owner")
    }

    fn material(key: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(key.to_string(), value.to_string())])
    }

    #[tokio::test]
    async fn create_user_owned_identity_requires_dsl_v4_feature() {
        let (_temp, manager, _identity_specs) = manager_with_features(Features::default());

        let error = manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("token"),
            )
            .await
            .expect_err("identity creation requires the dsl_v4 feature");

        assert!(
            matches!(&error, AppError::SourceUnservable(message) if message.contains("dsl_v4")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn create_user_owned_fixed_token_identity_stores_record_and_material() {
        let (temp, manager, _identity_specs) = manager_with_github_pat_spec();

        let record = manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("ghp_token"),
            )
            .await
            .expect("create identity");

        assert_eq!(record.name.as_str(), "github_local");
        assert_eq!(record.identity_spec, "github_pat");
        assert_eq!(record.issuer, "github");
        assert_eq!(record.identity_type, "fixed_token");
        assert!(
            record.identity_spec_fingerprint.is_some(),
            "identity should pin the identity spec fingerprint"
        );
        assert!(record.metadata.is_empty());

        let layout = test_layout(&temp);
        let raw =
            fs::read_to_string(layout.user_owned_identity_manifest_file("local", "github_local"))
                .expect("identity manifest");
        assert!(raw.contains("identity_spec: github_pat"));
        let material =
            fs::read_to_string(layout.user_owned_identity_material_file("local", "github_local"))
                .expect("identity material");
        assert!(material.contains("TOKEN=ghp_token"));
    }

    #[tokio::test]
    async fn identity_write_rejects_identity_spec_deleted_after_validation() {
        let (_temp, manager, identity_specs) = manager_with_github_pat_spec();
        let original_spec = identity_specs
            .get_identity_spec("github_pat")
            .expect("original identity spec");
        let record = IdentityInstanceRecord {
            name: github_local_name(),
            identity_spec: "github_pat".to_string(),
            identity_spec_fingerprint: Some(
                identity_spec_fingerprint(&original_spec.manifest).expect("fingerprint"),
            ),
            issuer: "github".to_string(),
            identity_type: IdentitySpecType::FixedToken.label().to_string(),
            metadata: BTreeMap::new(),
        };
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("remove original spec");

        let error = manager
            .store
            .replace_identity(
                &local_owner(),
                &record,
                &material(FIXED_TOKEN_MATERIAL_KEY, "ghp_token"),
            )
            .await
            .expect_err("deleted identity spec reference must be rejected");

        assert!(matches!(error, AppError::IdentitySpecNotFound(name) if name == "github_pat"));
    }

    #[tokio::test]
    async fn identity_write_rejects_identity_spec_changed_after_validation() {
        let (temp, manager, identity_specs) = manager_with_github_pat_spec();
        let original_spec = identity_specs
            .get_identity_spec("github_pat")
            .expect("original identity spec");
        let record = IdentityInstanceRecord {
            name: github_local_name(),
            identity_spec: "github_pat".to_string(),
            identity_spec_fingerprint: Some(
                identity_spec_fingerprint(&original_spec.manifest).expect("fingerprint"),
            ),
            issuer: "github".to_string(),
            identity_type: IdentitySpecType::FixedToken.label().to_string(),
            metadata: BTreeMap::new(),
        };
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("remove original spec");
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml_with_issuer("github_enterprise"))
            .expect("replace with changed spec");

        let error = manager
            .store
            .replace_identity(
                &local_owner(),
                &record,
                &material(FIXED_TOKEN_MATERIAL_KEY, "ghp_token"),
            )
            .await
            .expect_err("stale identity spec reference must be rejected");

        assert!(
            matches!(error, AppError::FailedPrecondition(ref message) if message.contains("changed before the identity could be stored")),
            "unexpected error: {error:?}"
        );
        let layout = test_layout(&temp);
        assert!(
            !layout
                .user_owned_identity_manifest_file("local", "github_local")
                .exists(),
            "rejected stale identity should not create a manifest"
        );
        assert!(
            !layout
                .user_owned_identity_material_file("local", "github_local")
                .exists(),
            "rejected stale identity should not create material"
        );
    }

    #[tokio::test]
    async fn lists_user_owned_identity_records() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        let principal = UserPrincipal::local();
        manager
            .create_user_owned_fixed_token_identity(&principal, github_local_command("ghp_token"))
            .await
            .expect("create identity");

        let listed = manager
            .list_user_owned_identities(&principal)
            .await
            .expect("list identities");

        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed.first().expect("listed identity").name.as_str(),
            "github_local"
        );
    }

    #[tokio::test]
    async fn identity_replacement_waits_for_in_flight_material_refresh() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("old-token"),
            )
            .await
            .expect("create identity");

        let refresh_guard = manager
            .store
            .material_guard(&local_owner(), &github_local_name())
            .await
            .expect("hold refresh lock");
        let replacement_manager = manager.clone();
        let mut replacement = tokio::spawn(async move {
            replacement_manager
                .create_user_owned_fixed_token_identity(
                    &UserPrincipal::local(),
                    github_local_command("replacement-token"),
                )
                .await
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut replacement)
                .await
                .is_err(),
            "identity replacement must wait behind an in-flight material refresh"
        );
        refresh_guard
            .write_material(&material(FIXED_TOKEN_MATERIAL_KEY, "stale-refreshed-token"))
            .await
            .expect("write stale refreshed material while refresh lock is held");
        drop(refresh_guard);
        replacement
            .await
            .expect("replacement task")
            .expect("replacement should succeed");

        let final_material = manager
            .store
            .material_guard(&local_owner(), &github_local_name())
            .await
            .expect("open final material guard")
            .read_material()
            .await
            .expect("read final material");
        assert_eq!(
            final_material
                .get(FIXED_TOKEN_MATERIAL_KEY)
                .map(String::as_str),
            Some("replacement-token"),
            "replacement material should win over stale in-flight refresh material"
        );
    }

    #[tokio::test]
    async fn refreshed_material_write_rejects_deleted_identity() {
        let (temp, manager, _identity_specs) = manager_with_github_pat_spec();
        manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("old-token"),
            )
            .await
            .expect("create identity");
        let layout = test_layout(&temp);
        fs::remove_dir_all(layout.user_owned_identity_dir("local", "github_local"))
            .expect("delete identity");

        let error = manager
            .store
            .material_guard(&local_owner(), &github_local_name())
            .await
            .expect("open material guard")
            .write_material(&material(FIXED_TOKEN_MATERIAL_KEY, "new-token"))
            .await
            .expect_err("deleted identity material must not be recreated");

        assert!(matches!(error, AppError::IdentityNotFound(name) if name == "github_local"));
        assert!(
            !layout
                .user_owned_identity_material_file("local", "github_local")
                .exists(),
            "deleted identity material should stay deleted"
        );
    }
}
