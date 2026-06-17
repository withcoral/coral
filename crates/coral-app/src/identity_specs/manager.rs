//! Filesystem-backed global identity-spec registry.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;

use coral_spec::{IdentityManifest, ManifestInputKind, parse_identity_manifest_yaml};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tracing::{info_span, warn};

use crate::bootstrap::AppError;
use crate::credentials::{CredentialSetId, CredentialStorageKind, CredentialStore};
use crate::features::Features;
use crate::identity::{parse_path_segment, unique_input_map};
use crate::state::{AppStateLayout, INSTALLED_IDENTITY_FILE_NAME};
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

const IDENTITY_SPEC_INPUT_MATERIAL_STATE_FILE_NAME: &str = "inputs.yaml";
const IDENTITY_SPEC_INPUT_MATERIAL_STATE_VERSION: u32 = 1;

/// One installed global identity spec.
#[derive(Debug, Clone)]
pub(crate) struct IdentitySpecRecord {
    pub(crate) manifest_yaml: String,
    pub(crate) manifest: IdentityManifest,
}

/// One setup input value for a globally installed identity spec.
#[derive(Debug, Clone)]
pub(crate) struct IdentitySpecInputValue {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Validated storage name for one installed identity spec.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IdentitySpecName(String);

impl IdentitySpecName {
    /// Parse and validate an identity-spec name for app-internal use.
    pub(crate) fn parse(name: &str) -> Result<Self, AppError> {
        parse_path_segment("identity spec", name).map(Self)
    }

    /// Borrow the normalized identity-spec name at string boundaries.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentitySpecName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdentitySpecName {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Validated identity of one identity-spec manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySpecManifestMetadata {
    /// Stable identity-spec id declared by the manifest.
    pub identity_spec_id: String,
    /// Authored identity-spec version.
    pub version: String,
}

/// Validates one identity-spec manifest and returns its registry identity.
///
/// # Errors
///
/// Returns [`AppError`] when the manifest is invalid.
pub fn identity_spec_manifest_metadata(
    manifest_yaml: &str,
) -> Result<IdentitySpecManifestMetadata, AppError> {
    let manifest = parse_identity_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(IdentitySpecManifestMetadata {
        identity_spec_id: manifest.name,
        version: manifest.version,
    })
}

/// Validates identity-spec setup inputs for one manifest.
///
/// # Errors
///
/// Returns [`AppError`] when the manifest or inputs are invalid.
pub fn identity_spec_input_material_from_manifest(
    manifest_yaml: &str,
    inputs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AppError> {
    let existing = BTreeMap::new();
    identity_spec_input_material_from_manifest_with_existing(manifest_yaml, &existing, inputs)
}

/// Validates identity-spec setup inputs and merges them over existing material.
///
/// # Errors
///
/// Returns [`AppError`] when the manifest or inputs are invalid.
pub fn identity_spec_input_material_from_manifest_with_existing(
    manifest_yaml: &str,
    existing_input_material: &BTreeMap<String, String>,
    inputs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AppError> {
    let record = parse_identity_spec_record(manifest_yaml)?;
    merge_identity_spec_input_material(&record.manifest, existing_input_material, inputs)
}

/// Durable record for one installed global identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySpecRegistryRecord {
    /// Raw identity-spec manifest YAML.
    pub manifest_yaml: String,
    /// Stored setup input material for later identity materialization.
    pub input_material: BTreeMap<String, String>,
}

/// Complete rollback snapshot for one installed global identity spec.
#[derive(Debug, Clone)]
pub(crate) struct IdentitySpecSnapshot {
    record: IdentitySpecRecord,
    input_material: BTreeMap<String, String>,
}

/// Durable storage backend for global identity specs.
pub trait IdentitySpecRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// Lists all installed identity specs.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the registry cannot be read.
    fn list_identity_specs(&self) -> Result<Vec<IdentitySpecRegistryRecord>, AppError>;

    /// Lists installed identity spec manifests without setup input material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the registry cannot be read.
    fn list_identity_spec_manifests(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .list_identity_specs()?
            .into_iter()
            .map(|record| record.manifest_yaml)
            .collect())
    }

    /// Fetches one identity spec by name.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the registry cannot be read.
    fn get_identity_spec(&self, name: &str)
    -> Result<Option<IdentitySpecRegistryRecord>, AppError>;

    /// Fetches one identity spec manifest without setup input material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the registry cannot be read.
    fn get_identity_spec_manifest(&self, name: &str) -> Result<Option<String>, AppError> {
        Ok(self
            .get_identity_spec(name)?
            .map(|record| record.manifest_yaml))
    }

    /// Returns whether one identity spec manifest exists.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the registry cannot be inspected.
    fn identity_spec_exists(&self, name: &str) -> Result<bool, AppError> {
        self.get_identity_spec_manifest(name)
            .map(|manifest| manifest.is_some())
    }

    /// Inserts or replaces one identity spec.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the record cannot be persisted.
    fn upsert_identity_spec(
        &self,
        name: &str,
        record: IdentitySpecRegistryRecord,
    ) -> Result<(), AppError>;

    /// Removes one identity spec.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the record cannot be removed.
    fn remove_identity_spec(&self, name: &str) -> Result<(), AppError>;
}

/// Reports stored identities that reference an installed identity spec.
///
/// Identity ownership is orthogonal to identity type. Product runtimes that
/// persist workspace-owned identities should install a provider so identity-spec
/// deletion can reject or report orphaning for those identities too.
pub trait IdentitySpecUsageProvider: Send + Sync + std::fmt::Debug + 'static {
    /// Returns how many stored identities currently depend on `identity_spec_name`.
    ///
    /// The count should include every relevant identity owner and type managed
    /// by the provider.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the provider cannot inspect its identity store.
    fn count_identities_for_spec(&self, identity_spec_name: &str) -> Result<u32, AppError>;
}

/// Manages identity specs installed for all workspaces.
#[derive(Clone)]
pub(crate) struct IdentitySpecManager {
    layout: AppStateLayout,
    registry: Arc<dyn IdentitySpecRegistry>,
    features: Features,
    usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
}

impl std::fmt::Debug for IdentitySpecManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentitySpecManager")
            .field("layout", &self.layout)
            .field("registry", &self.registry)
            .field("features", &self.features)
            .field("usage_providers", &self.usage_providers)
            .finish_non_exhaustive()
    }
}

impl IdentitySpecManager {
    #[cfg(test)]
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self::new_with_usage_providers(layout, crate::features::dsl_v4_features(), Vec::new())
    }

    #[cfg(test)]
    pub(crate) fn new_with_usage_providers(
        layout: AppStateLayout,
        features: Features,
        usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
    ) -> Self {
        let credential_store = CredentialStore::with_preference(
            layout.clone(),
            crate::credentials::CredentialStoragePreference::File,
        );
        Self::new_with_credential_store(layout, credential_store, features, usage_providers)
    }

    pub(crate) fn new_with_credential_store(
        layout: AppStateLayout,
        credential_store: CredentialStore,
        features: Features,
        usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
    ) -> Self {
        let registry = Arc::new(FileIdentitySpecRegistry::new(
            layout.clone(),
            credential_store,
        ));
        Self::new_with_registry(layout, registry, features, usage_providers)
    }

    pub(crate) fn new_with_registry(
        layout: AppStateLayout,
        registry: Arc<dyn IdentitySpecRegistry>,
        features: Features,
        usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
    ) -> Self {
        Self {
            layout,
            registry,
            features,
            usage_providers,
        }
    }

    /// Errors unless the preview DSL v4 runtime feature is enabled.
    ///
    /// Identity specs back DSL v4 source identities, so the whole `identity-spec`
    /// surface is gated behind `dsl_v4`. The mutating manager methods enforce the
    /// gate directly (and so cover the source-import bundle path); the read-only
    /// `get_identity_spec` is also used by query-time identity resolution, so its
    /// RPC enforces the gate from the service layer instead of the method.
    pub(crate) fn ensure_dsl_v4_enabled(&self) -> Result<(), AppError> {
        self.features.ensure_dsl_v4_enabled()
    }

    #[cfg(test)]
    pub(crate) fn add_identity_spec(
        &self,
        manifest_yaml: &str,
    ) -> Result<(IdentitySpecRecord, bool), AppError> {
        self.add_identity_spec_with_inputs(manifest_yaml, Vec::new())
    }

    pub(crate) fn add_identity_spec_with_inputs(
        &self,
        manifest_yaml: &str,
        inputs: Vec<IdentitySpecInputValue>,
    ) -> Result<(IdentitySpecRecord, bool), AppError> {
        let span = info_span!("coral.app.identity_specs.add");
        let _guard = span.enter();
        self.features.ensure_dsl_v4_enabled()?;
        let record = parse_identity_spec_record(manifest_yaml)?;
        let name = validate_identity_spec_name(&record.manifest.name)?;
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let existing = self.registry.get_identity_spec(name.as_str())?;
        let replaced = existing.is_some();
        let existing_input_material = existing
            .as_ref()
            .map(|record| record.input_material.clone())
            .unwrap_or_default();
        if let Some(existing) = &existing {
            match parse_identity_spec_record(&existing.manifest_yaml) {
                Ok(existing) => {
                    if existing.manifest != record.manifest {
                        let referencing_identities =
                            self.count_identities_for_spec_unlocked(&name)?;
                        if referencing_identities > 0 {
                            return Err(AppError::FailedPrecondition(format!(
                                "identity spec '{name}' is used by {referencing_identities} stored {} and cannot be replaced with a different manifest; remove it with --force only if you intend to recreate {} against a new spec",
                                plural_identity(referencing_identities),
                                plural_pronoun(referencing_identities)
                            )));
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        identity_spec = %name,
                        error = %error,
                        "replacing malformed existing identity spec record with valid manifest"
                    );
                }
            }
        }
        let provided_inputs = unique_input_map(
            inputs.into_iter().map(|input| (input.key, input.value)),
            "identity spec input",
        )?;
        let input_material = merge_identity_spec_input_material(
            &record.manifest,
            &existing_input_material,
            &provided_inputs,
        )?;
        self.registry.upsert_identity_spec(
            name.as_str(),
            IdentitySpecRegistryRecord {
                manifest_yaml: record.manifest_yaml.clone(),
                input_material,
            },
        )?;
        Ok((record, replaced))
    }

    pub(crate) fn list_identity_specs(&self) -> Result<Vec<IdentitySpecRecord>, AppError> {
        let span = info_span!("coral.app.identity_specs.list");
        let _guard = span.enter();
        self.features.ensure_dsl_v4_enabled()?;
        let _lock = FileLock::shared(self.layout.state_lock())?;
        let mut records = self
            .registry
            .list_identity_spec_manifests()?
            .into_iter()
            .map(|manifest_yaml| parse_identity_spec_record(&manifest_yaml))
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by(|left, right| left.manifest.name.cmp(&right.manifest.name));
        Ok(records)
    }

    pub(crate) fn get_identity_spec(&self, name: &str) -> Result<IdentitySpecRecord, AppError> {
        let span = info_span!("coral.app.identity_specs.get");
        let _guard = span.enter();
        let name = validate_identity_spec_name(name)?;
        let _lock = FileLock::shared(self.layout.state_lock())?;
        self.load_identity_spec_manifest_unlocked(&name)
    }

    pub(crate) fn load_identity_spec_manifest_unlocked_for_state_lock(
        &self,
        name: &str,
    ) -> Result<IdentitySpecRecord, AppError> {
        let name = validate_identity_spec_name(name)?;
        self.load_identity_spec_manifest_unlocked(&name)
    }

    pub(crate) fn snapshot_identity_spec(
        &self,
        name: &str,
    ) -> Result<Option<IdentitySpecSnapshot>, AppError> {
        self.features.ensure_dsl_v4_enabled()?;
        let name = validate_identity_spec_name(name)?;
        let _lock = FileLock::shared(self.layout.state_lock())?;
        let Some(stored) = self.registry.get_identity_spec(name.as_str())? else {
            return Ok(None);
        };
        let record = parse_identity_spec_record(&stored.manifest_yaml)?;
        Ok(Some(IdentitySpecSnapshot {
            record,
            input_material: stored.input_material,
        }))
    }

    pub(crate) fn rollback_identity_spec_snapshot_if_current_matches(
        &self,
        installed: &IdentitySpecSnapshot,
        previous: Option<&IdentitySpecSnapshot>,
    ) -> Result<bool, AppError> {
        self.features.ensure_dsl_v4_enabled()?;
        let name = validate_identity_spec_name(&installed.record.manifest.name)?;
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let Some(current) = self.registry.get_identity_spec(name.as_str())? else {
            return Ok(false);
        };
        if current.manifest_yaml != installed.record.manifest_yaml
            || current.input_material != installed.input_material
        {
            return Ok(false);
        }
        match previous {
            Some(previous) => self.registry.upsert_identity_spec(
                name.as_str(),
                IdentitySpecRegistryRecord {
                    manifest_yaml: previous.record.manifest_yaml.clone(),
                    input_material: previous.input_material.clone(),
                },
            )?,
            None => self.registry.remove_identity_spec(name.as_str())?,
        }
        Ok(true)
    }

    pub(crate) fn resolve_identity_spec_inputs(
        &self,
        manifest: &IdentityManifest,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let span = info_span!("coral.app.identity_specs.resolve_inputs");
        let _guard = span.enter();
        let name = validate_identity_spec_name(&manifest.name)?;
        let _lock = FileLock::shared(self.layout.state_lock())?;
        let stored = self
            .registry
            .get_identity_spec(name.as_str())?
            .map(|record| record.input_material)
            .unwrap_or_default();
        resolve_identity_spec_inputs_for_use(manifest, &stored)
    }

    pub(crate) fn remove_identity_spec(&self, name: &str, force: bool) -> Result<u32, AppError> {
        let span = info_span!("coral.app.identity_specs.remove");
        let _guard = span.enter();
        self.features.ensure_dsl_v4_enabled()?;
        let name = validate_identity_spec_name(name)?;
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        if !self.registry.identity_spec_exists(name.as_str())? {
            return Err(AppError::IdentitySpecNotFound(name.as_str().to_string()));
        }
        let orphaned_identities = self.count_identities_for_spec_unlocked(&name)?;
        if orphaned_identities > 0 && !force {
            return Err(AppError::FailedPrecondition(format!(
                "identity spec '{name}' is used by {orphaned_identities} stored {}; rerun with --force to orphan {}",
                plural_identity(orphaned_identities),
                plural_pronoun(orphaned_identities)
            )));
        }
        self.registry.remove_identity_spec(name.as_str())?;
        Ok(orphaned_identities)
    }

    fn load_identity_spec_manifest_unlocked(
        &self,
        name: &IdentitySpecName,
    ) -> Result<IdentitySpecRecord, AppError> {
        let Some(manifest_yaml) = self.registry.get_identity_spec_manifest(name.as_str())? else {
            return Err(AppError::IdentitySpecNotFound(name.as_str().to_string()));
        };
        let record = parse_identity_spec_record(&manifest_yaml)?;
        if record.manifest.name != name.as_str() {
            return Err(AppError::FailedPrecondition(format!(
                "identity spec registry record '{name}' contains identity spec '{}'",
                record.manifest.name
            )));
        }
        Ok(record)
    }

    fn count_identities_for_spec_unlocked(
        &self,
        identity_spec_name: &IdentitySpecName,
    ) -> Result<u32, AppError> {
        let mut count = self.count_stored_identities_for_spec_unlocked(identity_spec_name)?;
        for provider in &self.usage_providers {
            count = checked_add_identity_count(
                count,
                provider.count_identities_for_spec(identity_spec_name.as_str())?,
                identity_spec_name.as_str(),
            )?;
        }
        Ok(count)
    }

    fn count_stored_identities_for_spec_unlocked(
        &self,
        identity_spec_name: &IdentitySpecName,
    ) -> Result<u32, AppError> {
        let identities_root = self.layout.identities_root();
        if !identities_root.exists() {
            return Ok(0);
        }
        let mut count = 0u32;
        for owner_kind_entry in fs::read_dir(identities_root)? {
            let owner_kind_entry = owner_kind_entry?;
            if !owner_kind_entry.file_type()?.is_dir() {
                continue;
            }
            for owner_entry in fs::read_dir(owner_kind_entry.path())? {
                let owner_entry = owner_entry?;
                if !owner_entry.file_type()?.is_dir() {
                    continue;
                }
                for identity_entry in fs::read_dir(owner_entry.path())? {
                    let identity_entry = identity_entry?;
                    if !identity_entry.file_type()?.is_dir() {
                        continue;
                    }
                    let manifest_path = identity_entry.path().join(INSTALLED_IDENTITY_FILE_NAME);
                    if !manifest_path.exists() {
                        continue;
                    }
                    let raw = fs::read_to_string(&manifest_path)?;
                    let reference: StoredIdentitySpecReference = serde_yaml::from_str(&raw)?;
                    if reference.identity_spec == identity_spec_name.as_str() {
                        count = checked_add_identity_count(count, 1, identity_spec_name.as_str())?;
                    }
                }
            }
        }
        Ok(count)
    }
}

struct FileIdentitySpecRegistry {
    layout: AppStateLayout,
    credential_store: CredentialStore,
}

impl std::fmt::Debug for FileIdentitySpecRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIdentitySpecRegistry")
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl FileIdentitySpecRegistry {
    fn new(layout: AppStateLayout, credential_store: CredentialStore) -> Self {
        Self {
            layout,
            credential_store,
        }
    }

    fn input_material_state_file(&self, name: &IdentitySpecName) -> std::path::PathBuf {
        self.layout
            .identity_spec_dir(name)
            .join(IDENTITY_SPEC_INPUT_MATERIAL_STATE_FILE_NAME)
    }

    fn credential_set_id(name: &IdentitySpecName) -> CredentialSetId {
        CredentialSetId::for_identity_spec(name.as_str())
    }

    fn input_material_workspace() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn load_input_material_state(
        &self,
        name: &IdentitySpecName,
    ) -> Result<IdentitySpecInputMaterialState, AppError> {
        let path = self.input_material_state_file(name);
        if !path.exists() {
            return Ok(IdentitySpecInputMaterialState::default());
        }
        let raw = fs::read_to_string(&path)?;
        let state: IdentitySpecInputMaterialState = serde_yaml::from_str(&raw)?;
        if state.version != IDENTITY_SPEC_INPUT_MATERIAL_STATE_VERSION {
            return Err(AppError::FailedPrecondition(format!(
                "identity spec '{name}' input material state has unsupported version {}",
                state.version
            )));
        }
        Ok(state)
    }

    fn write_input_material_state(
        &self,
        name: &IdentitySpecName,
        state: &IdentitySpecInputMaterialState,
    ) -> Result<(), AppError> {
        let path = self.input_material_state_file(name);
        if state.credential_storage.is_none() {
            remove_file_if_exists(&path)?;
            return Ok(());
        }
        let raw = serde_yaml::to_string(state)?;
        storage_fs::write_atomic(&path, raw.as_bytes())?;
        Ok(())
    }

    fn read_input_material(
        &self,
        name: &IdentitySpecName,
        storage: CredentialStorageKind,
    ) -> Result<BTreeMap<String, String>, AppError> {
        self.credential_store.read_material(
            &Self::input_material_workspace(),
            &Self::credential_set_id(name),
            storage,
        )
    }

    fn replace_input_material(
        &self,
        name: &IdentitySpecName,
        existing_storage: Option<CredentialStorageKind>,
        next_storage: Option<CredentialStorageKind>,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        if let Some(existing_storage) = existing_storage
            && Some(existing_storage) != next_storage
        {
            self.credential_store.remove_material_unlocked(
                &Self::input_material_workspace(),
                &Self::credential_set_id(name),
                existing_storage,
            )?;
        }
        let Some(storage) = next_storage else {
            remove_file_if_exists(&self.layout.identity_spec_material_file(name))?;
            return Ok(());
        };
        self.credential_store.replace_material_unlocked(
            &Self::input_material_workspace(),
            &Self::credential_set_id(name),
            storage,
            material,
        )
    }

    fn restore_after_failed_upsert(
        &self,
        name: &IdentitySpecName,
        rollback: &IdentitySpecUpsertRollback,
    ) -> Result<(), AppError> {
        if let (Some(storage), Some(material)) = (
            rollback.previous_storage,
            rollback.previous_input_material.as_ref(),
        ) {
            self.replace_input_material(name, None, Some(storage), material)?;
        } else {
            if let Some(storage) = rollback.new_storage {
                self.credential_store.remove_material_unlocked(
                    &Self::input_material_workspace(),
                    &Self::credential_set_id(name),
                    storage,
                )?;
            }
            remove_file_if_exists(&self.layout.identity_spec_material_file(name))?;
        }

        match rollback.previous_material_state.as_ref() {
            Some(state) => self.write_input_material_state(name, state)?,
            None => remove_file_if_exists(&self.input_material_state_file(name))?,
        }

        match rollback.previous_manifest.as_ref() {
            Some(bytes) => storage_fs::write_atomic(&rollback.manifest_path, bytes)?,
            None => remove_file_if_exists(&rollback.manifest_path)?,
        }
        if rollback.previous_manifest.is_none()
            && let Some(parent) = rollback.manifest_path.parent()
        {
            drop(fs::remove_dir(parent));
        }
        Ok(())
    }

    fn remove_input_material(&self, name: &IdentitySpecName) -> Result<(), AppError> {
        let state = self.load_input_material_state(name)?;
        if let Some(storage) = state.credential_storage {
            self.credential_store.remove_material_unlocked(
                &Self::input_material_workspace(),
                &Self::credential_set_id(name),
                storage,
            )?;
        }
        remove_file_if_exists(&self.input_material_state_file(name))?;
        remove_file_if_exists(&self.layout.identity_spec_material_file(name))?;
        Ok(())
    }
}

impl IdentitySpecRegistry for FileIdentitySpecRegistry {
    fn list_identity_specs(&self) -> Result<Vec<IdentitySpecRegistryRecord>, AppError> {
        let root = self.layout.identity_specs_root();
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
            if let Some(record) = self.get_identity_spec(&name)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    fn list_identity_spec_manifests(&self) -> Result<Vec<String>, AppError> {
        let root = self.layout.identity_specs_root();
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut manifests = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(ToString::to_string) else {
                continue;
            };
            if let Some(manifest_yaml) = self.get_identity_spec_manifest(&name)? {
                manifests.push(manifest_yaml);
            }
        }
        Ok(manifests)
    }

    fn get_identity_spec(
        &self,
        name: &str,
    ) -> Result<Option<IdentitySpecRegistryRecord>, AppError> {
        let name = validate_identity_spec_name(name)?;
        let path = self.layout.identity_spec_manifest_file(&name);
        if !path.exists() {
            return Ok(None);
        }
        let manifest_yaml = fs::read_to_string(&path)?;
        let state = self.load_input_material_state(&name)?;
        let input_material = match state.credential_storage {
            Some(storage) => self.read_input_material(&name, storage)?,
            None => BTreeMap::new(),
        };
        Ok(Some(IdentitySpecRegistryRecord {
            manifest_yaml,
            input_material,
        }))
    }

    fn get_identity_spec_manifest(&self, name: &str) -> Result<Option<String>, AppError> {
        let name = validate_identity_spec_name(name)?;
        let path = self.layout.identity_spec_manifest_file(&name);
        if !path.exists() {
            return Ok(None);
        }
        fs::read_to_string(&path).map(Some).map_err(Into::into)
    }

    fn identity_spec_exists(&self, name: &str) -> Result<bool, AppError> {
        let name = validate_identity_spec_name(name)?;
        Ok(self.layout.identity_spec_manifest_file(&name).exists())
    }

    fn upsert_identity_spec(
        &self,
        name: &str,
        record: IdentitySpecRegistryRecord,
    ) -> Result<(), AppError> {
        let name = validate_identity_spec_name(name)?;
        let path = self.layout.identity_spec_manifest_file(&name);
        let previous_manifest = match fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let previous_material_state = if previous_manifest.is_some() {
            Some(self.load_input_material_state(&name)?)
        } else {
            None
        };
        let existing_storage = previous_material_state
            .as_ref()
            .and_then(|state| state.credential_storage);
        let previous_input_material = existing_storage
            .map(|storage| self.read_input_material(&name, storage))
            .transpose()?;
        let credential_storage = if record.input_material.is_empty() {
            None
        } else {
            existing_storage.or(Some(self.credential_store.default_write_storage()?))
        };
        let rollback = IdentitySpecUpsertRollback {
            manifest_path: path.clone(),
            previous_manifest,
            previous_material_state,
            previous_storage: existing_storage,
            previous_input_material,
            new_storage: credential_storage,
        };
        if let Some(parent) = path.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let result = self
            .replace_input_material(
                &name,
                existing_storage,
                credential_storage,
                &record.input_material,
            )
            .and_then(|()| {
                storage_fs::write_atomic(&path, record.manifest_yaml.as_bytes()).map_err(Into::into)
            })
            .and_then(|()| {
                self.write_input_material_state(
                    &name,
                    &IdentitySpecInputMaterialState {
                        credential_storage,
                        version: IDENTITY_SPEC_INPUT_MATERIAL_STATE_VERSION,
                    },
                )
            });
        if let Err(error) = result {
            if let Err(rollback_error) = self.restore_after_failed_upsert(&name, &rollback) {
                warn!(
                    identity_spec = %name,
                    error = %rollback_error,
                    "failed to roll back identity spec upsert"
                );
            }
            return Err(error);
        }
        Ok(())
    }

    fn remove_identity_spec(&self, name: &str) -> Result<(), AppError> {
        let name = validate_identity_spec_name(name)?;
        let path = self.layout.identity_spec_manifest_file(&name);
        if !path.exists() {
            return Ok(());
        }
        self.remove_input_material(&name)?;
        remove_file_if_exists(&path)?;
        if let Some(parent) = path.parent() {
            drop(fs::remove_dir(parent));
        }
        Ok(())
    }
}

fn checked_add_identity_count(
    left: u32,
    right: u32,
    identity_spec_name: &str,
) -> Result<u32, AppError> {
    left.checked_add(right).ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "too many stored identities reference identity spec '{identity_spec_name}'"
        ))
    })
}

/// Hashes the parsed identity manifest so stored identities can detect when
/// the spec they were created from has changed.
///
/// The manifest serializes with `serde` (struct fields in declaration order)
/// and `canonical_json_value` sorts any free-form JSON objects (such as
/// `audience` values), so semantically equal manifests fingerprint equally.
pub(crate) fn identity_spec_fingerprint(manifest: &IdentityManifest) -> Result<String, AppError> {
    let encode_error = |error: &dyn std::fmt::Display| {
        AppError::FailedPrecondition(format!(
            "failed to encode identity spec '{}' fingerprint: {error}",
            manifest.name
        ))
    };
    let value = canonical_json_value(serde_json::to_value(manifest).map_err(|e| encode_error(&e))?);
    let bytes = serde_json::to_vec(&value).map_err(|e| encode_error(&e))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn canonical_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries = map.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(canonical_json_value)
                .collect::<Vec<_>>(),
        ),
        scalar => scalar,
    }
}

#[derive(Debug, Deserialize)]
struct StoredIdentitySpecReference {
    identity_spec: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdentitySpecInputMaterialState {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    credential_storage: Option<CredentialStorageKind>,
}

struct IdentitySpecUpsertRollback {
    manifest_path: std::path::PathBuf,
    previous_manifest: Option<Vec<u8>>,
    previous_material_state: Option<IdentitySpecInputMaterialState>,
    previous_storage: Option<CredentialStorageKind>,
    previous_input_material: Option<BTreeMap<String, String>>,
    new_storage: Option<CredentialStorageKind>,
}

impl Default for IdentitySpecInputMaterialState {
    fn default() -> Self {
        Self {
            version: IDENTITY_SPEC_INPUT_MATERIAL_STATE_VERSION,
            credential_storage: None,
        }
    }
}

fn merge_identity_spec_input_material(
    manifest: &IdentityManifest,
    stored: &BTreeMap<String, String>,
    provided: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AppError> {
    let declared = manifest
        .inputs
        .iter()
        .map(|input| input.key.as_str())
        .collect::<BTreeSet<_>>();
    for key in provided.keys() {
        if !declared.contains(key.as_str()) {
            return Err(AppError::InvalidInput(format!(
                "unknown identity spec input '{key}' for identity spec '{}'",
                manifest.name
            )));
        }
    }

    let mut material = BTreeMap::new();
    for input in &manifest.inputs {
        if let Some(value) = provided
            .get(&input.key)
            .and_then(|value| trimmed_non_empty_value(value))
            .or_else(|| {
                stored
                    .get(&input.key)
                    .and_then(|value| trimmed_non_empty_value(value))
            })
        {
            material.insert(input.key.clone(), value);
        }
    }
    resolve_identity_spec_inputs_for_use(manifest, &material)?;
    Ok(material)
}

fn resolve_identity_spec_inputs_for_use(
    manifest: &IdentityManifest,
    material: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut resolved = BTreeMap::new();
    for input in &manifest.inputs {
        let value = material
            .get(&input.key)
            .and_then(|value| trimmed_non_empty_value(value))
            .or_else(|| {
                (input.kind == ManifestInputKind::Variable && !input.default_value.is_empty())
                    .then(|| input.default_value.clone())
            });
        if let Some(value) = value {
            resolved.insert(input.key.clone(), value);
        } else if input.required {
            return Err(AppError::FailedPrecondition(format!(
                "missing identity spec input '{}' for identity spec '{}'",
                input.key, manifest.name
            )));
        }
    }
    Ok(resolved)
}

fn trimmed_non_empty_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn remove_file_if_exists(path: &std::path::Path) -> Result<(), AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn plural_identity(count: u32) -> &'static str {
    if count == 1 { "identity" } else { "identities" }
}

fn plural_pronoun(count: u32) -> &'static str {
    if count == 1 { "it" } else { "them" }
}

fn parse_identity_spec_record(manifest_yaml: &str) -> Result<IdentitySpecRecord, AppError> {
    let manifest = parse_identity_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(IdentitySpecRecord {
        manifest_yaml: normalized_manifest_yaml(manifest_yaml),
        manifest,
    })
}

fn normalized_manifest_yaml(manifest_yaml: &str) -> String {
    if manifest_yaml.ends_with('\n') {
        manifest_yaml.to_string()
    } else {
        format!("{manifest_yaml}\n")
    }
}

pub(crate) fn validate_identity_spec_name(name: &str) -> Result<IdentitySpecName, AppError> {
    IdentitySpecName::parse(name)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use coral_spec::ManifestInputKind;

    use super::{
        IDENTITY_SPEC_INPUT_MATERIAL_STATE_FILE_NAME, IdentitySpecInputValue, IdentitySpecManager,
        IdentitySpecName, IdentitySpecRecord, IdentitySpecUsageProvider, identity_spec_fingerprint,
    };
    use crate::bootstrap::AppError;
    use crate::credentials::{CredentialStoragePreference, CredentialStore};
    use crate::features::{Features, dsl_v4_features};
    use crate::identities::{IdentityName, IdentityOwner};
    use crate::state::AppStateLayout;
    use crate::storage::env_file::parse_env_file;

    fn manager() -> (TempDir, IdentitySpecManager, AppStateLayout) {
        manager_with(dsl_v4_features(), Vec::new())
    }

    fn identity_spec_name(name: &str) -> IdentitySpecName {
        IdentitySpecName::parse(name).expect("identity spec name")
    }

    fn manager_with(
        features: Features,
        usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
    ) -> (TempDir, IdentitySpecManager, AppStateLayout) {
        let temp = TempDir::new().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout directories");
        let manager = IdentitySpecManager::new_with_usage_providers(
            layout.clone(),
            features,
            usage_providers,
        );
        (temp, manager, layout)
    }

    #[track_caller]
    fn add_spec(manager: &IdentitySpecManager, yaml: &str) -> (IdentitySpecRecord, bool) {
        manager.add_identity_spec(yaml).expect("add identity spec")
    }

    #[track_caller]
    fn add_spec_with_secret(
        manager: &IdentitySpecManager,
        yaml: &str,
    ) -> (IdentitySpecRecord, bool) {
        add_spec_with_secret_value(manager, yaml, "client-secret")
    }

    #[track_caller]
    fn add_spec_with_secret_value(
        manager: &IdentitySpecManager,
        yaml: &str,
        value: &str,
    ) -> (IdentitySpecRecord, bool) {
        manager
            .add_identity_spec_with_inputs(
                yaml,
                vec![IdentitySpecInputValue {
                    key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
                    value: value.to_string(),
                }],
            )
            .expect("add identity spec with inputs")
    }

    #[track_caller]
    fn remove_spec(manager: &IdentitySpecManager, name: &str, force: bool) -> u32 {
        manager
            .remove_identity_spec(name, force)
            .expect("remove identity spec")
    }

    #[track_caller]
    fn stored_spec(manager: &IdentitySpecManager, name: &str) -> IdentitySpecRecord {
        manager.get_identity_spec(name).expect("stored spec")
    }

    #[track_caller]
    fn resolved_inputs(
        manager: &IdentitySpecManager,
        record: &IdentitySpecRecord,
    ) -> BTreeMap<String, String> {
        manager
            .resolve_identity_spec_inputs(&record.manifest)
            .expect("resolve stored inputs")
    }

    #[track_caller]
    fn assert_input(resolved: &BTreeMap<String, String>, key: &str, expected: &str) {
        assert_eq!(resolved.get(key).map(String::as_str), Some(expected));
    }

    #[track_caller]
    fn assert_failed_precondition(error: &AppError, fragments: [&str; 2]) {
        match error {
            AppError::FailedPrecondition(message) => {
                for fragment in fragments {
                    assert!(
                        message.contains(fragment),
                        "missing {fragment:?} in: {message}"
                    );
                }
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn identity_spec_operations_require_dsl_v4_feature() {
        let (_temp, manager, _layout) = manager_with(Features::default(), Vec::new());

        let errors = [
            manager
                .add_identity_spec(&identity_yaml("github_oauth", "0.1.0"))
                .expect_err("add requires the dsl_v4 feature"),
            manager
                .list_identity_specs()
                .expect_err("list requires the dsl_v4 feature"),
            manager
                .remove_identity_spec("github_oauth", true)
                .expect_err("remove requires the dsl_v4 feature"),
        ];

        for error in errors {
            assert!(
                matches!(&error, AppError::SourceUnservable(message) if message.contains("dsl_v4")),
                "unexpected error: {error:?}"
            );
        }
    }

    #[derive(Debug)]
    struct StaticUsageProvider {
        identity_spec_name: String,
        count: u32,
    }

    impl IdentitySpecUsageProvider for StaticUsageProvider {
        fn count_identities_for_spec(&self, identity_spec_name: &str) -> Result<u32, AppError> {
            if identity_spec_name == self.identity_spec_name {
                Ok(self.count)
            } else {
                Ok(0)
            }
        }
    }

    fn identity_yaml(name: &str, version: &str) -> String {
        identity_yaml_with_audience(name, version, "github.com")
    }

    fn identity_yaml_with_audience(name: &str, version: &str, audience_host: &str) -> String {
        identity_yaml_with_audience_block(name, version, &format!("  host: {audience_host}\n"))
    }

    fn identity_yaml_with_audience_block(name: &str, version: &str, audience: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: {name}
version: {version}
description: Demo identity.
issuer: github
type: fixed_token
audience:
{audience}"
        )
    }

    fn oauth_identity_yaml_with_inputs() -> String {
        oauth_identity_yaml_with_inputs_default("tenant-a")
    }

    fn oauth_identity_yaml_with_inputs_default(default_tenant: &str) -> String {
        r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
description: Demo OAuth identity.
issuer: demo
type: oauth
audience:
  host: api.example.test
inputs:
  DEMO_TENANT:
    kind: variable
    required: false
    default: {{default_tenant}}
  DEMO_OAUTH_CLIENT_SECRET:
    kind: secret
    required: true
oauth:
  method:
    label: Demo OAuth
    flow:
      type: authorization_code
      pkce: required
    redirect_uri: http://127.0.0.1:53682/callback
    endpoints:
      authorization_url: https://auth.example.test/{{input.DEMO_TENANT}}/authorize
      token_url: https://auth.example.test/{{input.DEMO_TENANT}}/token
    client:
      id:
        default: demo-client
      secret:
        input: DEMO_OAUTH_CLIENT_SECRET
        transport: request_body
"
        .replace("{{default_tenant}}", default_tenant)
    }

    fn github_oauth_identity_yaml_with_defaulted_client_id_input() -> &'static str {
        r"
kind: identity
spec_version: 1
name: github_oauth
version: 0.1.0
description: GitHub OAuth access token.
issuer: github
type: oauth
audience:
  host: github.com
inputs:
  GITHUB_OAUTH_CLIENT_ID:
    kind: variable
    default: demo-client
oauth:
  method:
    label: Connect with GitHub device code
    flow:
      type: device_code
    endpoints:
      device_authorization_url: https://github.com/login/device/code
      token_url: https://github.com/login/oauth/access_token
    client:
      id:
        default: demo-client
        input: GITHUB_OAUTH_CLIENT_ID
"
    }

    fn malformed_github_oauth_identity_yaml_missing_client_id_input() -> &'static str {
        r"
kind: identity
spec_version: 1
name: github_oauth
version: 0.1.0
description: Bad prior install.
issuer: github
type: oauth
audience:
  host: github.com
oauth:
  method:
    flow:
      type: device_code
    endpoints:
      device_authorization_url: https://github.com/login/device/code
      token_url: https://github.com/login/oauth/access_token
    client:
      id:
        input: GITHUB_OAUTH_CLIENT_ID
"
    }

    /// Merges the previous standalone replace test into the CRUD round-trip:
    /// the version 0.2.0 re-add runs over the same stored spec.
    #[test]
    fn add_lists_gets_replaces_and_removes_identity_specs() {
        let (_temp, manager, _layout) = manager();

        let (record, replaced) = add_spec(&manager, &identity_yaml("github_oauth", "0.1.0"));
        assert!(!replaced);
        assert_eq!(record.manifest.name, "github_oauth");

        let list = manager.list_identity_specs().expect("list identity specs");
        assert_eq!(list.len(), 1);
        assert_eq!(
            list.first().expect("one identity spec").manifest.version,
            "0.1.0"
        );
        assert_eq!(
            stored_spec(&manager, "github_oauth").manifest.description,
            "Demo identity."
        );

        let (record, replaced) = add_spec(&manager, &identity_yaml("github_oauth", "0.2.0"));
        assert!(replaced);
        assert_eq!(record.manifest.version, "0.2.0");
        assert_eq!(
            stored_spec(&manager, "github_oauth").manifest.version,
            "0.2.0"
        );

        let orphaned = remove_spec(&manager, "github_oauth", false);
        assert_eq!(orphaned, 0);
        assert!(
            manager
                .list_identity_specs()
                .expect("list again")
                .is_empty()
        );
    }

    #[test]
    fn add_identity_spec_repairs_malformed_existing_record() {
        let (_temp, manager, layout) = manager();
        let manifest_path = layout.identity_spec_manifest_file(&identity_spec_name("github_oauth"));
        fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create identity spec dir");
        fs::write(
            &manifest_path,
            malformed_github_oauth_identity_yaml_missing_client_id_input(),
        )
        .expect("write malformed identity spec");
        let error = manager
            .get_identity_spec("github_oauth")
            .expect_err("stored spec is malformed");
        assert!(
            error
                .to_string()
                .contains("must reference a declared variable input"),
            "unexpected error: {error}"
        );

        let (record, replaced) = add_spec(
            &manager,
            github_oauth_identity_yaml_with_defaulted_client_id_input(),
        );

        assert!(replaced);
        assert_eq!(record.manifest.name, "github_oauth");
        assert!(record.manifest.inputs.iter().any(|input| {
            input.key == "GITHUB_OAUTH_CLIENT_ID" && input.kind == ManifestInputKind::Variable
        }));
        assert_eq!(
            stored_spec(&manager, "github_oauth").manifest.inputs.len(),
            1
        );
    }

    /// Merges the previous missing-input rejection, declared-input storage,
    /// and removal tests into one spec-owned input material lifecycle: the
    /// add fails without the required secret, succeeds with it (storing the
    /// material under the spec), and removal deletes the material with it.
    #[test]
    fn spec_owned_input_material_is_required_stored_on_spec_and_removed_with_it() {
        let (_temp, manager, layout) = manager();

        let error = manager
            .add_identity_spec(&oauth_identity_yaml_with_inputs())
            .expect_err("missing required input should fail");
        assert!(
            error
                .to_string()
                .contains("missing identity spec input 'DEMO_OAUTH_CLIENT_SECRET'"),
            "unexpected error: {error}"
        );

        let (record, replaced) = add_spec_with_secret(&manager, &oauth_identity_yaml_with_inputs());
        assert!(!replaced);
        let resolved = resolved_inputs(&manager, &record);
        assert_input(&resolved, "DEMO_TENANT", "tenant-a");
        assert_input(&resolved, "DEMO_OAUTH_CLIENT_SECRET", "client-secret");
        assert!(
            layout
                .identity_spec_material_file(&identity_spec_name("demo_oauth"))
                .exists(),
            "input material belongs under the installed identity spec"
        );
        let stored = parse_env_file(
            &fs::read_to_string(
                layout.identity_spec_material_file(&identity_spec_name("demo_oauth")),
            )
            .expect("identity spec material file"),
        )
        .expect("parse material");
        assert!(
            !stored.contains_key("DEMO_TENANT"),
            "manifest defaults must not be persisted as identity-spec input material"
        );

        remove_spec(&manager, "demo_oauth", false);

        assert!(
            !layout
                .identity_spec_material_file(&identity_spec_name("demo_oauth"))
                .exists()
        );
        assert!(
            !layout
                .identity_spec_dir(&identity_spec_name("demo_oauth"))
                .exists()
        );
    }

    #[test]
    fn spec_owned_input_material_is_trimmed_and_whitespace_only_is_missing() {
        let (_temp, trimming_manager, layout) = manager();
        let (record, _replaced) = trimming_manager
            .add_identity_spec_with_inputs(
                &oauth_identity_yaml_with_inputs(),
                vec![IdentitySpecInputValue {
                    key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
                    value: "  client-secret  \n".to_string(),
                }],
            )
            .expect("trimmed input should satisfy required secret");

        let resolved = resolved_inputs(&trimming_manager, &record);
        assert_input(&resolved, "DEMO_OAUTH_CLIENT_SECRET", "client-secret");
        let stored = parse_env_file(
            &fs::read_to_string(
                layout.identity_spec_material_file(&identity_spec_name("demo_oauth")),
            )
            .expect("identity spec material file"),
        )
        .expect("parse material");
        assert_eq!(
            stored.get("DEMO_OAUTH_CLIENT_SECRET").map(String::as_str),
            Some("client-secret")
        );

        let (_temp, manager, _layout) = manager();
        let error = manager
            .add_identity_spec_with_inputs(
                &oauth_identity_yaml_with_inputs(),
                vec![IdentitySpecInputValue {
                    key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
                    value: " \t\n ".to_string(),
                }],
            )
            .expect_err("whitespace-only secret should be missing");
        assert_failed_precondition(
            &error,
            ["missing identity spec input", "DEMO_OAUTH_CLIENT_SECRET"],
        );
    }

    #[test]
    fn remove_identity_spec_deletes_malformed_input_material() {
        let (_temp, manager, layout) = manager();
        add_spec_with_secret(&manager, &oauth_identity_yaml_with_inputs());

        let material_file = layout.identity_spec_material_file(&identity_spec_name("demo_oauth"));
        fs::write(&material_file, "not env material\n").expect("write malformed material");
        let record = manager
            .get_identity_spec("demo_oauth")
            .expect("metadata reads should not parse stored material");
        assert_eq!(record.manifest.name, "demo_oauth");
        assert_eq!(
            manager
                .list_identity_specs()
                .expect("list should not parse stored material")
                .len(),
            1
        );
        manager
            .resolve_identity_spec_inputs(&record.manifest)
            .expect_err("material resolution should still fail on malformed material");

        remove_spec(&manager, "demo_oauth", false);

        assert!(
            !material_file.exists(),
            "malformed material should be deleted"
        );
        assert!(
            !layout
                .identity_spec_dir(&identity_spec_name("demo_oauth"))
                .exists()
        );
    }

    #[test]
    fn failed_input_material_cleanup_keeps_identity_spec_retryable() {
        let temp = TempDir::new().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout directories");
        let credential_store = CredentialStore::with_unavailable_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::File,
        );
        let manager = IdentitySpecManager::new_with_credential_store(
            layout.clone(),
            credential_store,
            dsl_v4_features(),
            Vec::new(),
        );
        add_spec_with_secret(&manager, &oauth_identity_yaml_with_inputs());

        let manifest_file = layout.identity_spec_manifest_file(&identity_spec_name("demo_oauth"));
        let state_file = layout
            .identity_spec_dir(&identity_spec_name("demo_oauth"))
            .join(IDENTITY_SPEC_INPUT_MATERIAL_STATE_FILE_NAME);
        fs::write(&state_file, "version: 1\ncredential_storage: keychain\n")
            .expect("route cleanup through unavailable keychain");

        let error = manager
            .remove_identity_spec("demo_oauth", false)
            .expect_err("keychain cleanup should fail");
        assert!(
            error.to_string().contains("keychain is unavailable"),
            "unexpected error: {error}"
        );
        assert!(
            manifest_file.exists(),
            "manifest must remain so deletion can be retried"
        );

        fs::write(&state_file, "version: 1\ncredential_storage: file\n")
            .expect("repair input material state");
        remove_spec(&manager, "demo_oauth", false);

        assert!(!manifest_file.exists());
        assert!(
            !layout
                .identity_spec_dir(&identity_spec_name("demo_oauth"))
                .exists()
        );
    }

    #[test]
    fn failed_identity_spec_state_write_rolls_back_input_material() {
        let (_temp, manager, layout) = manager();
        let (record, _replaced) =
            add_spec_with_secret_value(&manager, &oauth_identity_yaml_with_inputs(), "old-secret");
        let state_file = layout
            .identity_spec_dir(&identity_spec_name("demo_oauth"))
            .join(IDENTITY_SPEC_INPUT_MATERIAL_STATE_FILE_NAME);
        let state_temp_file = state_file.with_file_name(format!(
            "{IDENTITY_SPEC_INPUT_MATERIAL_STATE_FILE_NAME}.tmp.{}",
            std::process::id()
        ));
        fs::create_dir_all(&state_temp_file).expect("block state atomic write temp path");

        let error = manager
            .add_identity_spec_with_inputs(
                &oauth_identity_yaml_with_inputs(),
                vec![IdentitySpecInputValue {
                    key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
                    value: "new-secret".to_string(),
                }],
            )
            .expect_err("blocked state write should fail");
        assert!(
            error.to_string().contains("Is a directory")
                || error.to_string().contains("is a directory"),
            "unexpected error: {error}"
        );
        fs::remove_dir_all(&state_temp_file).expect("unblock state temp path");

        let resolved = resolved_inputs(&manager, &record);
        assert_input(&resolved, "DEMO_OAUTH_CLIENT_SECRET", "old-secret");
        assert_eq!(
            stored_spec(&manager, "demo_oauth").manifest.version,
            "0.1.0"
        );
    }

    /// Merges the previous `readding_identity_spec_preserves_existing_input_material`
    /// coverage with the changed-default re-add: both re-adds run against the same
    /// stored material.
    #[test]
    fn readding_identity_spec_preserves_material_and_uses_new_manifest_default() {
        let (_temp, manager, _layout) = manager();
        let (record, _replaced) = add_spec_with_secret(
            &manager,
            &oauth_identity_yaml_with_inputs_default("tenant-a"),
        );

        // Re-adding the identical manifest keeps the stored input material.
        add_spec(
            &manager,
            &oauth_identity_yaml_with_inputs_default("tenant-a"),
        );
        let resolved = resolved_inputs(&manager, &record);
        assert_input(&resolved, "DEMO_OAUTH_CLIENT_SECRET", "client-secret");

        // Re-adding with a changed manifest default uses the new default without
        // persisting the old one.
        let (record, replaced) = add_spec(
            &manager,
            &oauth_identity_yaml_with_inputs_default("tenant-b"),
        );
        assert!(replaced);
        let resolved = resolved_inputs(&manager, &record);
        assert_input(&resolved, "DEMO_TENANT", "tenant-b");
        assert_input(&resolved, "DEMO_OAUTH_CLIENT_SECRET", "client-secret");
    }

    /// Merges the identical-manifest and changed-manifest re-add tests over
    /// one spec referenced by a stored identity: re-adding the identical
    /// manifest is allowed, while a different manifest is rejected and
    /// leaves the stored spec untouched.
    #[test]
    fn add_over_used_identity_spec_allows_identical_manifest_and_rejects_changes() {
        let (_temp, manager, layout) = manager();
        let manifest_yaml = identity_yaml_with_audience("github_oauth", "0.1.0", "github.com");
        add_spec(&manager, &manifest_yaml);
        write_identity_manifest(&layout, &local_user_owner(), "github_local", "github_oauth");

        let (record, replaced) = add_spec(&manager, &manifest_yaml);
        assert!(replaced);
        assert_eq!(record.manifest.version, "0.1.0");

        let error = manager
            .add_identity_spec(&identity_yaml_with_audience(
                "github_oauth",
                "0.1.0",
                "attacker.test",
            ))
            .expect_err("used identity spec replacement should fail");
        assert_failed_precondition(&error, ["1 stored identity", "cannot be replaced"]);
        assert_eq!(
            stored_spec(&manager, "github_oauth")
                .manifest
                .audience
                .get("host")
                .and_then(serde_json::Value::as_str),
            Some("github.com")
        );
    }

    #[test]
    fn identity_spec_fingerprint_canonicalizes_nested_json_object_order() {
        let left = coral_spec::parse_identity_manifest_yaml(&identity_yaml_with_audience_block(
            "github_oauth",
            "0.1.0",
            "  host: github.com\n  tenant: acme\n",
        ))
        .expect("left manifest");
        let right = coral_spec::parse_identity_manifest_yaml(&identity_yaml_with_audience_block(
            "github_oauth",
            "0.1.0",
            "  tenant: acme\n  host: github.com\n",
        ))
        .expect("right manifest");

        assert_eq!(left, right);
        assert_eq!(
            identity_spec_fingerprint(&left).expect("left fingerprint"),
            identity_spec_fingerprint(&right).expect("right fingerprint")
        );
    }

    #[test]
    fn conditional_identity_spec_rollback_removes_matching_import() {
        let (_temp, manager, _layout) = manager();
        add_spec(
            &manager,
            &identity_yaml_with_audience("github_oauth", "0.1.0", "github.com"),
        );
        let installed = manager
            .snapshot_identity_spec("github_oauth")
            .expect("snapshot")
            .expect("installed spec");

        let rolled_back = manager
            .rollback_identity_spec_snapshot_if_current_matches(&installed, None)
            .expect("rollback");

        assert!(rolled_back);
        assert!(matches!(
            manager.get_identity_spec("github_oauth"),
            Err(AppError::IdentitySpecNotFound(_))
        ));
    }

    #[test]
    fn conditional_identity_spec_rollback_skips_concurrent_update() {
        let (_temp, manager, _layout) = manager();
        add_spec(
            &manager,
            &identity_yaml_with_audience("github_oauth", "0.1.0", "github.com"),
        );
        let installed = manager
            .snapshot_identity_spec("github_oauth")
            .expect("snapshot")
            .expect("installed spec");
        add_spec(
            &manager,
            &identity_yaml_with_audience("github_oauth", "0.1.0", "api.github.com"),
        );

        let rolled_back = manager
            .rollback_identity_spec_snapshot_if_current_matches(&installed, None)
            .expect("rollback");

        assert!(!rolled_back);
        assert_eq!(
            stored_spec(&manager, "github_oauth")
                .manifest
                .audience
                .get("host")
                .and_then(serde_json::Value::as_str),
            Some("api.github.com")
        );
    }

    #[test]
    fn remove_missing_identity_spec_reports_not_found() {
        let (_temp, manager, _layout) = manager();

        let error = manager
            .remove_identity_spec("missing", false)
            .expect_err("missing identity spec");

        assert!(matches!(error, AppError::IdentitySpecNotFound(_)));
    }

    #[test]
    fn rejects_invalid_identity_manifest() {
        let (_temp, manager, _layout) = manager();

        let error = manager
            .add_identity_spec(
                r"
kind: identity
spec_version: 1
name: github_oauth
version: 0.1.0
issuer: github
type: oauth
",
            )
            .expect_err("invalid identity spec");

        assert!(
            error.to_string().contains("type oauth is missing oauth"),
            "unexpected error: {error}"
        );
    }

    /// Merges the previous without-force rejection test and the force-removal
    /// orphan report test into one flow over the same stored identities.
    #[test]
    fn remove_identity_spec_requires_force_and_reports_orphaned_identities() {
        let (_temp, manager, layout) = manager();
        add_spec(&manager, &identity_yaml("github_oauth", "0.1.0"));
        write_identity_manifest(&layout, &local_user_owner(), "github_local", "github_oauth");

        let error = manager
            .remove_identity_spec("github_oauth", false)
            .expect_err("remove should require force");

        assert_failed_precondition(&error, ["1 stored identity", "--force"]);
        manager
            .get_identity_spec("github_oauth")
            .expect("spec remains installed");

        write_identity_manifest(
            &layout,
            &default_workspace_owner(),
            "github_alt",
            "github_oauth",
        );
        write_identity_manifest(&layout, &local_user_owner(), "stripe_local", "stripe_oauth");

        let orphaned = remove_spec(&manager, "github_oauth", true);

        assert_eq!(orphaned, 2);
        assert!(matches!(
            manager
                .get_identity_spec("github_oauth")
                .expect_err("spec removed"),
            AppError::IdentitySpecNotFound(_)
        ));
    }

    #[test]
    fn remove_identity_spec_counts_external_identity_usage_provider() {
        let (_temp, manager, _layout) = manager_with(
            dsl_v4_features(),
            vec![Arc::new(StaticUsageProvider {
                identity_spec_name: "github_oauth".to_string(),
                count: 3,
            })],
        );
        add_spec(&manager, &identity_yaml("github_oauth", "0.1.0"));

        let error = manager
            .remove_identity_spec("github_oauth", false)
            .expect_err("remove should require force");
        assert_failed_precondition(&error, ["3 stored identities", "--force"]);

        let orphaned = remove_spec(&manager, "github_oauth", true);
        assert_eq!(orphaned, 3);
    }

    fn local_user_owner() -> IdentityOwner {
        IdentityOwner::user("local").expect("identity owner")
    }

    fn default_workspace_owner() -> IdentityOwner {
        IdentityOwner::workspace("default").expect("identity owner")
    }

    fn write_identity_manifest(
        layout: &AppStateLayout,
        owner: &IdentityOwner,
        identity_name: &str,
        identity_spec: &str,
    ) {
        let identity_name = IdentityName::new(identity_name).expect("identity name");
        let path = layout.identity_manifest_file(owner, &identity_name);
        fs::create_dir_all(path.parent().expect("identity dir")).expect("identity dir");
        fs::write(
            path,
            format!(
                r"version: 1
owner: {}
name: {identity_name}
identity_spec: {identity_spec}
issuer: github
identity_type: oauth
metadata: {{}}
",
                owner.kind().as_config_value()
            ),
        )
        .expect("write identity manifest");
    }
}
