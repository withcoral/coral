//! Persists the installed source catalog in top-level `config.toml`.

use std::collections::{BTreeMap, BTreeSet};

use coral_engine::{DependentJoinConfig, DependentJoinSourceConfig, MemorySize, QueryMemoryConfig};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};
use tracing::{info_span, warn};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::credentials::CredentialStorageKind;
use crate::functions::model::{FunctionName, InstalledFunction};
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::{DeletedWorkspace, WorkspaceName, WorkspaceRecord};

#[derive(Debug, Clone)]
pub(crate) struct AppConfig {
    version: u32,
    engine: PersistedEngineConfig,
    workspaces: WorkspaceCatalog,
    catalog: SourceCatalog,
    functions: FunctionCatalog,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: default_config_version(),
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog: SourceCatalog::default(),
            functions: FunctionCatalog::default(),
        }
    }
}

impl AppConfig {
    pub(crate) fn legacy_workspace_records(&self) -> Vec<WorkspaceRecord> {
        self.workspaces.list()
    }

    pub(crate) fn workspace_sources(&self, workspace_name: &WorkspaceName) -> Vec<InstalledSource> {
        self.catalog.workspace_sources(workspace_name)
    }

    pub(crate) fn get_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<InstalledSource> {
        self.catalog.get_source(workspace_name, source_name)
    }

    pub(crate) fn dependent_join_config(
        &self,
        selected_source_names: &[String],
    ) -> Result<DependentJoinConfig, AppError> {
        self.engine
            .dependent_join
            .clone()
            .try_into_runtime_config(selected_source_names)
    }

    pub(crate) fn memory_config(&self) -> Result<QueryMemoryConfig, AppError> {
        self.engine.memory.clone().try_into_runtime_config()
    }
}

fn default_config_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedAppConfig {
    #[serde(default = "default_config_version")]
    version: u32,
    #[serde(default)]
    engine: PersistedEngineConfig,
    #[serde(default)]
    workspaces: BTreeMap<String, PersistedWorkspaceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedEngineConfig {
    #[serde(default)]
    dependent_join: PersistedDependentJoinConfig,
    #[serde(default)]
    memory: PersistedMemoryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedMemoryConfig {
    #[serde(default)]
    limit: Option<toml::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedDependentJoinConfig {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    max_bindings: Option<usize>,
    #[serde(default)]
    max_resolver_rows: Option<usize>,
    #[serde(default)]
    max_rows_per_binding: Option<usize>,
    #[serde(default)]
    max_resolver_rows_per_binding: Option<usize>,
    #[serde(default)]
    max_concurrency: Option<usize>,
    #[serde(default)]
    per_source: BTreeMap<String, PersistedDependentJoinSourceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedDependentJoinSourceConfig {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    max_bindings: Option<usize>,
    #[serde(default)]
    max_resolver_rows: Option<usize>,
    #[serde(default)]
    max_rows_per_binding: Option<usize>,
    #[serde(default)]
    max_resolver_rows_per_binding: Option<usize>,
    #[serde(default)]
    max_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RawFeatureOverrides {
    entries: BTreeMap<String, RawFeatureValue>,
    container: RawFeatureContainerState,
}

impl RawFeatureOverrides {
    pub(crate) fn container(&self) -> RawFeatureContainerState {
        self.container
    }

    pub(crate) fn get(&self, key: &str) -> Option<RawFeatureValue> {
        self.entries.get(key).copied()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, RawFeatureValue)> + '_ {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), *value))
    }

    #[cfg(test)]
    pub(crate) fn from_entries_for_tests(
        entries: impl IntoIterator<Item = (&'static str, RawFeatureValue)>,
    ) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
            container: RawFeatureContainerState::Table,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum RawFeatureContainerState {
    #[default]
    Missing,
    Table,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawFeatureValue {
    Bool(bool),
    UnsupportedType,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedWorkspaceConfig {
    #[serde(default)]
    sources: BTreeMap<String, PersistedInstalledSource>,
    // The persisted TOML shape is `functions.<name> = {}` so existing workspace
    // configs keep round-tripping even though installed functions are membership-only.
    #[expect(
        clippy::zero_sized_map_values,
        reason = "persisted function membership uses the existing TOML map shape"
    )]
    #[serde(default)]
    functions: BTreeMap<String, PersistedInstalledFunction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedInstalledSource {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    variables: BTreeMap<String, String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    credential_storage: Option<CredentialStorageKind>,
    #[serde(default, skip_serializing_if = "Uuid::is_nil")]
    credential_revision: Uuid,
    origin: SourceOrigin,
}

impl PersistedInstalledSource {
    fn into_installed_source(self, source_name: SourceName) -> InstalledSource {
        InstalledSource {
            name: source_name,
            version: self.version,
            variables: self.variables,
            secrets: self.secrets,
            credential_storage: self.credential_storage,
            credential_revision: self.credential_revision,
            origin: self.origin,
        }
    }
}

impl From<&InstalledSource> for PersistedInstalledSource {
    fn from(value: &InstalledSource) -> Self {
        Self {
            version: value.version.clone(),
            variables: value.variables.clone(),
            secrets: value.secrets.clone(),
            credential_storage: value.credential_storage,
            credential_revision: value.credential_revision,
            origin: value.origin,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceCatalog(BTreeSet<WorkspaceName>);

impl Default for WorkspaceCatalog {
    fn default() -> Self {
        Self(BTreeSet::from([WorkspaceName::default()]))
    }
}

impl WorkspaceCatalog {
    pub(crate) fn list(&self) -> Vec<WorkspaceRecord> {
        self.0
            .iter()
            .cloned()
            .map(|name| WorkspaceRecord { name })
            .collect()
    }

    pub(crate) fn insert(&mut self, workspace_name: WorkspaceName) -> bool {
        self.0.insert(workspace_name)
    }

    pub(crate) fn remove(&mut self, workspace_name: &WorkspaceName) -> bool {
        self.0.remove(workspace_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedInstalledFunction {}

impl PersistedInstalledFunction {
    fn into_installed_function(function_name: FunctionName) -> InstalledFunction {
        InstalledFunction {
            name: function_name,
        }
    }
}

impl From<&InstalledFunction> for PersistedInstalledFunction {
    fn from(_value: &InstalledFunction) -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceCatalog(BTreeMap<WorkspaceName, BTreeMap<SourceName, InstalledSource>>);

impl SourceCatalog {
    pub(crate) fn workspace_sources(&self, workspace_name: &WorkspaceName) -> Vec<InstalledSource> {
        self.0
            .get(workspace_name)
            .map(|sources| sources.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn get_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<InstalledSource> {
        self.0
            .get(workspace_name)
            .and_then(|sources| sources.get(source_name))
            .cloned()
    }

    pub(crate) fn contains(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> bool {
        self.0
            .get(workspace_name)
            .is_some_and(|sources| sources.contains_key(source_name))
    }

    pub(crate) fn upsert_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source: InstalledSource,
    ) {
        self.0
            .entry(workspace_name.clone())
            .or_default()
            .insert(source.name.clone(), source);
    }

    pub(crate) fn remove_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<InstalledSource> {
        let mut removed = None;
        let remove_workspace = match self.0.get_mut(workspace_name) {
            Some(sources) => {
                removed = sources.remove(source_name);
                sources.is_empty()
            }
            None => false,
        };

        if remove_workspace {
            self.0.remove(workspace_name);
        }

        removed
    }

    pub(crate) fn remove_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Option<BTreeMap<SourceName, InstalledSource>> {
        self.0.remove(workspace_name)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FunctionCatalog(
    BTreeMap<WorkspaceName, BTreeMap<FunctionName, InstalledFunction>>,
);

impl FunctionCatalog {
    pub(crate) fn workspace_functions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Vec<InstalledFunction> {
        self.0
            .get(workspace_name)
            .map(|functions| functions.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn get_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Option<InstalledFunction> {
        self.0
            .get(workspace_name)
            .and_then(|functions| functions.get(function_name))
            .cloned()
    }

    pub(crate) fn upsert_function(
        &mut self,
        workspace_name: &WorkspaceName,
        function: InstalledFunction,
    ) {
        self.0
            .entry(workspace_name.clone())
            .or_default()
            .insert(function.name.clone(), function);
    }

    pub(crate) fn remove_function(
        &mut self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Option<InstalledFunction> {
        let mut removed = None;
        let remove_workspace = match self.0.get_mut(workspace_name) {
            Some(functions) => {
                removed = functions.remove(function_name);
                functions.is_empty()
            }
            None => false,
        };

        if remove_workspace {
            self.0.remove(workspace_name);
        }

        removed
    }

    pub(crate) fn remove_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Option<BTreeMap<FunctionName, InstalledFunction>> {
        self.0.remove(workspace_name)
    }
}

pub(crate) fn load_raw_feature_overrides(
    layout: &AppStateLayout,
) -> Result<RawFeatureOverrides, AppError> {
    if !layout.config_file().exists() {
        return Ok(RawFeatureOverrides::default());
    }

    let _lock = FileLock::shared(layout.state_lock())?;
    if !layout.config_file().exists() {
        return Ok(RawFeatureOverrides::default());
    }

    let raw = std::fs::read_to_string(layout.config_file())?;
    let doc = raw.parse::<DocumentMut>()?;
    Ok(raw_feature_overrides_from_document(&doc))
}

pub(crate) fn set_raw_feature_override(
    layout: &AppStateLayout,
    key: &str,
    enabled: bool,
) -> Result<(), AppError> {
    let _lock = FileLock::exclusive(layout.state_lock())?;
    let mut doc = read_config_document(layout)?;
    if doc.get("features").is_none() {
        doc.insert("features", toml_edit::table());
    }
    let Some(feature_table) = doc.get_mut("features").and_then(Item::as_table_mut) else {
        return Err(AppError::InvalidInput(
            "unsupported [features] config; expected a table".to_string(),
        ));
    };
    feature_table.insert(key, value(enabled));
    if doc.get("version").is_none() {
        doc.insert("version", value(i64::from(default_config_version())));
    }
    write_config_document(layout, &doc)
}

fn raw_feature_overrides_from_document(doc: &DocumentMut) -> RawFeatureOverrides {
    let Some(features) = doc.get("features") else {
        return RawFeatureOverrides::default();
    };

    let Some(table) = features.as_table() else {
        warn!("ignoring unsupported [features] config; expected a table");
        return RawFeatureOverrides {
            entries: BTreeMap::new(),
            container: RawFeatureContainerState::Unsupported,
        };
    };

    let entries = table
        .iter()
        .map(|(key, item)| {
            let value = item
                .as_bool()
                .map_or(RawFeatureValue::UnsupportedType, RawFeatureValue::Bool);
            (key.to_string(), value)
        })
        .collect();
    RawFeatureOverrides {
        entries,
        container: RawFeatureContainerState::Table,
    }
}

fn read_config_document(layout: &AppStateLayout) -> Result<DocumentMut, AppError> {
    if !layout.config_file().exists() {
        return Ok(DocumentMut::new());
    }
    let raw = std::fs::read_to_string(layout.config_file())?;
    Ok(raw.parse::<DocumentMut>()?)
}

fn write_config_document(layout: &AppStateLayout, doc: &DocumentMut) -> Result<(), AppError> {
    if let Some(parent) = layout.config_file().parent() {
        storage_fs::ensure_dir(parent)?;
    }
    storage_fs::write_atomic(layout.config_file(), doc.to_string().as_bytes())?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigStore {
    layout: AppStateLayout,
}

impl ConfigStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    fn load_unlocked(&self) -> Result<AppConfig, AppError> {
        if !self.layout.config_file().exists() {
            return Ok(AppConfig::default());
        }
        let raw = std::fs::read_to_string(self.layout.config_file())?;
        let persisted: PersistedAppConfig = toml::from_str(&raw).map_err(AppError::from)?;
        AppConfig::try_from(persisted)
    }

    fn save_unlocked(&self, config: &AppConfig) -> Result<(), AppError> {
        let existing_raw = if self.layout.config_file().exists() {
            Some(std::fs::read_to_string(self.layout.config_file())?)
        } else {
            None
        };
        let raw = render_config(&PersistedAppConfig::from(config), existing_raw.as_deref());
        if let Some(parent) = self.layout.config_file().parent() {
            storage_fs::ensure_dir(parent)?;
        }
        storage_fs::write_atomic(self.layout.config_file(), raw.as_bytes())?;
        Ok(())
    }

    pub(crate) fn state_lock_shared(&self) -> Result<FileLock, AppError> {
        FileLock::shared(self.layout.state_lock()).map_err(Into::into)
    }

    pub(crate) fn state_lock_exclusive(&self) -> Result<FileLock, AppError> {
        FileLock::exclusive(self.layout.state_lock()).map_err(Into::into)
    }

    /// Loads the app config without taking the app state lock.
    ///
    /// Callers must already hold the state lock in shared or exclusive mode
    /// while using any filesystem-backed source artifacts derived from the
    /// returned config.
    pub(crate) fn load_config_unlocked(&self) -> Result<AppConfig, AppError> {
        self.load_unlocked()
    }

    pub(crate) fn load_config(&self) -> Result<AppConfig, AppError> {
        let _lock = self.state_lock_shared()?;
        self.load_config_unlocked()
    }

    /// Loads the source catalog without taking the app state lock.
    ///
    /// Callers must already hold the state lock in shared or exclusive mode
    /// while using any filesystem-backed source artifacts derived from the
    /// returned catalog.
    pub(crate) fn load_catalog_unlocked(&self) -> Result<SourceCatalog, AppError> {
        self.load_config_unlocked().map(|config| config.catalog)
    }

    pub(crate) fn load_catalog(&self) -> Result<SourceCatalog, AppError> {
        let span = info_span!("coral.app.config.load_catalog");
        let _guard = span.enter();
        let _lock = self.state_lock_shared()?;
        self.load_catalog_unlocked()
    }

    fn update_config_unlocked<T>(
        &self,
        update: impl FnOnce(&mut AppConfig) -> Result<T, AppError>,
    ) -> Result<T, AppError> {
        let mut config = self.load_unlocked()?;
        let result = update(&mut config)?;
        self.save_unlocked(&config)?;
        Ok(result)
    }

    fn update_config<T>(
        &self,
        update: impl FnOnce(&mut AppConfig) -> Result<T, AppError>,
    ) -> Result<T, AppError> {
        let _lock = self.state_lock_exclusive()?;
        self.update_config_unlocked(update)
    }

    fn update_catalog_unlocked<T>(
        &self,
        update: impl FnOnce(&mut SourceCatalog) -> T,
    ) -> Result<T, AppError> {
        self.update_config_unlocked(|config| Ok(update(&mut config.catalog)))
    }

    #[cfg(test)]
    pub(crate) fn create_legacy_workspace_entry_for_tests(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        self.update_config(|config| {
            if config.workspaces.insert(workspace_name.clone()) {
                Ok(WorkspaceRecord {
                    name: workspace_name.clone(),
                })
            } else {
                Err(AppError::WorkspaceAlreadyExists(workspace_name.to_string()))
            }
        })
    }

    pub(crate) fn remove_workspace_config_entries(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<DeletedWorkspace>, AppError> {
        self.update_config(|config| {
            if workspace_name.is_default() {
                return Err(AppError::FailedPrecondition(
                    "default workspace cannot be removed".to_string(),
                ));
            }
            let removed = config.workspaces.remove(workspace_name);
            if removed {
                let sources = config
                    .catalog
                    .remove_workspace(workspace_name)
                    .map(BTreeMap::into_values)
                    .map(Iterator::collect)
                    .unwrap_or_default();
                config.functions.remove_workspace(workspace_name);
                return Ok(Some(DeletedWorkspace {
                    workspace: WorkspaceRecord {
                        name: workspace_name.clone(),
                    },
                    sources,
                }));
            }
            Ok(None)
        })
    }

    pub(crate) fn list_workspace_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        let config = self.load_config()?;
        Ok(config.workspace_sources(workspace_name))
    }

    /// Loads one installed source without taking the app state lock.
    ///
    /// Callers must already hold the state lock while using source artifacts
    /// associated with the returned config entry.
    pub(crate) fn get_source_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        self.load_catalog_unlocked()?
            .get_source(workspace_name, source_name)
            .ok_or_else(|| AppError::SourceNotFound(format!("{workspace_name}:{source_name}")))
    }

    pub(crate) fn get_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        let config = self.load_config()?;
        config
            .get_source(workspace_name, source_name)
            .ok_or_else(|| AppError::SourceNotFound(format!("{workspace_name}:{source_name}")))
    }

    /// Upserts one installed source without taking the app state lock.
    ///
    /// Callers must already hold the state lock in exclusive mode.
    pub(crate) fn upsert_source_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        source: InstalledSource,
    ) -> Result<(), AppError> {
        self.update_catalog_unlocked(|catalog| catalog.upsert_source(workspace_name, source))
    }

    #[cfg(test)]
    pub(crate) fn upsert_source(
        &self,
        workspace_name: &WorkspaceName,
        source: InstalledSource,
    ) -> Result<(), AppError> {
        self.update_config(|config| {
            config.catalog.upsert_source(workspace_name, source);
            Ok(())
        })
    }

    /// Removes one installed source without taking the app state lock.
    ///
    /// Callers must already hold the state lock in exclusive mode.
    pub(crate) fn remove_source_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), AppError> {
        self.update_catalog_unlocked(|catalog| {
            catalog.remove_source(workspace_name, source_name);
        })
    }

    #[cfg(test)]
    pub(crate) fn remove_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), AppError> {
        self.update_config(|config| {
            config.catalog.remove_source(workspace_name, source_name);
            Ok(())
        })
    }
}

impl ConfigStore {
    /// Lists installed functions without taking the app state lock.
    ///
    /// Callers must already hold the state lock while using function artifacts
    /// associated with the returned inventory.
    pub(crate) fn list_workspace_functions_unlocked(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledFunction>, AppError> {
        let config = self.load_config_unlocked()?;
        Ok(config.functions.workspace_functions(workspace_name))
    }

    #[cfg(test)]
    pub(crate) fn list_workspace_functions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledFunction>, AppError> {
        let _state_lock = self.state_lock_shared()?;
        self.list_workspace_functions_unlocked(workspace_name)
    }

    /// Loads one installed function without taking the app state lock.
    ///
    /// Callers must already hold the state lock while using the associated
    /// function artifact.
    pub(crate) fn get_function_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<InstalledFunction, AppError> {
        let config = self.load_config_unlocked()?;
        config
            .functions
            .get_function(workspace_name, function_name)
            .ok_or_else(|| AppError::FunctionNotFound(function_name.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn get_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<InstalledFunction, AppError> {
        let _state_lock = self.state_lock_shared()?;
        self.get_function_unlocked(workspace_name, function_name)
    }

    /// Upserts one installed function without taking the app state lock and
    /// reports whether an existing function was replaced.
    ///
    /// Callers must already hold the state lock in exclusive mode.
    pub(crate) fn upsert_function_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        function: InstalledFunction,
    ) -> Result<bool, AppError> {
        self.update_config_unlocked(|config| {
            let replaced = config
                .functions
                .get_function(workspace_name, &function.name)
                .is_some();
            config.functions.upsert_function(workspace_name, function);
            Ok(replaced)
        })
    }

    #[cfg(test)]
    pub(crate) fn upsert_function(
        &self,
        workspace_name: &WorkspaceName,
        function: InstalledFunction,
    ) -> Result<(), AppError> {
        let _state_lock = self.state_lock_exclusive()?;
        self.upsert_function_unlocked(workspace_name, function)
            .map(|_replaced| ())
    }

    /// Removes one installed function without taking the app state lock.
    ///
    /// Callers must already hold the state lock in exclusive mode.
    pub(crate) fn remove_function_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<(), AppError> {
        self.update_config_unlocked(|config| {
            let removed = config
                .functions
                .remove_function(workspace_name, function_name);
            if removed.is_none() {
                return Err(AppError::FunctionNotFound(function_name.to_string()));
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn remove_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<(), AppError> {
        let _state_lock = self.state_lock_exclusive()?;
        self.remove_function_unlocked(workspace_name, function_name)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "toml_edit indexing creates or accesses document paths while rebuilding the config table"
)]
fn render_config(config: &PersistedAppConfig, existing_raw: Option<&str>) -> String {
    let mut doc = existing_raw
        .and_then(|raw| raw.parse::<DocumentMut>().ok())
        .unwrap_or_default();

    doc["version"] = value(i64::from(config.version));
    render_engine_config(&mut doc, &config.engine);

    // Remove and fully rebuild the workspaces section so removed sources and
    // removed empty workspaces don't linger.
    doc.remove("workspaces");

    for workspace_name in config.workspaces.keys() {
        ensure_implicit_table(&mut doc["workspaces"]);
        ensure_explicit_table(&mut doc["workspaces"][workspace_name]);
    }

    for (workspace_name, workspace) in &config.workspaces {
        for (source_name, source) in &workspace.sources {
            ensure_implicit_table(&mut doc["workspaces"]);
            ensure_explicit_table(&mut doc["workspaces"][workspace_name]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]["sources"]);

            let source_item = &mut doc["workspaces"][workspace_name]["sources"][source_name];
            if !source_item.is_table() {
                *source_item = toml_edit::table();
            }

            if let Some(version) = &source.version {
                source_item["version"] = value(version.clone());
            } else {
                let source_table = source_item
                    .as_table_mut()
                    .expect("source config entry should be a table after initialization");
                source_table.remove("version");
            }
            source_item["variables"] = Item::Value(render_inline_table(&source.variables));
            source_item["secrets"] = Item::Value(render_string_array(&source.secrets));
            if let Some(credential_storage) = source.credential_storage {
                source_item["credential_storage"] = value(credential_storage.as_config_value());
            } else {
                let source_table = source_item
                    .as_table_mut()
                    .expect("source config entry should be a table after initialization");
                source_table.remove("credential_storage");
            }
            if source.credential_revision.is_nil() {
                let source_table = source_item
                    .as_table_mut()
                    .expect("source config entry should be a table after initialization");
                source_table.remove("credential_revision");
            } else {
                source_item["credential_revision"] = value(source.credential_revision.to_string());
            }
            source_item["origin"] = value(source.origin.as_config_value());
        }

        for function_name in workspace.functions.keys() {
            ensure_implicit_table(&mut doc["workspaces"]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]["functions"]);

            let function_item = &mut doc["workspaces"][workspace_name]["functions"][function_name];
            if !function_item.is_table() {
                *function_item = toml_edit::table();
            }
            let function_table = function_item
                .as_table_mut()
                .expect("function config entry should be a table after initialization");
            function_table.remove("origin");
            function_table.remove("enabled");
        }
    }

    doc.to_string()
}

fn render_engine_config(doc: &mut DocumentMut, engine: &PersistedEngineConfig) {
    let Some(limit) = &engine.memory.limit else {
        remove_engine_memory_limit(doc);
        return;
    };

    let engine_item = doc
        .as_table_mut()
        .entry("engine")
        .or_insert_with(toml_edit::table);
    if !engine_item.is_table() {
        *engine_item = toml_edit::table();
    }
    ensure_implicit_table(engine_item);

    let engine_table = engine_item
        .as_table_mut()
        .expect("engine config entry should be a table after initialization");
    let memory_item = engine_table
        .entry("memory")
        .or_insert_with(toml_edit::table);
    if !memory_item.is_table() {
        *memory_item = toml_edit::table();
    }
    memory_item
        .as_table_mut()
        .expect("engine memory config entry should be a table after initialization")["limit"] =
        render_memory_limit_value(limit);
}

fn render_memory_limit_value(limit: &toml::Value) -> Item {
    match limit {
        toml::Value::String(raw) => value(raw.clone()),
        toml::Value::Integer(raw) => value(*raw),
        toml::Value::Float(raw) => value(*raw),
        toml::Value::Boolean(raw) => value(*raw),
        toml::Value::Datetime(raw) => value(raw.to_string()),
        toml::Value::Array(_) | toml::Value::Table(_) => value(limit.to_string()),
    }
}

fn remove_engine_memory_limit(doc: &mut DocumentMut) {
    let Some(engine) = doc
        .as_table_mut()
        .get_mut("engine")
        .and_then(Item::as_table_mut)
    else {
        return;
    };
    let Some(memory) = engine.get_mut("memory").and_then(Item::as_table_mut) else {
        return;
    };
    memory.remove("limit");
}

fn ensure_implicit_table(item: &mut Item) {
    if !item.is_table() {
        *item = toml_edit::table();
    }
    item.as_table_mut()
        .expect("table item must be available")
        .set_implicit(true);
}

fn ensure_explicit_table(item: &mut Item) {
    if !item.is_table() {
        *item = toml_edit::table();
    }
    item.as_table_mut()
        .expect("table item must be available")
        .set_implicit(false);
}

impl TryFrom<PersistedAppConfig> for AppConfig {
    type Error = AppError;

    fn try_from(value: PersistedAppConfig) -> Result<Self, Self::Error> {
        let mut workspaces = WorkspaceCatalog::default();
        let mut catalog = SourceCatalog::default();
        let mut functions = FunctionCatalog::default();
        for (workspace_name, workspace_config) in value.workspaces {
            let workspace_name = WorkspaceName::parse(&workspace_name)?;
            workspaces.insert(workspace_name.clone());
            for (source_name, source) in workspace_config.sources {
                let source_name = SourceName::parse(&source_name)?;
                catalog.upsert_source(&workspace_name, source.into_installed_source(source_name));
            }
            for (function_name, _function) in workspace_config.functions {
                let function_name = FunctionName::parse(&function_name)?;
                functions.upsert_function(
                    &workspace_name,
                    PersistedInstalledFunction::into_installed_function(function_name),
                );
            }
        }
        Ok(Self {
            version: value.version,
            engine: value.engine,
            workspaces,
            catalog,
            functions,
        })
    }
}

impl From<&AppConfig> for PersistedAppConfig {
    fn from(value: &AppConfig) -> Self {
        let mut workspaces = BTreeMap::new();
        for workspace in value.workspaces.list() {
            workspaces
                .entry(workspace.name.as_str().to_string())
                .or_insert_with(PersistedWorkspaceConfig::default);
        }
        for (workspace_name, sources) in &value.catalog.0 {
            let workspace_config = workspaces
                .entry(workspace_name.as_str().to_string())
                .or_insert_with(PersistedWorkspaceConfig::default);
            for source in sources.values() {
                workspace_config.sources.insert(
                    source.name.as_str().to_string(),
                    PersistedInstalledSource::from(source),
                );
            }
        }
        for (workspace_name, functions) in &value.functions.0 {
            let workspace_config = workspaces
                .entry(workspace_name.as_str().to_string())
                .or_insert_with(PersistedWorkspaceConfig::default);
            for function in functions.values() {
                workspace_config.functions.insert(
                    function.name.as_str().to_string(),
                    PersistedInstalledFunction::from(function),
                );
            }
        }
        Self {
            version: value.version,
            engine: value.engine.clone(),
            workspaces,
        }
    }
}

impl PersistedMemoryConfig {
    fn try_into_runtime_config(self) -> Result<QueryMemoryConfig, AppError> {
        let limit = self
            .limit
            .as_ref()
            .map(parse_memory_limit_value)
            .transpose()?;
        Ok(QueryMemoryConfig::with_limit(limit))
    }
}

fn parse_memory_limit_value(value: &toml::Value) -> Result<MemorySize, AppError> {
    match value {
        toml::Value::String(raw) => raw
            .parse::<MemorySize>()
            .map_err(|error| AppError::InvalidInput(format!("engine.memory.limit: {error}"))),
        toml::Value::Integer(bytes) if *bytes > 0 => {
            let bytes = usize::try_from(*bytes).map_err(|_error| {
                AppError::InvalidInput("engine.memory.limit: memory limit is too large".to_string())
            })?;
            MemorySize::from_bytes(bytes)
                .map_err(|error| AppError::InvalidInput(format!("engine.memory.limit: {error}")))
        }
        toml::Value::Integer(_) => Err(AppError::InvalidInput(
            "engine.memory.limit: memory limit must be greater than 0".to_string(),
        )),
        _ => Err(AppError::InvalidInput(
            "engine.memory.limit: memory limit must be a string with binary unit Ki, Mi, Gi, or Ti, or a positive integer byte count".to_string(),
        )),
    }
}

impl PersistedDependentJoinConfig {
    fn try_into_runtime_config(
        self,
        selected_source_names: &[String],
    ) -> Result<DependentJoinConfig, AppError> {
        let default = DependentJoinConfig::default();
        let mut per_source = BTreeMap::new();
        for (source_name, source_config) in self.per_source {
            let source_name = SourceName::parse(&source_name)?;
            if !selected_source_names
                .iter()
                .any(|selected_source_name| selected_source_name == source_name.as_str())
            {
                continue;
            }
            per_source.insert(
                source_name.as_str().to_string(),
                source_config.try_into_runtime_config(source_name.as_str())?,
            );
        }
        Ok(DependentJoinConfig {
            enabled: self.enabled.unwrap_or(default.enabled),
            max_bindings: positive_or_default(
                "engine.dependent_join.max_bindings",
                self.max_bindings,
                default.max_bindings,
            )?,
            max_resolver_rows: positive_or_default(
                "engine.dependent_join.max_resolver_rows",
                self.max_resolver_rows,
                default.max_resolver_rows,
            )?,
            max_rows_per_binding: positive_or_default(
                "engine.dependent_join.max_rows_per_binding",
                self.max_rows_per_binding,
                default.max_rows_per_binding,
            )?,
            max_resolver_rows_per_binding: positive_or_default(
                "engine.dependent_join.max_resolver_rows_per_binding",
                self.max_resolver_rows_per_binding,
                default.max_resolver_rows_per_binding,
            )?,
            max_concurrency: positive_or_default(
                "engine.dependent_join.max_concurrency",
                self.max_concurrency,
                default.max_concurrency,
            )?,
            per_source,
        })
    }
}

impl PersistedDependentJoinSourceConfig {
    fn try_into_runtime_config(
        self,
        source_name: &str,
    ) -> Result<DependentJoinSourceConfig, AppError> {
        Ok(DependentJoinSourceConfig {
            enabled: self.enabled,
            max_bindings: positive_optional(
                &format!("engine.dependent_join.per_source.{source_name}.max_bindings"),
                self.max_bindings,
            )?,
            max_resolver_rows: positive_optional(
                &format!("engine.dependent_join.per_source.{source_name}.max_resolver_rows"),
                self.max_resolver_rows,
            )?,
            max_rows_per_binding: positive_optional(
                &format!("engine.dependent_join.per_source.{source_name}.max_rows_per_binding"),
                self.max_rows_per_binding,
            )?,
            max_resolver_rows_per_binding: positive_optional(
                &format!(
                    "engine.dependent_join.per_source.{source_name}.max_resolver_rows_per_binding"
                ),
                self.max_resolver_rows_per_binding,
            )?,
            max_concurrency: positive_optional(
                &format!("engine.dependent_join.per_source.{source_name}.max_concurrency"),
                self.max_concurrency,
            )?,
        })
    }
}

fn positive_or_default(
    field: &str,
    value: Option<usize>,
    default: usize,
) -> Result<usize, AppError> {
    match value {
        Some(0) => Err(AppError::InvalidInput(format!(
            "{field} must be greater than 0"
        ))),
        Some(value) => Ok(value),
        None => Ok(default),
    }
}

fn positive_optional(field: &str, value: Option<usize>) -> Result<Option<usize>, AppError> {
    match value {
        Some(0) => Err(AppError::InvalidInput(format!(
            "{field} must be greater than 0"
        ))),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

fn render_inline_table(values: &BTreeMap<String, String>) -> Value {
    let mut table = InlineTable::new();
    for (key, value) in values {
        table.insert(key, Value::from(value.clone()));
    }
    table.fmt();
    Value::InlineTable(table)
}

fn render_string_array(values: &[String]) -> Value {
    values.iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "loaded source order assertions intentionally fail loudly in tests"
    )]

    use std::collections::BTreeMap;

    use coral_engine::{DependentJoinConfig, DependentJoinSourceConfig};
    use tempfile::TempDir;

    use super::{
        AppConfig, AppError, ConfigStore, FunctionCatalog, PersistedAppConfig,
        PersistedEngineConfig, PersistedMemoryConfig, RawFeatureContainerState, RawFeatureValue,
        SourceCatalog, WorkspaceCatalog, load_raw_feature_overrides, render_config,
        set_raw_feature_override,
    };
    use crate::credentials::CredentialStorageKind;
    use crate::functions::model::{FunctionName, InstalledFunction};
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    fn default_workspace() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn installed_source(name: &str) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source"),
            version: Some("1.1.4".to_string()),
            variables: BTreeMap::from([(
                "GITHUB_API_BASE".to_string(),
                "https://api.github.com".to_string(),
            )]),
            secrets: vec!["GITHUB_TOKEN".to_string()],
            credential_storage: None,
            credential_revision: uuid::Uuid::default(),
            origin: SourceOrigin::Imported,
        }
    }

    fn installed_function(name: &str) -> InstalledFunction {
        InstalledFunction {
            name: FunctionName::parse(name).expect("function"),
        }
    }

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout")
    }

    fn write_config(layout: &AppStateLayout, raw: &str) {
        std::fs::create_dir_all(
            layout
                .config_file()
                .parent()
                .expect("config file should have parent"),
        )
        .expect("create config dir");
        std::fs::write(layout.config_file(), raw).expect("write config");
    }

    fn raw_feature_entries(layout: &AppStateLayout) -> BTreeMap<String, RawFeatureValue> {
        load_raw_feature_overrides(layout)
            .expect("feature overrides")
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn raw_feature_container(layout: &AppStateLayout) -> RawFeatureContainerState {
        load_raw_feature_overrides(layout)
            .expect("feature overrides")
            .container()
    }

    fn selected_sources(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn default_config_uses_canonical_version() {
        assert_eq!(AppConfig::default().version, 1);
    }

    #[test]
    fn renders_sources_under_workspace_keyed_tables() {
        let workspace_name = default_workspace();
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, installed_source("github"));
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog,
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(raw.contains("[workspaces.default.sources.github]"));
        assert!(raw.contains("variables = { GITHUB_API_BASE = \"https://api.github.com\" }"));
        assert!(raw.contains("secrets = [\"GITHUB_TOKEN\"]"));
        assert!(raw.contains("version = \"1.1.4\""));
        assert!(!raw.contains("[[sources]]"));
        assert!(!raw.contains("workspace = { name = \"default\" }"));
        assert!(!raw.contains("manifest_file"));
    }

    #[test]
    fn renders_empty_workspaces_as_explicit_tables() {
        let mut workspaces = WorkspaceCatalog::default();
        workspaces.insert(WorkspaceName::parse("work").expect("workspace"));
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces,
            catalog: SourceCatalog::default(),
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);

        assert!(raw.contains("[workspaces.default]"));
        assert!(raw.contains("[workspaces.work]"));
    }

    #[test]
    fn loads_legacy_empty_workspace_tables() {
        let raw = r"
version = 1

[workspaces.default]

[workspaces.work]
";

        let config = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(raw).expect("workspace config should parse"),
        )
        .expect("config");
        let names = config
            .legacy_workspace_records()
            .into_iter()
            .map(|workspace| workspace.name.as_str().to_string())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["default", "work"]);
    }

    #[test]
    fn renders_functions_under_workspace_keyed_tables() {
        let workspace_name = default_workspace();
        let mut functions = FunctionCatalog::default();
        functions.upsert_function(&workspace_name, installed_function("review_queue"));
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog: SourceCatalog::default(),
            functions,
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);

        assert!(raw.contains("[workspaces.default.functions.review_queue]"));
        assert!(!raw.contains("origin = \"user\""));
        assert!(!raw.contains("enabled = true"));
    }

    #[test]
    fn removes_legacy_function_origin_and_enabled_when_rendering() {
        let existing = r#"
version = 1

[workspaces.default.functions.review_queue]
origin = "user"
enabled = true
"#;
        let workspace_name = default_workspace();
        let mut functions = FunctionCatalog::default();
        functions.upsert_function(&workspace_name, installed_function("review_queue"));
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog: SourceCatalog::default(),
            functions,
        };

        let raw = render_config(&PersistedAppConfig::from(&config), Some(existing));

        assert!(raw.contains("[workspaces.default.functions.review_queue]"));
        assert!(!raw.contains("origin = \"user\""));
        assert!(!raw.contains("enabled = true"));
    }

    #[test]
    fn omits_empty_versions_from_rendered_source_entries() {
        let workspace_name = default_workspace();
        let mut source = installed_source("github");
        source.version = None;
        source.origin = SourceOrigin::Bundled;
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, source);
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog,
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(!raw.contains("version = \"\""));
        assert!(!raw.contains("version = \""));
    }

    #[test]
    fn loads_sources_from_workspace_keyed_tables() {
        let raw = r#"
version = 1

[workspaces.default.sources.github]
version = "1.1.4"
variables = { GITHUB_API_BASE = "https://api.github.com" }
secrets = ["GITHUB_TOKEN"]
origin = "bundled"
"#;

        let config = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(raw).expect("workspace-keyed config should parse"),
        )
        .expect("config");
        let sources = config.catalog.workspace_sources(&default_workspace());
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name.as_str(), "github");
        assert_eq!(sources[0].version.as_deref(), Some("1.1.4"));
        assert_eq!(
            sources[0].variables.get("GITHUB_API_BASE"),
            Some(&"https://api.github.com".to_string())
        );
        assert_eq!(sources[0].secrets, vec!["GITHUB_TOKEN".to_string()]);
        assert_eq!(sources[0].credential_storage, None);
        assert_eq!(
            sources[0].effective_credential_storage(),
            CredentialStorageKind::File
        );
    }

    #[test]
    fn scoped_config_store_methods_do_not_require_workspace() {
        let temp = TempDir::new().expect("temp dir");
        let store = ConfigStore::new(test_layout(&temp));
        let missing_workspace = WorkspaceName::parse("missing").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");
        let function_name = FunctionName::parse("review_queue").expect("function");

        assert!(
            store
                .list_workspace_sources(&missing_workspace)
                .expect("list source definitions")
                .is_empty()
        );
        assert!(matches!(
            store.get_source(&missing_workspace, &source_name),
            Err(AppError::SourceNotFound(_))
        ));
        store
            .upsert_source(&missing_workspace, installed_source("github"))
            .expect("upsert source definition");
        assert_eq!(
            store
                .get_source(&missing_workspace, &source_name)
                .expect("get source definition")
                .name,
            source_name
        );
        store
            .remove_source(&missing_workspace, &source_name)
            .expect("remove source definition");
        assert!(matches!(
            store.get_source(&missing_workspace, &source_name),
            Err(AppError::SourceNotFound(_))
        ));
        assert!(
            store
                .list_workspace_functions(&missing_workspace)
                .expect("list function definitions")
                .is_empty()
        );
        assert!(matches!(
            store.get_function(&missing_workspace, &function_name),
            Err(AppError::FunctionNotFound(_))
        ));
        store
            .upsert_function(&missing_workspace, installed_function("review_queue"))
            .expect("upsert function definition");
        assert_eq!(
            store
                .get_function(&missing_workspace, &function_name)
                .expect("get function definition")
                .name,
            function_name
        );
        store
            .remove_function(&missing_workspace, &function_name)
            .expect("remove function definition");
        assert!(matches!(
            store.get_function(&missing_workspace, &function_name),
            Err(AppError::FunctionNotFound(_))
        ));
    }

    #[test]
    fn remove_workspace_config_entries_removes_sources_and_functions() {
        let temp = TempDir::new().expect("temp dir");
        let store = ConfigStore::new(test_layout(&temp));
        let workspace_name = WorkspaceName::parse("work").expect("workspace");

        store
            .create_legacy_workspace_entry_for_tests(&workspace_name)
            .expect("create legacy workspace entry");
        store
            .upsert_source(&workspace_name, installed_source("github"))
            .expect("upsert source");
        store
            .upsert_function(&workspace_name, installed_function("review_queue"))
            .expect("upsert function");

        let deleted = store
            .remove_workspace_config_entries(&workspace_name)
            .expect("remove workspace config entries")
            .expect("workspace config should be removed");

        assert_eq!(deleted.workspace.name, workspace_name);
        assert_eq!(deleted.sources.len(), 1);
        assert_eq!(deleted.sources[0].name.as_str(), "github");
        assert!(
            store
                .list_workspace_sources(&deleted.workspace.name)
                .expect("list source definitions")
                .is_empty()
        );
        assert!(
            store
                .list_workspace_functions(&deleted.workspace.name)
                .expect("list function definitions")
                .is_empty()
        );

        let rendered = std::fs::read_to_string(store.layout.config_file()).expect("read config");
        assert!(!rendered.contains("[workspaces.work.functions.review_queue]"));
    }

    #[test]
    fn loads_functions_from_workspace_keyed_tables() {
        let raw = r"
version = 1

	[workspaces.default.functions.review_queue]
	";

        let config = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(raw).expect("workspace-keyed config should parse"),
        )
        .expect("config");
        let functions = config.functions.workspace_functions(&default_workspace());

        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].name.as_str(), "review_queue");
    }

    #[test]
    fn loads_dependent_join_engine_config() {
        let raw = r"
version = 1

[engine.dependent_join]
enabled = false
max_bindings = 7
max_resolver_rows = 11
max_rows_per_binding = 13
max_resolver_rows_per_binding = 17
max_concurrency = 19

[engine.dependent_join.per_source.github]
enabled = true
max_bindings = 21
max_concurrency = 4
";

        let config = toml::from_str::<PersistedAppConfig>(raw)
            .expect("dependent join config should parse")
            .engine
            .dependent_join
            .try_into_runtime_config(&selected_sources(&["github"]))
            .expect("dependent join config should be valid");

        assert_eq!(
            config,
            DependentJoinConfig {
                enabled: false,
                max_bindings: 7,
                max_resolver_rows: 11,
                max_rows_per_binding: 13,
                max_resolver_rows_per_binding: 17,
                max_concurrency: 19,
                per_source: BTreeMap::from([(
                    "github".to_string(),
                    DependentJoinSourceConfig {
                        enabled: Some(true),
                        max_bindings: Some(21),
                        max_concurrency: Some(4),
                        ..DependentJoinSourceConfig::default()
                    },
                )]),
            }
        );
    }

    #[test]
    fn matches_dependent_join_source_config_after_source_name_normalization() {
        let raw = r#"
version = 1

[engine.dependent_join.per_source." github "]
enabled = false
"#;

        let config = toml::from_str::<PersistedAppConfig>(raw)
            .expect("dependent join config should parse")
            .engine
            .dependent_join
            .try_into_runtime_config(&selected_sources(&["github"]))
            .expect("dependent join config should be valid");

        assert_eq!(
            config.per_source.get("github"),
            Some(&DependentJoinSourceConfig {
                enabled: Some(false),
                ..DependentJoinSourceConfig::default()
            })
        );
    }

    #[test]
    fn loads_engine_memory_config() {
        let raw = r#"
version = 1

[engine.memory]
limit = "2Gi"
"#;

        let config = toml::from_str::<PersistedAppConfig>(raw)
            .expect("memory config should parse")
            .engine
            .memory
            .try_into_runtime_config()
            .expect("memory config should be valid");

        assert_eq!(
            config.limit.expect("memory limit should be set").as_bytes(),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn loads_engine_memory_config_from_integer_bytes() {
        let raw = r"
version = 1

[engine.memory]
limit = 2147483648
";

        let config = toml::from_str::<PersistedAppConfig>(raw)
            .expect("memory config should parse")
            .engine
            .memory
            .try_into_runtime_config()
            .expect("memory config should be valid");

        assert_eq!(
            config.limit.expect("memory limit should be set").as_bytes(),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn renders_engine_memory_config() {
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig {
                memory: PersistedMemoryConfig {
                    limit: Some(toml::Value::String("2Gi".to_string())),
                },
                ..PersistedEngineConfig::default()
            },
            workspaces: WorkspaceCatalog::default(),
            catalog: SourceCatalog::default(),
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);

        assert!(raw.contains("[engine.memory]"));
        assert!(raw.contains("limit = \"2Gi\""));
    }

    #[test]
    fn rendering_engine_memory_preserves_unrelated_engine_sections() {
        let existing_raw = r"
version = 1

[engine.other]
flag = true
";
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig {
                memory: PersistedMemoryConfig {
                    limit: Some(toml::Value::String("2Gi".to_string())),
                },
                ..PersistedEngineConfig::default()
            },
            workspaces: WorkspaceCatalog::default(),
            catalog: SourceCatalog::default(),
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), Some(existing_raw));

        assert!(raw.contains("[engine.memory]"));
        assert!(raw.contains("limit = \"2Gi\""));
        assert!(raw.contains("[engine.other]"));
        assert!(raw.contains("flag = true"));
    }

    #[test]
    fn rendering_without_engine_memory_limit_removes_stale_limit() {
        let existing_raw = r#"
version = 1

[engine.memory]
limit = "2Gi"

[engine.other]
flag = true
"#;
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog: SourceCatalog::default(),
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), Some(existing_raw));

        assert!(!raw.contains("limit = \"2Gi\""));
        assert!(raw.contains("[engine.memory]"));
        assert!(raw.contains("[engine.other]"));
        assert!(raw.contains("flag = true"));
    }

    #[test]
    fn rejects_invalid_engine_memory_config() {
        let raw = r#"
version = 1

[engine.memory]
limit = "2GiB"
"#;

        let error = toml::from_str::<PersistedAppConfig>(raw)
            .expect("memory config should parse")
            .engine
            .memory
            .try_into_runtime_config()
            .expect_err("invalid memory limit should fail");

        assert!(
            error
                .to_string()
                .contains("engine.memory.limit: memory limit must use binary unit")
        );
    }

    #[test]
    fn rejects_unsupported_engine_memory_value_without_toml_decode_failure() {
        let raw = r"
version = 1

[engine.memory]
limit = true
";

        let error = toml::from_str::<PersistedAppConfig>(raw)
            .expect("memory config should parse")
            .engine
            .memory
            .try_into_runtime_config()
            .expect_err("unsupported memory limit should fail");

        assert!(
            error
                .to_string()
                .contains("engine.memory.limit: memory limit must be a string")
        );
    }

    #[test]
    fn rejects_zero_dependent_join_limits() {
        let raw = r"
version = 1

[engine.dependent_join]
max_concurrency = 0
";

        let error = toml::from_str::<PersistedAppConfig>(raw)
            .expect("dependent join config should parse")
            .engine
            .dependent_join
            .try_into_runtime_config(&[])
            .expect_err("zero limit should fail");

        assert!(
            error
                .to_string()
                .contains("engine.dependent_join.max_concurrency must be greater than 0")
        );
    }

    #[test]
    fn rejects_zero_dependent_join_source_limits() {
        let raw = r"
version = 1

[engine.dependent_join.per_source.github]
max_concurrency = 0
";

        let error = toml::from_str::<PersistedAppConfig>(raw)
            .expect("dependent join config should parse")
            .engine
            .dependent_join
            .try_into_runtime_config(&selected_sources(&["github"]))
            .expect_err("zero source limit should fail");

        assert!(error.to_string().contains(
            "engine.dependent_join.per_source.github.max_concurrency must be greater than 0"
        ));
    }

    #[test]
    fn ignores_dependent_join_source_limits_for_unselected_sources() {
        let raw = r"
version = 1

[engine.dependent_join.per_source.github]
max_concurrency = 4

[engine.dependent_join.per_source.linear]
max_concurrency = 0
";

        let config = toml::from_str::<PersistedAppConfig>(raw)
            .expect("dependent join config should parse")
            .engine
            .dependent_join
            .try_into_runtime_config(&selected_sources(&["github"]))
            .expect("unselected source override should not be validated");

        assert!(config.per_source.contains_key("github"));
        assert!(!config.per_source.contains_key("linear"));
    }

    #[test]
    fn round_trips_source_credential_storage() {
        let workspace_name = default_workspace();
        let mut source = installed_source("github");
        source.credential_storage = Some(CredentialStorageKind::Keychain);
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, source);
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog,
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(raw.contains("credential_storage = \"keychain\""));

        let loaded = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(&raw).expect("config should parse"),
        )
        .expect("config");
        let sources = loaded.catalog.workspace_sources(&workspace_name);
        assert_eq!(
            sources[0].credential_storage,
            Some(CredentialStorageKind::Keychain)
        );
    }

    #[test]
    fn round_trips_non_nil_source_credential_revision() {
        let workspace_name = default_workspace();
        let mut source = installed_source("github");
        source.credential_revision = uuid::Uuid::parse_str("c59a9c41-0e51-45d4-bfb7-f05df71eed53")
            .expect("credential revision");
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, source.clone());
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog,
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(raw.contains(&format!(
            "credential_revision = \"{}\"",
            source.credential_revision
        )));

        let loaded = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(&raw).expect("config should parse"),
        )
        .expect("config");
        let sources = loaded.catalog.workspace_sources(&workspace_name);
        assert_eq!(sources[0].credential_revision, source.credential_revision);
    }

    #[test]
    fn source_config_without_credential_revision_defaults_to_nil() {
        let raw = r#"
version = 1

[workspaces.default]

[workspaces.default.sources.github]
variables = {}
secrets = ["GITHUB_TOKEN"]
credential_storage = "file"
origin = "imported"
"#;
        let loaded = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(raw).expect("legacy config should parse"),
        )
        .expect("config");
        let sources = loaded.catalog.workspace_sources(&default_workspace());

        assert!(sources[0].credential_revision.is_nil());
    }

    #[test]
    fn catalog_upsert_replaces_existing_workspace_source_entry() {
        let workspace_name = default_workspace();
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, installed_source("github"));

        let mut updated = installed_source("github");
        updated.version = Some("2.0.0".to_string());
        updated.origin = SourceOrigin::Imported;
        catalog.upsert_source(&workspace_name, updated);

        let stored = catalog
            .get_source(
                &workspace_name,
                &SourceName::parse("github").expect("source"),
            )
            .expect("source should be present");
        assert_eq!(stored.version.as_deref(), Some("2.0.0"));
        assert_eq!(stored.origin, SourceOrigin::Imported);
        assert_eq!(catalog.workspace_sources(&workspace_name).len(), 1);
    }

    #[test]
    fn catalog_remove_drops_empty_workspace_bucket() {
        let default_workspace = default_workspace();
        let other_workspace_name = WorkspaceName::parse("other").expect("workspace");
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&default_workspace, installed_source("github"));
        catalog.upsert_source(&other_workspace_name, installed_source("slack"));

        catalog.remove_source(
            &default_workspace,
            &SourceName::parse("github").expect("source"),
        );

        assert!(
            catalog
                .get_source(
                    &default_workspace,
                    &SourceName::parse("github").expect("source")
                )
                .is_none()
        );
        assert!(catalog.workspace_sources(&default_workspace).is_empty());
        assert!(
            catalog
                .get_source(
                    &other_workspace_name,
                    &SourceName::parse("slack").expect("source")
                )
                .is_some()
        );
    }

    #[test]
    fn preserves_unrelated_sections_when_rendering_with_existing_config() {
        let existing = r#"
version = 1

[otel]
endpoint = "http://localhost:4318"
headers = "from=config"

	[trace_history]
	enabled = false
	retention_days = 3

[features]
feedback = true
future_feature = "not-yet-known"

[server]
bind_addr = "127.0.0.1:14555"

[engine.dependent_join]
enabled = false
max_bindings = 250

	[workspaces.default.sources.github]
version = "1.0.0"
variables = {}
secrets = []
origin = "bundled"
"#;

        let workspace_name = default_workspace();
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, installed_source("slack"));
        let config = AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            workspaces: WorkspaceCatalog::default(),
            catalog,
            functions: FunctionCatalog::default(),
        };

        let raw = render_config(&PersistedAppConfig::from(&config), Some(existing));

        // OTel section must survive the round-trip.
        assert!(raw.contains("[otel]"), "otel section should be preserved");
        assert!(
            raw.contains("endpoint = \"http://localhost:4318\""),
            "otel endpoint should be preserved"
        );
        assert!(
            raw.contains("headers = \"from=config\""),
            "otel headers should be preserved"
        );
        assert!(
            raw.contains("[trace_history]"),
            "trace history section should be preserved"
        );
        assert!(
            raw.contains("enabled = false"),
            "trace history enabled flag should be preserved"
        );
        assert!(
            raw.contains("retention_days = 3"),
            "trace history retention should be preserved"
        );
        assert!(
            raw.contains("[features]"),
            "features section should be preserved"
        );
        assert!(
            raw.contains("feedback = true"),
            "known feature override should be preserved"
        );
        assert!(
            raw.contains("future_feature = \"not-yet-known\""),
            "future feature override should be preserved"
        );
        assert!(
            raw.contains("[server]"),
            "server section should be preserved"
        );
        assert!(
            raw.contains("bind_addr = \"127.0.0.1:14555\""),
            "server bind address should be preserved"
        );
        assert!(
            raw.contains("[engine.dependent_join]"),
            "dependent join config should be preserved"
        );
        assert!(
            raw.contains("max_bindings = 250"),
            "dependent join limit should be preserved"
        );

        // The newly added source must be present.
        assert!(raw.contains("[workspaces.default.sources.slack]"));

        // The old source that was not in the updated catalog must be gone.
        assert!(!raw.contains("[workspaces.default.sources.github]"));
    }

    #[test]
    fn rejects_invalid_workspace_or_source_keys_when_loading() {
        let invalid_workspace = r#"
version = 1

[workspaces."bad\\workspace".sources.github]
origin = "bundled"
"#;
        let error = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(invalid_workspace)
                .expect("quoted workspace key should parse"),
        )
        .expect_err("invalid workspace key should fail");
        assert!(error.to_string().contains("workspace name"));

        let invalid_source = r#"
version = 1

[workspaces.default.sources."bad\\source"]
origin = "bundled"
"#;
        let error = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(invalid_source)
                .expect("quoted source key should parse"),
        )
        .expect_err("invalid source key should fail");
        assert!(error.to_string().contains("source name"));

        let invalid_function = r#"
version = 1

	[workspaces.default.functions."bad\\function"]
	"#;
        let error = AppConfig::try_from(
            toml::from_str::<PersistedAppConfig>(invalid_function)
                .expect("quoted function key should parse"),
        )
        .expect_err("invalid function key should fail");
        assert!(error.to_string().contains("function name"));
    }

    #[test]
    fn raw_feature_overrides_default_when_config_file_is_missing_without_creating_state() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("missing-config");
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");

        let entries = raw_feature_entries(&layout);

        assert!(entries.is_empty());
        assert!(
            !config_dir.exists(),
            "read-only feature loading should not create config state"
        );
    }

    #[test]
    fn raw_feature_overrides_load_supported_table_entries() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        write_config(
            &layout,
            r#"
version = 1

[features]
feedback = true
future_flag = false
wrong_type = "yes"

[features.nested]
enabled = true
"#,
        );

        let entries = raw_feature_entries(&layout);

        assert_eq!(entries.get("feedback"), Some(&RawFeatureValue::Bool(true)));
        assert_eq!(
            entries.get("future_flag"),
            Some(&RawFeatureValue::Bool(false))
        );
        assert_eq!(
            entries.get("wrong_type"),
            Some(&RawFeatureValue::UnsupportedType)
        );
        assert_eq!(
            entries.get("nested"),
            Some(&RawFeatureValue::UnsupportedType)
        );
    }

    #[test]
    fn raw_feature_overrides_accept_dotted_feature_table() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        write_config(&layout, "features.feedback = false\n");

        let entries = raw_feature_entries(&layout);

        assert_eq!(entries.get("feedback"), Some(&RawFeatureValue::Bool(false)));
    }

    #[test]
    fn raw_feature_overrides_ignore_inline_feature_table() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        write_config(&layout, "features = { feedback = true }\n");

        let entries = raw_feature_entries(&layout);

        assert!(entries.is_empty());
        assert_eq!(
            raw_feature_container(&layout),
            RawFeatureContainerState::Unsupported
        );
    }

    #[test]
    fn raw_feature_overrides_fail_for_invalid_toml() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        write_config(&layout, "[features\nfeedback = true\n");

        let error = load_raw_feature_overrides(&layout).expect_err("invalid TOML should fail");

        assert!(error.to_string().contains("TOML parse error"));
    }

    #[test]
    fn set_raw_feature_override_creates_config_file_with_features_table() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);

        set_raw_feature_override(&layout, "feedback", true).expect("set feature");

        let raw = std::fs::read_to_string(layout.config_file()).expect("config file");
        assert!(raw.contains("version = 1"));
        assert!(raw.contains("[features]"));
        assert!(raw.contains("feedback = true"));
    }

    #[test]
    fn set_raw_feature_override_preserves_unrelated_feature_entries() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        write_config(
            &layout,
            r#"
[features]
future_flag = "yes"
feedback = true
"#,
        );

        set_raw_feature_override(&layout, "feedback", false).expect("set feature");
        let raw = std::fs::read_to_string(layout.config_file()).expect("config file");
        assert!(raw.contains("feedback = false"));
        assert!(raw.contains("future_flag = \"yes\""));

        set_raw_feature_override(&layout, "feedback", true).expect("set feature");
        let raw = std::fs::read_to_string(layout.config_file()).expect("config file");
        assert!(raw.contains("feedback = true"));
        assert!(raw.contains("future_flag = \"yes\""));
    }

    #[test]
    fn feature_mutations_reject_inline_feature_container_without_rewriting_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let original = "features = { feedback = true }\n";
        write_config(&layout, original);

        let error =
            set_raw_feature_override(&layout, "feedback", true).expect_err("inline features");

        assert!(error.to_string().contains("unsupported [features] config"));
        let raw = std::fs::read_to_string(layout.config_file()).expect("config file");
        assert_eq!(raw, original);
    }
}
