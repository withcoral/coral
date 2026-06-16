//! Persists the installed source catalog in top-level `config.toml`.

use std::collections::BTreeMap;

use coral_engine::{DependentJoinConfig, DependentJoinSourceConfig};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};
use tracing::{info_span, warn};

use crate::bootstrap::AppError;
use crate::credentials::CredentialStorageKind;
use crate::identity::SourceIdentityBinding;
use crate::source_registry::{
    SourceRegistry, SourceRegistryRecord, installed_source_from_record,
    record_from_installed_source,
};
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct AppConfig {
    version: u32,
    engine: PersistedEngineConfig,
    catalog: SourceCatalog,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: default_config_version(),
            engine: PersistedEngineConfig::default(),
            catalog: SourceCatalog::default(),
        }
    }
}

impl AppConfig {
    pub(crate) fn dependent_join_config(
        &self,
        selected_source_names: &[String],
    ) -> Result<DependentJoinConfig, AppError> {
        self.engine
            .dependent_join
            .clone()
            .try_into_runtime_config(selected_source_names)
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedInstalledSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_spec_id: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    variables: BTreeMap<String, String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    credential_storage: Option<CredentialStorageKind>,
    #[serde(default)]
    identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    origin: SourceOrigin,
}

impl PersistedInstalledSource {
    fn into_installed_source(self, source_name: SourceName) -> Result<InstalledSource, AppError> {
        validate_identity_bindings(source_name.as_str(), &self.identity_bindings)?;
        Ok(InstalledSource {
            name: source_name,
            source_spec_id: self.source_spec_id,
            version: self.version,
            variables: self.variables,
            secrets: self.secrets,
            credential_storage: self.credential_storage,
            identity_bindings: self.identity_bindings,
            origin: self.origin,
        })
    }
}

impl From<&InstalledSource> for PersistedInstalledSource {
    fn from(value: &InstalledSource) -> Self {
        Self {
            version: value.version.clone(),
            source_spec_id: value.source_spec_id.clone(),
            variables: value.variables.clone(),
            secrets: value.secrets.clone(),
            credential_storage: value.credential_storage,
            identity_bindings: value.identity_bindings.clone(),
            origin: value.origin,
        }
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

    fn lock_shared(&self) -> Result<FileLock, AppError> {
        FileLock::shared(self.layout.state_lock()).map_err(Into::into)
    }

    fn lock_exclusive(&self) -> Result<FileLock, AppError> {
        FileLock::exclusive(self.layout.state_lock()).map_err(Into::into)
    }

    pub(crate) fn load_config(&self) -> Result<AppConfig, AppError> {
        let _lock = self.lock_shared()?;
        self.load_unlocked()
    }

    pub(crate) fn load_catalog(&self) -> Result<SourceCatalog, AppError> {
        let span = info_span!("coral.app.config.load_catalog");
        let _guard = span.enter();
        self.load_config().map(|config| config.catalog)
    }

    fn update_catalog<T>(
        &self,
        update: impl FnOnce(&mut SourceCatalog) -> T,
    ) -> Result<T, AppError> {
        let _lock = self.lock_exclusive()?;
        let mut config = self.load_unlocked()?;
        let result = update(&mut config.catalog);
        self.save_unlocked(&config)?;
        Ok(result)
    }

    pub(crate) fn list_workspace_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        self.load_catalog()
            .map(|catalog| catalog.workspace_sources(workspace_name))
    }

    pub(crate) fn get_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        self.load_catalog()?
            .get_source(workspace_name, source_name)
            .ok_or_else(|| AppError::SourceNotFound(format!("{workspace_name}:{source_name}")))
    }

    pub(crate) fn upsert_source(
        &self,
        workspace_name: &WorkspaceName,
        source: InstalledSource,
    ) -> Result<(), AppError> {
        self.update_catalog(|catalog| catalog.upsert_source(workspace_name, source))
    }

    pub(crate) fn remove_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), AppError> {
        self.update_catalog(|catalog| {
            catalog.remove_source(workspace_name, source_name);
        })
    }
}

impl SourceRegistry for ConfigStore {
    fn list_workspace_sources(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<SourceRegistryRecord>, AppError> {
        let workspace_name = WorkspaceName::parse(workspace_id)?;
        ConfigStore::list_workspace_sources(self, &workspace_name).map(|sources| {
            sources
                .into_iter()
                .map(|source| record_from_installed_source(&workspace_name, source))
                .collect()
        })
    }

    fn get_source(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<Option<SourceRegistryRecord>, AppError> {
        let workspace_name = WorkspaceName::parse(workspace_id)?;
        let source_name = SourceName::parse(source_name)?;
        match ConfigStore::get_source(self, &workspace_name, &source_name) {
            Ok(source) => Ok(Some(record_from_installed_source(&workspace_name, source))),
            Err(AppError::SourceNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn upsert_source(&self, record: SourceRegistryRecord) -> Result<(), AppError> {
        let workspace_name = WorkspaceName::parse(&record.workspace_id)?;
        let source = installed_source_from_record(&workspace_name, record)?;
        ConfigStore::upsert_source(self, &workspace_name, source)
    }

    fn remove_source(&self, workspace_id: &str, source_name: &str) -> Result<(), AppError> {
        let workspace_name = WorkspaceName::parse(workspace_id)?;
        let source_name = SourceName::parse(source_name)?;
        ConfigStore::remove_source(self, &workspace_name, &source_name)
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

    // Remove and fully rebuild the workspaces section so removed sources don't linger.
    doc.remove("workspaces");

    for (workspace_name, workspace) in &config.workspaces {
        for (source_name, source) in &workspace.sources {
            ensure_implicit_table(&mut doc["workspaces"]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]["sources"]);

            let source_item = &mut doc["workspaces"][workspace_name]["sources"][source_name];
            if !source_item.is_table() {
                *source_item = toml_edit::table();
            }

            if let Some(source_spec_id) = &source.source_spec_id {
                source_item["source_spec_id"] = value(source_spec_id.clone());
            } else {
                let source_table = source_item
                    .as_table_mut()
                    .expect("source config entry should be a table after initialization");
                source_table.remove("source_spec_id");
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
            if source.identity_bindings.is_empty() {
                let source_table = source_item
                    .as_table_mut()
                    .expect("source config entry should be a table after initialization");
                source_table.remove("identity_bindings");
            } else {
                source_item["identity_bindings"] =
                    Item::Value(render_identity_bindings(&source.identity_bindings));
            }
            source_item["origin"] = value(source.origin.as_config_value());
        }
    }

    doc.to_string()
}

fn ensure_implicit_table(item: &mut Item) {
    if !item.is_table() {
        *item = toml_edit::table();
    }
    item.as_table_mut()
        .expect("table item must be available")
        .set_implicit(true);
}

impl TryFrom<PersistedAppConfig> for AppConfig {
    type Error = AppError;

    fn try_from(value: PersistedAppConfig) -> Result<Self, Self::Error> {
        let mut catalog = SourceCatalog::default();
        for (workspace_name, workspace_config) in value.workspaces {
            let workspace_name = WorkspaceName::parse(&workspace_name)?;
            for (source_name, source) in workspace_config.sources {
                let source_name = SourceName::parse(&source_name)?;
                let installed_source = source.into_installed_source(source_name)?;
                catalog.upsert_source(&workspace_name, installed_source);
            }
        }
        Ok(Self {
            version: value.version,
            engine: value.engine,
            catalog,
        })
    }
}

impl From<&AppConfig> for PersistedAppConfig {
    fn from(value: &AppConfig) -> Self {
        let mut workspaces = BTreeMap::new();
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
        Self {
            version: value.version,
            engine: value.engine.clone(),
            workspaces,
        }
    }
}

fn validate_identity_bindings(
    source_name: &str,
    bindings: &BTreeMap<String, SourceIdentityBinding>,
) -> Result<(), AppError> {
    for (surface_id, binding) in bindings {
        validate_identity_binding_surface_id(source_name, surface_id)?;
        binding.validate().map_err(|error| {
            AppError::InvalidInput(format!(
                "source '{source_name}' identity binding for surface '{surface_id}' is invalid: {error}"
            ))
        })?;
    }
    Ok(())
}

fn validate_identity_binding_surface_id(
    source_name: &str,
    surface_id: &str,
) -> Result<(), AppError> {
    let mut chars = surface_id.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(AppError::InvalidInput(format!(
            "source '{source_name}' identity binding surface '{surface_id}' must match [a-z][a-z0-9_]*"
        )))
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

fn render_identity_bindings(values: &BTreeMap<String, SourceIdentityBinding>) -> Value {
    let mut table = InlineTable::new();
    for (surface_id, binding) in values {
        let mut binding_table = InlineTable::new();
        binding_table.insert("owner", Value::from(binding.owner.as_config_value()));
        if let Some(identity) = &binding.identity {
            binding_table.insert("identity", Value::from(identity.clone()));
        }
        if let Some(accepted_identity) = &binding.accepted_identity {
            binding_table.insert("accepted_identity", Value::from(accepted_identity.clone()));
        }
        binding_table.fmt();
        table.insert(surface_id, Value::InlineTable(binding_table));
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
        AppConfig, PersistedAppConfig, PersistedEngineConfig, RawFeatureContainerState,
        RawFeatureValue, SourceCatalog, load_raw_feature_overrides, render_config,
        set_raw_feature_override,
    };
    use crate::bootstrap::AppError;
    use crate::credentials::CredentialStorageKind;
    use crate::identity::{SourceIdentityBinding, SourceIdentityOwner};
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    fn default_workspace() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn source_name(name: &str) -> SourceName {
        SourceName::parse(name).expect("source")
    }

    fn installed_source(name: &str) -> InstalledSource {
        InstalledSource {
            name: source_name(name),
            source_spec_id: None,
            version: Some("1.1.4".to_string()),
            variables: BTreeMap::from([(
                "GITHUB_API_BASE".to_string(),
                "https://api.github.com".to_string(),
            )]),
            secrets: vec!["GITHUB_TOKEN".to_string()],
            credential_storage: None,
            identity_bindings: BTreeMap::new(),
            origin: SourceOrigin::Imported,
        }
    }

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout")
    }

    /// A fresh temp layout whose config file already holds `raw`.
    fn layout_with_config(raw: &str) -> (TempDir, AppStateLayout) {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        std::fs::create_dir_all(
            layout
                .config_file()
                .parent()
                .expect("config file should have parent"),
        )
        .expect("create config dir");
        std::fs::write(layout.config_file(), raw).expect("write config");
        (temp, layout)
    }

    fn raw_feature_entries(layout: &AppStateLayout) -> BTreeMap<String, RawFeatureValue> {
        load_raw_feature_overrides(layout)
            .expect("feature overrides")
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    /// An app config whose catalog holds `source` in the default workspace.
    fn config_with_source(source: InstalledSource) -> AppConfig {
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&default_workspace(), source);
        AppConfig {
            version: 1,
            engine: PersistedEngineConfig::default(),
            catalog,
        }
    }

    /// Parses raw TOML into the app config model.
    #[expect(
        clippy::unwrap_in_result,
        reason = "test helper: TOML fixtures must parse; only app-model conversion is under test"
    )]
    fn load_config(raw: &str) -> Result<AppConfig, AppError> {
        AppConfig::try_from(toml::from_str::<PersistedAppConfig>(raw).expect("config should parse"))
    }

    /// Prefixes `body` with the `version = 1` header every config file
    /// starts with.
    fn config_toml(body: &str) -> String {
        format!("version = 1\n\n{body}")
    }

    /// Parses a config whose `body` follows the `version = 1` header.
    fn load_config_body(body: &str) -> Result<AppConfig, AppError> {
        load_config(&config_toml(body))
    }

    /// A full config holding one `github_v4` source with the given inline
    /// `identity_bindings` TOML.
    fn github_v4_config(identity_bindings: &str) -> String {
        config_toml(&format!(
            r#"[workspaces.default.sources.github_v4]
variables = {{}}
secrets = []
identity_bindings = {identity_bindings}
origin = "imported"
"#
        ))
    }

    /// Parses a config body (`version = 1` header implied) and resolves the
    /// dependent-join runtime config for the selected source names.
    #[expect(
        clippy::unwrap_in_result,
        reason = "test helper: TOML fixtures must parse; only runtime conversion is under test"
    )]
    fn dependent_join_runtime_config(
        body: &str,
        selected: &[&str],
    ) -> Result<DependentJoinConfig, AppError> {
        let selected: Vec<String> = selected.iter().map(|name| (*name).to_string()).collect();
        toml::from_str::<PersistedAppConfig>(&config_toml(body))
            .expect("dependent join config should parse")
            .engine
            .dependent_join
            .try_into_runtime_config(&selected)
    }

    #[test]
    fn default_config_uses_canonical_version() {
        assert_eq!(AppConfig::default().version, 1);
    }

    #[test]
    fn renders_sources_under_workspace_keyed_tables() {
        let config = config_with_source(installed_source("github"));

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
    fn omits_empty_versions_from_rendered_source_entries() {
        let mut source = installed_source("github");
        source.version = None;
        source.origin = SourceOrigin::Bundled;
        let config = config_with_source(source);

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(!raw.contains("version = \"\""));
        assert!(!raw.contains("version = \""));
    }

    #[test]
    fn loads_sources_from_workspace_keyed_tables() {
        let raw = r#"
[workspaces.default.sources.github]
version = "1.1.4"
variables = { GITHUB_API_BASE = "https://api.github.com" }
secrets = ["GITHUB_TOKEN"]
origin = "bundled"
"#;

        let config = load_config_body(raw).expect("config");
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
    fn loads_dependent_join_engine_config() {
        let raw = r"
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

        let config = dependent_join_runtime_config(raw, &["github"])
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
[engine.dependent_join.per_source." github "]
enabled = false
"#;

        let config = dependent_join_runtime_config(raw, &["github"])
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
    fn rejects_zero_dependent_join_limits() {
        for (label, body, selected, expected) in [
            (
                "zero limit should fail",
                "[engine.dependent_join]\nmax_concurrency = 0\n",
                &[][..],
                "engine.dependent_join.max_concurrency must be greater than 0",
            ),
            (
                "zero source limit should fail",
                "[engine.dependent_join.per_source.github]\nmax_concurrency = 0\n",
                &["github"][..],
                "engine.dependent_join.per_source.github.max_concurrency must be greater than 0",
            ),
        ] {
            let error = dependent_join_runtime_config(body, selected).expect_err(label);

            assert!(error.to_string().contains(expected), "{label}: {error}");
        }
    }

    #[test]
    fn ignores_dependent_join_source_limits_for_unselected_sources() {
        let raw = r"
[engine.dependent_join.per_source.github]
max_concurrency = 4

[engine.dependent_join.per_source.linear]
max_concurrency = 0
";

        let config = dependent_join_runtime_config(raw, &["github"])
            .expect("unselected source override should not be validated");

        assert!(config.per_source.contains_key("github"));
        assert!(!config.per_source.contains_key("linear"));
    }

    #[test]
    fn round_trips_source_credential_storage() {
        let mut source = installed_source("github");
        source.credential_storage = Some(CredentialStorageKind::Keychain);
        let config = config_with_source(source);

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(raw.contains("credential_storage = \"keychain\""));

        let loaded = load_config(&raw).expect("config");
        let sources = loaded.catalog.workspace_sources(&default_workspace());
        assert_eq!(
            sources[0].credential_storage,
            Some(CredentialStorageKind::Keychain)
        );
    }

    #[test]
    fn round_trips_source_spec_id() {
        let mut source = installed_source("github_alias");
        source.source_spec_id = Some("github_v4".to_string());
        let config = config_with_source(source);

        let raw = render_config(&PersistedAppConfig::from(&config), None);
        assert!(raw.contains("source_spec_id = \"github_v4\""));

        let loaded = load_config(&raw).expect("config");
        let sources = loaded.catalog.workspace_sources(&default_workspace());
        assert_eq!(sources[0].source_spec_id.as_deref(), Some("github_v4"));
    }

    #[test]
    fn round_trips_source_identity_bindings() {
        let mut source = installed_source("github_v4");
        source
            .identity_bindings
            .insert("rest".to_string(), SourceIdentityBinding::user_owned());
        let config = config_with_source(source);

        let raw = render_config(&PersistedAppConfig::from(&config), None);

        assert!(raw.contains("identity_bindings"));
        assert!(raw.contains("owner = \"user\""));
        assert!(!raw.contains("github_local"));
        assert!(!raw.contains("accepted_identity"));
        let loaded = load_config(&raw).expect("config");
        let sources = loaded.catalog.workspace_sources(&default_workspace());
        let binding = sources[0]
            .identity_bindings
            .get("rest")
            .expect("rest identity binding");
        assert_eq!(binding.identity, None);
        assert_eq!(binding.owner, SourceIdentityOwner::User);
        assert_eq!(binding.accepted_identity, None);
    }

    #[test]
    fn loads_workspace_owned_source_identity_binding_from_config() {
        let raw = github_v4_config(
            r#"{ rest = { identity = "github_workspace", owner = "workspace" } }"#,
        );

        let config = load_config(&raw).expect("config");
        let sources = config.catalog.workspace_sources(&default_workspace());
        let binding = sources[0]
            .identity_bindings
            .get("rest")
            .expect("rest identity binding");

        assert_eq!(binding.identity.as_deref(), Some("github_workspace"));
        assert_eq!(binding.owner, SourceIdentityOwner::Workspace);
        assert_eq!(binding.accepted_identity, None);
    }

    #[test]
    fn rejects_invalid_source_identity_bindings_from_config() {
        for (identity_bindings, expected) in [
            (
                r#"{ "bad/rest" = { identity = "github_workspace", owner = "workspace" } }"#,
                "identity binding surface 'bad/rest' must match [a-z][a-z0-9_]*",
            ),
            (
                r#"{ rest = { identity = "bad/path", owner = "workspace" } }"#,
                "identity name must not contain",
            ),
            (
                r#"{ rest = { identity = "github_workspace", owner = "workspace", accepted_identity = "bad/path" } }"#,
                "accepted identity name must not contain",
            ),
            (
                r#"{ rest = { identity = "github_local", owner = "user" } }"#,
                "user-owned source identity bindings store only owner",
            ),
        ] {
            let error = load_config(&github_v4_config(identity_bindings))
                .expect_err("invalid identity binding should fail config load");

            assert!(
                error.to_string().contains(expected),
                "expected '{expected}' in '{error}'"
            );
        }
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
            .get_source(&workspace_name, &source_name("github"))
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

        catalog.remove_source(&default_workspace, &source_name("github"));

        assert!(
            catalog
                .get_source(&default_workspace, &source_name("github"))
                .is_none()
        );
        assert!(catalog.workspace_sources(&default_workspace).is_empty());
        assert!(
            catalog
                .get_source(&other_workspace_name, &source_name("slack"))
                .is_some()
        );
    }

    #[test]
    fn preserves_unrelated_sections_when_rendering_with_existing_config() {
        let existing = config_toml(
            r#"[otel]
endpoint = "http://localhost:4318"
headers = "from=config"

	[trace_history]
	enabled = false
	retention_days = 3

	[features]
	feedback = true
	future_feature = "not-yet-known"

[engine.dependent_join]
enabled = false
max_bindings = 250

	[workspaces.default.sources.github]
version = "1.0.0"
variables = {}
secrets = []
origin = "bundled"
"#,
        );

        let config = config_with_source(installed_source("slack"));

        let raw = render_config(&PersistedAppConfig::from(&config), Some(existing.as_str()));

        // Unrelated sections (otel, trace history, features, dependent join)
        // must survive the round-trip.
        for needle in [
            "[otel]",
            "endpoint = \"http://localhost:4318\"",
            "headers = \"from=config\"",
            "[trace_history]",
            "enabled = false",
            "retention_days = 3",
            "[features]",
            "feedback = true",
            "future_feature = \"not-yet-known\"",
            "[engine.dependent_join]",
            "max_bindings = 250",
        ] {
            assert!(raw.contains(needle), "{needle} should be preserved");
        }

        // The newly added source must be present.
        assert!(raw.contains("[workspaces.default.sources.slack]"));

        // The old source that was not in the updated catalog must be gone.
        assert!(!raw.contains("[workspaces.default.sources.github]"));
    }

    #[test]
    fn rejects_invalid_workspace_or_source_keys_when_loading() {
        let invalid_workspace = r#"
[workspaces."bad\\workspace".sources.github]
origin = "bundled"
"#;
        let error =
            load_config_body(invalid_workspace).expect_err("invalid workspace key should fail");
        assert!(error.to_string().contains("workspace name"));

        let invalid_source = r#"
[workspaces.default.sources."bad\\source"]
origin = "bundled"
"#;
        let error = load_config_body(invalid_source).expect_err("invalid source key should fail");
        assert!(error.to_string().contains("source name"));
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
        let (_temp, layout) = layout_with_config(&config_toml(
            r#"[features]
feedback = true
future_flag = false
wrong_type = "yes"

[features.nested]
enabled = true
"#,
        ));

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
        let (_temp, layout) = layout_with_config("features.feedback = false\n");

        let entries = raw_feature_entries(&layout);

        assert_eq!(entries.get("feedback"), Some(&RawFeatureValue::Bool(false)));
    }

    #[test]
    fn raw_feature_overrides_ignore_inline_feature_table() {
        let (_temp, layout) = layout_with_config("features = { feedback = true }\n");

        let overrides = load_raw_feature_overrides(&layout).expect("feature overrides");

        assert!(overrides.iter().next().is_none());
        assert_eq!(overrides.container(), RawFeatureContainerState::Unsupported);
    }

    #[test]
    fn raw_feature_overrides_fail_for_invalid_toml() {
        let (_temp, layout) = layout_with_config("[features\nfeedback = true\n");

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
        let (_temp, layout) = layout_with_config(
            r#"
[features]
future_flag = "yes"
feedback = true
"#,
        );

        for (value, expected) in [(false, "feedback = false"), (true, "feedback = true")] {
            set_raw_feature_override(&layout, "feedback", value).expect("set feature");

            let raw = std::fs::read_to_string(layout.config_file()).expect("config file");
            assert!(raw.contains(expected), "{expected}");
            assert!(raw.contains("future_flag = \"yes\""), "set {value}");
        }
    }

    #[test]
    fn feature_mutations_reject_inline_feature_container_without_rewriting_file() {
        let original = "features = { feedback = true }\n";
        let (_temp, layout) = layout_with_config(original);

        let error =
            set_raw_feature_override(&layout, "feedback", true).expect_err("inline features");

        assert!(error.to_string().contains("unsupported [features] config"));
        let raw = std::fs::read_to_string(layout.config_file()).expect("config file");
        assert_eq!(raw, original);
    }
}
