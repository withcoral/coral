//! Persists the installed source catalog in top-level `config.toml`.

use std::collections::BTreeMap;

use coral_api::v1::Workspace;
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};

use crate::bootstrap::AppError;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};

#[derive(Debug, Clone)]
struct AppConfig {
    version: u32,
    catalog: SourceCatalog,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: default_config_version(),
            catalog: SourceCatalog::default(),
        }
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
    workspaces: BTreeMap<String, PersistedWorkspaceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedWorkspaceConfig {
    #[serde(default)]
    sources: BTreeMap<String, PersistedInstalledSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedInstalledSource {
    #[serde(default)]
    version: String,
    #[serde(default)]
    variables: BTreeMap<String, String>,
    #[serde(default)]
    secrets: Vec<String>,
    origin: SourceOrigin,
}

impl PersistedInstalledSource {
    fn into_installed_source(self, source_name: String) -> InstalledSource {
        InstalledSource {
            name: source_name,
            version: self.version,
            variables: self.variables,
            secrets: self.secrets,
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
            origin: value.origin,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceCatalog(BTreeMap<String, BTreeMap<String, InstalledSource>>);

impl SourceCatalog {
    pub(crate) fn workspace_sources(&self, workspace: &Workspace) -> Vec<InstalledSource> {
        self.0
            .get(&workspace.name)
            .map(|sources| sources.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn get_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Option<InstalledSource> {
        self.0
            .get(&workspace.name)
            .and_then(|sources| sources.get(source_name))
            .cloned()
    }

    pub(crate) fn contains(&self, workspace: &Workspace, source_name: &str) -> bool {
        self.0
            .get(&workspace.name)
            .is_some_and(|sources| sources.contains_key(source_name))
    }

    pub(crate) fn upsert_source(&mut self, workspace: &Workspace, source: InstalledSource) {
        self.0
            .entry(workspace.name.clone())
            .or_default()
            .insert(source.name.clone(), source);
    }

    pub(crate) fn remove_source(
        &mut self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Option<InstalledSource> {
        let mut removed = None;
        let remove_workspace = match self.0.get_mut(&workspace.name) {
            Some(sources) => {
                removed = sources.remove(source_name);
                sources.is_empty()
            }
            None => false,
        };

        if remove_workspace {
            self.0.remove(&workspace.name);
        }

        removed
    }
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
        Ok(AppConfig::from(persisted))
    }

    fn save_unlocked(&self, config: &AppConfig) -> Result<(), AppError> {
        let raw = render_config(&PersistedAppConfig::from(config));
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

    pub(crate) fn load_catalog(&self) -> Result<SourceCatalog, AppError> {
        let _lock = self.lock_shared()?;
        self.load_unlocked().map(|config| config.catalog)
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
        workspace: &Workspace,
    ) -> Result<Vec<InstalledSource>, AppError> {
        self.load_catalog()
            .map(|catalog| catalog.workspace_sources(workspace))
    }

    pub(crate) fn get_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<InstalledSource, AppError> {
        self.load_catalog()?
            .get_source(workspace, source_name)
            .ok_or_else(|| AppError::SourceNotFound(format!("{}:{source_name}", workspace.name)))
    }

    pub(crate) fn upsert_source(
        &self,
        workspace: &Workspace,
        source: InstalledSource,
    ) -> Result<(), AppError> {
        self.update_catalog(|catalog| catalog.upsert_source(workspace, source))
    }

    pub(crate) fn remove_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<(), AppError> {
        self.update_catalog(|catalog| {
            catalog.remove_source(workspace, source_name);
        })
    }
}

fn render_config(config: &PersistedAppConfig) -> String {
    let mut doc = DocumentMut::new();
    doc["version"] = value(i64::from(config.version));

    for (workspace_name, workspace) in &config.workspaces {
        for (source_name, source) in &workspace.sources {
            ensure_implicit_table(&mut doc["workspaces"]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]);
            ensure_implicit_table(&mut doc["workspaces"][workspace_name]["sources"]);

            let source_item = &mut doc["workspaces"][workspace_name]["sources"][source_name];
            if !source_item.is_table() {
                *source_item = toml_edit::table();
            }

            if source.version.is_empty() {
                let source_table = source_item
                    .as_table_mut()
                    .expect("source config entry should be a table after initialization");
                source_table.remove("version");
            } else {
                source_item["version"] = value(source.version.clone());
            }
            source_item["variables"] = Item::Value(render_inline_table(&source.variables));
            source_item["secrets"] = Item::Value(render_string_array(&source.secrets));
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

impl From<PersistedAppConfig> for AppConfig {
    fn from(value: PersistedAppConfig) -> Self {
        let mut catalog = SourceCatalog::default();
        for (workspace_name, workspace_config) in value.workspaces {
            let workspace = Workspace {
                name: workspace_name.clone(),
            };
            for (source_name, source) in workspace_config.sources {
                catalog.upsert_source(&workspace, source.into_installed_source(source_name));
            }
        }
        Self {
            version: value.version,
            catalog,
        }
    }
}

impl From<&AppConfig> for PersistedAppConfig {
    fn from(value: &AppConfig) -> Self {
        let mut workspaces = BTreeMap::new();
        for (workspace_name, sources) in &value.catalog.0 {
            let workspace_config = workspaces
                .entry(workspace_name.clone())
                .or_insert_with(PersistedWorkspaceConfig::default);
            for source in sources.values() {
                workspace_config
                    .sources
                    .insert(source.name.clone(), PersistedInstalledSource::from(source));
            }
        }
        Self {
            version: value.version,
            workspaces,
        }
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
    use std::collections::BTreeMap;

    use coral_api::v1::Workspace;

    use super::{AppConfig, PersistedAppConfig, SourceCatalog, render_config};
    use crate::sources::model::{InstalledSource, SourceOrigin};

    fn default_workspace() -> Workspace {
        Workspace {
            name: "default".to_string(),
        }
    }

    fn installed_source(name: &str) -> InstalledSource {
        InstalledSource {
            name: name.to_string(),
            version: "1.1.4".to_string(),
            variables: BTreeMap::from([(
                "GITHUB_API_BASE".to_string(),
                "https://api.github.com".to_string(),
            )]),
            secrets: vec!["GITHUB_TOKEN".to_string()],
            origin: SourceOrigin::Imported,
        }
    }

    #[test]
    fn default_config_uses_canonical_version() {
        assert_eq!(AppConfig::default().version, 1);
    }

    #[test]
    fn renders_sources_under_workspace_keyed_tables() {
        let workspace = default_workspace();
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace, installed_source("github"));
        let config = AppConfig {
            version: 1,
            catalog,
        };

        let raw = render_config(&PersistedAppConfig::from(&config));
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
        let workspace = default_workspace();
        let mut source = installed_source("github");
        source.version.clear();
        source.origin = SourceOrigin::Bundled;
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace, source);
        let config = AppConfig {
            version: 1,
            catalog,
        };

        let raw = render_config(&PersistedAppConfig::from(&config));
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

        let config = AppConfig::from(
            toml::from_str::<PersistedAppConfig>(raw).expect("workspace-keyed config should parse"),
        );
        let sources = config.catalog.workspace_sources(&default_workspace());
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "github");
        assert_eq!(sources[0].version, "1.1.4");
        assert_eq!(
            sources[0].variables.get("GITHUB_API_BASE"),
            Some(&"https://api.github.com".to_string())
        );
        assert_eq!(sources[0].secrets, vec!["GITHUB_TOKEN".to_string()]);
    }

    #[test]
    fn catalog_upsert_replaces_existing_workspace_source_entry() {
        let workspace = default_workspace();
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace, installed_source("github"));

        let mut updated = installed_source("github");
        updated.version = "2.0.0".to_string();
        updated.origin = SourceOrigin::Imported;
        catalog.upsert_source(&workspace, updated);

        let stored = catalog
            .get_source(&workspace, "github")
            .expect("source should be present");
        assert_eq!(stored.version, "2.0.0");
        assert_eq!(stored.origin, SourceOrigin::Imported);
        assert_eq!(catalog.workspace_sources(&workspace).len(), 1);
    }

    #[test]
    fn catalog_remove_drops_empty_workspace_bucket() {
        let default_workspace = default_workspace();
        let other_workspace = Workspace {
            name: "other".to_string(),
        };
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&default_workspace, installed_source("github"));
        catalog.upsert_source(&other_workspace, installed_source("slack"));

        catalog.remove_source(&default_workspace, "github");

        assert!(catalog.get_source(&default_workspace, "github").is_none());
        assert!(catalog.workspace_sources(&default_workspace).is_empty());
        assert!(catalog.get_source(&other_workspace, "slack").is_some());
    }
}
