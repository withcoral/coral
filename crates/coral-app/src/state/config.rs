//! Persists the installed source catalog in top-level `config.toml`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

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
    version: Option<String>,
    #[serde(default)]
    variables: BTreeMap<String, String>,
    #[serde(default)]
    secrets: Vec<String>,
    origin: SourceOrigin,
}

impl PersistedInstalledSource {
    fn into_installed_source(self, source_name: SourceName) -> InstalledSource {
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
                catalog.upsert_source(&workspace_name, source.into_installed_source(source_name));
            }
        }
        Ok(Self {
            version: value.version,
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
    #![expect(
        clippy::indexing_slicing,
        reason = "loaded source order assertions intentionally fail loudly in tests"
    )]

    use std::collections::BTreeMap;

    use super::{AppConfig, PersistedAppConfig, SourceCatalog, render_config};
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
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
            origin: SourceOrigin::Imported,
        }
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
            catalog,
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
    fn omits_empty_versions_from_rendered_source_entries() {
        let workspace_name = default_workspace();
        let mut source = installed_source("github");
        source.version = None;
        source.origin = SourceOrigin::Bundled;
        let mut catalog = SourceCatalog::default();
        catalog.upsert_source(&workspace_name, source);
        let config = AppConfig {
            version: 1,
            catalog,
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
            catalog,
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
    }
}
