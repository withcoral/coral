//! Persists the registered source catalog in top-level `config.toml`.

use std::collections::BTreeMap;

use coral_api::v1::Workspace;
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};

use crate::bootstrap::AppError;
use crate::sources::model::ManagedSource;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};

#[derive(Debug, Clone)]
struct AppConfig {
    version: u32,
    sources: Vec<ManagedSource>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: default_config_version(),
            sources: Vec::new(),
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
    sources: BTreeMap<String, PersistedSourceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSourceConfig {
    #[serde(default)]
    version: String,
    #[serde(default)]
    variables: BTreeMap<String, String>,
    #[serde(default)]
    secrets: Vec<String>,
    origin: crate::sources::model::ManagedSourceOrigin,
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

    pub(crate) fn list_workspace_sources(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<ManagedSource>, AppError> {
        let _lock = self.lock_shared()?;
        self.load_unlocked().map(|config| {
            config
                .sources
                .into_iter()
                .filter(|source| source.workspace.name == workspace.name)
                .collect()
        })
    }

    pub(crate) fn get_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<ManagedSource, AppError> {
        let _lock = self.lock_shared()?;
        self.load_unlocked()?
            .sources
            .into_iter()
            .find(|source| source.workspace.name == workspace.name && source.name == source_name)
            .ok_or_else(|| AppError::SourceNotFound(format!("{}:{source_name}", workspace.name)))
    }

    pub(crate) fn upsert_source(&self, source: ManagedSource) -> Result<(), AppError> {
        let _lock = self.lock_exclusive()?;
        let mut config = self.load_unlocked()?;
        config.sources.retain(|existing| {
            !(existing.workspace.name == source.workspace.name && existing.name == source.name)
        });
        config.sources.push(source);
        config.sources.sort_by(|left, right| {
            (&left.workspace.name, &left.name).cmp(&(&right.workspace.name, &right.name))
        });
        self.save_unlocked(&config)
    }

    pub(crate) fn remove_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<(), AppError> {
        let _lock = self.lock_exclusive()?;
        let mut config = self.load_unlocked()?;
        config.sources.retain(|source| {
            !(source.workspace.name == workspace.name && source.name == source_name)
        });
        self.save_unlocked(&config)
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
        let mut sources = Vec::new();
        for (workspace_name, workspace) in value.workspaces {
            for (source_name, source) in workspace.sources {
                sources.push(ManagedSource {
                    workspace: Workspace {
                        name: workspace_name.clone(),
                    },
                    name: source_name,
                    version: source.version,
                    variables: source.variables,
                    secrets: source.secrets,
                    origin: source.origin,
                });
            }
        }
        sources.sort_by(|left, right| {
            (&left.workspace.name, &left.name).cmp(&(&right.workspace.name, &right.name))
        });
        Self {
            version: value.version,
            sources,
        }
    }
}

impl From<&AppConfig> for PersistedAppConfig {
    fn from(value: &AppConfig) -> Self {
        let mut workspaces = BTreeMap::new();
        for source in &value.sources {
            workspaces
                .entry(source.workspace.name.clone())
                .or_insert_with(PersistedWorkspaceConfig::default)
                .sources
                .insert(
                    source.name.clone(),
                    PersistedSourceConfig {
                        version: source.version.clone(),
                        variables: source.variables.clone(),
                        secrets: source.secrets.clone(),
                        origin: source.origin,
                    },
                );
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

    use super::{AppConfig, PersistedAppConfig, render_config};
    use crate::sources::model::{ManagedSource, ManagedSourceOrigin};

    #[test]
    fn default_config_uses_canonical_version() {
        assert_eq!(AppConfig::default().version, 1);
    }

    #[test]
    fn renders_sources_under_workspace_keyed_tables() {
        let config = AppConfig {
            version: 1,
            sources: vec![ManagedSource {
                workspace: Workspace {
                    name: "default".to_string(),
                },
                name: "github".to_string(),
                version: String::new(),
                variables: BTreeMap::from([(
                    "GITHUB_API_BASE".to_string(),
                    "https://api.github.com".to_string(),
                )]),
                secrets: vec!["GITHUB_TOKEN".to_string()],
                origin: ManagedSourceOrigin::Bundled,
            }],
        };

        let raw = render_config(&PersistedAppConfig::from(&config));
        assert!(raw.contains("[workspaces.default.sources.github]"));
        assert!(raw.contains("variables = { GITHUB_API_BASE = \"https://api.github.com\" }"));
        assert!(raw.contains("secrets = [\"GITHUB_TOKEN\"]"));
        assert!(!raw.contains("version = \""));
        assert!(!raw.contains("[[sources]]"));
        assert!(!raw.contains("workspace = { name = \"default\" }"));
        assert!(!raw.contains("manifest_file"));
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
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.sources[0].workspace.name, "default");
        assert_eq!(config.sources[0].name, "github");
        assert_eq!(config.sources[0].version, "1.1.4");
        assert_eq!(
            config.sources[0].variables.get("GITHUB_API_BASE"),
            Some(&"https://api.github.com".to_string())
        );
        assert_eq!(config.sources[0].secrets, vec!["GITHUB_TOKEN".to_string()]);
    }
}
