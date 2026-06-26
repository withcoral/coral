//! App storage backend configuration loaded from `config.toml`.

use std::path::PathBuf;

use serde::Deserialize;

use super::AppStorageError;
use crate::state::AppStateLayout;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AppStorageBackend {
    #[default]
    Sqlite,
    Postgres,
}

impl AppStorageBackend {
    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppStorageConfig {
    pub(crate) backend: AppStorageBackend,
    sqlite_path: Option<PathBuf>,
}

impl AppStorageConfig {
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppStorageError> {
        let raw = match std::fs::read_to_string(layout.config_file()) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error.into()),
        };
        let file = toml::from_str::<AppStorageConfigFile>(&raw)?;
        Ok(Self {
            backend: file.storage.backend,
            sqlite_path: file.storage.sqlite_path,
        })
    }

    pub(crate) fn sqlite_path(&self, layout: &AppStateLayout) -> PathBuf {
        match &self.sqlite_path {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => layout.config_dir().join(path),
            None => layout.app_database_file(),
        }
    }
}

impl Default for AppStorageConfig {
    fn default() -> Self {
        Self {
            backend: AppStorageBackend::Sqlite,
            sqlite_path: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AppStorageConfigFile {
    #[serde(default)]
    storage: AppStorageConfigSection,
}

#[derive(Debug, Deserialize)]
struct AppStorageConfigSection {
    #[serde(default)]
    backend: AppStorageBackend,
    #[serde(default, alias = "path")]
    sqlite_path: Option<PathBuf>,
}

impl Default for AppStorageConfigSection {
    fn default() -> Self {
        Self {
            backend: AppStorageBackend::Sqlite,
            sqlite_path: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{AppStorageBackend, AppStorageConfig};
    use crate::state::AppStateLayout;

    #[test]
    fn defaults_to_sqlite_at_layout_database_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");

        let config = AppStorageConfig::load(&layout).expect("load default");

        assert_eq!(config.backend, AppStorageBackend::Sqlite);
        assert_eq!(config.sqlite_path(&layout), layout.app_database_file());
    }

    #[test]
    fn parses_sqlite_storage_config() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        std::fs::create_dir_all(layout.config_dir()).expect("mkdir");
        std::fs::write(
            layout.config_file(),
            r#"
version = 1

[storage]
backend = "sqlite"
sqlite_path = "/tmp/coral-test.sqlite3"
"#,
        )
        .expect("write config");

        let config = AppStorageConfig::load(&layout).expect("load config");

        assert_eq!(config.backend, AppStorageBackend::Sqlite);
        assert_eq!(
            config.sqlite_path(&layout),
            std::path::PathBuf::from("/tmp/coral-test.sqlite3")
        );
    }

    #[test]
    fn resolves_relative_sqlite_path_under_config_dir() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        std::fs::create_dir_all(layout.config_dir()).expect("mkdir");
        std::fs::write(
            layout.config_file(),
            r#"
[storage]
sqlite_path = "db/coral.sqlite3"
"#,
        )
        .expect("write config");

        let config = AppStorageConfig::load(&layout).expect("load config");

        assert_eq!(
            config.sqlite_path(&layout),
            layout.config_dir().join("db/coral.sqlite3")
        );
    }

    #[test]
    fn accepts_postgres_as_future_backend_without_sqlite_path() {
        let file = toml::from_str::<super::AppStorageConfigFile>(
            r#"
[storage]
backend = "postgres"
"#,
        )
        .expect("parse config");

        assert_eq!(file.storage.backend, AppStorageBackend::Postgres);
        assert_eq!(file.storage.sqlite_path, None);
    }
}
