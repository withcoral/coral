use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

use super::DbError;
use crate::state::AppStateLayout;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DatabaseConfig {
    Sqlite { path: PathBuf },
    Postgres { url_env: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedDatabaseConfig {
    Sqlite { path: PathBuf },
    Postgres { url: String },
}

#[derive(Debug, Deserialize)]
struct PersistedConfig {
    #[serde(default)]
    database: Option<RawPersistedDatabaseConfig>,
}

#[derive(Debug, Deserialize)]
struct RawPersistedDatabaseConfig {
    #[serde(default)]
    backend: Option<PersistedDatabaseBackend>,
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    url_env: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PersistedDatabaseBackend {
    Sqlite,
    Postgres,
}

#[derive(Debug)]
enum PersistedDatabaseConfig {
    Sqlite { path: Option<PathBuf> },
    Postgres { url_env: String },
}

impl RawPersistedDatabaseConfig {
    fn into_config(self) -> Result<PersistedDatabaseConfig, DbError> {
        if let Some(field) = self.extra.keys().next() {
            return Err(DbError::Config(format!(
                "unsupported [database].{field} configuration key"
            )));
        }

        let backend = self.backend.ok_or_else(|| {
            DbError::Config("[database].backend is required when [database] is present".to_string())
        })?;

        match backend {
            PersistedDatabaseBackend::Sqlite => {
                if self.url_env.is_some() {
                    return Err(DbError::Config(
                        "database backend 'sqlite' does not support [database].url_env".to_string(),
                    ));
                }
                Ok(PersistedDatabaseConfig::Sqlite { path: self.path })
            }
            PersistedDatabaseBackend::Postgres => {
                if self.path.is_some() {
                    return Err(DbError::Config(
                        "database backend 'postgres' does not support [database].path".to_string(),
                    ));
                }
                let url_env = self.url_env.ok_or_else(|| {
                    DbError::Config(
                        "database backend 'postgres' requires [database].url_env".to_string(),
                    )
                })?;
                Ok(PersistedDatabaseConfig::Postgres { url_env })
            }
        }
    }
}

impl DatabaseConfig {
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, DbError> {
        if !layout.config_file().try_exists()? {
            return Ok(Self::default_sqlite(layout));
        }

        let raw = std::fs::read_to_string(layout.config_file())?;
        let persisted: PersistedConfig = toml::from_str(&raw)?;
        let Some(database) = persisted.database else {
            return Ok(Self::default_sqlite(layout));
        };

        match database.into_config()? {
            PersistedDatabaseConfig::Sqlite { path } => {
                let path = path.map_or_else(
                    || layout.database_file(),
                    |path| resolve_sqlite_path(layout, path),
                );
                Ok(Self::Sqlite { path })
            }
            PersistedDatabaseConfig::Postgres { url_env } => Ok(Self::Postgres { url_env }),
        }
    }

    fn default_sqlite(layout: &AppStateLayout) -> Self {
        Self::Sqlite {
            path: layout.database_file(),
        }
    }
}

fn resolve_sqlite_path(layout: &AppStateLayout, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    layout.config_dir().join(path)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{DatabaseConfig, DbError};
    use crate::state::AppStateLayout;

    #[test]
    fn defaults_to_sqlite_under_app_state() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");

        let config = DatabaseConfig::load(&layout).expect("db config");

        assert_eq!(
            config,
            DatabaseConfig::Sqlite {
                path: layout.database_file()
            }
        );
    }

    #[test]
    fn resolves_relative_sqlite_path_under_app_state() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        fs::create_dir_all(layout.config_dir()).expect("config dir");
        fs::write(
            layout.config_file(),
            "[database]\nbackend = \"sqlite\"\npath = \"state/custom.db\"\n",
        )
        .expect("config");

        let config = DatabaseConfig::load(&layout).expect("db config");

        assert_eq!(
            config,
            DatabaseConfig::Sqlite {
                path: layout.config_dir().join("state/custom.db")
            }
        );
    }

    #[test]
    fn loads_postgres_url_env_name() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        fs::create_dir_all(layout.config_dir()).expect("config dir");
        fs::write(
            layout.config_file(),
            "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
        )
        .expect("config");

        let config = DatabaseConfig::load(&layout).expect("db config");

        assert_eq!(
            config,
            DatabaseConfig::Postgres {
                url_env: "CORAL_DATABASE_URL".to_string()
            }
        );
    }

    #[test]
    fn database_section_requires_explicit_backend() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        fs::create_dir_all(layout.config_dir()).expect("config dir");
        fs::write(
            layout.config_file(),
            "[database]\nurl_env = \"CORAL_DATABASE_URL\"\n",
        )
        .expect("config");

        let error = DatabaseConfig::load(&layout).expect_err("db config should reject backend");

        assert!(
            matches!(error, DbError::Config(ref detail) if detail.contains("[database].backend is required")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn database_backend_rejects_unsupported_fields() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        fs::create_dir_all(layout.config_dir()).expect("config dir");

        for (raw_config, expected_error) in [
            (
                "[database]\nbackend = \"sqlite\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
                "database backend 'sqlite' does not support [database].url_env",
            ),
            (
                "[database]\nbackend = \"postgres\"\npath = \"coral.db\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
                "database backend 'postgres' does not support [database].path",
            ),
            (
                "[database]\nbackend = \"sqlite\"\nurl_environment = \"CORAL_DATABASE_URL\"\n",
                "unsupported [database].url_environment configuration key",
            ),
        ] {
            fs::write(layout.config_file(), raw_config).expect("config");

            let error = DatabaseConfig::load(&layout).expect_err("db config should reject field");

            assert!(
                matches!(error, DbError::Config(ref detail) if detail.contains(expected_error)),
                "unexpected error for config {raw_config:?}: {error}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn config_file_metadata_errors_are_reported() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempdir().expect("temp dir");
        let config_dir = temp.path().join("coral");
        fs::create_dir_all(&config_dir).expect("config dir");
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let original_mode = fs::metadata(&config_dir)
            .expect("config dir metadata")
            .permissions()
            .mode();
        fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o000))
            .expect("hide config dir");

        let result = DatabaseConfig::load(&layout);

        fs::set_permissions(&config_dir, fs::Permissions::from_mode(original_mode))
            .expect("restore config dir");
        let error = result.expect_err("db config should report metadata failure");
        assert!(matches!(error, DbError::Io(_)), "unexpected error: {error}");
    }
}
