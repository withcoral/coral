//! Application database configuration, before bootstrap resolves environment references.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DatabaseConfig {
    Sqlite { path: PathBuf },
    Postgres { url_env: String },
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
    fn into_config(self) -> Result<PersistedDatabaseConfig, AppError> {
        if let Some(field) = self.extra.keys().next() {
            return Err(invalid_config(&format!(
                "unsupported [database].{field} configuration key"
            )));
        }

        let backend = self.backend.ok_or_else(|| {
            invalid_config("[database].backend is required when [database] is present")
        })?;

        match backend {
            PersistedDatabaseBackend::Sqlite => {
                if self.url_env.is_some() {
                    return Err(invalid_config(
                        "database backend 'sqlite' does not support [database].url_env",
                    ));
                }
                Ok(PersistedDatabaseConfig::Sqlite { path: self.path })
            }
            PersistedDatabaseBackend::Postgres => {
                if self.path.is_some() {
                    return Err(invalid_config(
                        "database backend 'postgres' does not support [database].path",
                    ));
                }
                let url_env = self.url_env.ok_or_else(|| {
                    invalid_config("database backend 'postgres' requires [database].url_env")
                })?;
                Ok(PersistedDatabaseConfig::Postgres { url_env })
            }
        }
    }
}

impl DatabaseConfig {
    pub(crate) fn from_toml(raw: &str, layout: &AppStateLayout) -> Result<Self, AppError> {
        let persisted: PersistedConfig = crate::config_toml::decode(raw)?;
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

fn invalid_config(detail: &str) -> AppError {
    AppError::FailedPrecondition(format!("database configuration is invalid: {detail}"))
}

fn resolve_sqlite_path(layout: &AppStateLayout, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    layout.config_dir().join(path)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{AppError, DatabaseConfig};
    use crate::state::AppStateLayout;

    #[test]
    fn defaults_to_sqlite_under_app_state() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");

        let config = DatabaseConfig::from_toml("", &layout).expect("db config");

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
        let config = DatabaseConfig::from_toml(
            "[database]\nbackend = \"sqlite\"\npath = \"state/custom.db\"\n",
            &layout,
        )
        .expect("db config");

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
        let config = DatabaseConfig::from_toml(
            "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
            &layout,
        )
        .expect("db config");

        assert_eq!(
            config,
            DatabaseConfig::Postgres {
                url_env: "CORAL_DATABASE_URL".to_string()
            }
        );
    }

    #[test]
    fn malformed_toml_is_reported_as_a_decode_error() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let error =
            DatabaseConfig::from_toml("[database", &layout).expect_err("reject malformed TOML");

        assert!(matches!(error, AppError::TomlDecode(_)));
    }

    #[test]
    fn database_section_requires_explicit_backend() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let error =
            DatabaseConfig::from_toml("[database]\nurl_env = \"CORAL_DATABASE_URL\"\n", &layout)
                .expect_err("db config should reject backend");

        assert!(
            matches!(error, AppError::FailedPrecondition(ref detail) if detail.contains("[database].backend is required")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn database_backend_rejects_unsupported_fields() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");

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
            let error = DatabaseConfig::from_toml(raw_config, &layout)
                .expect_err("db config should reject field");

            assert!(
                matches!(error, AppError::FailedPrecondition(ref detail) if detail.contains(expected_error)),
                "unexpected error for config {raw_config:?}: {error}"
            );
        }
    }
}
