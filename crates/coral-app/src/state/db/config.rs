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
    database: Option<PersistedDatabaseConfig>,
}

#[derive(Debug, Deserialize)]
struct PersistedDatabaseConfig {
    #[serde(default)]
    backend: Option<PersistedDatabaseBackend>,
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    url_env: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PersistedDatabaseBackend {
    Sqlite,
    Postgres,
}

impl DatabaseConfig {
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, DbError> {
        if !layout.config_file().exists() {
            return Ok(Self::default_sqlite(layout));
        }

        let raw = std::fs::read_to_string(layout.config_file())?;
        let persisted: PersistedConfig = toml::from_str(&raw)?;
        let Some(database) = persisted.database else {
            return Ok(Self::default_sqlite(layout));
        };

        match database.backend.unwrap_or(PersistedDatabaseBackend::Sqlite) {
            PersistedDatabaseBackend::Sqlite => {
                let path = database.path.map_or_else(
                    || layout.database_file(),
                    |path| resolve_sqlite_path(layout, path),
                );
                Ok(Self::Sqlite { path })
            }
            PersistedDatabaseBackend::Postgres => {
                let url_env = database.url_env.ok_or_else(|| {
                    DbError::Config(
                        "database backend 'postgres' requires [database].url_env".to_string(),
                    )
                })?;
                Ok(Self::Postgres { url_env })
            }
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

    use super::DatabaseConfig;
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
}
