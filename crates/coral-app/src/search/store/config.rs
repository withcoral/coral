//! `[search]` configuration: which storage backend serves Universal Search.
//!
//! ```toml
//! [search]
//! backend = "postgres"   # "sqlite" | "postgres"; default: follow [database].backend
//! ```
//!
//! The section mirrors `[database]` (`state::db::config`) and carries no path
//! or URL of its own: `sqlite` derives per-Workspace files from the app-state
//! layout, and `postgres` reuses the resolved `[database]` URL with a small
//! pool of its own. The knob stays independently settable so a deployment can
//! keep app state on Postgres while search stays on `SQLite` during rollout.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::state::AppStateLayout;
use crate::state::db::ResolvedDatabaseConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SearchBackendKind {
    Sqlite,
    Postgres,
}

impl SearchBackendKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

/// The persisted `[search]` section. `backend: None` follows `[database]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchConfig {
    backend: Option<SearchBackendKind>,
}

/// The effective search backend after resolving against the database config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedSearchConfig {
    Sqlite,
    Postgres { url: String },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SearchConfigError {
    #[error("search configuration is invalid: {0}")]
    Config(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDecode(#[from] toml::de::Error),
}

#[derive(Debug, Deserialize)]
struct PersistedConfig {
    #[serde(default)]
    search: Option<RawPersistedSearchConfig>,
}

#[derive(Debug, Deserialize)]
struct RawPersistedSearchConfig {
    #[serde(default)]
    backend: Option<SearchBackendKind>,
    #[serde(flatten)]
    extra: BTreeMap<String, toml::Value>,
}

impl RawPersistedSearchConfig {
    fn into_backend(self) -> Result<SearchBackendKind, SearchConfigError> {
        if let Some(field) = self.extra.keys().next() {
            return Err(SearchConfigError::Config(format!(
                "unsupported [search].{field} configuration key"
            )));
        }
        self.backend.ok_or_else(|| {
            SearchConfigError::Config(
                "[search].backend is required when [search] is present".to_string(),
            )
        })
    }
}

impl SearchConfig {
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, SearchConfigError> {
        if !layout.config_file().try_exists()? {
            return Ok(Self { backend: None });
        }

        let raw = std::fs::read_to_string(layout.config_file())?;
        let persisted: PersistedConfig = toml::from_str(&raw)?;
        let Some(search) = persisted.search else {
            return Ok(Self { backend: None });
        };
        Ok(Self {
            backend: Some(search.into_backend()?),
        })
    }

    /// Resolves the effective backend. Postgres search reuses the database
    /// connection, so it needs the database to be Postgres too.
    pub(crate) fn resolve(
        &self,
        database: &ResolvedDatabaseConfig,
    ) -> Result<ResolvedSearchConfig, SearchConfigError> {
        let backend = self.backend.unwrap_or(match database {
            ResolvedDatabaseConfig::Sqlite { .. } => SearchBackendKind::Sqlite,
            ResolvedDatabaseConfig::Postgres { .. } => SearchBackendKind::Postgres,
        });
        match (backend, database) {
            (SearchBackendKind::Sqlite, _) => Ok(ResolvedSearchConfig::Sqlite),
            (SearchBackendKind::Postgres, ResolvedDatabaseConfig::Postgres { url }) => {
                Ok(ResolvedSearchConfig::Postgres { url: url.clone() })
            }
            (SearchBackendKind::Postgres, ResolvedDatabaseConfig::Sqlite { .. }) => {
                Err(SearchConfigError::Config(format!(
                    "search backend '{}' reuses the [database] connection and requires [database].backend = \"postgres\"",
                    backend.as_str()
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{ResolvedSearchConfig, SearchConfig, SearchConfigError};
    use crate::state::AppStateLayout;
    use crate::state::db::ResolvedDatabaseConfig;

    fn layout_with_config(raw_config: Option<&str>) -> (tempfile::TempDir, AppStateLayout) {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        if let Some(raw_config) = raw_config {
            fs::create_dir_all(layout.config_dir()).expect("config dir");
            fs::write(layout.config_file(), raw_config).expect("config");
        }
        (temp, layout)
    }

    fn sqlite_database() -> ResolvedDatabaseConfig {
        ResolvedDatabaseConfig::Sqlite {
            path: PathBuf::from("coral.db"),
        }
    }

    fn postgres_database() -> ResolvedDatabaseConfig {
        ResolvedDatabaseConfig::Postgres {
            url: "postgres://coral@127.0.0.1/coral".to_string(),
        }
    }

    #[test]
    fn follows_the_database_backend_when_unset() {
        for raw_config in [None, Some("[database]\nbackend = \"sqlite\"\n")] {
            let (_temp, layout) = layout_with_config(raw_config);
            let config = SearchConfig::load(&layout).expect("search config");

            assert_eq!(config, SearchConfig { backend: None });
            assert_eq!(
                config.resolve(&sqlite_database()).expect("resolve sqlite"),
                ResolvedSearchConfig::Sqlite
            );
            assert_eq!(
                config
                    .resolve(&postgres_database())
                    .expect("resolve postgres"),
                ResolvedSearchConfig::Postgres {
                    url: "postgres://coral@127.0.0.1/coral".to_string()
                }
            );
        }
    }

    #[test]
    fn explicit_sqlite_keeps_search_local_beside_a_postgres_database() {
        let (_temp, layout) = layout_with_config(Some("[search]\nbackend = \"sqlite\"\n"));

        let config = SearchConfig::load(&layout).expect("search config");

        assert_eq!(
            config.resolve(&postgres_database()).expect("resolve"),
            ResolvedSearchConfig::Sqlite
        );
    }

    #[test]
    fn explicit_postgres_reuses_the_database_url() {
        let (_temp, layout) = layout_with_config(Some("[search]\nbackend = \"postgres\"\n"));

        let config = SearchConfig::load(&layout).expect("search config");

        assert_eq!(
            config.resolve(&postgres_database()).expect("resolve"),
            ResolvedSearchConfig::Postgres {
                url: "postgres://coral@127.0.0.1/coral".to_string()
            }
        );
    }

    #[test]
    fn explicit_postgres_requires_a_postgres_database() {
        let (_temp, layout) = layout_with_config(Some("[search]\nbackend = \"postgres\"\n"));

        let error = SearchConfig::load(&layout)
            .expect("search config")
            .resolve(&sqlite_database())
            .expect_err("postgres search over a sqlite database must be rejected");

        assert!(
            matches!(error, SearchConfigError::Config(ref detail) if detail.contains("requires [database].backend = \"postgres\"")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn search_section_requires_explicit_backend() {
        let (_temp, layout) = layout_with_config(Some("[search]\n"));

        let error = SearchConfig::load(&layout).expect_err("search config should reject backend");

        assert!(
            matches!(error, SearchConfigError::Config(ref detail) if detail.contains("[search].backend is required")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn search_section_rejects_unsupported_fields() {
        for (raw_config, expected_error) in [
            (
                "[search]\nbackend = \"sqlite\"\npath = \"search.sqlite3\"\n",
                "unsupported [search].path configuration key",
            ),
            (
                "[search]\nbackend = \"postgres\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
                "unsupported [search].url_env configuration key",
            ),
        ] {
            let (_temp, layout) = layout_with_config(Some(raw_config));

            let error = SearchConfig::load(&layout).expect_err("search config should reject field");

            assert!(
                matches!(error, SearchConfigError::Config(ref detail) if detail.contains(expected_error)),
                "unexpected error for config {raw_config:?}: {error}"
            );
        }
    }

    #[test]
    fn unknown_backend_values_are_rejected() {
        let (_temp, layout) = layout_with_config(Some("[search]\nbackend = \"paradedb\"\n"));

        let error = SearchConfig::load(&layout).expect_err("unknown backend must be rejected");

        assert!(
            matches!(error, SearchConfigError::TomlDecode(_)),
            "unexpected error: {error}"
        );
    }
}
