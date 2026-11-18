//! Errors surfaced by process bootstrap and composition.

use std::path::PathBuf;

use crate::auth::AuthServerError;

/// Why process bootstrap or composition failed.
///
/// A message that starts `failed precondition:` names something the operator
/// corrects before the next start. No variant holds a secret's value.
///
/// A variant whose message already prints its cause does not also expose it
/// as a `source`, so a reporter that prints the chain prints it once.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// A setting broke a rule that decoding alone cannot check.
    #[error("failed precondition: {message}")]
    Config {
        /// The refused setting: its key path in `config.toml`, or the name of
        /// what the launching process supplied.
        setting: String,
        /// The whole refusal. It names the setting and never its value.
        message: String,
    },
    /// `config.toml` does not decode into the server's settings.
    #[error("failed precondition: server configuration is invalid: {0}")]
    ConfigDecode(crate::ConfigDecodeError),
    /// `config.toml` is not TOML, or its `[database]` section does not
    /// decode. The state fence reads that section before the server's
    /// settings are decoded.
    #[error("configuration is invalid: {0}")]
    TomlDecode(crate::ConfigDecodeError),
    /// The auth settings are invalid, or a secret they name is unusable.
    #[error("failed precondition: {0}")]
    Auth(AuthServerError),
    /// The environment variable a setting names is not set.
    #[error("failed precondition: {setting} requires environment variable `{variable}`")]
    MissingSecret {
        /// The setting that names the variable.
        setting: String,
        /// The variable's name.
        variable: String,
    },
    /// The environment variable a setting names holds an unusable value.
    #[error("failed precondition: {message}")]
    InvalidSecret {
        /// The setting that names the variable.
        setting: String,
        /// The variable's name.
        variable: String,
        /// The whole refusal. It names the variable and never its value.
        message: String,
    },
    /// The state root or the configured database holds state this version
    /// does not start on. Nothing was changed.
    #[error("failed precondition: {0}")]
    StateRefused(StateRefusal),
    /// Another runtime holds the lock on this state root. The condition is
    /// temporary: it clears when that runtime exits.
    #[error(
        "another Coral runtime is using the state directory at '{}' (it holds the lock file '{}'); stop that runtime or wait for it to exit, then start Coral again",
        .path.display(),
        .path.join(crate::state::LOCK_FILE_NAME).display()
    )]
    StateLocked {
        /// The locked state root.
        path: PathBuf,
    },
    /// The Coral config directory could not be discovered from defaults.
    #[error("failed to determine Coral config directory")]
    MissingConfigDir,
    /// Filesystem access failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The database options cannot be used as configured.
    #[error("failed precondition: {0}")]
    DatabaseOptions(coral_db::Error),
    /// A rolling upgrade reached this Coral out of order: it skips a
    /// version, or this version is too old for its database. Nothing was
    /// changed.
    #[error("failed precondition: {0}")]
    UpgradeRefused(coral_db::Error),
    /// Durable app-state database access failed.
    #[error("database error: {0}")]
    Database(coral_db::Error),
    /// The database lacks a feature Universal Search needs, or holds a
    /// vector table of another shape.
    #[error("failed precondition: {0}")]
    SearchStorage(coral_search::StorageError),
    /// The credential key file could not be created or read.
    #[error("failed precondition: credential storage initialization failed")]
    CredentialKey(#[source] coral_credentials::Error),
    /// The local Organization Owner's login could not be provisioned.
    #[error("failed precondition: local authority initialization failed")]
    LocalAuthority(#[source] coral_workspace::Error),
    /// An outbound HTTP client could not be built.
    #[error("failed precondition: {purpose} client initialization failed")]
    HttpClient {
        /// What the client is for, as the message says it.
        purpose: &'static str,
        /// Why the client could not be built.
        #[source]
        source: reqwest::Error,
    },
    /// A module of this Coral Instance did not start.
    #[error("failed precondition: {message}")]
    EngineBoot {
        /// Which module, and what it reported.
        message: String,
    },
    /// A telemetry exporter could not be built from `[otel]`.
    #[error("telemetry initialization failed: {0}")]
    Telemetry(crate::telemetry::ExporterError),
    /// `gRPC` transport setup or shutdown failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// Background server task failed to join cleanly.
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    /// A `gRPC` connection stayed open past the shutdown grace period and
    /// the listener task was aborted.
    #[error("gRPC listener shutdown timed out; task aborted")]
    ShutdownTimedOut,
}

impl From<crate::ConfigDecodeError> for AppError {
    fn from(error: crate::ConfigDecodeError) -> Self {
        Self::TomlDecode(error)
    }
}

impl AppError {
    /// Refuses `setting` with `message`, which names it.
    pub(crate) fn config(setting: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Config {
            setting: setting.into(),
            message: message.into(),
        }
    }

    /// Refuses the value of `variable`, which `setting` names, with
    /// `message`.
    pub(crate) fn invalid_secret(setting: &str, variable: &str, message: String) -> Self {
        Self::InvalidSecret {
            setting: setting.to_owned(),
            variable: variable.to_owned(),
            message,
        }
    }

    /// A module's boot failure, with `message` as the operator reads it.
    pub(crate) fn engine_boot(message: impl Into<String>) -> Self {
        Self::EngineBoot {
            message: message.into(),
        }
    }
}

/// Why the state fence refused the state it found.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StateRefusal {
    /// The state root holds files an earlier version wrote.
    #[error(
        "pre-v1 Coral state detected at '{}'; move or remove it before starting this version of Coral",
        .path.display()
    )]
    PreV1Root {
        /// The state root.
        path: PathBuf,
    },
    /// The state root's format marker is not the current one.
    #[error(
        "the Coral state format marker at '{}' is malformed; move or remove the state root before starting this version of Coral",
        .path.display()
    )]
    MalformedRootMarker {
        /// The marker file.
        path: PathBuf,
    },
    /// The database holds objects an earlier version wrote.
    #[error(
        "pre-v1 Coral database state detected; move or remove it, or configure a fresh database before starting this version of Coral"
    )]
    PreV1Database,
    /// The database's format marker or migration ledger is not one this
    /// version writes.
    #[error(
        "the Coral database state format marker is malformed; move or remove the database before starting this version of Coral"
    )]
    MalformedDatabaseMarker,
    /// The migration ledger does not agree with this version's plan.
    #[error(
        "the Coral database has applied migrations this version of Coral does not recognize: a migration was edited, removed or reordered; move or remove the database before starting this version of Coral"
    )]
    UnknownMigrations,
    /// The `SQLite` file could not be opened to inspect it.
    #[error(
        "the configured SQLite database could not be inspected read-only; correct its permissions or availability and retry; no state was changed"
    )]
    DatabaseUninspectable,
}

impl From<StateRefusal> for AppError {
    fn from(refusal: StateRefusal) -> Self {
        Self::StateRefused(refusal)
    }
}

impl From<coral_db::Error> for AppError {
    fn from(error: coral_db::Error) -> Self {
        match error {
            coral_db::Error::Config(_) | coral_db::Error::MissingDatabaseParent(_) => {
                Self::DatabaseOptions(error)
            }
            coral_db::Error::SkippedStep { .. } | coral_db::Error::CoralTooOld { .. } => {
                Self::UpgradeRefused(error)
            }
            coral_db::Error::Io(error) => Self::Io(error),
            coral_db::Error::Sqlx(_)
            | coral_db::Error::WriterBusy
            | coral_db::Error::ReadSnapshotHold
            | coral_db::Error::Migration(_)
            | coral_db::Error::MigrationForeignKeys { .. }
            | coral_db::Error::ObservationUnavailable => Self::Database(error),
        }
    }
}

impl From<coral_search::StorageError> for AppError {
    fn from(error: coral_search::StorageError) -> Self {
        match error {
            coral_search::StorageError::Database(error) => error.into(),
            coral_search::StorageError::MissingFeature { .. }
            | coral_search::StorageError::UnexpectedVectorTable { .. }
            | coral_search::StorageError::UnexpectedSqliteVectorTables { .. } => {
                Self::SearchStorage(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppError;
    use coral_db::Error;
    use coral_search::StorageError;

    /// Whether an error is the one a case expects.
    type Expected = fn(&AppError) -> bool;

    /// The operator corrects the database options, and ends a refused
    /// upgrade by starting another Coral version; every other database
    /// failure is the database's own. Each keeps the error it came from.
    #[test]
    fn database_failures_keep_their_category_and_their_cause() {
        let cases: [(Error, Expected); 7] = [
            (Error::Config("missing backend".to_string()), |error| {
                matches!(error, AppError::DatabaseOptions(Error::Config(detail))
                    if detail == "missing backend")
            }),
            (Error::MissingDatabaseParent("coral.db".into()), |error| {
                matches!(error, AppError::DatabaseOptions(Error::MissingDatabaseParent(path))
                    if path.as_os_str() == "coral.db")
            }),
            (
                Error::Io(std::io::ErrorKind::PermissionDenied.into()),
                |error| {
                    matches!(error, AppError::Io(error)
                        if error.kind() == std::io::ErrorKind::PermissionDenied)
                },
            ),
            (Error::Sqlx(sqlx::Error::PoolClosed), |error| {
                matches!(
                    error,
                    AppError::Database(Error::Sqlx(sqlx::Error::PoolClosed))
                )
            }),
            (
                Error::Migration(sqlx::migrate::MigrateError::VersionMissing(1)),
                |error| {
                    matches!(
                        error,
                        AppError::Database(Error::Migration(
                            sqlx::migrate::MigrateError::VersionMissing(1)
                        ))
                    )
                },
            ),
            (
                Error::SkippedStep {
                    migration: 82,
                    required: 81,
                    release: "v1.9.0".to_owned(),
                },
                |error| {
                    matches!(
                        error,
                        AppError::UpgradeRefused(Error::SkippedStep {
                            migration: 82,
                            required: 81,
                            ..
                        })
                    ) && error.to_string().contains("Start Coral v1.9.0 first")
                },
            ),
            (
                Error::CoralTooOld {
                    required: 81,
                    release: "v1.9.0".to_owned(),
                },
                |error| {
                    matches!(
                        error,
                        AppError::UpgradeRefused(Error::CoralTooOld { required: 81, .. })
                    ) && error.to_string().contains(
                        "Start the newest Coral version that has run on this database. It is \
                         v1.9.0 or later",
                    )
                },
            ),
        ];
        for (cause, expected) in cases {
            let error = AppError::from(cause);
            assert!(expected(&error), "{error:?}");
        }
    }

    #[test]
    fn search_storage_failures_an_operator_can_fix_are_preconditions() {
        let cases: [(StorageError, Expected); 4] = [
            (
                StorageError::MissingFeature {
                    database: "Postgres 17.10".to_string(),
                    feature: "pg_trgm extension",
                    cause: "permission denied".to_string(),
                    remedy: "install it",
                },
                |error| {
                    matches!(
                        error,
                        AppError::SearchStorage(StorageError::MissingFeature {
                            feature: "pg_trgm extension",
                            ..
                        })
                    )
                },
            ),
            (
                StorageError::UnexpectedVectorTable {
                    column_type: Some("vector(384)".to_string()),
                    expected_dimension: 384,
                },
                |error| {
                    matches!(
                        error,
                        AppError::SearchStorage(StorageError::UnexpectedVectorTable {
                            column_type: Some(column_type),
                            expected_dimension: 384,
                        }) if column_type == "vector(384)"
                    )
                },
            ),
            (
                StorageError::UnexpectedSqliteVectorTables {
                    expected_dimension: 384,
                },
                |error| {
                    matches!(
                        error,
                        AppError::SearchStorage(StorageError::UnexpectedSqliteVectorTables {
                            expected_dimension: 384,
                        })
                    )
                },
            ),
            (StorageError::Database(Error::WriterBusy), |error| {
                matches!(error, AppError::Database(Error::WriterBusy))
            }),
        ];
        for (cause, expected) in cases {
            let error = AppError::from(cause);
            assert!(expected(&error), "{error:?}");
        }
    }
}
