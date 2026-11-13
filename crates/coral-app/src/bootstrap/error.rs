//! Errors surfaced by process bootstrap and composition.

/// Errors surfaced by the local application layer.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Caller-supplied input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// Startup requires additional operator configuration.
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// Filesystem access failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// `config.toml` decoding failed.
    #[error("configuration is invalid: {0}")]
    TomlDecode(#[from] crate::ConfigDecodeError),
    /// `JSON` encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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
    /// Durable app-state database access failed.
    #[error("database error: {0}")]
    Database(String),
    /// The Coral config directory could not be discovered from defaults.
    #[error("failed to determine Coral config directory")]
    MissingConfigDir,
    /// Another runtime holds the lock on this state root. The condition is
    /// temporary: it clears when that runtime exits.
    #[error(
        "another Coral runtime is using the state directory at '{}' (it holds the lock file '{}'); stop that runtime or wait for it to exit, then start Coral again",
        .path.display(),
        .path.join(crate::state::LOCK_FILE_NAME).display()
    )]
    StateLocked {
        /// The locked state root.
        path: std::path::PathBuf,
    },
}

impl From<coral_db::Error> for AppError {
    fn from(error: coral_db::Error) -> Self {
        match error {
            coral_db::Error::Config(_) | coral_db::Error::MissingDatabaseParent(_) => {
                Self::FailedPrecondition(error.to_string())
            }
            coral_db::Error::Io(error) => Self::Io(error),
            coral_db::Error::Sqlx(_)
            | coral_db::Error::WriterBusy
            | coral_db::Error::Migration(_)
            | coral_db::Error::MigrationForeignKeys { .. }
            | coral_db::Error::ObservationUnavailable => Self::Database(error.to_string()),
        }
    }
}

impl From<coral_search::StorageError> for AppError {
    fn from(error: coral_search::StorageError) -> Self {
        match error {
            coral_search::StorageError::Database(error) => error.into(),
            coral_search::StorageError::MissingFeature { .. }
            | coral_search::StorageError::UnexpectedVectorTable { .. } => {
                Self::FailedPrecondition(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppError;
    use coral_db::Error;

    #[test]
    fn database_configuration_errors_remain_actionable() {
        let error = AppError::from(Error::Config("missing backend".to_string()));
        assert!(matches!(error, AppError::FailedPrecondition(_)));
        assert!(error.to_string().contains("missing backend"));
    }

    #[test]
    fn database_failures_keep_their_error_category() {
        let missing_parent = AppError::from(Error::MissingDatabaseParent("coral.db".into()));
        assert!(matches!(missing_parent, AppError::FailedPrecondition(_)));
        assert!(missing_parent.to_string().contains("coral.db"));

        let io = AppError::from(Error::Io(std::io::ErrorKind::PermissionDenied.into()));
        assert!(
            matches!(io, AppError::Io(ref error) if error.kind() == std::io::ErrorKind::PermissionDenied)
        );

        let query = AppError::from(Error::Sqlx(sqlx::Error::PoolClosed));
        assert!(matches!(query, AppError::Database(_)));

        let migration = AppError::from(Error::Migration(
            sqlx::migrate::MigrateError::VersionMissing(1),
        ));
        assert!(matches!(migration, AppError::Database(_)));
    }

    #[test]
    fn search_storage_failures_an_operator_can_fix_are_preconditions() {
        for (error, named) in [
            (
                coral_search::StorageError::MissingFeature {
                    database: "Postgres 17.10".to_string(),
                    feature: "pg_trgm extension",
                    cause: "permission denied".to_string(),
                    remedy: "install it",
                },
                "pg_trgm extension",
            ),
            (
                coral_search::StorageError::UnexpectedVectorTable {
                    column_type: Some("vector(384)".to_string()),
                    expected_dimension: 384,
                },
                "found vector(384)",
            ),
        ] {
            let error = AppError::from(error);
            assert!(matches!(error, AppError::FailedPrecondition(_)), "{error}");
            assert!(error.to_string().contains(named), "{error}");
        }
    }
}
