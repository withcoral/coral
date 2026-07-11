use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DbError {
    #[error("database configuration is invalid: {0}")]
    Config(String),
    /// A row was read successfully but cannot be decoded into app domain types.
    #[error("database contains corrupt data: {0}")]
    CorruptData(String),
    #[error("database file parent directory is missing for {0}")]
    MissingDatabaseParent(PathBuf),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDecode(#[from] toml::de::Error),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}

impl DbError {
    pub(crate) fn is_unique_violation(&self) -> bool {
        match self {
            Self::Sqlx(sqlx::Error::Database(error)) => error.is_unique_violation(),
            Self::Config(_)
            | Self::CorruptData(_)
            | Self::MissingDatabaseParent(_)
            | Self::Io(_)
            | Self::TomlDecode(_)
            | Self::Sqlx(_)
            | Self::Migration(_) => false,
        }
    }
}
