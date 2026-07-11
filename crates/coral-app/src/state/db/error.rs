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
    #[error("retryable database transaction conflict: {0}")]
    RetryableTransactionConflict(sqlx::Error),
    #[error(transparent)]
    Sqlx(sqlx::Error),
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
            | Self::RetryableTransactionConflict(_)
            | Self::Sqlx(_)
            | Self::Migration(_) => false,
        }
    }
}

impl From<sqlx::Error> for DbError {
    fn from(error: sqlx::Error) -> Self {
        if is_retryable_transaction_conflict(&error) {
            Self::RetryableTransactionConflict(error)
        } else {
            Self::Sqlx(error)
        }
    }
}

fn is_retryable_transaction_conflict(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };

    if let Some(postgres) = database.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
        return is_retryable_postgres_code(postgres.code());
    }
    if database
        .try_downcast_ref::<sqlx::sqlite::SqliteError>()
        .is_some()
    {
        return database
            .code()
            .and_then(|code| code.parse::<i32>().ok())
            .is_some_and(is_retryable_sqlite_code);
    }
    false
}

fn is_retryable_postgres_code(code: &str) -> bool {
    matches!(code, "40001" | "40P01")
}

fn is_retryable_sqlite_code(extended_code: i32) -> bool {
    matches!(extended_code & 0xff, 5 | 6)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_postgres_serialization_and_deadlock_conflicts() {
        assert!(is_retryable_postgres_code("40001"));
        assert!(is_retryable_postgres_code("40P01"));
        assert!(!is_retryable_postgres_code("23505"));
    }

    #[test]
    fn classifies_sqlite_busy_and_locked_extended_codes_by_primary_code() {
        for code in [5, 6, 5 | (2 << 8), 6 | (3 << 8)] {
            assert!(is_retryable_sqlite_code(code), "code {code}");
        }
        assert!(!is_retryable_sqlite_code(19));
    }
}
