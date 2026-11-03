use std::path::PathBuf;

/// A persistence-mechanics failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The committed-change notification connection is unavailable.
    #[error("database observation is unavailable")]
    ObservationUnavailable,
    /// Backend options are invalid or unsafe.
    #[error("database configuration is invalid: {0}")]
    Config(String),
    /// The `SQLite` path has no usable parent directory.
    #[error("database file parent directory is missing for {0}")]
    MissingDatabaseParent(PathBuf),
    /// Preparing the `SQLite` file failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Another writer in this process held the `SQLite` writer past the busy
    /// timeout.
    #[error("database is busy")]
    WriterBusy,
    /// A database connection or statement failed.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    /// A migration could not be validated or applied.
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// `SQLite` migrations left rows whose foreign keys match no row.
    #[error("migrations left foreign keys that match no row in: {tables}")]
    MigrationForeignKeys {
        /// The tables that hold those rows, comma-separated.
        tables: String,
    },
}

impl Error {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ObservationUnavailable { .. } => "observation_unavailable",
            Self::Config { .. } => "config",
            Self::MissingDatabaseParent { .. } => "missing_database_parent",
            Self::Io { .. } => "io",
            Self::WriterBusy { .. } => "writer_busy",
            Self::Sqlx { .. } => "sqlx",
            Self::Migration { .. } => "migration",
            Self::MigrationForeignKeys { .. } => "migration_foreign_keys",
        }
    }

    /// Reports whether a database constraint rejected a duplicate identity.
    #[must_use]
    pub fn is_unique_violation(&self) -> bool {
        matches!(self, Self::Sqlx(error) if sqlx_error_is_unique_violation(error))
    }

    /// Reports whether a writer lost a lock or serialization race.
    #[must_use]
    pub fn is_serialization_conflict(&self) -> bool {
        match self {
            Self::WriterBusy => true,
            Self::Sqlx(error) => sqlx_error_is_serialization_conflict(error),
            _ => false,
        }
    }

    /// Reports whether the backend gave no answer to a write: the connection
    /// or pool failed, or a lock or serialization race left the statement
    /// unapplied. The write may be retried; whether it took effect is for the
    /// caller to read back. Every other error is the backend's definite
    /// answer, a constraint violation included.
    #[must_use]
    pub fn is_unanswered(&self) -> bool {
        match self {
            Self::WriterBusy => true,
            Self::Sqlx(error) => match error {
                sqlx::Error::Database(_) => sqlx_error_is_serialization_conflict(error),
                sqlx::Error::Io(_)
                | sqlx::Error::Tls(_)
                | sqlx::Error::Protocol(_)
                | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::WorkerCrashed => true,
                _ => false,
            },
            _ => false,
        }
    }
}

/// Reports a timed-out wait for the `SQLite` writer as [`Error::WriterBusy`].
pub(crate) fn writer_busy(error: sqlx::Error) -> Error {
    match error {
        sqlx::Error::PoolTimedOut => Error::WriterBusy,
        error => Error::Sqlx(error),
    }
}

/// Reports whether a driver error rejected a duplicate identity.
#[must_use]
pub fn sqlx_error_is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.is_unique_violation())
}

/// Reports whether a driver error lost a backend lock or serialization race.
#[must_use]
pub fn sqlx_error_is_serialization_conflict(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.code().is_some_and(|code| is_conflict_code(&code)))
}

fn is_conflict_code(code: &str) -> bool {
    const SQLITE_BUSY: u32 = 5;
    const SQLITE_LOCKED: u32 = 6;

    if code.len() == 5 {
        return matches!(code, "40001" | "40P01");
    }
    code.parse::<u32>()
        .is_ok_and(|code| matches!(code & 0xFF, SQLITE_BUSY | SQLITE_LOCKED))
}

#[cfg(test)]
mod tests {
    use super::{Error, is_conflict_code};

    #[test]
    fn an_unanswered_write_is_a_lost_connection_or_a_race_never_a_rejection() {
        assert!(Error::WriterBusy.is_unanswered());
        assert!(Error::Sqlx(sqlx::Error::PoolTimedOut).is_unanswered());
        assert!(Error::Sqlx(sqlx::Error::PoolClosed).is_unanswered());
        assert!(Error::Sqlx(sqlx::Error::WorkerCrashed).is_unanswered());
        assert!(
            Error::Sqlx(sqlx::Error::Io(std::io::Error::other("connection reset"))).is_unanswered()
        );
        assert!(!Error::Sqlx(sqlx::Error::RowNotFound).is_unanswered());
        assert!(!Error::Sqlx(sqlx::Error::ColumnNotFound("x".into())).is_unanswered());
        assert!(!Error::ObservationUnavailable.is_unanswered());
        assert!(!Error::Config("bad".into()).is_unanswered());
    }

    /// A busy writer the backend reports is unanswered; a constraint it
    /// enforces is a definite rejection.
    #[tokio::test]
    async fn a_busy_writer_is_unanswered_and_a_constraint_violation_is_not() {
        use sqlx::Connection as _;
        let directory = tempfile::tempdir().expect("temp dir");
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(directory.path().join("errors.sqlite"))
            .create_if_missing(true)
            .busy_timeout(std::time::Duration::ZERO);
        let mut first = sqlx::SqliteConnection::connect_with(&options)
            .await
            .expect("first connection");
        let mut second = sqlx::SqliteConnection::connect_with(&options)
            .await
            .expect("second connection");
        sqlx::raw_sql("CREATE TABLE rows (id INTEGER PRIMARY KEY); INSERT INTO rows VALUES (1)")
            .execute(&mut first)
            .await
            .expect("arrange");

        let duplicate = sqlx::raw_sql("INSERT INTO rows VALUES (1)")
            .execute(&mut first)
            .await
            .expect_err("a duplicate key is rejected");
        let duplicate = Error::Sqlx(duplicate);
        assert!(duplicate.is_unique_violation());
        assert!(!duplicate.is_unanswered(), "{duplicate}");

        sqlx::raw_sql("BEGIN IMMEDIATE")
            .execute(&mut first)
            .await
            .expect("hold the writer");
        let busy = sqlx::raw_sql("BEGIN IMMEDIATE")
            .execute(&mut second)
            .await
            .expect_err("the second writer is busy");
        let busy = Error::Sqlx(busy);
        assert!(busy.is_serialization_conflict());
        assert!(busy.is_unanswered(), "{busy}");
    }

    #[test]
    fn conflict_codes_cover_both_backends() {
        for code in ["40001", "40P01", "5", "6", "517", "261", "262"] {
            assert!(is_conflict_code(code), "{code} must be a conflict");
        }
        for code in ["23505", "57014", "40003", "1", "", "not-a-code"] {
            assert!(!is_conflict_code(code), "{code} must not be a conflict");
        }
    }
}
