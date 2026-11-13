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
    /// or pool failed, the backend ended or refused the session, or a lock or
    /// serialization race left the statement unapplied. The write may be
    /// retried; whether it took effect is for the caller to read back. Every
    /// other error is the backend's definite answer, a constraint violation
    /// included.
    #[must_use]
    pub fn is_unanswered(&self) -> bool {
        match self {
            Self::WriterBusy => true,
            Self::Sqlx(error) => match error {
                sqlx::Error::Database(error) => {
                    error.code().is_some_and(|code| is_unanswered_code(&code))
                }
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

/// Reports whether a backend error code leaves a write unanswered.
///
/// Postgres sends every error it can still deliver as a database error, a
/// session it is ending included, so only the SQLSTATE tells an ended session
/// from a rejected statement. A conflict is one kind of unanswered write; the
/// rest are Postgres codes, since `SQLite` runs in this process.
fn is_unanswered_code(code: &str) -> bool {
    is_conflict_code(code)
        || matches!(
            code,
            // The session failed or was never established. `08P01` is left
            // out: it is the server's answer to a message it could not accept.
            "08000" | "08001" | "08003" | "08004" | "08006" | "08007"
            // The server does not know whether the statement completed.
            | "40003"
            // No connection slot was free, so no statement was read.
            | "53300"
            // The server ended or refused the session: a shutdown, a crash of
            // another backend, a server that is starting up or shutting down,
            // an idle session's timeout.
            | "57P01" | "57P02" | "57P03" | "57P05"
        )
}

#[cfg(test)]
mod tests {
    use super::{Error, is_conflict_code, is_unanswered_code};

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

    /// A conflict, an ended or refused session and an unknown completion are
    /// unanswered; a statement the server ran and refused is its definite
    /// answer.
    #[test]
    fn unanswered_codes_add_ended_and_refused_sessions_to_the_conflicts() {
        assert!(is_unanswered_code("40P01"), "a conflict is unanswered");
        assert!(is_unanswered_code("40003"), "an unknown completion");
        let sessions = [
            "08000", "08001", "08003", "08004", "08006", "08007", "53300", "57P01", "57P02",
            "57P03", "57P05",
        ];
        for code in sessions {
            assert!(is_unanswered_code(code), "{code} must be unanswered");
            assert!(!is_conflict_code(code), "{code} must not be a conflict");
        }
        // A constraint, a syntax error, an aborted transaction; a protocol
        // violation, a rollback for a constraint or for no named cause; a
        // statement that ran out of a resource; a canceled statement, a
        // dropped database; `SQLite` errors other than a busy or locked writer.
        let definite = [
            "23505", "23503", "42601", "25P02", "08P01", "40000", "40002", "53000", "53100",
            "53200", "53400", "57000", "57014", "57P04", "1", "19", "2067", "",
        ];
        for code in definite {
            assert!(!is_unanswered_code(code), "{code} must be definite");
        }
    }
}
