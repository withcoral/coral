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

    /// Reports whether a writer lost a lock or serialization race rather than
    /// hitting a fault of its own.
    ///
    /// This is a classifier on the driver error, not an error kind: the same
    /// `Sqlx` failure is a transient loss for a writer that can be retried or
    /// reported as a refusal, and a hard failure for one that cannot. Callers
    /// that contend for the same rows use it to keep a lost race from reading
    /// as a defect.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the concurrency contracts classify lost races before the RPCs that surface them"
        )
    )]
    pub(crate) fn is_serialization_conflict(&self) -> bool {
        match self {
            Self::Sqlx(sqlx::Error::Database(error)) => {
                error.code().is_some_and(|code| is_conflict_code(&code))
            }
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

/// Recognizes the contention codes of both backends.
///
/// The two code spaces never overlap: a Postgres SQLSTATE is always five
/// characters, while `SQLite` reports a shorter decimal result code. `SQLite`
/// carries the primary code (`SQLITE_BUSY`, `SQLITE_LOCKED`) in the low byte
/// of an extended code, so the sub-code is masked off before comparing.
fn is_conflict_code(code: &str) -> bool {
    const SQLITE_BUSY: u32 = 5;
    const SQLITE_LOCKED: u32 = 6;

    if code.len() == 5 {
        // 40001 is a serialization failure and 40P01 a detected deadlock.
        return matches!(code, "40001" | "40P01");
    }
    code.parse::<u32>()
        .is_ok_and(|code| matches!(code & 0xFF, SQLITE_BUSY | SQLITE_LOCKED))
}

#[cfg(test)]
mod tests {
    use super::is_conflict_code;

    #[test]
    fn conflict_codes_span_both_backends_and_nothing_else() {
        for code in ["40001", "40P01", "5", "6", "517", "261", "262"] {
            assert!(is_conflict_code(code), "'{code}' must read as a conflict");
        }
        // A unique violation, a cancelled query, and a plain `SQLITE_ERROR`
        // are all failures the caller owns, not races it lost.
        for code in ["23505", "57014", "40003", "1", "", "not-a-code"] {
            assert!(
                !is_conflict_code(code),
                "'{code}' must not read as a conflict"
            );
        }
    }
}
