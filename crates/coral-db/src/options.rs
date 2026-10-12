use std::path::PathBuf;

/// Fully resolved database connection options.
///
/// Process bootstrap must resolve environment variables before it constructs
/// this value. It intentionally does not implement `Debug` because a Postgres
/// URL can contain credentials.
#[derive(Clone, PartialEq, Eq)]
pub enum DatabaseOptions {
    /// An embedded `SQLite` database at one local path.
    Sqlite {
        /// Local database file path.
        path: PathBuf,
    },
    /// A Postgres connection URL.
    Postgres {
        /// Fully resolved URL, which can contain credentials.
        url: String,
    },
}
