use sqlx::{PgPool, SqlitePool};

/// The configured database system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseSystem {
    /// An embedded `SQLite` database for one local Coral process.
    Sqlite,
    /// A remote Postgres database that can serve several Coral workers.
    Postgres,
}

pub(crate) enum DatabaseBackend {
    /// Reads share a pool that cannot write. Writes queue in arrival order for
    /// one connection: `SQLite` admits one writer, and its busy handler polls
    /// without fairness, so it only arbitrates writers in other processes.
    /// The pool releases that connection only after it finishes any commit or
    /// rollback, so a dropped or cancelled transaction cannot overlap the next
    /// writer. A read-only handle names its one read-only pool twice.
    Sqlite {
        reads: SqlitePool,
        writes: SqlitePool,
        /// Read-only handles name the same pool twice; observe it only once.
        shared_pool: bool,
    },
    Postgres(PgPool),
}
