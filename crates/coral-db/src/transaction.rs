use std::time::Instant;

use coral_telemetry::metric::{self, DbSystem, TransactionOutcome};
use sea_query::SelectStatement;
use sea_query_sqlx::SqlxBinder;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Postgres, Sqlite};

use crate::backend::DatabaseBackend;
use crate::{Database, DatabaseSystem, Error};

/// One short database transaction.
///
/// Dropping this value rolls the transaction back. Domain Modules own when to
/// commit and which repository facets join the transaction.
pub struct Transaction<'a> {
    token: TransactionToken,
    backend: TransactionBackend,
    changes: &'a crate::changes::Changes,
    invalidated: bool,
    measure: Measure<'a>,
}

/// Records how long one transaction was open and how it ended. A
/// transaction dropped without a commit or a rollback rolled back.
struct Measure<'a> {
    pools: &'a DatabaseBackend,
    started: Instant,
    outcome: Option<TransactionOutcome>,
}

impl<'a> Measure<'a> {
    fn start(pools: &'a DatabaseBackend) -> Self {
        Self {
            pools,
            started: Instant::now(),
            outcome: None,
        }
    }
}

impl Drop for Measure<'_> {
    fn drop(&mut self) {
        metric::transaction_ended(
            db_system(self.pools),
            self.outcome.unwrap_or(TransactionOutcome::RolledBack),
            self.started.elapsed(),
        );
    }
}

fn db_system(pools: &DatabaseBackend) -> DbSystem {
    match pools {
        DatabaseBackend::Sqlite { .. } => DbSystem::Sqlite,
        DatabaseBackend::Postgres(_) => DbSystem::Postgresql,
    }
}

/// How many of a pool's `size` connections are not `idle`.
fn in_use(size: u32, idle: usize) -> u64 {
    u64::from(size).saturating_sub(u64::try_from(idle).unwrap_or(u64::MAX))
}

impl Database {
    /// Observes the pools' connections in use from the first transaction
    /// that begins under the metrics layer: the database connects before
    /// telemetry starts.
    fn observe_pools(&self) {
        if self.access.pool_metric.get().is_some() {
            return;
        }
        let observed = match &self.backend {
            DatabaseBackend::Sqlite { reads, writes } => {
                let (reads, writes) = (reads.clone(), writes.clone());
                metric::observe_pool_in_use(DbSystem::Sqlite, move || {
                    in_use(reads.size(), reads.num_idle())
                        .saturating_add(in_use(writes.size(), writes.num_idle()))
                })
            }
            DatabaseBackend::Postgres(pool) => {
                let pool = pool.clone();
                metric::observe_pool_in_use(DbSystem::Postgresql, move || {
                    in_use(pool.size(), pool.num_idle())
                })
            }
        };
        if let Some(observed) = observed {
            // Another transaction may have set it first; this one then drops.
            self.access.pool_metric.get_or_init(|| observed);
        }
    }
}

/// Counts a transaction on `pools` that failed to begin or commit.
fn record_failure(pools: &DatabaseBackend, error: &Error) {
    metric::transaction_failed(db_system(pools), error.code());
}

enum TransactionBackend {
    Sqlite(sqlx::Transaction<'static, Sqlite>),
    Postgres(sqlx::Transaction<'static, Postgres>),
}

impl Database {
    /// Begins a short read-write transaction.
    ///
    /// `SQLite` writers wait in arrival order for the one writer connection,
    /// then take the write lock at `BEGIN`. A deferred transaction that reads
    /// first cannot wait for the lock when it later writes: `SQLite` skips the
    /// busy handler for that upgrade and reports `database is locked`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriterBusy`] when another `SQLite` writer in this
    /// process holds the writer connection past the busy timeout, or an error
    /// when no pooled connection is available or the backend rejects
    /// transaction creation.
    pub async fn begin(&self) -> Result<Transaction<'_>, Error> {
        self.observe_pools();
        let acquisition =
            metric::transaction_acquiring(db_system(&self.backend), metric::TransactionMode::Write);
        let backend = match &self.backend {
            DatabaseBackend::Sqlite { writes, .. } => writes
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map(TransactionBackend::Sqlite)
                .map_err(crate::error::writer_busy),
            DatabaseBackend::Postgres(pool) => begin_postgres(pool, None)
                .await
                .map(TransactionBackend::Postgres),
        }
        .inspect_err(|error| record_failure(&self.backend, error));
        acquisition.finish(if backend.is_ok() {
            metric::AcquisitionOutcome::Ready
        } else {
            metric::AcquisitionOutcome::Failed
        });
        let backend = backend?;
        Ok(Transaction {
            backend,
            changes: &self.access.changes,
            invalidated: false,
            token: TransactionToken(std::sync::Arc::new(())),
            measure: Measure::start(&self.backend),
        })
    }

    /// Begins a transaction with one coherent multi-query read snapshot.
    ///
    /// Use it for work that only reads: unlike [`Self::begin`], it takes no
    /// write lock, and both backends reject writes. Postgres uses a
    /// repeatable-read, read-only transaction. `SQLite` uses a deferred
    /// transaction on a query-only connection, which fixes its snapshot on the
    /// first read.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend rejects transaction creation.
    pub async fn begin_read_snapshot(&self) -> Result<Transaction<'_>, Error> {
        self.observe_pools();
        let acquisition = metric::transaction_acquiring(
            db_system(&self.backend),
            metric::TransactionMode::ReadSnapshot,
        );
        let backend = match &self.backend {
            DatabaseBackend::Sqlite { reads, .. } => reads
                .begin()
                .await
                .map(TransactionBackend::Sqlite)
                .map_err(Error::from),
            DatabaseBackend::Postgres(pool) => begin_postgres(
                pool,
                Some("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"),
            )
            .await
            .map(TransactionBackend::Postgres),
        }
        .inspect_err(|error| record_failure(&self.backend, error));
        acquisition.finish(if backend.is_ok() {
            metric::AcquisitionOutcome::Ready
        } else {
            metric::AcquisitionOutcome::Failed
        });
        let backend = backend?;
        Ok(Transaction {
            backend,
            changes: &self.access.changes,
            invalidated: false,
            token: TransactionToken(std::sync::Arc::new(())),
            measure: Measure::start(&self.backend),
        })
    }
}

/// Begins a Postgres transaction that a canceled caller cannot leave open.
///
/// sqlx counts the transaction only once `BEGIN` replies. A caller canceled
/// before then returns its connection to the pool inside the transaction, and
/// the next `BEGIN` there joins it: a writer would inherit a read snapshot's
/// `READ ONLY`. A detached task finishes `BEGIN`, so dropping its result rolls
/// the transaction back.
async fn begin_postgres(
    pool: &sqlx::PgPool,
    statement: Option<&'static str>,
) -> Result<sqlx::Transaction<'static, Postgres>, Error> {
    let pool = pool.clone();
    let transaction = tokio::spawn(async move {
        match statement {
            Some(statement) => pool.begin_with(statement).await,
            None => pool.begin().await,
        }
    })
    .await
    .map_err(|_join| sqlx::Error::WorkerCrashed)??;
    Ok(transaction)
}

/// How a read inside a write transaction holds the rows it reads.
///
/// A decision that reads shared fact rows (a Workspace, a Provider, a policy
/// rule, an Account) holds them [`Self::Shared`] so that concurrent decisions
/// read side by side while a writer's `UPDATE` of those rows waits until the
/// decisions have committed: a change that commits first is seen, one that
/// commits later does not undo a recorded decision. On Postgres this is
/// `SELECT ... FOR SHARE`. On `SQLite` a write transaction takes the one
/// writer lock at `BEGIN IMMEDIATE`, which already excludes every writer, so
/// the read runs as it is. Use the exclusive no-op `UPDATE` hold only for a
/// row the transaction goes on to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLock {
    /// A plain read, as in a read snapshot.
    None,
    /// Hold the matched rows shared for the rest of the transaction. Only in
    /// a read-write transaction: Postgres refuses `FOR SHARE` in a read-only
    /// one.
    Shared,
}

/// Opaque identity of one transaction, usable to bind prepared domain evidence.
#[derive(Clone)]
pub struct TransactionToken(std::sync::Arc<()>);

impl TransactionToken {
    /// Checks that evidence is being consumed in the transaction that prepared it.
    #[must_use]
    pub fn matches(&self, transaction: &Transaction<'_>) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &transaction.token.0)
    }
}

impl Transaction<'_> {
    /// Returns an opaque token for transaction-bound domain evidence.
    #[must_use]
    pub fn token(&self) -> TransactionToken {
        self.token.clone()
    }

    /// Identifies this transaction's Adapter for backend-specific queries or locking.
    #[must_use]
    pub const fn system(&self) -> DatabaseSystem {
        match self.backend {
            TransactionBackend::Sqlite(_) => DatabaseSystem::Sqlite,
            TransactionBackend::Postgres(_) => DatabaseSystem::Postgres,
        }
    }

    /// Commits every change in this transaction.
    ///
    /// # Errors
    ///
    /// Returns a backend commit error. The caller must not publish in-memory
    /// effects before this method succeeds.
    pub async fn commit(mut self) -> Result<(), Error> {
        // A commit abandoned before it answers may still land, so it did
        // not roll back.
        self.measure.outcome = Some(TransactionOutcome::Failed);
        let committed = commit_backend(self.backend, self.changes, self.invalidated).await;
        self.measure.outcome = Some(if committed.is_ok() {
            TransactionOutcome::Committed
        } else {
            TransactionOutcome::Failed
        });
        if let Err(error) = &committed {
            record_failure(self.measure.pools, error);
        }
        committed
    }
    /// Explicitly rolls back every change in this transaction.
    ///
    /// # Errors
    ///
    /// Returns a backend rollback error.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.measure.outcome = Some(TransactionOutcome::RolledBack);
        match self.backend {
            TransactionBackend::Sqlite(transaction) => transaction.rollback().await?,
            TransactionBackend::Postgres(transaction) => transaction.rollback().await?,
        }
        Ok(())
    }

    /// Executes an observable domain mutation. Nonzero affected rows invalidate
    /// subscribers only after commit. Use `execute` for locks and internal leases.
    /// Conditional predicates remain the owning repository's responsibility.
    /// # Errors
    /// Returns the same backend errors as `execute`.
    pub async fn execute_change<S>(&mut self, statement: S) -> Result<u64, Error>
    where
        S: SqlxBinder,
    {
        let rows = self.execute(statement).await?;
        self.invalidated |= rows > 0;
        Ok(rows)
    }

    /// Executes a domain-owned statement and returns its affected row count.
    ///
    /// # Errors
    ///
    /// Returns a backend query or constraint error.
    pub async fn execute<S>(&mut self, statement: S) -> Result<u64, Error>
    where
        S: SqlxBinder,
    {
        match &mut self.backend {
            TransactionBackend::Sqlite(transaction) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                let result = sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **transaction)
                    .await?;
                Ok(result.rows_affected())
            }
            TransactionBackend::Postgres(transaction) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                let result = sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **transaction)
                    .await?;
                Ok(result.rows_affected())
            }
        }
    }

    /// Executes one statement written for this transaction's backend, with
    /// positional binds, and returns its affected row count.
    ///
    /// The caller selects the statement text by [`Self::system`]: `?1` binds
    /// on `SQLite`, `$1` on Postgres. Every value travels as a bind. Like
    /// [`Self::execute`], this does not wake change subscribers.
    ///
    /// # Errors
    ///
    /// Returns a backend query or constraint error.
    pub async fn execute_sql(
        &mut self,
        sql: &str,
        values: sea_query::Values,
    ) -> Result<u64, Error> {
        let values = sea_query_sqlx::SqlxValues(values);
        let rows = match &mut self.backend {
            TransactionBackend::Sqlite(transaction) => {
                sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **transaction)
                    .await?
                    .rows_affected()
            }
            TransactionBackend::Postgres(transaction) => {
                sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **transaction)
                    .await?
                    .rows_affected()
            }
        };
        Ok(rows)
    }

    /// Fetches every row of one query written for this transaction's
    /// backend, with positional binds, as [`Self::execute_sql`] runs a
    /// statement.
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error.
    pub async fn fetch_all_sql<T>(
        &mut self,
        sql: &str,
        values: sea_query::Values,
    ) -> Result<Vec<T>, Error>
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        let values = sea_query_sqlx::SqlxValues(values);
        match &mut self.backend {
            TransactionBackend::Sqlite(transaction) => {
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(&mut **transaction)
                    .await
                    .map_err(Into::into)
            }
            TransactionBackend::Postgres(transaction) => {
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(&mut **transaction)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    /// Executes a script of one or more statements written for this
    /// transaction's backend, without binds.
    ///
    /// Domain migration streams use this for backend-specific DDL. The script
    /// runs statement by statement until the first failure; the caller's
    /// transaction then decides whether anything is kept.
    ///
    /// # Errors
    ///
    /// Returns the first backend statement error.
    pub async fn execute_script(&mut self, script: &str) -> Result<(), Error> {
        match &mut self.backend {
            TransactionBackend::Sqlite(transaction) => {
                sqlx::raw_sql(sqlx::AssertSqlSafe(script))
                    .execute(&mut **transaction)
                    .await?;
            }
            TransactionBackend::Postgres(transaction) => {
                sqlx::raw_sql(sqlx::AssertSqlSafe(script))
                    .execute(&mut **transaction)
                    .await?;
            }
        }
        Ok(())
    }

    /// Fetches the first matching domain row in this transaction.
    ///
    /// Returns `None` when no row matches. This does not verify that the
    /// statement matches at most one row.
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error.
    pub async fn fetch_optional<T>(
        &mut self,
        statement: SelectStatement,
    ) -> Result<Option<T>, Error>
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        self.fetch_first(statement).await
    }

    /// Fetches the first matching domain row and holds the matched rows as
    /// `lock` says for the rest of this transaction. With [`RowLock::None`]
    /// this is [`Self::fetch_optional`].
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error, including Postgres's
    /// refusal of a shared hold in a read-only transaction.
    pub async fn fetch_optional_locked<T>(
        &mut self,
        mut statement: SelectStatement,
        lock: RowLock,
    ) -> Result<Option<T>, Error>
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        self.apply_lock(&mut statement, lock);
        self.fetch_first(statement).await
    }

    /// The backend idiom for `lock`: Postgres appends `FOR SHARE`; `SQLite`
    /// relies on the writer lock its read-write transaction already holds.
    fn apply_lock(&self, statement: &mut SelectStatement, lock: RowLock) {
        match (lock, &self.backend) {
            (RowLock::None, _) | (RowLock::Shared, TransactionBackend::Sqlite(_)) => {}
            (RowLock::Shared, TransactionBackend::Postgres(_)) => {
                statement.lock_shared();
            }
        }
    }

    /// Runs one statement with a `RETURNING` clause and fetches its first row.
    ///
    /// Use it for a row lock that also reads the locked row. Like
    /// [`Self::execute`], it does not wake change subscribers, so do not use it
    /// for observable mutations.
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error.
    pub async fn fetch_optional_returning<S, T>(&mut self, statement: S) -> Result<Option<T>, Error>
    where
        S: SqlxBinder,
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        self.fetch_first(statement).await
    }

    async fn fetch_first<S, T>(&mut self, statement: S) -> Result<Option<T>, Error>
    where
        S: SqlxBinder,
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        match &mut self.backend {
            TransactionBackend::Sqlite(transaction) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_optional(&mut **transaction)
                    .await
                    .map_err(Into::into)
            }
            TransactionBackend::Postgres(transaction) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_optional(&mut **transaction)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    /// Fetches and fully materializes all matching rows in this transaction.
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error.
    pub async fn fetch_all<T>(&mut self, statement: SelectStatement) -> Result<Vec<T>, Error>
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        match &mut self.backend {
            TransactionBackend::Sqlite(transaction) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(&mut **transaction)
                    .await
                    .map_err(Into::into)
            }
            TransactionBackend::Postgres(transaction) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(&mut **transaction)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}

/// Commits `backend`, then wakes the change subscribers when it changed
/// something.
async fn commit_backend(
    backend: TransactionBackend,
    subscribers: &crate::changes::Changes,
    invalidated: bool,
) -> Result<(), Error> {
    match backend {
        TransactionBackend::Sqlite(transaction) if invalidated => {
            let changed = subscribers.changed.clone();
            // SQLite commits on a worker thread, so a cancelled commit can still
            // land after the caller stops awaiting it. A drop guard would then
            // wake readers before the commit is visible; notify from the task
            // that observes the commit instead.
            tokio::spawn(async move {
                transaction.commit().await?;
                changed.send_replace(());
                Ok::<_, sqlx::Error>(())
            })
            .await
            .map_err(|_join| sqlx::Error::WorkerCrashed)??;
        }
        TransactionBackend::Sqlite(transaction) => transaction.commit().await?,
        TransactionBackend::Postgres(mut transaction) => {
            if let Some(channel) = subscribers
                .postgres_channel
                .as_ref()
                .filter(|_| invalidated)
            {
                sqlx::query("SELECT pg_notify($1, '')")
                    .bind(channel)
                    .execute(&mut *transaction)
                    .await?;
            }
            transaction.commit().await?;
        }
    }
    Ok(())
}
