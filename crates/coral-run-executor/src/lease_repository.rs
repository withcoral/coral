//! The Run Executor lease rows: `run_executors(id, kind, lease_until)`.
//!
//! The pull stream renews a row; the Run sweeper reads the lapsed ones,
//! holds a row at the lease it read while it releases that Run Executor's
//! Runs, and deletes it ([`leases`](crate::leases)).

use coral_db::{Database, QueryExecutor, RowLock, Transaction, UnixNanos};
use sea_query::{Alias, Condition, Expr, ExprTrait as _, Iden, OnConflict, Order, Query};

use crate::pull::Kind;

#[derive(Iden)]
enum RunExecutors {
    Table,
    Id,
    Kind,
    LeaseUntil,
}

/// Writes Run Executor `id`'s row with its lease until `lease_until`. A lease
/// never moves back: two pull streams that claim one id both renew it.
pub(crate) async fn renew(
    database: &Database,
    id: &str,
    kind: Kind,
    lease_until: UnixNanos,
) -> Result<(), coral_db::Error> {
    let upsert = Query::insert()
        .into_table(RunExecutors::Table)
        .columns([
            RunExecutors::Id,
            RunExecutors::Kind,
            RunExecutors::LeaseUntil,
        ])
        .values_panic([
            Expr::val(id),
            Expr::val(kind.as_storage_str()),
            Expr::val(lease_until),
        ])
        .on_conflict(
            OnConflict::column(RunExecutors::Id)
                .update_columns([RunExecutors::Kind, RunExecutors::LeaseUntil])
                .action_and_where(
                    Expr::col((Alias::new("excluded"), RunExecutors::LeaseUntil))
                        .gt(Expr::col((RunExecutors::Table, RunExecutors::LeaseUntil))),
                )
                .to_owned(),
        )
        .to_owned();
    // A lease write, which wakes no observer.
    database.execute(upsert).await.map(drop)
}

/// A Run Executor's row as one read found it: its id and its lease. Only a
/// read makes one, so a hold or a delete is always at a lease that was read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunExecutorLease {
    id: String,
    lease_until: UnixNanos,
}

impl RunExecutorLease {
    /// The Run Executor's self-declared id, as each Run it holds records it.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// When its lease lapses, or lapsed, as the read found it.
    #[must_use]
    pub const fn lease_until(&self) -> UnixNanos {
        self.lease_until
    }
}

async fn read_leases(
    executor: impl QueryExecutor,
    condition: Condition,
    limit: u64,
) -> Result<Vec<RunExecutorLease>, coral_db::Error> {
    let rows: Vec<(String, UnixNanos)> = executor
        .fetch_all(
            Query::select()
                .columns([RunExecutors::Id, RunExecutors::LeaseUntil])
                .from(RunExecutors::Table)
                .cond_where(condition)
                .order_by(RunExecutors::LeaseUntil, Order::Asc)
                .order_by(RunExecutors::Id, Order::Asc)
                .limit(limit)
                .to_owned(),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, lease_until)| RunExecutorLease { id, lease_until })
        .collect())
}

/// The rows whose lease lapsed by `now`, oldest lease first, up to
/// `limit`.
///
/// # Errors
///
/// Returns the read's database error.
pub async fn lapsed(
    executor: impl QueryExecutor,
    now: UnixNanos,
    limit: u64,
) -> Result<Vec<RunExecutorLease>, coral_db::Error> {
    read_leases(
        executor,
        Condition::all().add(Expr::col(RunExecutors::LeaseUntil).lte(now)),
        limit,
    )
    .await
}

/// The local children's rows, whatever their lease, oldest lease first, up
/// to `limit`.
///
/// # Errors
///
/// Returns the read's database error.
pub async fn local(
    executor: impl QueryExecutor,
    limit: u64,
) -> Result<Vec<RunExecutorLease>, coral_db::Error> {
    read_leases(
        executor,
        Condition::all().add(Expr::col(RunExecutors::Kind).eq(Kind::Local.as_storage_str())),
        limit,
    )
    .await
}

/// Run Executor `id`'s row, if it has one.
///
/// # Errors
///
/// Returns the read's database error.
pub async fn find(
    executor: impl QueryExecutor,
    id: &str,
) -> Result<Option<RunExecutorLease>, coral_db::Error> {
    Ok(read_leases(
        executor,
        Condition::all().add(Expr::col(RunExecutors::Id).eq(id)),
        1,
    )
    .await?
    .pop())
}

/// The row of `lease` while its lease is still the one read.
fn at_lease(lease: &RunExecutorLease) -> Condition {
    Condition::all()
        .add(Expr::col(RunExecutors::Id).eq(lease.id.as_str()))
        .add(Expr::col(RunExecutors::LeaseUntil).eq(lease.lease_until))
}

/// Holds `lease`'s row exclusively while its lease is still the one read.
/// `false` when its lease moved, because the Run Executor renewed it and its
/// Runs are its own again, or when the row is gone, because another pass
/// released it.
///
/// # Errors
///
/// Returns the read's database error.
pub async fn hold(
    transaction: &mut Transaction<'_>,
    lease: &RunExecutorLease,
) -> Result<bool, coral_db::Error> {
    let held: Option<(String,)> = transaction
        .fetch_optional_locked(
            Query::select()
                .column(RunExecutors::Id)
                .from(RunExecutors::Table)
                .cond_where(at_lease(lease))
                .to_owned(),
            RowLock::Exclusive,
        )
        .await?;
    Ok(held.is_some())
}

/// Deletes `lease`'s row while its lease is still the one read. Like the
/// renewal, it wakes no observer.
///
/// # Errors
///
/// Returns the write's database error.
pub async fn delete(
    transaction: &mut Transaction<'_>,
    lease: &RunExecutorLease,
) -> Result<bool, coral_db::Error> {
    let deleted = transaction
        .execute(
            Query::delete()
                .from_table(RunExecutors::Table)
                .cond_where(at_lease(lease))
                .to_owned(),
        )
        .await?;
    Ok(deleted == 1)
}
