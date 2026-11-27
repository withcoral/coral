//! The Run sweeper's statements: its election lease, the Runs
//! each pass reads, and the put-back of a taken Run that never started.
//!
//! Every read of Runs starts from [`running_without_worker_claim`]: an older
//! release's Run Owner's Run is never one.

use coral_db::{Database, QueryExecutor, Transaction, UnixNanos};
use coral_run_executor::Kind;
use coral_workspace::WorkspaceId;
use sea_query::{Condition, Expr, ExprTrait as _, Iden, OnConflict, Order, Query};

use super::run_row::{UndecodableRun, taken_kind};
use super::{ProgramRunRecords, running_without_worker_claim, taken};
use crate::types::HeldRunId;
use crate::{CoralInstanceId, Error, RunId};

#[derive(Iden)]
enum RunSweeperLease {
    Table,
    Id,
    HolderInstanceId,
    LeaseUntil,
}

/// The key of the lease table's one row.
const LEASE_ROW: i64 = 1;

/// Writes `instance` as the sweeper until `lease_until`. With `free_at`,
/// only while the row is free then: no row yet, a lapsed lease, or
/// `instance`'s own; without it, whatever its lease. `true` when the write
/// landed. It wakes no observer.
pub(crate) async fn elect_sweeper(
    database: &Database,
    instance: &CoralInstanceId,
    lease_until: UnixNanos,
    free_at: Option<UnixNanos>,
) -> Result<bool, Error> {
    let mut on_conflict = OnConflict::column(RunSweeperLease::Id);
    on_conflict.update_columns([
        RunSweeperLease::HolderInstanceId,
        RunSweeperLease::LeaseUntil,
    ]);
    if let Some(now) = free_at {
        on_conflict.action_and_where(
            Expr::col((RunSweeperLease::Table, RunSweeperLease::LeaseUntil))
                .lte(now)
                .or(
                    Expr::col((RunSweeperLease::Table, RunSweeperLease::HolderInstanceId))
                        .eq(instance.as_str()),
                ),
        );
    }
    let upsert = Query::insert()
        .into_table(RunSweeperLease::Table)
        .columns([
            RunSweeperLease::Id,
            RunSweeperLease::HolderInstanceId,
            RunSweeperLease::LeaseUntil,
        ])
        .values_panic([
            Expr::val(LEASE_ROW),
            Expr::val(instance.as_str()),
            Expr::val(lease_until),
        ])
        .on_conflict(on_conflict)
        .to_owned();
    Ok(database.execute(upsert).await? == 1)
}

/// A Run a Run Executor has taken, as a pass read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldRun {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) run_id: RunId,
    /// The claim its take gave it, which a release is predicated on.
    pub(crate) claim: String,
    /// The Run Executor the take gave it to.
    pub(crate) run_executor_id: Option<String>,
    /// That Run Executor's kind, as the take recorded it.
    pub(crate) run_executor_kind: Option<Kind>,
}

type HeldRow = (String, String, String, Option<String>, Option<String>);

impl HeldRun {
    /// The Run a row names, or the error that names the Run when its stored
    /// identity does not decode.
    fn decode(
        (workspace_id, run_id, claim, run_executor_id, run_executor_kind): HeldRow,
    ) -> Result<Self, UndecodableRun> {
        let parsed = RunId::parse(&run_id).ok();
        let invalid = || UndecodableRun {
            id: parsed.clone().map(HeldRunId::of_held_row),
            error: Box::new(Error::CorruptData("Run has invalid identity".into())),
        };
        Ok(Self {
            workspace_id: WorkspaceId::parse(&workspace_id).map_err(|_error| invalid())?,
            run_id: parsed.clone().ok_or_else(invalid)?,
            claim,
            run_executor_id,
            run_executor_kind: taken_kind(run_executor_kind.as_deref()),
        })
    }
}

/// The taken Runs that also match `condition`, ordered by `order`, up to
/// `limit`. Each decodes on its own.
async fn held_runs(
    executor: impl QueryExecutor,
    condition: Condition,
    order: ProgramRunRecords,
    limit: u64,
) -> Result<Vec<Result<HeldRun, UndecodableRun>>, Error> {
    let rows: Vec<HeldRow> = executor
        .fetch_all(
            Query::select()
                .columns([
                    ProgramRunRecords::WorkspaceId,
                    ProgramRunRecords::Id,
                    ProgramRunRecords::ExecutionClaimId,
                    ProgramRunRecords::RunExecutorId,
                    ProgramRunRecords::RunExecutorKind,
                ])
                .from(ProgramRunRecords::Table)
                .cond_where(taken().add(condition))
                .order_by(order, Order::Asc)
                .order_by(ProgramRunRecords::Id, Order::Asc)
                .limit(limit)
                .to_owned(),
        )
        .await?;
    Ok(rows.into_iter().map(HeldRun::decode).collect())
}

/// The Runs a Run Executor holds whose deadline passed by `now`, earliest
/// deadline first, up to `limit`.
pub(crate) async fn past_deadline(
    executor: impl QueryExecutor,
    now: UnixNanos,
    limit: u64,
) -> Result<Vec<Result<HeldRun, UndecodableRun>>, Error> {
    held_runs(
        executor,
        Condition::all().add(Expr::col(ProgramRunRecords::DeadlineAtUnixNanos).lte(now)),
        ProgramRunRecords::DeadlineAtUnixNanos,
        limit,
    )
    .await
}

/// The taken Runs with no start mark whose take came before
/// `taken_before`, oldest take first, up to `limit`.
pub(crate) async fn unstarted_takes(
    executor: impl QueryExecutor,
    taken_before: UnixNanos,
    limit: u64,
) -> Result<Vec<Result<HeldRun, UndecodableRun>>, Error> {
    held_runs(
        executor,
        Condition::all()
            .add(Expr::col(ProgramRunRecords::ExecutionStartedAtUnixNanos).is_null())
            .add(Expr::col(ProgramRunRecords::TakenAtUnixNanos).lt(taken_before)),
        ProgramRunRecords::TakenAtUnixNanos,
        limit,
    )
    .await
}

/// The running Runs that name Run Executor `run_executor_id`, oldest
/// acceptance first, up to `limit`.
pub(crate) async fn held_by_run_executor(
    executor: impl QueryExecutor,
    run_executor_id: &str,
    limit: u64,
) -> Result<Vec<Result<HeldRun, UndecodableRun>>, Error> {
    held_runs(
        executor,
        Condition::all().add(Expr::col(ProgramRunRecords::RunExecutorId).eq(run_executor_id)),
        ProgramRunRecords::AcceptedAtUnixNanos,
        limit,
    )
    .await
}

/// Whether any running Run names Run Executor `run_executor_id`.
pub(crate) async fn names_run_executor(
    transaction: &mut Transaction<'_>,
    run_executor_id: &str,
) -> Result<bool, Error> {
    let named: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProgramRunRecords::Id)
                .from(ProgramRunRecords::Table)
                .cond_where(
                    running_without_worker_claim()
                        .add(Expr::col(ProgramRunRecords::RunExecutorId).eq(run_executor_id)),
                )
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(named.is_some())
}

/// Puts `held` back to waiting: clears its claim, its take time and its Run
/// Executor, predicated on the claim the pass read and the missing start
/// mark. `false` when the row no longer matches: the Run started, ended or
/// was taken again. Like the take, it records no Event and wakes no
/// observer; the Run keeps its place. It rings [`super::QUEUE`], so a take
/// loop takes the Run again at once.
pub(crate) async fn put_back(
    transaction: &mut Transaction<'_>,
    held: &HeldRun,
) -> Result<bool, Error> {
    let written = transaction
        .execute(
            Query::update()
                .table(ProgramRunRecords::Table)
                .values([
                    (
                        ProgramRunRecords::ExecutionClaimId,
                        Expr::val(Option::<String>::None),
                    ),
                    (
                        ProgramRunRecords::TakenAtUnixNanos,
                        Expr::val(Option::<i64>::None),
                    ),
                    (
                        ProgramRunRecords::RunExecutorId,
                        Expr::val(Option::<String>::None),
                    ),
                    (
                        ProgramRunRecords::RunExecutorKind,
                        Expr::val(Option::<String>::None),
                    ),
                ])
                .cond_where(
                    running_without_worker_claim()
                        .add(
                            Expr::col(ProgramRunRecords::WorkspaceId)
                                .eq(held.workspace_id.to_string()),
                        )
                        .add(Expr::col(ProgramRunRecords::Id).eq(held.run_id.to_string()))
                        .add(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(held.claim.as_str()))
                        .add(Expr::col(ProgramRunRecords::ExecutionStartedAtUnixNanos).is_null()),
                )
                .to_owned(),
        )
        .await?;
    if written == 1 {
        transaction.ring_on_commit(super::QUEUE);
    }
    Ok(written == 1)
}
