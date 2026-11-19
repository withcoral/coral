//! The take (ADR 0027): claims the oldest waiting Runs for one pull stream.

use coral_db::{Database, RowLock, Transaction};
use coral_run_executor::Kind;
use sea_query::{CaseStatement, Condition, Expr, ExprTrait as _, Order, Query};
use uuid::Uuid;

use super::ProgramRunRecords;
use crate::Error;

/// The Run Executor a take claims Runs for, as each Run records it.
pub(crate) struct Taker<'a> {
    pub(crate) run_executor_id: &'a str,
    pub(crate) run_executor_kind: Kind,
}

/// One Run a take claimed, with its own claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Claimed {
    pub(crate) run_id: String,
    pub(crate) claim_id: String,
}

/// A waiting Run: running, with no claim and no start mark. A Run Owner's
/// Run is never one: it holds a worker claim from its acceptance until its
/// start writes the execution claim. The state is a literal, so every plan
/// can use the partial index of waiting Runs.
fn waiting() -> Condition {
    super::running_without_owner()
        .add(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null())
        .add(Expr::col(ProgramRunRecords::ExecutionStartedAtUnixNanos).is_null())
}

/// Whether any Run waits, read without the writer lock, so a take with
/// nothing to claim takes none.
pub(crate) async fn any_waiting(database: &Database) -> Result<bool, Error> {
    let oldest = Query::select()
        .column(ProgramRunRecords::Id)
        .from(ProgramRunRecords::Table)
        .cond_where(waiting())
        .limit(1)
        .to_owned();
    Ok(database
        .fetch_optional::<(String,)>(oldest)
        .await?
        .is_some())
}

/// Claims the oldest waiting Runs, up to `limit`, for `taker` at
/// `taken_at_unix_nanos`, each with its own claim. Rows another take holds
/// are passed over, so concurrent takes claim disjoint Runs.
pub(crate) async fn take_oldest(
    transaction: &mut Transaction<'_>,
    limit: u32,
    taker: &Taker<'_>,
    taken_at_unix_nanos: i64,
) -> Result<Vec<Claimed>, Error> {
    let oldest = Query::select()
        .column(ProgramRunRecords::Id)
        .from(ProgramRunRecords::Table)
        .cond_where(waiting())
        .order_by(ProgramRunRecords::AcceptedAtUnixNanos, Order::Asc)
        .order_by(ProgramRunRecords::Id, Order::Asc)
        .limit(limit.into())
        .to_owned();
    let ids: Vec<(String,)> = transaction
        .fetch_all_locked(oldest, RowLock::SkipLocked)
        .await?;
    let claimed: Vec<Claimed> = ids
        .into_iter()
        .map(|(run_id,)| Claimed {
            run_id,
            claim_id: Uuid::new_v4().hyphenated().to_string(),
        })
        .collect();
    if claimed.is_empty() {
        return Ok(claimed);
    }
    let claims = claimed.iter().fold(CaseStatement::new(), |claims, run| {
        claims.case(
            Expr::col(ProgramRunRecords::Id).eq(run.run_id.as_str()),
            run.claim_id.as_str(),
        )
    });
    let claim = Query::update()
        .table(ProgramRunRecords::Table)
        .value(ProgramRunRecords::ExecutionClaimId, claims)
        .value(ProgramRunRecords::TakenAtUnixNanos, taken_at_unix_nanos)
        .value(ProgramRunRecords::RunExecutorId, taker.run_executor_id)
        .value(
            ProgramRunRecords::RunExecutorKind,
            taker.run_executor_kind.as_storage_str(),
        )
        .and_where(
            Expr::col(ProgramRunRecords::Id).is_in(claimed.iter().map(|run| run.run_id.as_str())),
        )
        .cond_where(waiting())
        .to_owned();
    // The take records no Event and changes no state a reader shows, so it
    // wakes no observer, and the take loop does not wake itself.
    let written = transaction.execute(claim).await?;
    if usize::try_from(written).ok() != Some(claimed.len()) {
        return Err(Error::CorruptData(
            "a take claimed other Runs than it held".to_string(),
        ));
    }
    Ok(claimed)
}
