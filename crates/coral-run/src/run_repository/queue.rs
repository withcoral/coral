//! The Run queue's counts, which every Coral Instance reads on a period and
//! reports alike (ADR 0027, Telemetry).

use coral_db::{Transaction, UnixNanos};
use sea_query::{Asterisk, Condition, Expr, ExprTrait as _, Query};

use super::take::waiting;
use super::{ProgramRunRecords, taken};
use crate::Error;

/// The Run queue across every Workspace, as one transaction reads it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueCounts {
    /// Runs accepted and not yet taken.
    pub(crate) waiting: u64,
    /// The acceptance of the oldest of them; none while none waits.
    pub(crate) oldest_accepted: Option<UnixNanos>,
    /// Runs a Run Executor took and that have not ended, started or not.
    pub(crate) held: u64,
}

/// Counts the waiting Runs and the held ones in two statements, whatever
/// their number. The partial index of waiting Runs serves the first; the
/// second reads the running Runs alone, by the index that `state` leads.
/// An older release's Run Owner's Run is neither: a worker claim fences
/// it, or, once that release recorded its interruption, it started with
/// no Run Executor.
pub(crate) async fn queue_counts(tx: &mut Transaction<'_>) -> Result<QueueCounts, Error> {
    let (waiting, oldest_accepted): (i64, Option<UnixNanos>) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(Asterisk).count())
                .expr(Expr::col(ProgramRunRecords::AcceptedAtUnixNanos).min())
                .from(ProgramRunRecords::Table)
                .cond_where(waiting())
                .to_owned(),
        )
        .await?
        .unwrap_or_default();
    let (held,): (i64,) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(Asterisk).count())
                .from(ProgramRunRecords::Table)
                .cond_where(
                    // As `RunRecord::runs_on_run_executor`: a started Run
                    // is held only when a take gave it to a Run Executor.
                    taken().add(
                        Condition::any()
                            .add(
                                Expr::col(ProgramRunRecords::ExecutionStartedAtUnixNanos).is_null(),
                            )
                            .add(Expr::col(ProgramRunRecords::RunExecutorKind).is_not_null()),
                    ),
                )
                .to_owned(),
        )
        .await?
        .unwrap_or_default();
    Ok(QueueCounts {
        waiting: u64::try_from(waiting).unwrap_or_default(),
        oldest_accepted,
        held: u64::try_from(held).unwrap_or_default(),
    })
}
