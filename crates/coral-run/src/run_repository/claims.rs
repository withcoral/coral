//! The execution-start mark and the worker-claim release.
//!
//! The worker claim itself is written by acceptance, so a Run is claimed from
//! its first durable moment, and the Run Owner renews the lease with every
//! chunk it commits. Recovery still releases an expired claim that never
//! reached its start mark, leaving that Run unclaimed until the expired-lease
//! sweep (slice 9) ends it.

use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Query};

use super::ledger::commit_chunk_with;
use super::{Chunk, ProgramRunRecords};
use crate::lifecycle::RunRecord;
use crate::{Error, RunState};

/// The Run Owner's first chunk: the execution-start mark under the claim the
/// acceptance transaction wrote, with the worker lease renewed. Built before
/// any execution claim exists, so it is predicated on the claim column being
/// unset, the head, and a running Run.
pub(crate) async fn start_execution(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    started_at_unix_nanos: i64,
    lease_until_unix_nanos: i64,
) -> Result<(), Error> {
    commit_chunk_with(
        transaction,
        Chunk::new(run, None),
        vec![
            (ProgramRunRecords::ExecutionClaimId, Expr::val(claim_id)),
            (
                ProgramRunRecords::ExecutionStartedAtUnixNanos,
                Expr::val(started_at_unix_nanos),
            ),
            (
                ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                Expr::val(lease_until_unix_nanos),
            ),
        ],
    )
    .await
}

pub(crate) async fn release_unstarted_worker_claim(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
) -> Result<bool, Error> {
    let Some(claim_id) = run.worker_claim_id.as_deref() else {
        return Ok(false);
    };
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .values([
            (
                ProgramRunRecords::WorkerClaimId,
                Expr::val(Option::<String>::None),
            ),
            (
                ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                Expr::val(Option::<i64>::None),
            ),
        ])
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null())
        .and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(claim_id))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

/// Gives a staged Run `claim_id` as its worker and execution claim, as a test's
/// own writer: no owner exists for it, so nothing fences the test's chunks.
#[cfg(test)]
pub(crate) async fn claim_for_test(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    lease_until_unix_nanos: i64,
    started_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .values([
            (ProgramRunRecords::WorkerClaimId, Expr::val(claim_id)),
            (
                ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                Expr::val(lease_until_unix_nanos),
            ),
            (ProgramRunRecords::ExecutionClaimId, Expr::val(claim_id)),
            (
                ProgramRunRecords::ExecutionStartedAtUnixNanos,
                Expr::val(started_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null())
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::CorruptData(
            "test claim found no unclaimed running Run".to_string(),
        ));
    }
    Ok(())
}
