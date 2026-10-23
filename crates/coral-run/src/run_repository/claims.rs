//! The execution-start mark.
//!
//! The worker claim itself is written by acceptance, so a Run is claimed from
//! its first durable moment, and the Run Owner renews the lease with every
//! chunk it commits. A Run whose lease lapses with no owner is the
//! expired-lease sweep's, which takes it over in its recovered owner's one
//! chunk.

use coral_db::Transaction;
use sea_query::Expr;
#[cfg(test)]
use sea_query::{ExprTrait as _, Query};

use super::ledger::commit_chunk_with;
use super::{Chunk, ProgramRunRecords};
use crate::Error;
#[cfg(test)]
use crate::RunState;
use crate::lifecycle::RunRecord;

/// The Run Owner's first chunk: the execution-start mark under the claim the
/// acceptance transaction wrote, with the worker lease renewed. Built before
/// any execution claim exists, so it is predicated on the claim column being
/// unset, the worker claim still held (a recovered owner's chunk releases
/// it), the head, and a running Run.
pub(crate) async fn start_execution(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    started_at_unix_nanos: i64,
    lease_until_unix_nanos: i64,
) -> Result<(), Error> {
    commit_chunk_with(
        transaction,
        Chunk::new(run, None).held_by_worker(claim_id),
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
