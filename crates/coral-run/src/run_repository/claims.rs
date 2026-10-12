//! Worker-claim and execution-start conditional writes.

use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Query};

use super::ProgramRunRecords;
use crate::lifecycle::RunRecord;
use crate::{Error, RunId, RunState};

pub(crate) async fn claim_execution_worker(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    claimed_until_unix_nanos: i64,
    now_unix_nanos: i64,
) -> Result<bool, Error> {
    let mut statement = Query::update();
    statement
        .table(ProgramRunRecords::Table)
        .values([
            (ProgramRunRecords::WorkerClaimId, Expr::val(claim_id)),
            (
                ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                Expr::val(claimed_until_unix_nanos),
            ),
        ])
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null())
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null())
        .and_where(Expr::col(ProgramRunRecords::DeadlineAtUnixNanos).gt(now_unix_nanos));
    match (
        run.worker_claim_id.as_deref(),
        run.worker_claim_expires_at_unix_nanos,
    ) {
        (None, None) => {
            statement.and_where(Expr::col(ProgramRunRecords::WorkerClaimId).is_null());
        }
        (Some(current_claim), Some(expires_at)) if expires_at <= now_unix_nanos => {
            statement
                .and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(current_claim))
                .and_where(
                    Expr::col(ProgramRunRecords::WorkerClaimExpiresAtUnixNanos).eq(expires_at),
                );
        }
        _ => return Ok(false),
    }
    Ok(transaction.execute(statement).await? == 1)
}

pub(crate) async fn mark_execution_started(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    started_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .values([
            (ProgramRunRecords::ExecutionClaimId, Expr::val(claim_id)),
            (
                ProgramRunRecords::ExecutionStartedAtUnixNanos,
                Expr::val(started_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null())
        .and_where(Expr::col(ProgramRunRecords::DeadlineAtUnixNanos).gt(started_at_unix_nanos))
        .and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(claim_id))
        .and_where(
            Expr::col(ProgramRunRecords::WorkerClaimExpiresAtUnixNanos).gt(started_at_unix_nanos),
        )
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null())
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(crate) async fn renew_worker_claim(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    claim_id: &str,
    now_unix_nanos: i64,
    claimed_until_unix_nanos: i64,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .value(
            ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
            claimed_until_unix_nanos,
        )
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null())
        .and_where(Expr::col(ProgramRunRecords::DeadlineAtUnixNanos).gt(now_unix_nanos))
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id))
        .and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(claim_id))
        .and_where(Expr::col(ProgramRunRecords::WorkerClaimExpiresAtUnixNanos).gt(now_unix_nanos))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
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
