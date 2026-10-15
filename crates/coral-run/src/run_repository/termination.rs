//! Termination intent and the first-wins terminal Run write.

use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Query};

use super::ProgramRunRecords;
use super::events::{NewEvent, insert_event};
use crate::lifecycle::{RunRecord, RunTermination, RunTerminationKind};
use crate::{Error, FinalValue, RunState, RunTerminalOutcome};

pub(crate) async fn request_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    request: &crate::lifecycle::RequestedTermination<'_>,
) -> Result<bool, Error> {
    let RunTermination {
        kind,
        started_at_unix_nanos,
        deadline_at_unix_nanos,
    } = request.termination;
    let next_event_sequence = i64::try_from(request.next_event_sequence)
        .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let mut statement = Query::update();
    statement
        .table(ProgramRunRecords::Table)
        .values([
            (
                ProgramRunRecords::TerminationKind,
                Expr::val(kind.as_storage_str()),
            ),
            (
                ProgramRunRecords::TerminationStartedAtUnixNanos,
                Expr::val(started_at_unix_nanos),
            ),
            (
                ProgramRunRecords::TerminationDeadlineAtUnixNanos,
                Expr::val(deadline_at_unix_nanos),
            ),
            (
                ProgramRunRecords::NextEventSequence,
                Expr::val(next_event_sequence),
            ),
        ])
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null());
    if let Some(requester) = request.requester {
        statement.values([
            (
                ProgramRunRecords::CancellationRequestedAtUnixNanos,
                Expr::val(started_at_unix_nanos),
            ),
            (
                ProgramRunRecords::CancellationPrincipalId,
                Expr::val(requester.id().as_str()),
            ),
            (
                ProgramRunRecords::CancellationPrincipalKind,
                Expr::val(requester.kind().as_storage_str()),
            ),
        ]);
    }
    if request.release_worker_claim {
        statement.values([
            (
                ProgramRunRecords::WorkerClaimId,
                Expr::val(Option::<String>::None),
            ),
            (
                ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                Expr::val(Option::<i64>::None),
            ),
        ]);
    }
    Ok(transaction.execute(statement).await? == 1)
}

pub(crate) async fn finish_unstarted_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<bool, Error> {
    finish_terminal_run(
        transaction,
        run,
        ExecutionFence::Unstarted,
        outcome,
        terminal_at_unix_nanos,
    )
    .await
}

/// Persists the caller-selected outcome and terminal Event under the Run hold.
/// The caller must apply the Final Value budget in this same transaction.
pub(crate) async fn finish_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<bool, Error> {
    finish_terminal_run(
        transaction,
        run,
        ExecutionFence::Claimed(claim_id),
        outcome,
        terminal_at_unix_nanos,
    )
    .await
}

pub(crate) async fn finish_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    kind: RunTerminationKind,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<bool, Error> {
    finish_terminal_run(
        transaction,
        run,
        ExecutionFence::Termination(kind),
        outcome,
        terminal_at_unix_nanos,
    )
    .await
}

enum ExecutionFence<'a> {
    Unstarted,
    Claimed(&'a str),
    Termination(RunTerminationKind),
}

async fn finish_terminal_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    fence: ExecutionFence<'_>,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let (failure_code, failure_message, final_value_json) = terminal_fields(outcome);
    let final_value_bytes = final_value_json.as_deref().map_or(0, str::len);
    let failure_error_json = match outcome {
        RunTerminalOutcome::Failed(failure) => failure
            .error()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_error| {
                Error::CorruptData("Program error could not be encoded".to_string())
            })?,
        _ => None,
    };
    let next_event_sequence = run
        .next_event_sequence
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let mut statement = Query::update();
    statement
        .table(ProgramRunRecords::Table)
        .values([
            (
                ProgramRunRecords::State,
                Expr::val(outcome.state().as_storage_str()),
            ),
            (
                ProgramRunRecords::TerminalAtUnixNanos,
                Expr::val(terminal_at_unix_nanos),
            ),
            (ProgramRunRecords::FailureCode, Expr::val(failure_code)),
            (
                ProgramRunRecords::FailureMessage,
                Expr::val(failure_message),
            ),
            (
                ProgramRunRecords::FailureErrorJson,
                Expr::val(failure_error_json),
            ),
            (
                ProgramRunRecords::FinalValueJson,
                Expr::val(final_value_json),
            ),
            (
                ProgramRunRecords::WorkerClaimId,
                Expr::val(Option::<String>::None),
            ),
            (
                ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                Expr::val(Option::<i64>::None),
            ),
            (
                ProgramRunRecords::NextEventSequence,
                Expr::val(next_event_sequence),
            ),
        ])
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()));
    match fence {
        ExecutionFence::Unstarted => {
            statement.and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null());
        }
        ExecutionFence::Claimed(claim_id) => {
            statement
                .and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null())
                .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id))
                .and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(claim_id));
        }
        ExecutionFence::Termination(kind) => {
            statement
                .and_where(Expr::col(ProgramRunRecords::TerminationKind).eq(kind.as_storage_str()));
        }
    }
    if transaction.execute(statement).await? != 1 {
        return Ok(false);
    }
    crate::middleware_repository::settle_running(
        transaction,
        &run.workspace_id,
        &run.id,
        terminal_at_unix_nanos,
    )
    .await?;
    insert_event(
        transaction,
        &run.workspace_id,
        &run.id,
        run.next_event_sequence,
        NewEvent::Ended(outcome.state(), final_value_bytes),
        terminal_at_unix_nanos,
    )
    .await?;
    Ok(true)
}

type TerminalFields = (Option<String>, Option<String>, Option<String>);

fn terminal_fields(outcome: &RunTerminalOutcome) -> TerminalFields {
    match outcome {
        RunTerminalOutcome::Succeeded { final_value } => {
            let value = match final_value {
                FinalValue::Absent => None,
                FinalValue::Present(value) => Some(value.encoded_json().to_string()),
            };
            (None, None, value)
        }
        RunTerminalOutcome::Failed(failure) => (
            Some(failure.code().as_storage_str().to_string()),
            Some(failure.message().to_string()),
            None,
        ),
        RunTerminalOutcome::Canceled
        | RunTerminalOutcome::TimedOut
        | RunTerminalOutcome::Interrupted => (None, None, None),
    }
}
