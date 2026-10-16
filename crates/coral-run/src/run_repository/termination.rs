//! Termination intent and the terminal Run write, each one chunk commit.

use coral_db::Transaction;
use sea_query::Expr;

use super::ledger::commit_chunk_with;
use super::{Chunk, NewEvent, ProgramRunRecords};
use crate::lifecycle::{RunRecord, RunTermination};
use crate::{Error, FinalValue, RunTerminalOutcome};

/// Records termination intent, and `cancellation.requested` when a Principal
/// asked for it, as one chunk built on `run` under the Run hold.
///
/// # Errors
///
/// Returns the chunk commit's claim or head mismatch; the caller decided in
/// memory that no termination is recorded yet.
pub(crate) async fn request_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    request: &crate::lifecycle::RequestedTermination<'_>,
) -> Result<(), Error> {
    let RunTermination {
        kind,
        started_at_unix_nanos,
        deadline_at_unix_nanos,
    } = request.termination;
    let mut values = vec![
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
    ];
    let mut chunk = Chunk::new(run, run.execution_claim_id.as_deref());
    if let Some(requester) = request.requester {
        values.extend([
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
        chunk = chunk.event(
            NewEvent::CancellationRequested(requester),
            started_at_unix_nanos,
        );
    }
    if request.release_worker_claim {
        values.extend(released_worker_claim());
    }
    commit_chunk_with(transaction, chunk, values).await
}

pub(crate) async fn finish_unstarted_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    finish_terminal_run(transaction, run, None, outcome, terminal_at_unix_nanos).await
}

/// Persists the caller-selected outcome and terminal Event under the Run hold.
/// The caller must apply the Final Value budget in this same transaction.
pub(crate) async fn finish_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    finish_terminal_run(
        transaction,
        run,
        Some(claim_id),
        outcome,
        terminal_at_unix_nanos,
    )
    .await
}

/// Ends a Run whose termination `run` records, under whichever claim it holds.
pub(crate) async fn finish_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    finish_terminal_run(
        transaction,
        run,
        run.execution_claim_id.as_deref(),
        outcome,
        terminal_at_unix_nanos,
    )
    .await
}

/// The terminal chunk: the outcome, a released worker claim, and `run.ended`.
/// No Event follows it, because every later chunk is built on a head it moved.
async fn finish_terminal_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    execution_claim_id: Option<&str>,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
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
    let mut values = vec![
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
    ];
    values.extend(released_worker_claim());
    let chunk = Chunk::new(run, execution_claim_id).event(
        NewEvent::Ended(outcome.state(), final_value_bytes),
        terminal_at_unix_nanos,
    );
    commit_chunk_with(transaction, chunk, values).await
}

fn released_worker_claim() -> [(ProgramRunRecords, Expr); 2] {
    [
        (
            ProgramRunRecords::WorkerClaimId,
            Expr::val(Option::<String>::None),
        ),
        (
            ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
            Expr::val(Option::<i64>::None),
        ),
    ]
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
