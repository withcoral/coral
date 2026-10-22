//! Termination intent and the terminal Run write, each one chunk commit.

use coral_db::Transaction;
use sea_query::Expr;

use super::ledger::{commit_chunk, commit_chunk_with};
use super::{Chunk, NewEvent, OwnedEvent, ProgramRunRecords};
use crate::lifecycle::{RequestedTermination, RunRecord, RunTermination};
use crate::{Error, FinalValue, RunTerminalOutcome};

/// Records termination intent, and `cancellation.requested` when a Principal
/// asked for it, as one chunk built on `run` as the writer read it: the Run
/// Owner's own writes outside its chunk commits (a Run that times out before
/// it starts, the terminal write of a Program that ended without one).
///
/// # Errors
///
/// Returns the chunk commit's claim or head mismatch; the caller decided in
/// memory that no termination is recorded yet.
pub(crate) async fn request_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    request: &RequestedTermination,
) -> Result<(), Error> {
    let mut chunk = Chunk::new(run, run.execution_claim_id.as_deref());
    if let Some(requester) = &request.requester {
        chunk = chunk.event(
            NewEvent::CancellationRequested(requester),
            request.termination.started_at_unix_nanos,
        );
    }
    commit_chunk(transaction, chunk.termination(Some(request))).await
}

/// The Run row columns a termination intent writes: the termination, the
/// cancellation's requester and instant when a Principal asked for it, and
/// the released worker claim when the intent releases it. A chunk that
/// carries the intent writes them in its head update.
pub(super) fn request_values(request: &RequestedTermination) -> Vec<(ProgramRunRecords, Expr)> {
    let mut values = termination_values(&request.termination);
    if let Some(requester) = &request.requester {
        values.extend([
            (
                ProgramRunRecords::CancellationRequestedAtUnixNanos,
                Expr::val(request.termination.started_at_unix_nanos),
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
    if request.releases_worker_claim() {
        values.extend(released_worker_claim());
    }
    values
}

pub(crate) async fn finish_unstarted_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    finish_terminal_run(
        transaction,
        run,
        None,
        &[],
        outcome,
        terminal_at_unix_nanos,
        Vec::new(),
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
) -> Result<(), Error> {
    finish_terminal_run(
        transaction,
        run,
        Some(claim_id),
        &[],
        outcome,
        terminal_at_unix_nanos,
        Vec::new(),
    )
    .await
}

/// Ends a Run whose termination `run` records, under whichever claim it holds.
/// `invocation_events` are the `invocation.ended` Events of the Invocations
/// the same transaction ended for the termination; they precede `run.ended`
/// in the one terminal chunk.
pub(crate) async fn finish_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation_events: &[OwnedEvent],
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    finish_terminal_run(
        transaction,
        run,
        run.execution_claim_id.as_deref(),
        invocation_events,
        outcome,
        terminal_at_unix_nanos,
        Vec::new(),
    )
    .await
}

/// The one chunk that interrupts a Run its owner can no longer run: the
/// termination intent and the terminal outcome together, the worker claim
/// released, the `invocation.ended` Events of its ended Invocations, and
/// `run.ended`. Predicated on the claim `run` holds (its execution claim, or
/// the worker claim of a Run that never started). `new_execution_claim_id`
/// is the claim id a recovering writer mints when it takes the Run over from
/// a started owner that can no longer write: every commit still in flight
/// under the old claim then mismatches.
pub(crate) async fn interrupt_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation_events: &[OwnedEvent],
    termination: &RunTermination,
    new_execution_claim_id: Option<&str>,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    let mut values = termination_values(termination);
    // A Run that never started has no execution claim to take over: its
    // row pairs the execution claim with the start mark, and the released
    // worker claim is all the fence a late start mark needs.
    if let (Some(new_claim_id), Some(_started)) =
        (new_execution_claim_id, run.execution_claim_id.as_deref())
    {
        values.push((ProgramRunRecords::ExecutionClaimId, Expr::val(new_claim_id)));
    }
    finish_terminal_run(
        transaction,
        run,
        run.execution_claim_id.as_deref(),
        invocation_events,
        &termination.kind.outcome(),
        terminal_at_unix_nanos,
        values,
    )
    .await
}

/// The terminal chunk: the outcome, a released worker claim, and `run.ended`
/// after `invocation_events`. No Event follows it: a chunk built before it
/// mismatches the head it moved, and a chunk built after it finds no running
/// Run.
async fn finish_terminal_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    execution_claim_id: Option<&str>,
    invocation_events: &[OwnedEvent],
    outcome: &RunTerminalOutcome,
    terminal_at_unix_nanos: i64,
    mut values: Vec<(ProgramRunRecords, Expr)>,
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
    values.extend([
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
    ]);
    values.extend(released_worker_claim());
    let chunk = Chunk::new(run, execution_claim_id)
        .owned_events(invocation_events)
        .event(
            NewEvent::Ended(outcome.state(), final_value_bytes),
            terminal_at_unix_nanos,
        );
    commit_chunk_with(transaction, chunk, values).await
}

/// The Run row columns that record a termination.
fn termination_values(termination: &RunTermination) -> Vec<(ProgramRunRecords, Expr)> {
    vec![
        (
            ProgramRunRecords::TerminationKind,
            Expr::val(termination.kind.as_storage_str()),
        ),
        (
            ProgramRunRecords::TerminationStartedAtUnixNanos,
            Expr::val(termination.started_at_unix_nanos),
        ),
        (
            ProgramRunRecords::TerminationDeadlineAtUnixNanos,
            Expr::val(termination.deadline_at_unix_nanos),
        ),
    ]
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
