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
    let mut chunk = Chunk::new(
        run,
        run.execution_claim_id.as_deref(),
        request.termination.started_at_unix_nanos,
    );
    if let Some(requester) = &request.requester {
        chunk = chunk.event(
            NewEvent::CancellationRequested(requester),
            request.termination.started_at_unix_nanos,
        );
    }
    commit_chunk(transaction, chunk.termination(request)).await
}

/// The Run row columns a termination intent writes: the termination, and the
/// cancellation's requester and instant when a Principal asked for it. A
/// chunk that carries the intent writes them in its head update.
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
    values
}

/// Ends a taken Run that never started, under the take's claim, which it
/// releases: no claim stays on a Run with no start mark once it has ended.
pub(crate) async fn finish_taken_run(
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
        vec![(
            ProgramRunRecords::ExecutionClaimId,
            Expr::val(None::<String>),
        )],
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

/// The one chunk that ends a Run no stream ends: the termination intent and
/// the terminal outcome together, the Events its writer applied (the
/// `invocation.ended` Events of its ended Invocations among them), and
/// `run.ended`. Predicated on the claim `run` holds, if any.
pub(crate) async fn interrupt_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    events: &[OwnedEvent],
    termination: &RequestedTermination,
    terminal_at_unix_nanos: i64,
) -> Result<(), Error> {
    let mut values = request_values(termination);
    // A taken Run that never started keeps no claim once it has ended, so a
    // late `Open` or start finds none.
    if run.execution_claim_id.is_some() && !run.execution_started() {
        values.push((
            ProgramRunRecords::ExecutionClaimId,
            Expr::val(None::<String>),
        ));
    }
    finish_terminal_run(
        transaction,
        run,
        run.execution_claim_id.as_deref(),
        events,
        &termination.termination.kind.outcome(),
        terminal_at_unix_nanos,
        values,
    )
    .await
}

/// The terminal chunk: the outcome and `run.ended`
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
    let chunk = Chunk::new(run, execution_claim_id, terminal_at_unix_nanos)
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
