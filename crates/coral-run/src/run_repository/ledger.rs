//! The Run Ledger head and the one chunk-commit write path (ADR 0018).
//!
//! A chunk is everything one Run write carries: its Events, the Run Output
//! Budget total after them, and, for the Run Owner's chunks, the worker lease
//! renewal.
//! [`commit_chunk`] writes it in the caller's transaction under one
//! predicate, the execution claim id and the head the chunk was built on, and
//! moves the head by one. The Run-scoped rows a chunk carries (Invocation
//! rows, Approval Request rows) join the same transaction through their owning
//! repositories; the Run row's own changes (the termination intent the chunk
//! carries, the terminal outcome) ride in the head update as further Run row
//! columns.

use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Query};

use super::events::{NewEvent, OwnedEvent, insert_events};
use super::termination::request_values;
use super::{ProgramRunRecords, RunRecord};
use crate::lifecycle::RequestedTermination;
use crate::{Error, RunId, RunState};

/// One Event of a chunk and when it occurred.
pub(crate) struct ChunkEvent<'a> {
    pub(crate) event: NewEvent<'a>,
    pub(crate) occurred_at_unix_nanos: i64,
}

/// One Run Ledger write, built on the Run as its writer read it.
pub(crate) struct Chunk<'a> {
    workspace_id: &'a WorkspaceId,
    run_id: &'a RunId,
    /// The execution claim id the chunk was built on. `None` before execution
    /// starts: acceptance leaves the claim unset until the worker's start mark.
    execution_claim_id: Option<&'a str>,
    /// The Run Ledger head the chunk was built on.
    head: u64,
    events: Vec<ChunkEvent<'a>>,
    /// The Run Output Budget total after this chunk, written as a plain value.
    output_bytes: usize,
    /// Renews the worker lease to this instant when the writer holds the lease.
    lease_until_unix_nanos: Option<i64>,
    /// The termination intent this chunk records in its head update.
    termination: Option<&'a RequestedTermination>,
    /// The worker claim the Run row must still hold for this chunk to land.
    worker_claim_id: Option<&'a str>,
}

impl<'a> Chunk<'a> {
    /// Starts a chunk on `run` as the writer read it under its hold, predicated
    /// on `execution_claim_id`: the worker's own claim, or the claim on the row
    /// for writers outside the worker.
    pub(crate) fn new(run: &'a RunRecord, execution_claim_id: Option<&'a str>) -> Self {
        Self {
            workspace_id: &run.workspace_id,
            run_id: &run.id,
            execution_claim_id,
            head: run.chunk_sequence,
            events: Vec::new(),
            output_bytes: run.accumulated_output_bytes,
            lease_until_unix_nanos: None,
            termination: None,
            worker_claim_id: None,
        }
    }

    /// Predicates the chunk on the Run row still holding `claim_id` as its
    /// worker claim: the start mark, written before any execution claim
    /// exists, must not land on a worker claim released meanwhile (by the
    /// previous release's recovery, or a recovered owner's chunk).
    pub(crate) const fn held_by_worker(mut self, claim_id: &'a str) -> Self {
        self.worker_claim_id = Some(claim_id);
        self
    }

    /// Appends an Event after the ones already in the chunk.
    pub(crate) fn event(mut self, event: NewEvent<'a>, occurred_at_unix_nanos: i64) -> Self {
        self.events.push(ChunkEvent {
            event,
            occurred_at_unix_nanos,
        });
        self
    }

    /// Appends `events` in order after the ones already in the chunk.
    pub(crate) fn owned_events(mut self, events: impl IntoIterator<Item = &'a OwnedEvent>) -> Self {
        for event in events {
            self = self.event(event.as_new(), event.occurred_at_unix_nanos);
        }
        self
    }

    /// Sets the Run Output Budget total this chunk records.
    pub(crate) const fn output_bytes(mut self, total: usize) -> Self {
        self.output_bytes = total;
        self
    }

    /// Renews the worker lease to `until_unix_nanos` when the Run row still
    /// holds this chunk's claim as its worker claim. The Run Owner renews on
    /// every chunk it commits; a released worker claim is never re-leased.
    pub(crate) const fn renew_lease(mut self, until_unix_nanos: i64) -> Self {
        self.lease_until_unix_nanos = Some(until_unix_nanos);
        self
    }

    /// Records `intent` in this chunk's head update: the Run Owner's
    /// cancellation beside its `cancellation.requested` Event, or the
    /// interruption a recovering writer records for a Run it takes over.
    pub(crate) const fn termination(mut self, intent: Option<&'a RequestedTermination>) -> Self {
        self.termination = intent;
        self
    }

    #[cfg(test)]
    pub(crate) const fn built_on_head(mut self, head: u64) -> Self {
        self.head = head;
        self
    }
}

/// Commits `chunk` in `transaction`: asserts the claim, the head and a running
/// Run, moves the head by one, writes the output total, the termination intent
/// the chunk carries and the lease renewal, and appends the Events after the
/// Run's last one. The caller commits the transaction.
///
/// The update also advances `next_event_sequence` by the chunk's Event count.
/// This release never reads it, but the previous release numbers its Events
/// from it, and during a rolling update both may write one Run: a cancel or
/// an approval decision served by the new process must leave the counter
/// where the old process's next write expects it.
///
/// # Errors
///
/// [`Error::ClaimMismatch`] when the Run row holds another execution claim,
/// [`Error::RunEnded`] when it holds the claim but the Run is terminal (no
/// Event follows `run.ended`), [`Error::HeadMismatch`] when it holds the claim
/// and is running but the head has moved. None of them writes anything.
pub(crate) async fn commit_chunk(
    transaction: &mut Transaction<'_>,
    chunk: Chunk<'_>,
) -> Result<(), Error> {
    commit_chunk_with(transaction, chunk, Vec::new()).await
}

/// [`commit_chunk`] with `run_values` as further Run row columns written in
/// the head update: the terminal outcome, a released worker claim.
pub(super) async fn commit_chunk_with(
    transaction: &mut Transaction<'_>,
    chunk: Chunk<'_>,
    run_values: Vec<(ProgramRunRecords, Expr)>,
) -> Result<(), Error> {
    let mut run_values = run_values;
    if let Some(intent) = chunk.termination {
        run_values.extend(request_values(intent));
    }
    let head = i64::try_from(chunk.head)
        .map_err(|_error| Error::CorruptData("Run Ledger head overflowed".to_string()))?;
    let next_head = head
        .checked_add(1)
        .ok_or_else(|| Error::CorruptData("Run Ledger head overflowed".to_string()))?;
    let output_bytes = i64::try_from(chunk.output_bytes)
        .map_err(|_error| Error::CorruptData("Run output bytes overflowed".to_string()))?;
    let event_count = i64::try_from(chunk.events.len())
        .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let mut statement = Query::update();
    statement
        .table(ProgramRunRecords::Table)
        .values([
            (ProgramRunRecords::ChunkSequence, Expr::val(next_head)),
            (
                ProgramRunRecords::AccumulatedOutputBytes,
                Expr::val(output_bytes),
            ),
            (
                ProgramRunRecords::NextEventSequence,
                Expr::col(ProgramRunRecords::NextEventSequence).add(event_count),
            ),
        ])
        .values(run_values)
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(chunk.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(chunk.run_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::ChunkSequence).eq(head));
    if let Some(worker_claim_id) = chunk.worker_claim_id {
        statement.and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(worker_claim_id));
    }
    match chunk.execution_claim_id {
        Some(claim_id) => {
            statement.and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id));
            if let Some(until) = chunk.lease_until_unix_nanos {
                statement.value(
                    ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
                    Expr::case(
                        Expr::col(ProgramRunRecords::WorkerClaimId).eq(claim_id),
                        Expr::val(until),
                    )
                    .finally(Expr::col(ProgramRunRecords::WorkerClaimExpiresAtUnixNanos)),
                );
            }
        }
        None => {
            statement.and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).is_null());
        }
    }
    if transaction.execute(statement).await? != 1 {
        return Err(rejection(transaction, &chunk).await?);
    }
    insert_events(transaction, chunk.workspace_id, chunk.run_id, &chunk.events).await
}

/// Reads why the head update matched no row. Under the Run hold the row cannot
/// change between the update and this read. A missing row counts as a claim
/// mismatch: no row holds the chunk's claim.
async fn rejection(transaction: &mut Transaction<'_>, chunk: &Chunk<'_>) -> Result<Error, Error> {
    let row: Option<(Option<String>, Option<String>, String)> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    ProgramRunRecords::ExecutionClaimId,
                    ProgramRunRecords::WorkerClaimId,
                    ProgramRunRecords::State,
                ])
                .from(ProgramRunRecords::Table)
                .and_where(
                    Expr::col(ProgramRunRecords::WorkspaceId).eq(chunk.workspace_id.to_string()),
                )
                .and_where(Expr::col(ProgramRunRecords::Id).eq(chunk.run_id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(match row {
        Some((claim_id, worker_claim_id, state))
            if claim_id.as_deref() == chunk.execution_claim_id
                && chunk
                    .worker_claim_id
                    .is_none_or(|held| worker_claim_id.as_deref() == Some(held)) =>
        {
            if RunState::from_storage_str(&state)?.is_terminal() {
                Error::RunEnded
            } else {
                Error::HeadMismatch
            }
        }
        Some(_) | None => Error::ClaimMismatch,
    })
}
