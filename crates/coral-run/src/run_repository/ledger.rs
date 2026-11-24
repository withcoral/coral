//! The Run Ledger head and the one chunk-commit write path (ADR 0018, kept by ADR 0027).
//!
//! A chunk is everything one Run write carries: its Events, the Run Output
//! Budget total after them, and its commit time.
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
    /// The execution claim id the chunk was built on. `None` before a take:
    /// acceptance leaves the claim unset.
    execution_claim_id: Option<&'a str>,
    /// The Run Ledger head the chunk was built on.
    head: u64,
    /// The writer's own time for this commit, which the row records so a
    /// reader can show how long the Run has been silent.
    committed_at_unix_nanos: i64,
    events: Vec<ChunkEvent<'a>>,
    /// The Run Output Budget total after this chunk, written as a plain value.
    output_bytes: usize,
    /// The termination intent this chunk records in its head update.
    termination: Option<&'a RequestedTermination>,
    /// The highest Run stream request number committed with this chunk.
    request_number: Option<u64>,
}

impl<'a> Chunk<'a> {
    /// Starts a chunk on `run` as the writer read it, predicated on
    /// `execution_claim_id`: the claim the take gave the Run, or `None` for a
    /// Run no take claimed. It commits at `committed_at_unix_nanos`.
    pub(crate) fn new(
        run: &'a RunRecord,
        execution_claim_id: Option<&'a str>,
        committed_at_unix_nanos: i64,
    ) -> Self {
        Self {
            workspace_id: &run.workspace_id,
            run_id: &run.id,
            execution_claim_id,
            head: run.chunk_sequence,
            committed_at_unix_nanos,
            events: Vec::new(),
            output_bytes: run.accumulated_output_bytes,
            termination: None,
            request_number: None,
        }
    }

    /// Records `number` as the highest Run stream request number committed.
    pub(crate) const fn request_number(mut self, number: u64) -> Self {
        self.request_number = Some(number);
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

    /// Records `intent` in this chunk's head update, a cancellation's beside
    /// its `cancellation.requested` Event.
    pub(crate) const fn termination(mut self, intent: &'a RequestedTermination) -> Self {
        self.termination = Some(intent);
        self
    }

    #[cfg(test)]
    pub(crate) const fn built_on_head(mut self, head: u64) -> Self {
        self.head = head;
        self
    }
}

/// Commits `chunk` in `transaction`: asserts the claim, the head and a running
/// Run, moves the head by one, writes the output total, the commit time, the
/// termination intent the chunk carries, and appends the
/// Events after the Run's last one. The caller commits the transaction.
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
/// the head update: the terminal outcome.
pub(super) async fn commit_chunk_with(
    transaction: &mut Transaction<'_>,
    chunk: Chunk<'_>,
    run_values: Vec<(ProgramRunRecords, Expr)>,
) -> Result<(), Error> {
    let mut run_values = run_values;
    if let Some(intent) = chunk.termination {
        run_values.extend(request_values(intent));
    }
    if let Some(number) = chunk.request_number {
        let number = i64::try_from(number)
            .map_err(|_error| Error::CorruptData("Run request number overflowed".to_string()))?;
        run_values.push((ProgramRunRecords::CommittedRequestNumber, Expr::val(number)));
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
            (
                ProgramRunRecords::LastCommitAtUnixNanos,
                Expr::val(chunk.committed_at_unix_nanos),
            ),
        ])
        .values(run_values)
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(chunk.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(chunk.run_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::ChunkSequence).eq(head));
    match chunk.execution_claim_id {
        Some(claim_id) => {
            statement.and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id));
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
    let row: Option<(Option<String>, String)> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    ProgramRunRecords::ExecutionClaimId,
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
        Some((claim_id, state)) if claim_id.as_deref() == chunk.execution_claim_id => {
            if RunState::from_storage_str(&state)?.is_terminal() {
                Error::RunEnded
            } else {
                Error::HeadMismatch
            }
        }
        Some(_) | None => Error::ClaimMismatch,
    })
}
