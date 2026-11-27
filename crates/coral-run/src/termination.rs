//! Run termination: a cancellation, committed directly under the Run's
//! claim and head, and the one chunk that ends a Run with every nonterminal
//! Invocation (ADR 0027: a termination is written first, with no grace).

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::lifecycle::{RequestedTermination, RunRecord, RunTerminationKind};
use crate::observation::validate_cursor;
use crate::run_repository::{PendingEvent, PendingEventKind};
use crate::trace::RunEnd;
use crate::{
    Error, RunId, RunModule, RunObservation, RunState, RunUpdate, invocation_lifecycle,
    run_repository,
};

impl RunModule {
    /// Cancels the Run and returns it as recorded. For a Run that waits for,
    /// or is held by, a Run Executor, whichever Coral Instance receives the
    /// cancellation ends the Run in one chunk under its claim and head
    /// ([ADR 0027](../../docs/adr/0027-run-executors-compile-and-execute-programs.md)).
    /// The first termination wins, and a repeat receives the recorded answer;
    /// a terminal Run is shown as it is.
    ///
    /// # Errors
    ///
    /// Applies the same concealment and cursor rules as [`Self::observe`].
    /// [`Error::RunRequestNotYetRecorded`] when other writers moved the Run
    /// under every attempt, or for a Run that an older release's Run Owner
    /// still holds: retry.
    pub async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<RunUpdate, Error> {
        validate_cursor(run_id, observation)?;
        // A snapshot answers a caller who may not see the Run, and a terminal
        // Run, without the write lock.
        let mut snapshot = self.database.begin_read_snapshot().await?;
        let run = self
            .visible_run_in_transaction(&mut snapshot, actor, workspace_id, run_id)
            .await?;
        snapshot.commit().await?;
        // The caller may see the Run: this request links to its trace once.
        crate::trace::link_request(run.trace.as_ref());
        if run.runs_on_run_executor() {
            self.cancel_on_run_executor(actor, workspace_id, run_id)
                .await?;
        } else if !run.state.is_terminal() {
            // An older release's Run Owner holds it, and only that owner
            // writes it: its Coral Instance ends the Run when it stops.
            return Err(Error::RunRequestNotYetRecorded);
        }
        self.observe_linking(actor, workspace_id, run_id, observation, false)
            .await
    }

    /// Ends the Run a Run Executor holds as canceled by `actor`, in one
    /// chunk under its claim and head: the Run's stream loop, whichever Coral
    /// Instance holds it, sees the Run end and terminates the Program. Every
    /// nonterminal Invocation ends, and the open Approval Requests close. A Run waiting for a take, or taken and not
    /// started, ends at once too. It compares no clock: the head orders it
    /// against a deadline that the loop or the sweeper writes. A Run that
    /// ended, or whose termination is recorded, writes nothing: the record
    /// answers.
    ///
    /// # Errors
    ///
    /// [`Error::RunRequestNotYetRecorded`] when other writers moved the Run
    /// under every attempt.
    async fn cancel_on_run_executor(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<(), Error> {
        crate::run_wake::until_written(|| self.cancel_once(actor, workspace_id, run_id)).await
    }

    /// One attempt of [`Self::cancel_on_run_executor`]. It reads the Run
    /// without a hold, as the loop's chunk does, so both take their rows in
    /// one order: the Invocation and Approval Request rows, then the Run row.
    async fn cancel_once(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let run = self
            .visible_run_in_transaction(&mut transaction, actor, workspace_id, run_id)
            .await?;
        if run.state.is_terminal() || run.termination.is_some() {
            transaction.commit().await?;
            return Ok(());
        }
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        let cancellation = RequestedTermination::new(
            RunTerminationKind::Canceled,
            Some(actor.principal().clone()),
            now,
        )?;
        commit_end(transaction, &run, Some(cancellation), now).await?;
        self.wakes.wake_for(run_id);
        Ok(())
    }

    /// The Run as `actor` may see it, read in `transaction`.
    async fn visible_run_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<RunRecord, Error> {
        let include_workspace_agents = self
            .visibility_scope_in_transaction(transaction, actor, workspace_id)
            .await?;
        run_repository::find_visible_in_transaction(
            transaction,
            workspace_id,
            run_id,
            actor.principal(),
            include_workspace_agents,
        )
        .await?
        .ok_or(Error::RunNotFound)
    }

    /// Interrupts the Run `claim_id` owns in one chunk: the Program's pending
    /// Invocations end `run_terminated`, dispatching ones `indeterminate`,
    /// their Approval Requests close (a timeout expires them), and the Run
    /// ends `interrupted` (or `timed_out` past its deadline), or with the
    /// termination an older build recorded. A Run stream's loop passes the
    /// termination it `decided` (its deadline, a limit or a lost Program
    /// host, or the end of the Shutdown Drain for a local child), which a
    /// recorded one still precedes. A reopen that finds the Run's Principal
    /// gone, its `Open` broken, or an older build's termination, ends it
    /// too, as [`Interrupter::Reopen`]. The Run row is read without a hold:
    /// the write's predicate on the claim and the head fences it.
    ///
    /// # Errors
    ///
    /// [`Error::ClaimMismatch`] when the Run row no longer holds `claim_id`
    /// (another writer took the Run) or, for an [`Interrupter::Loop`]
    /// on a Run a Run Executor holds, names another
    /// Coral Instance as its stream's holder (its stream reopened there),
    /// [`Error::RunEnded`] when the Run is
    /// terminal already; neither writes anything. An operational error leaves
    /// the outcome unknown, and the next attempt's read reads which it was:
    /// `attempted` carries the end an earlier attempt of this interruption
    /// wrote, as the attempt sets it before its commit.
    pub(crate) async fn interrupt_owned_run(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        claim_id: &str,
        decided: Option<&RequestedTermination>,
        by: Interrupter,
        attempted: &mut Option<RunTerminationKind>,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let run = run_repository::find_exact_in_transaction(&mut transaction, workspace_id, run_id)
            .await?
            .ok_or(Error::ClaimMismatch)?;
        if run.state.is_terminal() {
            // An earlier attempt of this very interruption may have landed
            // without an answer: the row then carries the claim it wrote and
            // the outcome it chose. Only that attempt knows which end it
            // chose, the decided one or one an older build recorded, since
            // an older build also records the end of a Run it ended itself.
            let recorded_by_us = run.execution_claim_id.as_deref() == Some(claim_id)
                && attempted.map_or(
                    matches!(run.state, RunState::Interrupted | RunState::TimedOut),
                    |kind| run.terminal_outcome == Some(kind.outcome()),
                );
            return if recorded_by_us {
                Ok(())
            } else {
                Err(Error::RunEnded)
            };
        }
        if run.held_claim() != Some(claim_id) {
            return Err(Error::ClaimMismatch);
        }
        // A Run Executor's Run keeps its claim when its stream reopens on
        // another Coral Instance, which records itself as the holder in the
        // chunk that moves the head: the head fences a reopen after this
        // read, and the holder one before it.
        if by == Interrupter::Loop
            && run.run_executor_kind.is_some()
            && run.holder_instance_id.as_ref() != Some(&self.instance)
        {
            return Err(Error::ClaimMismatch);
        }
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        let (kind, ended_at) =
            end_in_one_chunk(&mut transaction, &run, decided.cloned(), now).await?;
        *attempted = Some(kind);
        commit_ended(transaction, &run, kind, ended_at).await
    }

    /// [`Self::interrupt_owned_run`] until it lands: the last write a Run
    /// gets, so every answer but a claim mismatch or an ended Run is followed
    /// by another attempt after a bounded backoff (a rejection included,
    /// which a chunk commit would not retry), and the next read reads whether
    /// the previous one landed. Never ends on the clock.
    pub(crate) async fn record_interruption(
        &self,
        workspace_id: &WorkspaceId,
        held: &crate::types::HeldRunId,
        claim_id: &str,
        decided: Option<&RequestedTermination>,
        by: Interrupter,
    ) -> Result<(), Error> {
        let run_id = held.run_id();
        let mut backoff = crate::execution::Backoff::new(held, "interruption");
        let mut attempted = None;
        loop {
            match self
                .interrupt_owned_run(workspace_id, run_id, claim_id, decided, by, &mut attempted)
                .await
            {
                Err(fenced @ (Error::ClaimMismatch | Error::RunEnded)) => return Err(fenced),
                Err(error) => backoff.wait(&error).await,
                Ok(()) => return Ok(()),
            }
        }
    }
}

/// The one chunk that ends `run`, as read in `transaction`. The Run ends
/// with the termination an older build recorded on its row, else the one
/// the writer `decided` (a Principal's cancellation, with its
/// `cancellation.requested`, or a Run stream loop's end), else timed out at
/// `now_unix_nanos` past its deadline (as every deadline path records), else
/// interrupted then. Every nonterminal Invocation ends (`run_terminated`
/// before its dispatch, `indeterminate` once dispatching, `run_terminated`
/// for a Tool whose body ran in the Program) with its Approval
/// Request closed (a timeout expires it and fails the Invocation
/// `approval_expired`), then `run.ended`, never before the termination. One
/// head move, predicated on the claim and the
/// head `run` shows. Returns the termination it wrote and the Run's end
/// time, never before that termination started.
pub(crate) async fn end_in_one_chunk(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    decided: Option<RequestedTermination>,
    now_unix_nanos: i64,
) -> Result<(RunTerminationKind, i64), Error> {
    let mut events = Vec::new();
    let termination = match (run.termination, decided) {
        (Some(recorded), _) => RequestedTermination::recorded(recorded),
        (None, Some(decided)) => {
            events.extend(decided.requester.clone().map(|requester| {
                PendingEvent::new(
                    PendingEventKind::CancellationRequested(requester),
                    decided.termination.started_at_unix_nanos,
                )
            }));
            decided
        }
        (None, None) if run.deadline_passed(now_unix_nanos) => {
            RequestedTermination::unrequested(RunTerminationKind::TimedOut, now_unix_nanos)?
        }
        (None, None) => {
            RequestedTermination::unrequested(RunTerminationKind::Interrupted, now_unix_nanos)?
        }
    };
    let ended_at_unix_nanos = now_unix_nanos.max(termination.termination.started_at_unix_nanos);
    events.extend(
        invocation_lifecycle::end_nonterminal_invocations(
            transaction,
            run,
            termination.termination.kind,
            ended_at_unix_nanos,
        )
        .await?,
    );
    run_repository::interrupt_run(transaction, run, &events, &termination, ended_at_unix_nanos)
        .await?;
    Ok((termination.termination.kind, ended_at_unix_nanos))
}

/// Who writes an interruption: it decides how a Run that a Run Executor
/// holds fences it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interrupter {
    /// A Run stream's loop. It writes its Run only while the row names its
    /// Coral Instance as the stream's holder: a reopen elsewhere keeps the
    /// claim.
    Loop,
    /// A reopen that found the Run's Principal gone, its `Open` broken, or
    /// an older build's termination. It read the row just now and does not
    /// hold the stream yet, so the claim and the head fence it.
    Reopen,
}

/// [`end_in_one_chunk`], committed, and the
/// Run's end traced (a Run that never started as unstarted).
pub(crate) async fn commit_end(
    mut transaction: Transaction<'_>,
    run: &RunRecord,
    decided: Option<RequestedTermination>,
    now_unix_nanos: i64,
) -> Result<(), Error> {
    let (kind, ended_at) = end_in_one_chunk(&mut transaction, run, decided, now_unix_nanos).await?;
    commit_ended(transaction, run, kind, ended_at).await
}

/// Commits the chunk [`end_in_one_chunk`] built in `transaction`, and traces
/// the Run's end (a Run that never started as unstarted).
async fn commit_ended(
    transaction: Transaction<'_>,
    run: &RunRecord,
    kind: RunTerminationKind,
    ended_at: i64,
) -> Result<(), Error> {
    transaction.commit().await?;
    let outcome = kind.outcome();
    let end = if run.execution_started() {
        RunEnd::Stopped {
            outcome: &outcome,
            reason: kind,
        }
    } else {
        RunEnd::Unstarted(&outcome)
    };
    crate::trace::record_run_end(run, &end, ended_at);
    Ok(())
}
