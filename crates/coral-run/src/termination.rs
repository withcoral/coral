//! Run termination: a cancellation through the Run Inbox, the settle
//! of a recorded termination (the terminal chunk that ends every nonterminal
//! Invocation and the Run), and the Interrupted record.

use std::time::Duration;

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use tokio::time::Instant;

use crate::lifecycle::{RequestedTermination, RunRecord, RunTerminationKind};
use crate::observation::read_until;
use crate::observation::validate_cursor;
use crate::run_inbox::{InboxRequest, OutsideRequest};
use crate::run_repository::{OwnedEvent, OwnedEventKind};
use crate::trace::RunEnd;
use crate::{
    Error, RunId, RunModule, RunObservation, RunState, RunTerminalOutcome, RunUpdate,
    invocation_lifecycle, invocation_repository, run_repository,
};

/// How much longer than its shutdown grace a cancellation, or the end of the
/// Shutdown Drain, waits for the owner to end the Run: the owner's last
/// settle attempt and its terminal write.
pub(crate) const SETTLE_MARGIN: Duration = Duration::from_secs(1);

impl RunModule {
    /// Puts a cancellation in the Run's Run Inbox, whichever Coral Instance
    /// owns the Run, waits for its owner to record it and end the Run, and
    /// returns the Run as recorded.
    ///
    /// The cancellation is a Run Request identified by the Run: the first
    /// termination wins, and a repeat receives the recorded answer. The owner
    /// terminates the Program at once and records the intent, with
    /// `cancellation.requested`, in its next chunk, which deletes the row.
    /// The wait is bounded by the Run's shutdown grace and the settle margin,
    /// from the call: a Run still ending then is shown as recorded. A Run
    /// whose termination is recorded already puts nothing in the Run Inbox,
    /// and a terminal Run is shown as it is.
    ///
    /// # Errors
    ///
    /// Applies the same concealment and cursor rules as [`Self::observe`].
    /// [`Error::RunRequestNotYetRecorded`] when the owner has not recorded
    /// the cancellation within the wait: retry.
    pub async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<RunUpdate, Error> {
        let arrived = Instant::now();
        validate_cursor(run_id, observation)?;
        let request = InboxRequest {
            workspace_id: workspace_id.clone(),
            run_id: run_id.clone(),
            request: OutsideRequest::Cancel(actor.principal().clone()),
        };
        // A snapshot answers a caller who may not see the Run, and a terminal
        // Run, without the write lock.
        let mut snapshot = self.database.begin_read_snapshot().await?;
        let run = self
            .visible_run_in_transaction(&mut snapshot, actor, workspace_id, run_id)
            .await?;
        snapshot.commit().await?;
        // The caller may see the Run: this request links to its trace once,
        // whether or not the owner records it within the wait.
        crate::trace::link_request(run.trace.as_ref());
        if !run.state.is_terminal() {
            // The caller's access is read again in the transaction that
            // inserts the row, which holds no Run row.
            let mut transaction = self.database.begin().await?;
            self.visible_run_in_transaction(&mut transaction, actor, workspace_id, run_id)
                .await?;
            self.inbox
                .submit(&mut transaction, &request, self.now()?)
                .await?;
            transaction.commit().await?;
            self.inbox.wake_for(run_id);
            // The recorded intent gives the dispatches in flight the shutdown
            // grace; the owner ends the Run itself once it is out.
            let until = arrived + crate::nanos_duration(run.shutdown_grace_nanos) + SETTLE_MARGIN;
            if !self.inbox.recorded(&request, until).await? {
                return Err(Error::RunRequestNotYetRecorded);
            }
            let database = &self.database;
            read_until(database, until, move || async move {
                Ok(run_repository::find_exact(database, workspace_id, run_id)
                    .await?
                    .is_none_or(|run| run.state.is_terminal()))
            })
            .await?;
        }
        self.observe_linking(actor, workspace_id, run_id, observation, false)
            .await
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

    /// One settle attempt of a recorded termination: every nonterminal
    /// Invocation and `run.ended` in one terminal chunk, once no dispatch is
    /// in flight or `grace_expired` (judged by the caller on its own clock).
    /// `Ok(true)` once the Run is terminal, by this write or another's;
    /// `Ok(false)` while a dispatch is still in flight within the grace. The
    /// Run row is read without a hold: the owner runs its writes one at a
    /// time; a head another writer moved meanwhile (another process that
    /// took the Run over), or an Invocation it ended first, makes the next
    /// attempt read the row again.
    pub(crate) async fn settle_termination(
        &self,
        shutdown: &ShutdownAction,
        grace_expired: bool,
    ) -> Result<bool, Error> {
        let (workspace_id, run_id) = (&shutdown.workspace_id, &shutdown.run_id);
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::find_exact_in_transaction(&mut transaction, workspace_id, run_id)
                .await?
        else {
            return Ok(true);
        };
        if run.state == RunState::Running && run.termination.is_some() {
            if !grace_expired
                && invocation_repository::has_dispatching_for_run(
                    &mut transaction,
                    workspace_id,
                    run_id,
                )
                .await?
            {
                transaction.commit().await?;
                return Ok(false);
            }
            let termination_started_at = run
                .termination
                .map_or(run.accepted_at_unix_nanos, |termination| {
                    termination.started_at_unix_nanos
                });
            let terminal_at = self
                .now()?
                .max(run.accepted_at_unix_nanos)
                .max(termination_started_at);
            match self
                .finish_termination_in_transaction(&mut transaction, &run, terminal_at)
                .await
            {
                Ok(()) => {}
                // Another writer ended the Run, or moved its head, between
                // the read and the write: the next attempt reads it again.
                Err(Error::RunEnded) => return Ok(true),
                Err(Error::HeadMismatch) => return Ok(false),
                Err(error) => return Err(error),
            }
            transaction.commit().await?;
            if let Some(termination) = run.termination {
                crate::trace::record_run_end(
                    &run,
                    &RunEnd::Stopped {
                        outcome: &termination.kind.outcome(),
                        reason: termination.kind,
                    },
                    terminal_at,
                );
            }
            return Ok(true);
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub(crate) async fn request_termination(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        kind: RunTerminationKind,
        started_at_unix_nanos: i64,
    ) -> Result<RunRecord, Error> {
        let request = run.request_termination(kind, started_at_unix_nanos)?;
        self.record_termination(transaction, run, &request).await
    }

    /// Records `intent` on the Run row and returns the row as recorded.
    pub(crate) async fn record_termination(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        intent: &RequestedTermination,
    ) -> Result<RunRecord, Error> {
        run_repository::request_termination(transaction, run, intent).await?;
        run_repository::find_exact_in_transaction(transaction, &run.workspace_id, &run.id)
            .await?
            .ok_or(Error::RunNotFound)
    }

    /// The terminal chunk of a recorded termination: every nonterminal
    /// Invocation ended, their `invocation.ended` Events, and `run.ended`,
    /// in one head move.
    pub(crate) async fn finish_termination_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        terminal_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        let termination = run
            .termination
            .ok_or_else(|| Error::CorruptData("Run has no termination to finish".into()))?;
        let events = invocation_lifecycle::end_nonterminal_invocations(
            transaction,
            run,
            terminal_at_unix_nanos,
        )
        .await?;
        run_repository::finish_termination(
            transaction,
            run,
            &events,
            &termination.kind.outcome(),
            terminal_at_unix_nanos,
        )
        .await
    }

    /// Interrupts the Run `claim_id` owns in one chunk: the Program's pending
    /// Invocations end `run_terminated`, dispatching ones `indeterminate`,
    /// their Approval Requests close, and the Run ends `interrupted` (or
    /// `timed_out` past its deadline), or with the termination already
    /// recorded (a cancellation wins). The self-interrupt path of a Run
    /// Owner that cannot commit, and of the supervisor that takes over a Run
    /// whose owner task ended without retiring; `takeover_claim_id` is the
    /// claim the supervisor mints so that nothing still in flight under
    /// `claim_id` can land afterwards. The Run row is read without a hold:
    /// the write's predicate on the claim and the head fences it.
    ///
    /// # Errors
    ///
    /// [`Error::ClaimMismatch`] when the Run row no longer holds `claim_id`
    /// (another writer took the Run), [`Error::RunEnded`] when the Run is
    /// terminal already; neither writes anything. An operational error leaves
    /// the outcome unknown, and the next attempt's read reads which it was.
    pub(crate) async fn interrupt_owned_run(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        claim_id: &str,
        takeover_claim_id: Option<&str>,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let run = run_repository::find_exact_in_transaction(&mut transaction, workspace_id, run_id)
            .await?
            .ok_or(Error::ClaimMismatch)?;
        if run.state.is_terminal() {
            // An earlier attempt of this very interruption may have landed
            // without an answer: the row then carries the claim it wrote and
            // the outcome it chose.
            let recorded_by_us = matches!(run.state, RunState::Interrupted | RunState::TimedOut)
                && run.execution_claim_id.as_deref() == Some(takeover_claim_id.unwrap_or(claim_id));
            return if recorded_by_us {
                Ok(())
            } else {
                Err(Error::RunEnded)
            };
        }
        if run.held_claim() != Some(claim_id) {
            return Err(Error::ClaimMismatch);
        }
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        let ended = end_in_one_chunk(
            &mut transaction,
            &run,
            None,
            Vec::new(),
            takeover_claim_id.map(|claim_id| run_repository::Takeover {
                claim_id,
                instance: &self.instance,
            }),
            now,
        )
        .await?;
        transaction.commit().await?;
        crate::trace::record_run_end(
            &run,
            &RunEnd::Stopped {
                outcome: &ended,
                reason: RunTerminationKind::Interrupted,
            },
            now,
        );
        Ok(())
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
        takeover_claim_id: Option<&str>,
    ) -> Result<(), Error> {
        let run_id = held.run_id();
        let mut backoff = crate::execution::Backoff::new(held, "interruption");
        loop {
            match self
                .interrupt_owned_run(workspace_id, run_id, claim_id, takeover_claim_id)
                .await
            {
                Err(fenced @ (Error::ClaimMismatch | Error::RunEnded)) => return Err(fenced),
                Err(error) => backoff.wait(&error).await,
                Ok(()) => return Ok(()),
            }
        }
    }
}

/// The one chunk that ends `run`, as read in `transaction`. `events`, from
/// work the caller already ran in `transaction`, come first. The Run ends
/// with the termination its row records, else the `cancellation` the
/// writer applied, with its `cancellation.requested`, else timed out at
/// `now_unix_nanos` past its deadline (as every deadline path records), else
/// interrupted then. Every nonterminal Invocation ends (`run_terminated`
/// before its dispatch, `indeterminate` once dispatching) with its Approval
/// Request closed, then `run.ended`, never before the termination. The
/// worker claim is released and, for a started Run, the execution claim
/// taken over by `takeover`. One head move, predicated on the claim and the
/// head `run` shows. Returns the terminal state it wrote.
pub(crate) async fn end_in_one_chunk(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    cancellation: Option<RequestedTermination>,
    mut events: Vec<OwnedEvent>,
    takeover: Option<run_repository::Takeover<'_>>,
    now_unix_nanos: i64,
) -> Result<RunTerminalOutcome, Error> {
    let termination = match (run.termination, cancellation) {
        (Some(recorded), _) => RequestedTermination::recorded(recorded),
        (None, Some(cancellation)) => {
            events.extend(cancellation.requester.clone().map(|requester| {
                OwnedEvent::new(
                    OwnedEventKind::CancellationRequested(requester),
                    cancellation.termination.started_at_unix_nanos,
                )
            }));
            cancellation
        }
        (None, None) if now_unix_nanos >= run.deadline_at_unix_nanos => {
            run.request_termination(RunTerminationKind::TimedOut, now_unix_nanos)?
        }
        (None, None) => run.request_termination(RunTerminationKind::Interrupted, now_unix_nanos)?,
    };
    let ended_at_unix_nanos = now_unix_nanos.max(termination.termination.started_at_unix_nanos);
    events.extend(
        invocation_lifecycle::end_nonterminal_invocations(transaction, run, ended_at_unix_nanos)
            .await?,
    );
    run_repository::interrupt_run(
        transaction,
        run,
        &events,
        &termination,
        takeover,
        ended_at_unix_nanos,
    )
    .await?;
    Ok(termination.termination.kind.outcome())
}

/// A recorded termination to settle: which Run, and until when its
/// dispatches in flight may still record their results.
#[derive(Debug, Clone)]
pub(crate) struct ShutdownAction {
    run_id: RunId,
    workspace_id: WorkspaceId,
    accepted_at_unix_nanos: i64,
    deadline_at_unix_nanos: i64,
    /// The Run Ledger head as the terminal write left it: the one the owner
    /// goes on from while it settles.
    head: u64,
}

impl ShutdownAction {
    /// A shutdown for the Run Owner's in-memory tests, which never settle it.
    #[cfg(test)]
    pub(crate) fn for_apply_test() -> Self {
        Self {
            run_id: RunId::generate().expect("the random source is available"),
            workspace_id: WorkspaceId::parse(&uuid::Uuid::nil().to_string())
                .expect("the nil UUID parses"),
            accepted_at_unix_nanos: 0,
            deadline_at_unix_nanos: i64::MAX,
            head: 0,
        }
    }

    pub(crate) fn from_run(run: &RunRecord) -> Result<Self, Error> {
        let termination = run.termination.ok_or_else(|| {
            Error::CorruptData("started Run shutdown has no termination evidence".to_string())
        })?;
        Ok(Self {
            run_id: run.id.clone(),
            workspace_id: run.workspace_id.clone(),
            accepted_at_unix_nanos: run.accepted_at_unix_nanos,
            deadline_at_unix_nanos: termination.deadline_at_unix_nanos,
            head: run.chunk_sequence,
        })
    }

    /// The Run Ledger head the terminal write left.
    pub(crate) const fn head(&self) -> u64 {
        self.head
    }

    pub(crate) const fn accepted_at_unix_nanos(&self) -> i64 {
        self.accepted_at_unix_nanos
    }

    /// When the shutdown grace of the recorded termination ends.
    pub(crate) const fn deadline_at_unix_nanos(&self) -> i64 {
        self.deadline_at_unix_nanos
    }
}
