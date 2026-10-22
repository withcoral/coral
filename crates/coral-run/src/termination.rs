//! Run termination: a cancellation as a request to the Run Owner, the settle
//! of a recorded termination (the terminal chunk that ends every nonterminal
//! Invocation and the Run), and the Interrupted record.

use std::time::Duration;

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use tokio::time::timeout;

use crate::approval::{ApprovalId, DecideWork};
use crate::lifecycle::{RequestedTermination, RunRecord, RunTermination, RunTerminationKind};
use crate::observation::validate_cursor;
use crate::owner::ChunkWork as _;
use crate::{
    Error, RunId, RunModule, RunObservation, RunState, RunUpdate, invocation_lifecycle,
    invocation_repository, owner, run_repository,
};

/// How much longer than its shutdown grace a cancellation, or the end of the
/// Shutdown Drain, waits for the owner to end the Run: the owner's last
/// settle attempt and its terminal write.
pub(crate) const SETTLE_MARGIN: Duration = Duration::from_secs(1);

impl RunModule {
    /// Asks the Run's owner to cancel it, waits for the owner to end the Run
    /// within the shutdown grace, and returns the Run as recorded.
    ///
    /// The cancellation is a Run Request identified by the Run: the first
    /// termination wins, and a repeat receives the recorded answer. The owner
    /// terminates the Program at once and records the intent, with
    /// `cancellation.requested`, in its next chunk; the request is answered
    /// once that chunk is recorded. A Run already terminal is shown as it is.
    ///
    /// # Errors
    ///
    /// Applies the same concealment and cursor rules as [`Self::observe`].
    /// [`Error::OwnedElsewhere`] when the Run is nonterminal and no owner for
    /// it lives in this process (the expired-lease sweep ends it once its
    /// lease lapses, and a retry is then answered from the record);
    /// [`Error::OwnerLost`] when the owner stopped before it recorded the
    /// request (the outcome is unknown: read again).
    pub async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<RunUpdate, Error> {
        validate_cursor(run_id, observation)?;
        let run = self
            .visible_run_snapshot(actor, workspace_id, run_id)
            .await?;
        if let Some(owner) =
            owner::local_owner(&self.owners, &self.database, workspace_id, &run).await?
        {
            owner.cancel(actor.principal().clone()).await?;
            // The recorded intent gives the dispatches in flight the shutdown
            // grace; the owner ends the Run itself once it is out.
            let grace = Duration::from_nanos(u64::try_from(run.shutdown_grace_nanos).unwrap_or(0));
            let _ended_in_time = timeout(grace + SETTLE_MARGIN, owner.wait_stopped()).await;
        }
        self.observe(actor, workspace_id, run_id, observation).await
    }

    /// The Run as `actor` may see it, from a read snapshot.
    async fn visible_run_snapshot(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<RunRecord, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let include_workspace_agents = self
            .visibility_scope_in_transaction(&mut transaction, actor, workspace_id)
            .await?;
        let run = run_repository::find_visible_in_transaction(
            &mut transaction,
            workspace_id,
            run_id,
            actor.principal(),
            include_workspace_agents,
        )
        .await?
        .ok_or(Error::RunNotFound)?;
        transaction.commit().await?;
        Ok(run)
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
        // Never before a termination already recorded.
        let now = self.now()?.max(run.accepted_at_unix_nanos).max(
            run.termination
                .map_or(run.accepted_at_unix_nanos, |termination| {
                    termination.started_at_unix_nanos
                }),
        );
        // Past the deadline the Run timed out, as every deadline path records.
        let termination = match run.termination {
            Some(termination) => termination,
            None if now >= run.deadline_at_unix_nanos => {
                run.request_termination(RunTerminationKind::TimedOut, now)?
                    .termination
            }
            None => {
                run.request_termination(RunTerminationKind::Interrupted, now)?
                    .termination
            }
        };
        end_in_one_chunk(
            &mut transaction,
            &run,
            &termination,
            &[],
            takeover_claim_id,
            now,
        )
        .await?;
        transaction.commit().await?;
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
        run_id: &RunId,
        claim_id: &str,
        takeover_claim_id: Option<&str>,
    ) -> Result<(), Error> {
        let mut backoff = crate::execution::Backoff::new(run_id, "interruption");
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

/// The one chunk that ends `run`, as read in `transaction`, with
/// `termination`: the `expired` Approval Requests expire first, dated at
/// their deadlines, and end their Invocations; every other nonterminal
/// Invocation then ends (`run_terminated` before its dispatch,
/// `indeterminate` once dispatching) with its Approval Request closed; and
/// `run.ended` follows, the worker claim released and, for a started Run,
/// the execution claim taken over by `takeover_claim_id`. One head move,
/// predicated on the claim and the head `run` shows.
pub(crate) async fn end_in_one_chunk(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    termination: &RunTermination,
    expired: &[ApprovalId],
    takeover_claim_id: Option<&str>,
    ended_at_unix_nanos: i64,
) -> Result<(), Error> {
    let mut events = Vec::new();
    for approval in expired {
        let mut expiry = DecideWork::expiry(approval.clone());
        events.extend(expiry.run(transaction, run, ended_at_unix_nanos).await?);
    }
    events.extend(
        invocation_lifecycle::end_nonterminal_invocations(transaction, run, ended_at_unix_nanos)
            .await?,
    );
    run_repository::interrupt_run(
        transaction,
        run,
        &events,
        termination,
        takeover_claim_id,
        ended_at_unix_nanos,
    )
    .await
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
