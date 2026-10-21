//! Run termination: a cancellation as a request to the Run Owner, the settle
//! of a recorded termination (the terminal chunk that ends every nonterminal
//! Invocation and the Run), the Interrupted record, and recovery's explicit
//! pass.

use std::future::Future;
use std::time::Duration;

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use tokio::time::timeout;

use crate::lifecycle::{RequestedTermination, RunRecord, RunTerminationKind};
use crate::observation::validate_cursor;
use crate::{
    Error, RunId, RunModule, RunObservation, RunState, RunUpdate, duration_until,
    invocation_lifecycle, invocation_repository, owner, run_repository,
};

/// How much longer than its shutdown grace a cancellation waits for the
/// owner to end the Run before it shows the Run as it stands: the owner's
/// last settle attempt and its terminal write.
const SETTLE_MARGIN: Duration = Duration::from_secs(1);

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
    /// it lives in this process; [`Error::OwnerLost`] when the owner stopped
    /// before it recorded the request (the outcome is unknown: read again).
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

    /// Recovers expired durable worker claims without replaying started JavaScript.
    ///
    /// This is a host-maintenance operation rather than part of the MCP or Reef
    /// interface. It performs one explicit reconciliation pass. Expired
    /// pre-execution claims become eligible for another worker, but this method
    /// does not schedule that work and does not install startup or polling
    /// behavior. Lost started executions have their claim revoked before pending
    /// children become Run-terminated, uncertain dispatches become indeterminate,
    /// and the Run becomes interrupted. Until the expired-lease sweep (slice 9)
    /// replaces it, this is the one writer that touches a Run row from outside
    /// its owner; a live owner sees its write as a released worker claim at
    /// its next commit and stops.
    ///
    /// # Errors
    ///
    /// Returns a clock, persistence, or corrupt durable-state failure.
    pub async fn recover(&self) -> Result<(), Error> {
        loop {
            let now = self.now()?;
            let recoverable = run_repository::recoverable_runs(self.database.as_ref(), now).await?;
            if recoverable.is_empty() {
                return Ok(());
            }
            for candidate in recoverable {
                self.recover_run(&candidate.workspace_id, &candidate.id)
                    .await?;
            }
        }
    }

    /// Recovery's pass over one Run, as the sweep and the deadline task of a
    /// retired owner run it.
    pub(crate) async fn recover_run(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(mut run) =
            run_repository::hold_run(&mut transaction, workspace_id, run_id).await?
        else {
            return Ok(());
        };
        if run.state != RunState::Running {
            transaction.commit().await?;
            return Ok(());
        }
        let now_unix_nanos = self.now()?.max(run.accepted_at_unix_nanos);
        if run.termination.is_none() && now_unix_nanos >= run.deadline_at_unix_nanos {
            run = self
                .request_termination(
                    &mut transaction,
                    &run,
                    RunTerminationKind::TimedOut,
                    now_unix_nanos.max(run.accepted_at_unix_nanos),
                )
                .await?;
        } else if run.termination.is_none()
            && run
                .worker_claim_expires_at_unix_nanos
                .is_some_and(|expires_at| expires_at <= now_unix_nanos)
        {
            if !run.execution_started() {
                let _released =
                    run_repository::release_unstarted_worker_claim(&mut transaction, &run).await?;
                transaction.commit().await?;
                return Ok(());
            }
            run = self
                .request_termination(
                    &mut transaction,
                    &run,
                    RunTerminationKind::Interrupted,
                    now_unix_nanos.max(run.accepted_at_unix_nanos),
                )
                .await?;
        }
        if run.termination.is_none() {
            transaction.commit().await?;
            return Ok(());
        }
        if !run.execution_started() {
            self.finish_termination_in_transaction(
                &mut transaction,
                &run,
                now_unix_nanos.max(run.accepted_at_unix_nanos),
            )
            .await?;
            transaction.commit().await?;
            return Ok(());
        }
        let shutdown = ShutdownAction::from_run(&run)?;
        transaction.commit().await?;
        self.complete_shutdown(shutdown).await
    }

    /// Recovery's settle of the termination it recorded: stops the Run's
    /// owner when this process holds it and waits for the owner to retire,
    /// then ends the Run within the shutdown grace. Without a local owner
    /// the Run is settled elsewhere; this waits for it, and ends the Run
    /// itself once the grace is out.
    pub(crate) async fn complete_shutdown(&self, shutdown: ShutdownAction) -> Result<(), Error> {
        // A live owner here ends its Run itself, within its own shutdown
        // grace, and recovery writes only once it retired: never beside it.
        let owned_here = self.owners.find(&shutdown.run_id, &shutdown.claim_id);
        if let Some(owner) = &owned_here {
            owner.request_stop();
            owner.wait_stopped().await;
        }
        let now = self.now()?.max(shutdown.accepted_at_unix_nanos);
        let remaining = duration_until(shutdown.deadline_at_unix_nanos, now);
        let drain = async {
            if owned_here.is_some() {
                self.settle_loop(&shutdown).await
            } else {
                self.wait_for_terminal_state(&shutdown.workspace_id, &shutdown.run_id)
                    .await
            }
        };
        self.settle_or_force(remaining, &shutdown, drain).await
    }

    async fn settle_or_force(
        &self,
        remaining: Duration,
        shutdown: &ShutdownAction,
        drain: impl Future<Output = Result<(), Error>>,
    ) -> Result<(), Error> {
        match timeout(remaining, drain).await {
            Ok(result) => result,
            Err(_elapsed) => self.settle_termination(shutdown, true).await.map(drop),
        }
    }

    /// Stopping V8 does not settle detached Invocation tasks. A dispatched
    /// Invocation gets the remainder of the shutdown grace to record its
    /// upstream result. An undispatched one can no longer write, so
    /// termination ends it without waiting.
    async fn settle_loop(&self, shutdown: &ShutdownAction) -> Result<(), Error> {
        while !self.settle_termination(shutdown, false).await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    async fn wait_for_terminal_state(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<(), Error> {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut transaction = self.database.begin_read_snapshot().await?;
            let terminal =
                run_repository::find_exact_in_transaction(&mut transaction, workspace_id, run_id)
                    .await?
                    .is_none_or(|run| run.state.is_terminal());
            transaction.commit().await?;
            if terminal {
                return Ok(());
            }
        }
    }

    /// One settle attempt of a recorded termination: every nonterminal
    /// Invocation and `run.ended` in one terminal chunk, once no dispatch is
    /// in flight or `grace_expired` (judged by the caller on its own clock).
    /// `Ok(true)` once the Run is terminal, by this write or another's;
    /// `Ok(false)` while a dispatch is still in flight within the grace. The
    /// Run row is read without a hold: the owner runs its writes one at a
    /// time, and recovery's settle runs after the owner here retired; a head
    /// another writer moved meanwhile, or an Invocation it ended first, makes
    /// the next attempt read the row again.
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
        let held_claim = run
            .execution_claim_id
            .as_deref()
            .or(run.worker_claim_id.as_deref());
        if held_claim != Some(claim_id) {
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
        let events =
            invocation_lifecycle::end_nonterminal_invocations(&mut transaction, &run, now).await?;
        run_repository::interrupt_run(
            &mut transaction,
            &run,
            &events,
            &termination,
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

/// A recorded termination to settle: which Run, under which claim, and
/// until when its dispatches in flight may still record their results.
#[derive(Debug, Clone)]
pub(crate) struct ShutdownAction {
    run_id: RunId,
    workspace_id: WorkspaceId,
    claim_id: String,
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
            run_id: RunId::generate(),
            workspace_id: WorkspaceId::parse(&uuid::Uuid::nil().to_string())
                .expect("the nil UUID parses"),
            claim_id: "claim".to_string(),
            accepted_at_unix_nanos: 0,
            deadline_at_unix_nanos: i64::MAX,
            head: 0,
        }
    }

    pub(crate) fn from_run(run: &RunRecord) -> Result<Self, Error> {
        let termination = run.termination.ok_or_else(|| {
            Error::CorruptData("started Run shutdown has no termination evidence".to_string())
        })?;
        let claim_id = run.execution_claim_id.clone().ok_or_else(|| {
            Error::CorruptData("started Run shutdown has no execution claim".to_string())
        })?;
        Ok(Self {
            run_id: run.id.clone(),
            workspace_id: run.workspace_id.clone(),
            claim_id,
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
