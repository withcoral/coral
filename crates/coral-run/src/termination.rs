//! Run termination: durable cancellation, deadline and recovery intent, bounded
//! child shutdown, and the terminal write that settles pending Invocations.

use std::time::Duration;

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, Principal, WorkspaceId};
use tokio::time::timeout;

use crate::lifecycle::{RunRecord, RunTerminationKind};
use crate::observation::validate_cursor;
use crate::types::AcceptedRun;
use crate::{
    Error, RunId, RunModule, RunObservation, RunState, RunUpdate, duration_until,
    invocation_lifecycle, invocation_repository, run_repository,
};

impl RunModule {
    /// Durably requests cancellation and returns the first terminal winner.
    ///
    /// Cancellation and completion serialize on the Run record. Cancellation
    /// intent is durable before V8 termination and bounded child shutdown; a
    /// completion that already won remains unchanged.
    ///
    /// # Errors
    ///
    /// Applies the same concealment and cursor rules as [`Self::observe`].
    pub async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<RunUpdate, Error> {
        validate_cursor(run_id, observation)?;
        let mut transaction = self.database.begin().await?;
        let Some(_) = run_repository::hold_run(&mut transaction, workspace_id, run_id).await?
        else {
            return Err(Error::RunNotFound);
        };
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
        let shutdown = if run.state == RunState::Running {
            let requested_at = self.now()?.max(run.accepted_at_unix_nanos);
            let run = if run.termination.is_none() {
                let (kind, requester) = if requested_at >= run.deadline_at_unix_nanos {
                    (RunTerminationKind::TimedOut, None)
                } else {
                    (RunTerminationKind::Canceled, Some(actor.principal()))
                };
                self.request_termination(&mut transaction, &run, kind, requester, requested_at)
                    .await?
            } else {
                run
            };
            if run.execution_started() {
                Some(ShutdownAction::from_run(&run)?)
            } else {
                self.finish_termination_in_transaction(&mut transaction, &run, requested_at)
                    .await?;
                None
            }
        } else {
            None
        };
        transaction.commit().await?;
        if let Some(shutdown) = shutdown {
            self.complete_shutdown(shutdown).await?;
        }
        self.observe(actor, workspace_id, run_id, observation).await
    }

    /// Recovers expired durable worker claims without replaying started JavaScript.
    ///
    /// This is a host-maintenance operation rather than part of the MCP or Reef
    /// interface. It performs one explicit reconciliation pass. Expired
    /// pre-execution claims become eligible for another worker, but this method
    /// does not schedule that work and does not install startup or polling
    /// behavior. Lost started executions have their claim revoked before pending
    /// children become Run-terminated, uncertain dispatches become indeterminate,
    /// and the Run becomes interrupted.
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

    async fn recover_run(&self, workspace_id: &WorkspaceId, run_id: &RunId) -> Result<(), Error> {
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
                    None,
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
                    None,
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

    pub(crate) async fn begin_deadline_shutdown(
        &self,
        accepted: &AcceptedRun,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
                .await?
        else {
            return Ok(());
        };
        if run.state != RunState::Running {
            transaction.commit().await?;
            return Ok(());
        }
        let now = self
            .now()?
            .max(run.accepted_at_unix_nanos)
            .max(run.deadline_at_unix_nanos);
        let run = if run.termination.is_none() {
            self.request_termination(
                &mut transaction,
                &run,
                RunTerminationKind::TimedOut,
                None,
                now,
            )
            .await?
        } else {
            run
        };
        let shutdown = if run.execution_started() {
            Some(ShutdownAction::from_run(&run)?)
        } else {
            self.finish_termination_in_transaction(&mut transaction, &run, now)
                .await?;
            None
        };
        transaction.commit().await?;
        if let Some(shutdown) = shutdown {
            self.complete_shutdown(shutdown).await?;
        }
        Ok(())
    }

    pub(crate) async fn complete_shutdown(&self, shutdown: ShutdownAction) -> Result<(), Error> {
        let now = self.now()?.max(shutdown.accepted_at_unix_nanos);
        let remaining = duration_until(shutdown.deadline_at_unix_nanos, now);
        let drain = async {
            if let Some(active) = self
                .active_executions
                .find(&shutdown.run_id, &shutdown.claim_id)
            {
                active.request_stop();
                active.wait_stopped().await;
            } else {
                return self
                    .wait_for_terminal_state(&shutdown.workspace_id, &shutdown.run_id)
                    .await;
            }
            // Stopping V8 does not settle detached Invocation tasks. A dispatched
            // Invocation gets the remainder of the shutdown grace to record its
            // upstream result. An undispatched one can no longer write, so
            // termination ends it without waiting.
            while !self
                .settle_termination(&shutdown.workspace_id, &shutdown.run_id, false)
                .await?
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        };
        match timeout(remaining, drain).await {
            Ok(result) => result,
            Err(_elapsed) => self
                .settle_termination(&shutdown.workspace_id, &shutdown.run_id, true)
                .await
                .map(drop),
        }
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

    async fn settle_termination(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        grace_expired: bool,
    ) -> Result<bool, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) = run_repository::hold_run(&mut transaction, workspace_id, run_id).await?
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
            self.finish_termination_in_transaction(&mut transaction, &run, terminal_at)
                .await?;
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub(crate) async fn request_termination(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        kind: RunTerminationKind,
        requester: Option<&Principal>,
        started_at_unix_nanos: i64,
    ) -> Result<RunRecord, Error> {
        let request = run.request_termination(kind, requester, started_at_unix_nanos)?;
        run_repository::request_termination(transaction, run, &request).await?;
        run_repository::find_exact_in_transaction(transaction, &run.workspace_id, &run.id)
            .await?
            .ok_or(Error::RunNotFound)
    }

    pub(crate) async fn finish_termination_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        terminal_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        invocation_lifecycle::end_nonterminal_for_run(transaction, run, terminal_at_unix_nanos)
            .await?;
        let run =
            run_repository::find_exact_in_transaction(transaction, &run.workspace_id, &run.id)
                .await?
                .ok_or(Error::RunNotFound)?;
        let termination = run
            .termination
            .ok_or_else(|| Error::CorruptData("Run has no termination to finish".into()))?;
        run_repository::finish_termination(
            transaction,
            &run,
            &termination.kind.outcome(),
            terminal_at_unix_nanos,
        )
        .await
    }
}

pub(crate) struct ShutdownAction {
    run_id: RunId,
    workspace_id: WorkspaceId,
    claim_id: String,
    accepted_at_unix_nanos: i64,
    deadline_at_unix_nanos: i64,
}

impl ShutdownAction {
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
        })
    }
}
