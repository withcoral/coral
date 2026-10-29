//! One Run's execution under its Run Owner: the owner's first chunk (the
//! execution-start mark under the claim acceptance wrote) and the terminal
//! write once the Program has ended.
//!
//! The owner's own writes outside its chunk commits (the start mark and the
//! terminal write here, the Interrupted record and the settle beside them)
//! read the Run row without holding it: the owner runs one write at a time,
//! nothing writes a nonterminal Run's row from outside it while it lives
//! (the expired-lease sweep recovers a Run whose owner retired without
//! ending it, once its lease lapses), and every write is predicated on the
//! claim and the head the row showed.

use coral_workspace::AuthenticatedPrincipal;
use tracing::Instrument as _;

use crate::engine::ExecutionCompletion;
use crate::lifecycle::{RequestedTermination, RunRecord, RunTerminationKind};
use crate::owner::{self, OwnerHandle, OwnerStart, StartedExecution};
use crate::termination::ShutdownAction;
use crate::types::AcceptedRun;
use crate::{
    Error, FinalValue, RunFailure, RunFailureCode, RunModule, RunState, RunTerminalOutcome,
    invocation_repository, run_repository,
};

impl RunModule {
    /// Creates the Run Owner for a durably accepted Run. The owner is
    /// registered before its task starts, so a stop that arrives first still
    /// finds it; the Run deadline is the owner's own timer. An owner that
    /// retires without ending its Run (a write it could not land, a claim it
    /// lost) renews nothing more, and the expired-lease sweep ends the Run
    /// once its lease lapses, unless another process took it.
    pub(crate) fn schedule_execution(&self, run: AcceptedRun, actor: AuthenticatedPrincipal) {
        let (handle, mailbox) = OwnerHandle::new(run.id.clone(), run.claim_id.clone());
        let registration = self.owners.register(handle.clone());
        self.inbox.owner_registered(
            &self.owners,
            &self.instance,
            #[cfg(test)]
            &self.execution_observer,
        );
        let supervised = run.clone();
        let owner_task = tokio::spawn(owner::host(
            self.clone(),
            run,
            OwnerStart::Program(actor),
            handle,
            mailbox,
            registration,
        ));
        let supervisor = self.clone();
        let _supervisor_task = tokio::spawn(async move {
            if let Err(ended) = owner_task.await {
                supervisor
                    .recover_from_owner_failure(&supervised, &ended)
                    .await;
            }
        });
    }

    /// The supervisor's answer to an owner task that ended without retiring
    /// (a panic unwound it): the Run has no writer, so the supervisor takes
    /// the claim over with a new claim id and records the interruption in one
    /// chunk, as the owner's self-interrupt would. Nothing still in flight
    /// under the old claim can land afterwards.
    async fn recover_from_owner_failure(&self, run: &AcceptedRun, ended: &tokio::task::JoinError) {
        let span =
            crate::trace::phase_span(run.trace.as_ref(), &run.id, crate::trace::Phase::Complete);
        self.interrupt_after_owner_failure(run, ended)
            .instrument(span)
            .await;
    }

    async fn interrupt_after_owner_failure(
        &self,
        run: &AcceptedRun,
        ended: &tokio::task::JoinError,
    ) {
        let takeover_claim_id = uuid::Uuid::new_v4().hyphenated().to_string();
        tracing::error!(
            coral.run.id = %run.id,
            coral.run.claim.id = %run.claim_id,
            coral.run.takeover_claim.id = %takeover_claim_id,
            error.type = if ended.is_panic() { "panic" } else { "cancelled" },
            "Run Owner task ended without retiring; interrupting its Run"
        );
        match self
            .record_interruption(
                &run.workspace_id,
                &run.id,
                &run.claim_id,
                Some(&takeover_claim_id),
            )
            .await
        {
            Ok(()) => {}
            Err(Error::ClaimMismatch | Error::RunEnded) => {
                tracing::warn!(
                    coral.run.id = %run.id,
                    "Run of a failed Run Owner was already ended"
                );
            }
            Err(error) => {
                tracing::error!(
                    coral.run.id = %run.id,
                    error.type = coral_telemetry::error_type(&error),
                    "Run of a failed Run Owner could not be interrupted"
                );
            }
        }
    }

    /// The owner's first chunk: rechecks the Run and the Principal's current
    /// authority, then writes the execution-start mark and returns the
    /// package to execute with the head the mark left. `None` when there is
    /// nothing to run: the Run ended or was terminated first, its deadline
    /// passed (recorded here), its lease lapsed, or authority was lost (the
    /// Run fails here). Run again after an attempt the database did not
    /// answer, it returns the package when that attempt's start mark landed,
    /// so the Program starts exactly once.
    pub(crate) async fn start_execution(
        &self,
        accepted: &AcceptedRun,
        actor: &AuthenticatedPrincipal,
    ) -> Result<Option<StartedExecution>, Error> {
        let claim_id = accepted.claim_id.as_str();
        let mut transaction = self.database.begin().await?;
        let Some(run) = run_repository::find_exact_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        else {
            return Ok(None);
        };
        if run.state == RunState::Running
            && run.termination.is_none()
            && run.execution_claim_id.as_deref() == Some(claim_id)
        {
            return self.resume_start(transaction, &run, accepted).await;
        }
        if run.state != RunState::Running
            || run.termination.is_some()
            || run.execution_started()
            || run.worker_claim_id.as_deref() != Some(claim_id)
        {
            transaction.commit().await?;
            return Ok(None);
        }

        let authority = self
            .workspaces
            .check_run_admission_in_transaction(
                &mut transaction,
                actor,
                &accepted.workspace_id,
                self.now()?,
            )
            .await
            .map_err(Error::from)
            .and_then(|evidence| {
                (actor.principal() == &run.principal)
                    .then_some(evidence)
                    .ok_or(Error::RunNotFound)
            });

        let package = run_repository::load_package_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        .ok_or_else(|| Error::CorruptData("accepted Run has no Program package".to_string()))?;
        // The start mark is predicated on the lease and the deadline at its own
        // instant, after the authority and package reads under the hold.
        let started_at = self.now()?.max(run.accepted_at_unix_nanos);
        if run
            .worker_claim_expires_at_unix_nanos
            .is_none_or(|expires_at| started_at >= expires_at)
        {
            transaction.commit().await?;
            return Ok(None);
        }
        if started_at >= run.deadline_at_unix_nanos {
            return self
                .time_out_before_start(transaction, &run, started_at)
                .await;
        }
        if let Err(error) = authority.and_then(|evidence| {
            evidence.require_current_at(started_at)?;
            Ok(())
        }) {
            // A database that failed, definitely or not, has not said
            // anything about authority.
            if error.database_error().is_some() {
                return Err(error);
            }
            run_repository::finish_unstarted_run(
                &mut transaction,
                &run,
                &RunTerminalOutcome::Failed(RunFailure::new(
                    RunFailureCode::RunAuthorityLost,
                    "Run authority was lost before Program execution started".to_string(),
                )),
                started_at,
            )
            .await?;
            transaction.commit().await?;
            crate::trace::record_run_end(RunState::Failed);
            return Ok(None);
        }

        let lease_until = self.execution_policy.lease_until(started_at)?;
        run_repository::start_execution(&mut transaction, &run, claim_id, started_at, lease_until)
            .await?;
        transaction.commit().await?;
        Ok(Some(StartedExecution {
            package,
            head: run.chunk_sequence + 1,
        }))
    }

    /// The package of a Run whose start mark this owner's earlier attempt
    /// landed without an answer: the Program has not started yet, and this
    /// is its one start. A deadline that passed meanwhile is recorded here
    /// instead, as the first attempt would have.
    async fn resume_start(
        &self,
        mut transaction: coral_db::Transaction<'_>,
        run: &RunRecord,
        accepted: &AcceptedRun,
    ) -> Result<Option<StartedExecution>, Error> {
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if now >= run.deadline_at_unix_nanos {
            return self.time_out_before_start(transaction, run, now).await;
        }
        let package = run_repository::load_package_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        .ok_or_else(|| Error::CorruptData("started Run has no Program package".to_string()))?;
        transaction.commit().await?;
        Ok(Some(StartedExecution {
            package,
            head: run.chunk_sequence,
        }))
    }

    /// Ends a Run whose deadline passed before its Program started: the
    /// timeout and its terminal chunk, committed here, and nothing to run.
    async fn time_out_before_start(
        &self,
        mut transaction: coral_db::Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Option<StartedExecution>, Error> {
        let run = self
            .request_termination(
                &mut transaction,
                run,
                RunTerminationKind::TimedOut,
                now_unix_nanos,
            )
            .await?;
        self.finish_termination_in_transaction(&mut transaction, &run, now_unix_nanos)
            .await?;
        transaction.commit().await?;
        crate::trace::record_run_end(RunState::TimedOut);
        Ok(None)
    }

    /// Records the intent the owner applied but no chunk recorded (the
    /// deadline, or a cancellation whose chunk did not land) before the
    /// terminal write decides: it never records less than the owner decided.
    /// A termination already on the row stands.
    async fn record_owner_intent(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        run: RunRecord,
        intent: &RequestedTermination,
    ) -> Result<RunRecord, Error> {
        if run.termination.is_some() {
            return Ok(run);
        }
        self.record_termination(transaction, &run, intent).await
    }

    /// The terminal write for the Program's completion, after the owner has
    /// recorded every chunk it applied. Returns the shutdown still to settle
    /// when a termination is recorded instead of a completion: one the
    /// owner's chunks carried (a cancellation, the deadline), or one found
    /// or recorded here, with the head the owner goes on from.
    ///
    /// # Errors
    ///
    /// A Run cannot complete while a child Invocation is nonterminal; the Run
    /// then keeps its claim for recovery.
    pub(crate) async fn complete_execution(
        &self,
        accepted: &AcceptedRun,
        completion: &ExecutionCompletion,
        intent: Option<&RequestedTermination>,
    ) -> Result<Option<ShutdownAction>, Error> {
        let claim_id = accepted.claim_id.as_str();
        let mut transaction = self.database.begin().await?;
        let Some(mut run) = run_repository::find_exact_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        else {
            return Ok(None);
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state != RunState::Running || run.execution_claim_id.as_deref() != Some(claim_id) {
            transaction.commit().await?;
            return Ok(None);
        }
        if let Some(intent) = intent {
            run = self
                .record_owner_intent(&mut transaction, run, intent)
                .await?;
        }
        if let Some(kind) = termination_for_completion(&run, completion, claim_id, now) {
            run = self
                .request_termination(&mut transaction, &run, kind, now)
                .await?;
        }
        if run.termination.is_some() {
            let shutdown = ShutdownAction::from_run(&run)?;
            transaction.commit().await?;
            return Ok(Some(shutdown));
        }

        let has_nonterminal_invocation = invocation_repository::has_nonterminal_for_run(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?;
        #[cfg(test)]
        self.invocation_observer
            .run_completion_checked(has_nonterminal_invocation);
        if has_nonterminal_invocation {
            transaction.rollback().await?;
            return Err(Error::InvocationPersistence(
                "Program Run has a nonterminal Tool Invocation".to_string(),
            ));
        }
        let completed_at = self.now()?.max(run.accepted_at_unix_nanos);
        if completed_at >= run.deadline_at_unix_nanos
            || !run.program_work_is_current(claim_id, completed_at)
        {
            let kind = if completed_at >= run.deadline_at_unix_nanos {
                RunTerminationKind::TimedOut
            } else {
                RunTerminationKind::Interrupted
            };
            let run = self
                .request_termination(&mut transaction, &run, kind, completed_at)
                .await?;
            self.finish_termination_in_transaction(&mut transaction, &run, completed_at)
                .await?;
            transaction.commit().await?;
            crate::trace::record_run_end(kind.outcome().state());
            return Ok(None);
        }
        let outcome = match completion {
            ExecutionCompletion::Succeeded(final_value) => RunTerminalOutcome::Succeeded {
                final_value: final_value.clone(),
            },
            ExecutionCompletion::Failed(failure) => RunTerminalOutcome::Failed(failure.clone()),
            ExecutionCompletion::ProgramHostTerminated
            | ExecutionCompletion::MemoryLimitExceeded => unreachable!(
                "host termination creates durable interruption evidence before completion"
            ),
        };
        // The Final Value is decided against the recorded total, which the
        // owner's last chunk wrote from its exact in-memory total.
        let outcome = if let RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        } = &outcome
            && !run.output_fits(value.encoded_json().len())
        {
            RunTerminalOutcome::Failed(RunFailure::new(
                RunFailureCode::ResourceLimitExceeded,
                "Run output exceeds its limit".to_string(),
            ))
        } else {
            outcome
        };
        run_repository::finish_run(&mut transaction, &run, claim_id, &outcome, completed_at)
            .await?;
        transaction.commit().await?;
        crate::trace::record_run_end(outcome.state());
        Ok(None)
    }
}

/// The termination the terminal write records for a Run with none: the
/// timeout past its deadline, an interruption when the Program host ended
/// it or its claim is no longer current, the memory limit; `None` for a
/// Program that completed on its own.
fn termination_for_completion(
    run: &RunRecord,
    completion: &ExecutionCompletion,
    claim_id: &str,
    now_unix_nanos: i64,
) -> Option<RunTerminationKind> {
    if run.termination.is_some() {
        None
    } else if now_unix_nanos >= run.deadline_at_unix_nanos {
        Some(RunTerminationKind::TimedOut)
    } else if *completion == ExecutionCompletion::ProgramHostTerminated
        || !run.program_work_is_current(claim_id, now_unix_nanos)
    {
        Some(RunTerminationKind::Interrupted)
    } else if *completion == ExecutionCompletion::MemoryLimitExceeded {
        Some(RunTerminationKind::MemoryLimitExceeded)
    } else {
        None
    }
}
