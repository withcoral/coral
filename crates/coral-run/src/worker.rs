//! One Run's execution under its Run Owner: the owner's first chunk (the
//! execution-start mark under the claim acceptance wrote), the deadline task,
//! and the terminal write once the Program has ended.

use coral_workspace::AuthenticatedPrincipal;

use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::lifecycle::RunTerminationKind;
use crate::owner::{self, OwnerHandle};
use crate::termination::ShutdownAction;
use crate::types::AcceptedRun;
use crate::{
    Error, FinalValue, RunFailure, RunFailureCode, RunModule, RunState, RunTerminalOutcome,
    duration_until, invocation_repository, run_repository,
};

impl RunModule {
    /// Creates the Run Owner for a durably accepted Run and the task that
    /// records its deadline. The owner is registered before its task starts,
    /// so a stop that arrives first still finds it.
    pub(crate) fn schedule_execution(&self, run: AcceptedRun, actor: AuthenticatedPrincipal) {
        let (handle, mailbox) = OwnerHandle::new(run.id.clone(), run.claim_id.clone());
        let registration = self.owners.register(handle.clone());
        let owner_task = tokio::spawn(owner::host(
            self.clone(),
            run.clone(),
            actor,
            handle,
            mailbox,
            registration,
        ));
        let supervisor = self.clone();
        let supervised = run.clone();
        let _supervisor_task = tokio::spawn(async move {
            if let Err(ended) = owner_task.await {
                supervisor
                    .recover_from_owner_failure(&supervised, &ended)
                    .await;
            }
        });
        let deadline_module = self.clone();
        let _deadline_task = tokio::spawn(async move {
            let now = deadline_module
                .now()
                .unwrap_or(run.accepted_at_unix_nanos)
                .max(run.accepted_at_unix_nanos);
            tokio::time::sleep(duration_until(run.deadline_at_unix_nanos, now)).await;
            let _timed_out = deadline_module.begin_deadline_shutdown(&run).await;
        });
    }

    /// The supervisor's answer to an owner task that ended without retiring
    /// (a panic unwound it): the Run has no writer, so the supervisor takes
    /// the claim over with a new claim id and records the interruption in one
    /// chunk, as the owner's self-interrupt would. Nothing still in flight
    /// under the old claim can land afterwards.
    async fn recover_from_owner_failure(&self, run: &AcceptedRun, ended: &tokio::task::JoinError) {
        let takeover_claim_id = uuid::Uuid::new_v4().hyphenated().to_string();
        tracing::error!(
            run_id = %run.id,
            claim_id = %run.claim_id,
            takeover_claim_id = %takeover_claim_id,
            ended = %ended,
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
                    run_id = %run.id,
                    "Run of a failed Run Owner was ended by another writer first"
                );
            }
            Err(error) => {
                tracing::error!(
                    run_id = %run.id,
                    error = %error,
                    "Run of a failed Run Owner could not be interrupted"
                );
            }
        }
    }

    /// The owner's first chunk: rechecks the Run and the Principal's current
    /// authority under the Run hold, then writes the execution-start mark and
    /// returns the package to execute. `None` when there is nothing to run:
    /// the Run ended or was terminated first, its deadline passed (recorded
    /// here), its lease lapsed, or authority was lost (the Run fails here).
    /// Run again after an attempt the database did not answer, it returns the
    /// package when that attempt's start mark landed, so the Program starts
    /// exactly once.
    pub(crate) async fn start_execution(
        &self,
        accepted: &AcceptedRun,
        actor: &AuthenticatedPrincipal,
    ) -> Result<Option<ProgramPackage>, Error> {
        let claim_id = accepted.claim_id.as_str();
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
                .await?
        else {
            return Ok(None);
        };
        if run.state == RunState::Running
            && run.termination.is_none()
            && run.execution_claim_id.as_deref() == Some(claim_id)
        {
            return Self::package_of_started_run(transaction, accepted).await;
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
            let run = self
                .request_termination(
                    &mut transaction,
                    &run,
                    RunTerminationKind::TimedOut,
                    None,
                    started_at,
                )
                .await?;
            self.finish_termination_in_transaction(&mut transaction, &run, started_at)
                .await?;
            transaction.commit().await?;
            return Ok(None);
        }
        if let Err(error) = authority.and_then(|evidence| {
            evidence.require_current_at(started_at)?;
            Ok(())
        }) {
            if error.is_operational() {
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
            return Ok(None);
        }

        let lease_until = self.execution_policy.lease_until(started_at)?;
        run_repository::start_execution(&mut transaction, &run, claim_id, started_at, lease_until)
            .await?;
        transaction.commit().await?;
        Ok(Some(package))
    }

    /// The package of a Run whose start mark this owner's earlier attempt
    /// landed without an answer: the Program has not started yet, and this
    /// is its one start.
    async fn package_of_started_run(
        mut transaction: coral_db::Transaction<'_>,
        accepted: &AcceptedRun,
    ) -> Result<Option<ProgramPackage>, Error> {
        let package = run_repository::load_package_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        .ok_or_else(|| Error::CorruptData("started Run has no Program package".to_string()))?;
        transaction.commit().await?;
        Ok(Some(package))
    }

    /// The terminal write for the Program's completion, after the owner has
    /// recorded every chunk it applied. Returns the shutdown still to settle
    /// when a termination is recorded instead of a completion.
    ///
    /// # Errors
    ///
    /// A Run cannot complete while a child Invocation is nonterminal; the Run
    /// then keeps its claim for recovery.
    pub(crate) async fn complete_execution(
        &self,
        accepted: &AcceptedRun,
        completion: &ExecutionCompletion,
    ) -> Result<Option<ShutdownAction>, Error> {
        let claim_id = accepted.claim_id.as_str();
        let mut transaction = self.database.begin().await?;
        let Some(mut run) =
            run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
                .await?
        else {
            return Ok(None);
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state != RunState::Running || run.execution_claim_id.as_deref() != Some(claim_id) {
            transaction.commit().await?;
            return Ok(None);
        }
        let new_termination = if run.termination.is_some() {
            None
        } else if now >= run.deadline_at_unix_nanos {
            Some(RunTerminationKind::TimedOut)
        } else if *completion == ExecutionCompletion::ProgramHostTerminated
            || !run.program_work_is_current(claim_id, now)
        {
            Some(RunTerminationKind::Interrupted)
        } else if *completion == ExecutionCompletion::MemoryLimitExceeded {
            Some(RunTerminationKind::MemoryLimitExceeded)
        } else {
            None
        };
        if let Some(kind) = new_termination {
            run = self
                .request_termination(&mut transaction, &run, kind, None, now)
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
                "Program Run has a nonterminal Operation Invocation".to_string(),
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
                .request_termination(&mut transaction, &run, kind, None, completed_at)
                .await?;
            self.finish_termination_in_transaction(&mut transaction, &run, completed_at)
                .await?;
            transaction.commit().await?;
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
        Ok(None)
    }
}
