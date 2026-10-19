//! One worker's execution of an accepted Run: the leased claim, the durable
//! execution-start mark, claim heartbeats, and the completing terminal write.

use std::sync::Arc;

use coral_workspace::AuthenticatedPrincipal;
use uuid::Uuid;

use crate::engine::{self, ExecutionCompletion, ProgramPackage};
use crate::execution::ActiveExecution;
#[cfg(test)]
use crate::execution::ExecutionObserver;
use crate::invocation::OperationInvoker;
use crate::lifecycle::RunTerminationKind;
use crate::program_output::ProgramOutputWriter;
use crate::termination::ShutdownAction;
use crate::types::AcceptedRun;
use crate::{
    Error, FinalValue, RunFailure, RunFailureCode, RunModule, RunState, RunTerminalOutcome,
    duration_until, invocation_repository, run_repository,
};
#[cfg(test)]
use crate::{RunId, WorkspaceId};

impl RunModule {
    pub(crate) fn schedule_execution(&self, run: AcceptedRun, actor: AuthenticatedPrincipal) {
        let execution_module = self.clone();
        let execution_run = run.clone();
        let _execution_task = tokio::spawn(async move {
            if execution_module
                .execute_accepted(execution_run, actor)
                .await
                .is_err()
            {
                // The durable worker claim is the recovery source. An
                // operational failure cannot safely invent a terminal result.
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

    async fn execute_accepted(
        &self,
        accepted: AcceptedRun,
        actor: AuthenticatedPrincipal,
    ) -> Result<(), Error> {
        #[cfg(test)]
        let _attempt = ExecutionAttemptObservation(self.execution_observer.clone());
        let Some(claim_id) = self.claim_execution(&accepted).await? else {
            return Ok(());
        };
        let registration = self
            .active_executions
            .register(accepted.id.clone(), claim_id.clone());
        let active = registration.active();
        let Some(package) = self
            .start_claimed_execution(&accepted, &actor, &claim_id)
            .await?
        else {
            return Ok(());
        };
        let stop = active.stop();
        let output = ProgramOutputWriter::new(
            self.database.clone(),
            accepted.id.clone(),
            accepted.workspace_id.clone(),
            claim_id.clone(),
            self.clock.clone(),
            accepted.value_limits,
        );
        let invoker = OperationInvoker::new(
            self.accounts.clone(),
            self.providers.clone(),
            self.human_interface.clone(),
            self.database.clone(),
            self.workspaces.clone(),
            actor,
            accepted.id.clone(),
            accepted.workspace_id.clone(),
            accepted.principal.clone(),
            claim_id.clone(),
            accepted.deadline_at_unix_nanos,
            accepted.value_limits,
            stop.clone(),
            self.upstream.clone(),
            self.clock.clone(),
            #[cfg(test)]
            self.invocation_observer.clone(),
        );
        let heartbeat = self.schedule_claim_heartbeat(&accepted, &claim_id, active.clone());
        let completion = engine::execute(
            self.engine.clone(),
            package,
            invoker,
            output,
            accepted.value_limits,
            stop,
        )
        .await;
        // Normal execution drained its children. On an engine/host failure,
        // also wake any detached Invocation tasks before durable shutdown.
        active.request_stop();
        active.mark_stopped();
        heartbeat.abort();
        self.complete_execution(&accepted, &claim_id, completion)
            .await
    }

    async fn claim_execution(&self, accepted: &AcceptedRun) -> Result<Option<String>, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
                .await?
        else {
            return Ok(None);
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state != RunState::Running || run.termination.is_some() || run.execution_started() {
            transaction.commit().await?;
            return Ok(None);
        }
        if now >= run.deadline_at_unix_nanos {
            let run = self
                .request_termination(
                    &mut transaction,
                    &run,
                    RunTerminationKind::TimedOut,
                    None,
                    now,
                )
                .await?;
            self.finish_termination_in_transaction(&mut transaction, &run, now)
                .await?;
            transaction.commit().await?;
            return Ok(None);
        }
        let claimed_until = now
            .checked_add(self.execution_policy.claim_lease_nanos())
            .ok_or_else(|| Error::Clock("worker claim deadline overflowed".to_string()))?;
        let claim_id = Uuid::new_v4().hyphenated().to_string();
        let claimed = run_repository::claim_execution_worker(
            &mut transaction,
            &run,
            &claim_id,
            claimed_until,
            now,
        )
        .await?;
        transaction.commit().await?;
        if claimed {
            #[cfg(test)]
            self.execution_observer
                .worker_claimed(&accepted.id, &claim_id)
                .await;
            Ok(Some(claim_id))
        } else {
            Ok(None)
        }
    }

    async fn start_claimed_execution(
        &self,
        accepted: &AcceptedRun,
        actor: &AuthenticatedPrincipal,
        claim_id: &str,
    ) -> Result<Option<ProgramPackage>, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
                .await?
        else {
            return Ok(None);
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state != RunState::Running
            || run.termination.is_some()
            || run.execution_started()
            || run.worker_claim_id.as_deref() != Some(claim_id)
            || run
                .worker_claim_expires_at_unix_nanos
                .is_none_or(|expires_at| now >= expires_at)
        {
            transaction.commit().await?;
            return Ok(None);
        }
        if now >= run.deadline_at_unix_nanos {
            let run = self
                .request_termination(
                    &mut transaction,
                    &run,
                    RunTerminationKind::TimedOut,
                    None,
                    now,
                )
                .await?;
            self.finish_termination_in_transaction(&mut transaction, &run, now)
                .await?;
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
        let started_at = self.now()?.max(run.accepted_at_unix_nanos);
        if let Err(error) = authority.and_then(|evidence| {
            evidence.require_current_at(started_at)?;
            Ok(())
        }) {
            if is_operational_error(&error) {
                return Err(error);
            }
            run_repository::finish_unstarted_run(
                &mut transaction,
                &run,
                &RunTerminalOutcome::Failed(RunFailure::new(
                    RunFailureCode::RunAuthorityLost,
                    "Run authority was lost before Program execution started".to_string(),
                )),
                now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(None);
        }

        let started =
            run_repository::mark_execution_started(&mut transaction, &run, claim_id, started_at)
                .await?;
        transaction.commit().await?;
        Ok(started.then_some(package))
    }

    fn schedule_claim_heartbeat(
        &self,
        accepted: &AcceptedRun,
        claim_id: &str,
        active: Arc<ActiveExecution>,
    ) -> tokio::task::JoinHandle<()> {
        let module = self.clone();
        let accepted = accepted.clone();
        let claim_id = claim_id.to_string();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(module.execution_policy.claim_heartbeat()).await;
                if active.stop().is_requested() {
                    return;
                }
                let renewed = async {
                    let mut transaction = module.database.begin().await?;
                    if run_repository::hold_run(
                        &mut transaction,
                        &accepted.workspace_id,
                        &accepted.id,
                    )
                    .await?
                    .is_none()
                    {
                        return Ok::<bool, Error>(false);
                    }
                    let now = module.now()?.max(accepted.accepted_at_unix_nanos);
                    let claimed_until = now
                        .checked_add(module.execution_policy.claim_lease_nanos())
                        .ok_or_else(|| {
                            Error::Clock("worker claim deadline overflowed".to_string())
                        })?;
                    let renewed = run_repository::renew_worker_claim(
                        &mut transaction,
                        &accepted.workspace_id,
                        &accepted.id,
                        &claim_id,
                        now,
                        claimed_until,
                    )
                    .await?;
                    transaction.commit().await?;
                    Ok(renewed)
                }
                .await;
                if !matches!(renewed, Ok(true)) {
                    active.request_stop();
                    return;
                }
            }
        })
    }

    async fn complete_execution(
        &self,
        accepted: &AcceptedRun,
        claim_id: &str,
        completion: ExecutionCompletion,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(mut run) =
            run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
                .await?
        else {
            return Ok(());
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state != RunState::Running || run.execution_claim_id.as_deref() != Some(claim_id) {
            transaction.commit().await?;
            return Ok(());
        }
        let new_termination = if run.termination.is_some() {
            None
        } else if now >= run.deadline_at_unix_nanos {
            Some(RunTerminationKind::TimedOut)
        } else if completion == ExecutionCompletion::ProgramHostTerminated
            || !run.program_work_is_current(claim_id, now)
        {
            Some(RunTerminationKind::Interrupted)
        } else if completion == ExecutionCompletion::MemoryLimitExceeded {
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
            return self.complete_shutdown(shutdown).await;
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
            return Ok(());
        }
        let outcome = match completion {
            ExecutionCompletion::Succeeded(final_value) => {
                RunTerminalOutcome::Succeeded { final_value }
            }
            ExecutionCompletion::Failed(failure) => RunTerminalOutcome::Failed(failure),
            ExecutionCompletion::ProgramHostTerminated
            | ExecutionCompletion::MemoryLimitExceeded => unreachable!(
                "host termination creates durable interruption evidence before completion"
            ),
        };
        // Select the outcome under the same Run hold and transaction as its write.
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
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn retry_execution(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let run = run_repository::find_exact_in_transaction(&mut transaction, workspace_id, run_id)
            .await?
            .ok_or(Error::RunNotFound)?;
        transaction.commit().await?;
        let accepted = AcceptedRun {
            id: run.id,
            workspace_id: run.workspace_id,
            principal: run.principal,
            accepted_at_unix_nanos: run.accepted_at_unix_nanos,
            deadline_at_unix_nanos: run.deadline_at_unix_nanos,
            shutdown_grace_nanos: run.shutdown_grace_nanos,
            value_limits: run.value_limits,
        };
        self.execute_accepted(accepted, actor.clone()).await
    }
}

#[cfg(test)]
struct ExecutionAttemptObservation(Arc<dyn ExecutionObserver>);

#[cfg(test)]
impl Drop for ExecutionAttemptObservation {
    fn drop(&mut self) {
        self.0.execution_attempt_finished();
    }
}

fn is_operational_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Database(_) | Error::Workspace(coral_workspace::Error::Database(_))
    )
}
