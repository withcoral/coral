//! One Run stream's execution writes outside its loop's chunk commits: the
//! timeout of a Run whose deadline passed before its Program started, and
//! the terminal write once the Program has ended.
//!
//! Each reads the Run row without holding it and is predicated on the claim
//! and the head the row showed, so that the Run's other writers (the Coral
//! Instance that receives an outside request, and the sweeper) order with it
//! by the head (ADR 0027).

use crate::engine::ExecutionCompletion;
use crate::lifecycle::{RunRecord, RunTerminationKind};
use crate::owner::{self, OwnerHandle};
use crate::termination::ShutdownAction;
use crate::trace::RunEnd;
use crate::types::AcceptedRun;
use crate::{
    Error, FinalValue, RunFailure, RunFailureCode, RunModule, RunState, RunTerminalOutcome,
    invocation_repository, run_repository,
};

impl RunModule {
    /// Registers a Run stream's owner, which detaches the one it replaces
    /// ([`owner::Owners::register_stream`]), and starts the wake-up reader
    /// of this Coral Instance's loops.
    pub(crate) fn register_stream_owner(&self, handle: &OwnerHandle) -> owner::OwnerRegistration {
        let registration = self.owners.register_stream(handle.clone());
        self.wakes.loop_registered(
            &self.owners,
            #[cfg(test)]
            &self.execution_observer,
        );
        registration
    }

    /// Ends a Run whose deadline passed before its Program started: the
    /// timeout and its terminal chunk, committed here, and nothing to run.
    pub(crate) async fn time_out_before_start(
        &self,
        mut transaction: coral_db::Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<(), Error> {
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
        crate::trace::record_run_end(
            &run,
            &RunEnd::Unstarted(&RunTerminalOutcome::TimedOut),
            now_unix_nanos,
        );
        Ok(())
    }

    /// The terminal write for the Program's completion, after the loop has
    /// recorded every chunk it applied. Returns the shutdown still to settle
    /// when a termination is recorded instead of a completion: one another
    /// writer recorded, or one recorded here (a CPU or memory limit, a lost
    /// Program host), with the head the loop goes on from.
    ///
    /// # Errors
    ///
    /// A Run cannot complete while a child Invocation is nonterminal; the Run
    /// then keeps its claim, and the sweeper ends it at its deadline.
    pub(crate) async fn complete_execution(
        &self,
        accepted: &AcceptedRun,
        completion: &ExecutionCompletion,
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
        if run.deadline_passed(completed_at) || !run.program_work_is_current(claim_id, completed_at)
        {
            let kind = if run.deadline_passed(completed_at) {
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
            crate::trace::record_run_end(
                &run,
                &RunEnd::Returned {
                    outcome: &kind.outcome(),
                    final_value_over_budget: false,
                },
                completed_at,
            );
            return Ok(None);
        }
        let (outcome, final_value_over_budget) = returned_outcome(&run, completion);
        run_repository::finish_run(&mut transaction, &run, claim_id, &outcome, completed_at)
            .await?;
        transaction.commit().await?;
        crate::trace::record_run_end(
            &run,
            &RunEnd::Returned {
                outcome: &outcome,
                final_value_over_budget,
            },
            completed_at,
        );
        Ok(None)
    }
}

/// The outcome of a Program that returned on its own, and whether its Final
/// Value was over the Run's output budget, which fails the Run instead. The
/// Final Value is decided against the recorded total, which the owner's last
/// chunk wrote from its exact in-memory total.
fn returned_outcome(
    run: &RunRecord,
    completion: &ExecutionCompletion,
) -> (RunTerminalOutcome, bool) {
    let outcome = match completion {
        ExecutionCompletion::Succeeded(final_value) => RunTerminalOutcome::Succeeded {
            final_value: final_value.clone(),
        },
        ExecutionCompletion::Failed(failure) => RunTerminalOutcome::Failed(failure.clone()),
        ExecutionCompletion::ProgramHostTerminated
        | ExecutionCompletion::MemoryLimitExceeded
        | ExecutionCompletion::CpuLimitExceeded => {
            unreachable!("host termination creates durable interruption evidence before completion")
        }
    };
    let over_budget = matches!(&outcome, RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    } if !run.output_fits(value.encoded_json().len()));
    if over_budget {
        let failure = RunFailure::new(
            RunFailureCode::ResourceLimitExceeded,
            "Run output exceeds its limit".to_string(),
        );
        (RunTerminalOutcome::Failed(failure), true)
    } else {
        (outcome, false)
    }
}

/// The termination the terminal write records for a Run with none: the
/// timeout past its deadline, an interruption when the Program host ended
/// it or its claim is no longer current, the memory limit, the CPU limit;
/// `None` for a Program that completed on its own.
fn termination_for_completion(
    run: &RunRecord,
    completion: &ExecutionCompletion,
    claim_id: &str,
    now_unix_nanos: i64,
) -> Option<RunTerminationKind> {
    if run.termination.is_some() {
        None
    } else if run.deadline_passed(now_unix_nanos) {
        Some(RunTerminationKind::TimedOut)
    } else if *completion == ExecutionCompletion::ProgramHostTerminated
        || !run.program_work_is_current(claim_id, now_unix_nanos)
    {
        Some(RunTerminationKind::Interrupted)
    } else if *completion == ExecutionCompletion::MemoryLimitExceeded {
        Some(RunTerminationKind::MemoryLimitExceeded)
    } else if *completion == ExecutionCompletion::CpuLimitExceeded {
        Some(RunTerminationKind::CpuLimitExceeded)
    } else {
        None
    }
}
