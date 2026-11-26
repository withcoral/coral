//! One Run stream's execution writes outside its loop's chunk commits: the
//! timeout of a Run whose deadline passed before its Program started, and
//! the terminal write once the Program has ended.
//!
//! Each reads the Run row without holding it and is predicated on the claim
//! and the head the row showed, so that the Run's other writers (the Coral
//! Instance that receives an outside request, and the sweeper) order with it
//! by the head (ADR 0027).

use crate::engine::ExecutionCompletion;
use crate::lifecycle::{RequestedTermination, RunRecord, RunTerminationKind};
use crate::termination::{commit_end, end_in_one_chunk};
use crate::trace::RunEnd;
use crate::types::AcceptedRun;
use crate::{
    Error, FinalValue, RunFailure, RunFailureCode, RunModule, RunState, RunTerminalOutcome,
    invocation_repository, run_repository,
};

impl RunModule {
    /// Ends a Run whose deadline passed before its Program started: the
    /// timeout in one terminal chunk, committed here, and nothing to run.
    pub(crate) async fn time_out_before_start(
        &self,
        mut transaction: coral_db::Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<(), Error> {
        let timeout =
            RequestedTermination::unrequested(RunTerminationKind::TimedOut, now_unix_nanos)?;
        let (kind, ended_at) =
            end_in_one_chunk(&mut transaction, run, Some(timeout), now_unix_nanos).await?;
        transaction.commit().await?;
        crate::trace::record_run_end(run, &RunEnd::Unstarted(&kind.outcome()), ended_at);
        Ok(())
    }

    /// The terminal write for the Program's completion, after the loop has
    /// recorded every chunk it applied. A Run that ends otherwise ends in
    /// one chunk with its open Invocations, and no grace (ADR 0027): a
    /// termination an older build recorded and did not end the Run with, or
    /// one decided here (past the deadline, a claim no longer current, a
    /// limit or a lost Program host that reached a loop whose commit was
    /// refused).
    ///
    /// # Errors
    ///
    /// A Run cannot complete while a child Invocation is nonterminal; the Run
    /// then keeps its claim, and the sweeper ends it at its deadline.
    pub(crate) async fn complete_execution(
        &self,
        accepted: &AcceptedRun,
        completion: &ExecutionCompletion,
    ) -> Result<(), Error> {
        let claim_id = accepted.claim_id.as_str();
        let mut transaction = self.database.begin().await?;
        let Some(run) = run_repository::find_exact_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        else {
            return Ok(());
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state != RunState::Running || run.execution_claim_id.as_deref() != Some(claim_id) {
            transaction.commit().await?;
            return Ok(());
        }
        // An older build's recorded termination precedes the decided one.
        let decided = termination_for_completion(&run, completion, claim_id, now);
        if run.termination.is_some() || decided.is_some() {
            let end = decided
                .map(|kind| RequestedTermination::unrequested(kind, now))
                .transpose()?;
            return commit_end(transaction, &run, end, now).await;
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
        if let Some(kind) = termination_for_completion(&run, completion, claim_id, completed_at) {
            let end = RequestedTermination::unrequested(kind, completed_at)?;
            let (kind, ended_at) =
                end_in_one_chunk(&mut transaction, &run, Some(end), completed_at).await?;
            transaction.commit().await?;
            crate::trace::record_run_end(
                &run,
                &RunEnd::Returned {
                    outcome: &kind.outcome(),
                    final_value_over_budget: false,
                },
                ended_at,
            );
            return Ok(());
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
        Ok(())
    }
}

/// The outcome of a Program that returned on its own, and whether its Final
/// Value was over the Run's output budget, which fails the Run instead. The
/// Final Value is decided against the recorded total, which the loop's last
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
            unreachable!("a limit or a lost Program host ends the Run before this write")
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

/// The termination the terminal write decides: the timeout past the Run's
/// deadline, an interruption when its claim is no longer current, else the
/// one the Program's end names ([`ExecutionCompletion::termination`]);
/// `None` for a Program that completed on its own within its deadline.
fn termination_for_completion(
    run: &RunRecord,
    completion: &ExecutionCompletion,
    claim_id: &str,
    now_unix_nanos: i64,
) -> Option<RunTerminationKind> {
    if run.deadline_passed(now_unix_nanos) {
        Some(RunTerminationKind::TimedOut)
    } else if !run.program_work_is_current(claim_id, now_unix_nanos) {
        Some(RunTerminationKind::Interrupted)
    } else {
        completion.termination()
    }
}
