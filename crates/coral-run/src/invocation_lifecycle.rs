//! Invocation completion and termination decisions. Every write joins the
//! caller's Run-held transaction: the Invocation and Approval rows through
//! their repositories, the Events through one Run Ledger chunk.

use crate::invocation::{AdmittedInvocation, InvocationCompletion};
use crate::lifecycle::RunRecord;
use crate::run_repository::{Chunk, NewEvent};
use crate::{
    CoralFailure, Error, InvocationId, InvocationResultKind, approval, invocation_repository,
    run_repository,
};
use coral_db::Transaction;

#[derive(Debug, Clone, Copy)]
pub(super) enum InvocationState {
    Pending,
    Dispatching,
    Ended,
}

pub(super) struct UnfinishedInvocation {
    pub(super) id: InvocationId,
    pub(super) state: InvocationState,
}

impl UnfinishedInvocation {
    fn termination_result(&self) -> Result<InvocationResultKind, Error> {
        match self.state {
            InvocationState::Pending => Ok(InvocationResultKind::RunTerminated),
            InvocationState::Dispatching => Ok(InvocationResultKind::Indeterminate),
            InvocationState::Ended => Err(Error::CorruptData(
                "Invocation has invalid nonterminal state".into(),
            )),
        }
    }
}

/// Ends the Invocation and commits `invocation.ended` under the worker's claim.
/// Returns `false` when another completion already won.
pub(super) async fn finish(
    transaction: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    run: &RunRecord,
    completion: &InvocationCompletion,
    ended_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let expected = match completion {
        InvocationCompletion::Upstream { .. } => InvocationState::Dispatching,
        InvocationCompletion::Failure(_) if admitted.dispatch_started => {
            InvocationState::Dispatching
        }
        InvocationCompletion::Failure(_) => InvocationState::Pending,
    };
    if !invocation_repository::finish(
        transaction,
        admitted,
        expected,
        completion,
        ended_at_unix_nanos,
    )
    .await?
    {
        // Deliver the durable human decision, not a worker's stale gate failure.
        // No second Invocation-ended Event is written for an existing winner.
        if let InvocationCompletion::Failure(failure) = completion {
            let existing = invocation_repository::completed_failure(
                transaction,
                &admitted.workspace_id,
                &admitted.id,
            )
            .await?;
            return match existing {
                // Per-result and cumulative output limits intentionally share one code.
                Some(existing) if existing.code() == failure.code() => Ok(true),
                Some(existing) if approval::prevents_continuation(existing) => {
                    Err(Error::InvocationRejected(existing))
                }
                _ => Ok(false),
            };
        }
        return Ok(false);
    }
    let output_bytes = match completion {
        InvocationCompletion::Upstream { result_json } => run
            .accumulated_output_bytes
            .checked_add(result_json.len())
            .ok_or_else(|| Error::CorruptData("Run output bytes overflowed".into()))?,
        InvocationCompletion::Failure(_) => run.accumulated_output_bytes,
    };
    approval::close_for_invocation(
        transaction,
        &admitted.workspace_id,
        &admitted.id,
        ended_at_unix_nanos,
    )
    .await?;
    run_repository::commit_chunk(
        transaction,
        Chunk::new(run, Some(&admitted.claim_id))
            .event(
                NewEvent::InvocationEnded(&admitted.id, completion.result_kind()),
                ended_at_unix_nanos,
            )
            .output_bytes(output_bytes),
    )
    .await?;
    Ok(true)
}

pub(super) async fn finish_approval_failure(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
    failure: CoralFailure,
    now: i64,
) -> Result<(), Error> {
    if !invocation_repository::end_pending_with_failure(tx, run, invocation, failure, now).await? {
        return Err(Error::InvocationPersistence(
            "Approval lost its pending Invocation".into(),
        ));
    }
    let claim = run
        .execution_claim_id
        .as_deref()
        .ok_or(Error::ProgramHostTerminated)?;
    run_repository::commit_chunk(
        tx,
        Chunk::new(run, Some(claim)).event(
            NewEvent::InvocationEnded(invocation, InvocationResultKind::CoralFailure),
            now,
        ),
    )
    .await
}

pub(super) async fn end_nonterminal_for_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    ended_at_unix_nanos: i64,
) -> Result<(), Error> {
    let invocations = invocation_repository::nonterminal_for_run(transaction, run).await?;
    if invocations.is_empty() {
        return Ok(());
    }
    let Some(claim) = run.execution_claim_id.as_deref() else {
        return Err(Error::CorruptData(
            "unclaimed Run has nonterminal Invocations".into(),
        ));
    };
    // One chunk ends every nonterminal Invocation: their rows, their pending
    // Approval Requests, and their `invocation.ended` Events commit together.
    let mut chunk = Chunk::new(run, Some(claim));
    for invocation in &invocations {
        let result_kind = invocation.termination_result()?;
        if !invocation_repository::end_for_termination(
            transaction,
            run,
            invocation,
            result_kind,
            ended_at_unix_nanos,
        )
        .await?
        {
            return Err(Error::InvocationPersistence(
                "Operation Invocation termination lost its race".into(),
            ));
        }
        approval::close_for_invocation(
            transaction,
            &run.workspace_id,
            &invocation.id,
            ended_at_unix_nanos,
        )
        .await?;
        chunk = chunk.event(
            NewEvent::InvocationEnded(&invocation.id, result_kind),
            ended_at_unix_nanos,
        );
    }
    run_repository::commit_chunk(transaction, chunk).await
}
