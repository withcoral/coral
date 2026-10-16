//! Invocation completion and termination decisions. Every write joins the
//! caller's Run-held transaction, including Approval closure and Run Events.

use crate::invocation::{AdmittedInvocation, InvocationCompletion};
use crate::lifecycle::RunRecord;
use crate::run_repository::NewEvent;
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
    pub(super) call_path: String,
}

impl UnfinishedInvocation {
    /// How the Invocation ends with its Run, and the Operation Result recovery
    /// can still establish. A lost send is indeterminate. A lost
    /// `createProvider` is not: Coral's own Publication shows what happened.
    async fn settle_for_termination(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
    ) -> Result<(InvocationResultKind, Option<String>), Error> {
        match self.state {
            InvocationState::Pending => Ok((InvocationResultKind::RunTerminated, None)),
            InvocationState::Dispatching
                if crate::builtin::names_create_provider(&self.call_path) =>
            {
                crate::builtin::recover_create_provider(transaction, run, &self.id).await
            }
            InvocationState::Dispatching => Ok((InvocationResultKind::Indeterminate, None)),
            InvocationState::Ended => Err(Error::CorruptData(
                "Invocation has invalid nonterminal state".into(),
            )),
        }
    }
}

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
                    Err(Error::InvocationRejected(existing.into()))
                }
                _ => Ok(false),
            };
        }
        return Ok(false);
    }
    if let InvocationCompletion::Upstream { result_json } = completion {
        run_repository::record_invocation_output(
            transaction,
            run,
            &admitted.claim_id,
            result_json.len(),
        )
        .await?;
    }
    approval::close_for_invocation(
        transaction,
        &admitted.workspace_id,
        &admitted.id,
        ended_at_unix_nanos,
    )
    .await?;
    if admitted.dispatch_started {
        run_repository::advance_event_sequence_after_dispatch(
            transaction,
            &admitted.workspace_id,
            &admitted.run_id,
            &admitted.claim_id,
            run.next_event_sequence,
            1,
        )
        .await?;
    } else {
        run_repository::advance_event_sequence(transaction, run, &admitted.claim_id, 1, 0).await?;
    }
    run_repository::insert_event(
        transaction,
        &admitted.workspace_id,
        &admitted.run_id,
        run.next_event_sequence,
        NewEvent::InvocationEnded(&admitted.id, completion.result_kind()),
        ended_at_unix_nanos,
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
    run_repository::advance_event_sequence(tx, run, claim, 1, 0).await?;
    run_repository::insert_event(
        tx,
        &run.workspace_id,
        &run.id,
        run.next_event_sequence,
        NewEvent::InvocationEnded(invocation, InvocationResultKind::CoralFailure),
        now,
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
    if run.execution_claim_id.is_none() {
        return Err(Error::CorruptData(
            "unclaimed Run has nonterminal Invocations".into(),
        ));
    }
    let increment = u64::try_from(invocations.len())
        .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".into()))?;
    run_repository::advance_event_sequence_during_termination(
        transaction,
        &run.workspace_id,
        &run.id,
        run.next_event_sequence,
        increment,
    )
    .await?;
    for (offset, invocation) in invocations.into_iter().enumerate() {
        let (result_kind, result_json) =
            invocation.settle_for_termination(transaction, run).await?;
        if !invocation_repository::end_for_termination(
            transaction,
            run,
            &invocation,
            result_kind,
            result_json.as_deref(),
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
        let offset = u64::try_from(offset)
            .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".into()))?;
        let sequence = run
            .next_event_sequence
            .checked_add(offset)
            .ok_or_else(|| Error::CorruptData("Run Event sequence overflowed".into()))?;
        run_repository::insert_event(
            transaction,
            &run.workspace_id,
            &run.id,
            sequence,
            NewEvent::InvocationEnded(&invocation.id, result_kind),
            ended_at_unix_nanos,
        )
        .await?;
    }
    Ok(())
}
