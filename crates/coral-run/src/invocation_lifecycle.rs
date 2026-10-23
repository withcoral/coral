//! Invocation completion and termination decisions. Every write joins the
//! caller's transaction: the Invocation and Approval rows through their
//! repositories, and the `invocation.ended` Events are returned for the
//! caller's Run Ledger chunk: the owner's, its terminal chunk, or a
//! recovered owner's one chunk.

use crate::invocation::{AdmittedInvocation, InvocationCompletion};
use crate::lifecycle::RunRecord;
use crate::run_repository::{OwnedEvent, OwnedEventKind};
use crate::{
    CoralFailure, Error, InvocationId, InvocationResultKind, approval, invocation_repository,
};
use coral_db::Transaction;

#[derive(Debug, Clone, Copy)]
pub(crate) enum InvocationState {
    Pending,
    Dispatching,
    Ended,
}

pub(crate) struct UnfinishedInvocation {
    pub(crate) id: InvocationId,
    pub(crate) state: InvocationState,
    /// The Generation and Operation the Invocation called.
    pub(crate) generation: coral_catalog::ProviderGenerationId,
    pub(crate) operation: coral_catalog::OperationKey,
}

impl UnfinishedInvocation {
    /// How the Invocation ends with its Run, and the Operation Result recovery
    /// can still establish. A lost send is indeterminate. A lost Built-in
    /// Operation may not be: an `addProvider`'s own Publication shows what
    /// happened.
    async fn settle_for_termination(
        &self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
    ) -> Result<(InvocationResultKind, Option<String>), Error> {
        match self.state {
            InvocationState::Pending => Ok((InvocationResultKind::RunTerminated, None)),
            InvocationState::Dispatching => Ok(crate::builtin::recover(
                transaction,
                run,
                &self.id,
                &self.generation,
                &self.operation,
            )
            .await?
            .unwrap_or((InvocationResultKind::Indeterminate, None))),
            InvocationState::Ended => Err(Error::CorruptData(
                "Invocation has invalid nonterminal state".into(),
            )),
        }
    }
}

/// What ending an Invocation found.
pub(crate) enum FinishOutcome {
    /// The Invocation ended now; the Event rides the caller's chunk.
    Finished(OwnedEvent),
    /// Another completion already won with the same failure code: the durable
    /// decision is the one delivered, and no second Event is written.
    SameFailure,
    /// Another completion already won with a failure that prevents
    /// continuation: deliver the durable human decision.
    Superseded(CoralFailure),
    /// Another completion already won with something else.
    Lost,
}

/// Ends the Invocation and returns its `invocation.ended` for the chunk.
pub(crate) async fn finish(
    transaction: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    dispatch_started: bool,
    completion: &InvocationCompletion,
    ended_at_unix_nanos: i64,
) -> Result<FinishOutcome, Error> {
    let expected = match completion {
        InvocationCompletion::Upstream { .. } => InvocationState::Dispatching,
        InvocationCompletion::Failure(_) if dispatch_started => InvocationState::Dispatching,
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
        if let InvocationCompletion::Failure(failure) = completion {
            let existing = invocation_repository::completed_failure(
                transaction,
                &admitted.workspace_id,
                &admitted.id,
            )
            .await?;
            return Ok(match existing {
                // Per-result and cumulative output limits intentionally share one code.
                Some(existing) if existing.code() == failure.code() => FinishOutcome::SameFailure,
                Some(existing) if approval::prevents_continuation(existing) => {
                    FinishOutcome::Superseded(existing)
                }
                _ => FinishOutcome::Lost,
            });
        }
        return Ok(FinishOutcome::Lost);
    }
    approval::close_for_invocation(
        transaction,
        &admitted.workspace_id,
        &admitted.id,
        ended_at_unix_nanos,
    )
    .await?;
    Ok(FinishOutcome::Finished(OwnedEvent::new(
        OwnedEventKind::InvocationEnded(admitted.id.clone(), completion.result_kind()),
        ended_at_unix_nanos,
    )))
}

/// Ends a pending Invocation with `failure` from an approval decision and
/// returns its `invocation.ended` for the caller's chunk. `None` when the
/// Invocation is no longer pending: an earlier chunk ended it, and the
/// decision's record stands on its own.
pub(crate) async fn finish_approval_failure(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
    failure: CoralFailure,
    now: i64,
) -> Result<Option<OwnedEvent>, Error> {
    if !invocation_repository::end_pending_with_failure(tx, run, invocation, failure, now).await? {
        tracing::warn!(
            run_id = %run.id,
            invocation_id = %invocation,
            "approval decision found its Invocation no longer pending"
        );
        return Ok(None);
    }
    Ok(Some(OwnedEvent::new(
        OwnedEventKind::InvocationEnded(invocation.clone(), InvocationResultKind::CoralFailure),
        now,
    )))
}

/// Ends every nonterminal Invocation of `run` for its termination: pending
/// ones as `run_terminated`, dispatching ones as `indeterminate` (or with
/// the Operation Result Coral's own records establish), their pending
/// Approval Requests closed. Returns their `invocation.ended` Events for the
/// caller's terminal chunk, so the Invocation rows, the Approval rows, the
/// Events and `run.ended` commit together.
///
/// # Errors
///
/// [`Error::HeadMismatch`] when another terminal writer ended one of the
/// Invocations meanwhile: the caller reads the Run again, as it would after
/// its own head update mismatched.
pub(crate) async fn end_nonterminal_invocations(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    ended_at_unix_nanos: i64,
) -> Result<Vec<OwnedEvent>, Error> {
    let invocations = invocation_repository::nonterminal_for_run(transaction, run).await?;
    if invocations.is_empty() {
        return Ok(Vec::new());
    }
    if run.execution_claim_id.is_none() {
        return Err(Error::CorruptData(
            "unclaimed Run has nonterminal Invocations".into(),
        ));
    }
    let mut events = Vec::with_capacity(invocations.len());
    for invocation in &invocations {
        let (result_kind, result_json) =
            invocation.settle_for_termination(transaction, run).await?;
        let event = end_for_termination(
            transaction,
            run,
            invocation,
            result_kind,
            result_json.as_deref(),
            ended_at_unix_nanos,
        )
        .await?
        // Another terminal writer ended this Invocation between the read and
        // the write, and its terminal chunk moved the head with it: this
        // writer's own head update would mismatch, so it stops here and reads
        // the Run again.
        .ok_or(Error::HeadMismatch)?;
        events.push(event);
    }
    Ok(events)
}

/// Ends one nonterminal Invocation for a termination: its row, its pending
/// Approval Request, and its `invocation.ended` for the caller's chunk, with
/// the Operation Result recovery established, if any. `None` when the row is
/// no longer in the expected state.
pub(crate) async fn end_for_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &UnfinishedInvocation,
    result_kind: InvocationResultKind,
    result_json: Option<&str>,
    ended_at_unix_nanos: i64,
) -> Result<Option<OwnedEvent>, Error> {
    if !invocation_repository::end_for_termination(
        transaction,
        run,
        invocation,
        result_kind,
        result_json,
        ended_at_unix_nanos,
    )
    .await?
    {
        return Ok(None);
    }
    approval::close_for_invocation(
        transaction,
        &run.workspace_id,
        &invocation.id,
        ended_at_unix_nanos,
    )
    .await?;
    Ok(Some(OwnedEvent::new(
        OwnedEventKind::InvocationEnded(invocation.id.clone(), result_kind),
        ended_at_unix_nanos,
    )))
}
