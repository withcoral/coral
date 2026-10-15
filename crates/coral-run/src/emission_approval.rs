//! Human approval of one intercepted Program Emission.
//!
//! Operation Middleware gives every intercepted `coral.emit` an original policy
//! of `enabled` and may change its current policy. When the final policy is
//! `approval_required`, the Run's host creates an Approval Request bound to the
//! exact final payload bytes and waits. Nothing is emitted while it waits. An
//! approval appends exactly those bytes; a decline, expiry, authority loss or
//! Run end settles the Program's `coral.emit` Promise without an Emission.
//!
//! The request shares the Approval Request identity, decision interface,
//! lifecycle evidence and deadline bound with Invocation approvals. It has no
//! Provider, Account or Operation catalog target, so none of those checks apply.
//! Every transition holds the Run first, as Invocation approvals do.

use std::sync::Arc;

use coral_db::{Database, Transaction};
use coral_workspace::{Principal, TraceId, WorkspaceId};

use crate::approval::{APPROVAL_WAIT_NANOS, ApprovalDecision, ApprovalId, ApprovalState};
use crate::emission_approval_repository::{self as repository, StoredEmissionApproval};
use crate::engine::Clock;
use crate::execution::ExecutionStop;
use crate::invocation::InvocationHostError;
use crate::lifecycle::RunRecord;
use crate::{
    CoralFailure, Error, ProgramValue, RunId, RunState, SourceLocation, approval_repository,
    run_repository,
};

/// Longest an Emission wait goes without a held recheck, as for Invocations.
const WAIT_FALLBACK_POLL: std::time::Duration = std::time::Duration::from_secs(5);
/// Shortest gap between rechecks of one waiting Emission.
const WAIT_MIN_RECHECK_GAP: std::time::Duration = std::time::Duration::from_millis(500);

/// The Run-bound waits for approval-gated Emissions of one execution.
pub(crate) struct EmissionApprovals {
    database: Arc<Database>,
    workspace_id: WorkspaceId,
    run_id: RunId,
    claim_id: String,
    clock: Arc<dyn Clock>,
    stop: Arc<ExecutionStop>,
    limits: coral_value::ValueLimits,
}

/// Where one waiting request stands after a held recheck.
enum Settled {
    Pending(i64),
    Approved,
}

impl EmissionApprovals {
    pub(crate) fn new(
        database: Arc<Database>,
        workspace_id: WorkspaceId,
        run_id: RunId,
        claim_id: String,
        clock: Arc<dyn Clock>,
        stop: Arc<ExecutionStop>,
        limits: coral_value::ValueLimits,
    ) -> Self {
        Self {
            database,
            workspace_id,
            run_id,
            claim_id,
            clock,
            stop,
            limits,
        }
    }

    /// Requests approval for these exact bytes, waits for the outcome, and
    /// returns the approved value to append. Nothing is emitted here.
    ///
    /// # Errors
    ///
    /// A decline, expiry, closed request or lost authority is a catchable
    /// rejection. Run termination and persistence faults are fatal.
    pub(crate) async fn wait_for_approval(
        &self,
        value_json: String,
        location: Option<&SourceLocation>,
    ) -> Result<(ProgramValue, ApprovalId), InvocationHostError> {
        let value = ProgramValue::with_limits(value_json, self.limits).map_err(|error| {
            InvocationHostError::Rejected(match error {
                Error::ProgramValueTooLarge => CoralFailure::InputTooLarge,
                _ => CoralFailure::InvalidInput,
            })
        })?;
        // Subscribe before the request commits, so a decision right after it wakes this wait.
        let mut changes = self.database.subscribe_changes().ok();
        let id = self.request(&value, location).await?;
        loop {
            match self.settle(&id).await? {
                Settled::Approved => return Ok((value, id)),
                Settled::Pending(deadline) => self.wait(&mut changes, deadline).await?,
            }
        }
    }

    async fn request(
        &self,
        value: &ProgramValue,
        location: Option<&SourceLocation>,
    ) -> Result<ApprovalId, Error> {
        let mut tx = self.database.begin().await?;
        let run = self.hold_current_run(&mut tx).await?;
        let now = self.now()?;
        let id = ApprovalId::new();
        repository::insert_pending(
            &mut tx,
            &self.workspace_id,
            &repository::NewEmissionApproval {
                id: &id,
                run: &self.run_id,
                payload_json: value.encoded_json(),
                source_location: location,
                deadline_at_unix_nanos: std::cmp::min(
                    run.deadline_at_unix_nanos,
                    now.saturating_add(APPROVAL_WAIT_NANOS),
                ),
            },
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Held recheck: expires a request past its deadline and closes it when
    /// the invoking Principal lost Workspace authority.
    async fn settle(&self, id: &ApprovalId) -> Result<Settled, InvocationHostError> {
        let mut tx = self.database.begin().await.map_err(Error::from)?;
        let run = self.hold_current_run(&mut tx).await?;
        let now = self.now()?;
        let mut row = repository::find_for_run(&mut tx, &self.workspace_id, &self.run_id, id)
            .await?
            .ok_or(Error::ApprovalNotFound)?;
        let authority_current =
            crate::history_repository::invoking_authority_current(&mut tx, &run, now).await?;
        // A request closed while the invoking Principal lacks authority, here or by a
        // decision attempt, settles as lost authority.
        let failure = (!authority_current).then_some(CoralFailure::AuthorityLost);
        if row.state == ApprovalState::Pending {
            let next = if now >= row.deadline_at_unix_nanos {
                Some(ApprovalState::Expired)
            } else if !authority_current {
                Some(ApprovalState::Closed)
            } else {
                None
            };
            if let Some(state) = next {
                transition(
                    &mut tx,
                    &self.workspace_id,
                    &mut row,
                    state,
                    now,
                    None,
                    &TraceId::current_or_new(),
                )
                .await?;
            }
        }
        tx.commit().await.map_err(Error::from)?;
        match row.state {
            ApprovalState::Pending => Ok(Settled::Pending(row.deadline_at_unix_nanos)),
            ApprovalState::Approved => Ok(Settled::Approved),
            ApprovalState::Closed => Err(InvocationHostError::Rejected(
                failure.unwrap_or(CoralFailure::ApprovalRequired),
            )),
            state => Err(InvocationHostError::Rejected(rejection(state))),
        }
    }

    async fn wait(
        &self,
        changes: &mut Option<coral_db::ChangeSubscription>,
        deadline: i64,
    ) -> Result<(), Error> {
        let checked_at = tokio::time::Instant::now();
        let wake =
            checked_at + crate::duration_until(deadline, self.now()?).min(WAIT_FALLBACK_POLL);
        tokio::select! {
            () = self.stop.requested() => return Err(Error::ProgramHostTerminated),
            result = crate::invocation::next_change(changes) => if result.is_err() { *changes = None },
            () = tokio::time::sleep_until(wake) => {}
        }
        // Commits elsewhere wake every waiter. Coalesce bursts into bounded rechecks.
        tokio::select! {
            () = self.stop.requested() => Err(Error::ProgramHostTerminated),
            () = tokio::time::sleep_until(checked_at + WAIT_MIN_RECHECK_GAP) => Ok(()),
        }
    }

    async fn hold_current_run(&self, tx: &mut Transaction<'_>) -> Result<RunRecord, Error> {
        let run = run_repository::hold_run(tx, &self.workspace_id, &self.run_id)
            .await?
            .ok_or(Error::RunNotFound)?;
        if !run.program_work_is_current(&self.claim_id, self.now()?) {
            return Err(Error::ProgramHostTerminated);
        }
        Ok(run)
    }

    fn now(&self) -> Result<i64, Error> {
        self.clock.now_unix_nanos().map_err(Error::Clock)
    }
}

/// Inside the transaction that appends an approved Emission: the request is
/// approved, unexpired, and binds exactly these bytes.
///
/// # Errors
///
/// An expired approval is a catchable rejection; anything else is corrupt
/// host state, because only the waiting host appends approved bytes.
pub(crate) async fn require_approved(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    id: &ApprovalId,
    value: &ProgramValue,
    now: i64,
) -> Result<(), Error> {
    let row = repository::find_for_run(tx, &run.workspace_id, &run.id, id)
        .await?
        .ok_or(Error::ApprovalNotFound)?;
    if row.state != ApprovalState::Approved || row.payload_json != value.encoded_json() {
        return Err(Error::CorruptData(
            "approved Emission does not match its request".into(),
        ));
    }
    if now >= row.deadline_at_unix_nanos {
        return Err(Error::InvocationRejected(CoralFailure::ApprovalExpired));
    }
    Ok(())
}

/// A human decision on one Emission request, inside the decision transaction
/// that already holds the authorized Run.
pub(crate) async fn decide(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    row: &mut StoredEmissionApproval,
    decider: (&Principal, Option<&str>),
    decision: ApprovalDecision,
    now: i64,
    trace: &TraceId,
) -> Result<(), Error> {
    if row.state != ApprovalState::Pending {
        return Ok(());
    }
    let state = if run.state != RunState::Running
        || run.termination.is_some()
        || now >= run.deadline_at_unix_nanos
        || !crate::history_repository::invoking_authority_current(tx, run, now).await?
    {
        ApprovalState::Closed
    } else if now >= row.deadline_at_unix_nanos {
        ApprovalState::Expired
    } else {
        match decision {
            ApprovalDecision::Approve => ApprovalState::Approved,
            ApprovalDecision::Decline => ApprovalState::Declined,
        }
    };
    let human = matches!(state, ApprovalState::Approved | ApprovalState::Declined);
    transition(
        tx,
        &run.workspace_id,
        row,
        state,
        now,
        human.then_some(decider),
        trace,
    )
    .await
}

/// Closes every pending Emission request of a Run that is ending, so a
/// decision can no longer emit.
pub(crate) async fn close_pending_for_run(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    now: i64,
) -> Result<(), Error> {
    for id in repository::pending_ids_for_run(tx, &run.workspace_id, &run.id).await? {
        if repository::update_pending(tx, &run.workspace_id, &id, ApprovalState::Closed, now, None)
            .await?
        {
            approval_repository::insert_lifecycle_event(
                tx,
                &run.workspace_id,
                &id,
                ApprovalState::Closed,
                now,
                None,
                &TraceId::current_or_new(),
            )
            .await?;
        }
    }
    Ok(())
}

/// Whether the request is current and the decider may act on it now.
pub(crate) fn actionable(row: &StoredEmissionApproval, run: &RunRecord, now: i64) -> bool {
    row.state == ApprovalState::Pending
        && now < row.deadline_at_unix_nanos
        && crate::required_actions::run_accepts_actions(run, now)
}

const fn rejection(state: ApprovalState) -> CoralFailure {
    match state {
        ApprovalState::Declined => CoralFailure::ApprovalDeclined,
        ApprovalState::Expired => CoralFailure::ApprovalExpired,
        ApprovalState::Pending | ApprovalState::Approved | ApprovalState::Closed => {
            CoralFailure::ApprovalRequired
        }
    }
}

async fn transition(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    row: &mut StoredEmissionApproval,
    state: ApprovalState,
    now: i64,
    human: Option<(&Principal, Option<&str>)>,
    trace: &TraceId,
) -> Result<(), Error> {
    let actor = human.map(|(actor, _)| actor);
    if !repository::update_pending(tx, workspace, &row.id, state, now, human).await? {
        return Err(Error::InvocationPersistence(
            "Emission approval decision lost its Run hold".into(),
        ));
    }
    approval_repository::insert_lifecycle_event(tx, workspace, &row.id, state, now, actor, trace)
        .await?;
    row.state = state;
    row.decided_at_unix_nanos = Some(now);
    row.approver = actor.cloned();
    row.comment = human.and_then(|(_, comment)| comment).map(str::to_string);
    Ok(())
}
