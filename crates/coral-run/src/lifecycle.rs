//! Run lifecycle evidence and decisions, independent of storage representation.

use crate::{Error, RunFailure, RunFailureCode, RunId, RunState, RunTerminalOutcome};
use coral_workspace::{Principal, WorkspaceId};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RunRecord {
    pub(crate) id: RunId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) principal: Principal,
    pub(crate) state: RunState,
    pub(crate) accepted_at_unix_nanos: i64,
    pub(crate) deadline_at_unix_nanos: i64,
    pub(crate) worker_claim_id: Option<String>,
    pub(crate) worker_claim_expires_at_unix_nanos: Option<i64>,
    pub(crate) execution_claim_id: Option<String>,
    pub(crate) execution_started_at_unix_nanos: Option<i64>,
    pub(crate) termination: Option<RunTermination>,
    pub(crate) shutdown_grace_nanos: i64,
    /// The Run Ledger head: the number of chunk commits since acceptance.
    pub(crate) chunk_sequence: u64,
    /// The last Event sequence on the Run as of the read, `0` before
    /// `run.started`. Read beside the row; the chunk commit numbers its
    /// Events itself and never from this.
    pub(crate) last_event_sequence: Option<u64>,
    pub(crate) accumulated_output_bytes: usize,
    pub(crate) terminal_outcome: Option<RunTerminalOutcome>,
    pub(crate) terminal_at_unix_nanos: Option<i64>,
    pub(crate) value_limits: coral_value::ValueLimits,
}

impl RunRecord {
    /// Checks Emissions, Logs, Invocation results, and the Final Value against
    /// the shared budget. The Final Value is checked only at Run completion.
    pub(crate) fn output_fits(&self, additional: usize) -> bool {
        self.output_total(additional)
            .is_ok_and(|total| total <= self.value_limits.run_output_bytes())
    }

    /// The Run Output Budget total after `additional` bytes, as a chunk writes it.
    pub(crate) fn output_total(&self, additional: usize) -> Result<usize, Error> {
        self.accumulated_output_bytes
            .checked_add(additional)
            .ok_or_else(|| Error::CorruptData("Run output bytes overflowed".into()))
    }

    /// Whether the Run row still takes the Run Owner's chunks: a running Run
    /// whose claim columns both hold `claim_id`. Never the clock: the owner
    /// applies the Run deadline itself and records the timeout like a
    /// cancellation. A lapsed lease does not end ownership (a local owner
    /// continues while its commits show a matching claim), and a recorded
    /// termination intent does not stop the owner recording what it applied
    /// before the intent.
    pub(crate) fn accepts_owner_chunk(&self, claim_id: &str) -> bool {
        self.state == RunState::Running
            && self.execution_claim_id.as_deref() == Some(claim_id)
            && self.worker_claim_id.as_deref() == Some(claim_id)
    }

    /// Whether the owner holding `claim_id` may still do Program work on
    /// this Run. True when the Run is running, no termination is recorded,
    /// both claim columns hold `claim_id`, and the deadline has not passed.
    ///
    /// The lease is deliberately not part of this check. The lease decides
    /// which process owns the Run; every commit renews it. Inside its own
    /// process an owner keeps working after its lease lapsed, as long as
    /// its commits still find its claim on the row. What ends such a Run is
    /// the owner's own liveness bound (see the Run Owner), or another
    /// process taking the claim, which the owner then sees as a claim
    /// mismatch.
    pub(crate) fn program_work_is_current(&self, claim_id: &str, now_unix_nanos: i64) -> bool {
        self.state == RunState::Running
            && self.termination.is_none()
            && self.execution_claim_id.as_deref() == Some(claim_id)
            && self.worker_claim_id.as_deref() == Some(claim_id)
            && now_unix_nanos < self.deadline_at_unix_nanos
    }

    pub(crate) const fn execution_started(&self) -> bool {
        self.execution_started_at_unix_nanos.is_some()
    }

    /// The claim the Run row holds: its execution claim, or the worker claim
    /// of a Run that has not started; `None` for a Run with no claim.
    pub(crate) fn held_claim(&self) -> Option<&str> {
        self.execution_claim_id
            .as_deref()
            .or(self.worker_claim_id.as_deref())
    }

    /// Whether no process owns the Run at `now_unix_nanos` (ADR 0018,
    /// Recovery): its lease lapsed, or it has no claim and was accepted more
    /// than one lease ago. The previous release claimed a moment after
    /// acceptance, so a fresh Run with no claim belongs to the process that
    /// accepted it and is not abandoned. The sweep's query,
    /// `run_repository::lapsed_leases`, is the same predicate.
    pub(crate) fn lease_lapsed(&self, now_unix_nanos: i64, lease_nanos: i64) -> bool {
        match self.worker_claim_expires_at_unix_nanos {
            Some(expires_at) => expires_at <= now_unix_nanos,
            None => self.accepted_at_unix_nanos.saturating_add(lease_nanos) < now_unix_nanos,
        }
    }

    /// A termination intent no Principal asked for: the deadline, an
    /// interruption, or the memory limit. A cancellation's intent is the Run
    /// Owner's to apply.
    pub(crate) fn request_termination(
        &self,
        kind: RunTerminationKind,
        started_at_unix_nanos: i64,
    ) -> Result<RequestedTermination, Error> {
        RequestedTermination::new(kind, None, started_at_unix_nanos, self.shutdown_grace_nanos)
    }
}

/// A termination intent as a writer records it: the termination and the
/// Principal who asked for a cancellation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestedTermination {
    pub(crate) termination: RunTermination,
    pub(crate) requester: Option<Principal>,
}

impl RequestedTermination {
    pub(crate) fn new(
        kind: RunTerminationKind,
        requester: Option<Principal>,
        started_at_unix_nanos: i64,
        shutdown_grace_nanos: i64,
    ) -> Result<Self, Error> {
        if (kind == RunTerminationKind::Canceled) != requester.is_some() {
            return Err(Error::CorruptData(
                "Run termination has inconsistent requester evidence".into(),
            ));
        }
        let deadline_at_unix_nanos = started_at_unix_nanos
            .checked_add(shutdown_grace_nanos)
            .ok_or_else(|| Error::CorruptData("Run termination deadline overflowed".into()))?;
        Ok(Self {
            termination: RunTermination {
                kind,
                started_at_unix_nanos,
                deadline_at_unix_nanos,
            },
            requester,
        })
    }

    /// Whether recording this intent releases the worker claim: an
    /// interruption does, the Run having no live owner.
    pub(crate) fn releases_worker_claim(&self) -> bool {
        self.termination.kind == RunTerminationKind::Interrupted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunTerminationKind {
    Canceled,
    TimedOut,
    Interrupted,
    MemoryLimitExceeded,
}

impl RunTerminationKind {
    pub(crate) fn outcome(self) -> RunTerminalOutcome {
        match self {
            Self::Canceled => RunTerminalOutcome::Canceled,
            Self::TimedOut => RunTerminalOutcome::TimedOut,
            Self::Interrupted => RunTerminalOutcome::Interrupted,
            Self::MemoryLimitExceeded => RunTerminalOutcome::Failed(RunFailure::new(
                RunFailureCode::MemoryLimitExceeded,
                "Program memory limit exceeded".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunTermination {
    pub(crate) kind: RunTerminationKind,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) deadline_at_unix_nanos: i64,
}
