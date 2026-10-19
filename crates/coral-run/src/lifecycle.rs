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
    /// The last Event sequence on the Run as of a read snapshot, `0` before
    /// `run.started`. `None` on a Run read under `hold_run`, which does not
    /// read the Event rows: the chunk commit numbers its Events itself.
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
    /// whose claim columns both hold `claim_id`, before its deadline. A lapsed
    /// lease does not end ownership (a local owner continues while its commits
    /// show a matching claim), and a recorded termination intent does not stop
    /// the owner recording what it applied before the intent.
    pub(crate) fn accepts_owner_chunk(&self, claim_id: &str, now_unix_nanos: i64) -> bool {
        self.state == RunState::Running
            && self.execution_claim_id.as_deref() == Some(claim_id)
            && self.worker_claim_id.as_deref() == Some(claim_id)
            && now_unix_nanos < self.deadline_at_unix_nanos
    }

    pub(crate) fn program_work_is_current(&self, claim_id: &str, now_unix_nanos: i64) -> bool {
        self.state == RunState::Running
            && self.termination.is_none()
            && self.execution_claim_id.as_deref() == Some(claim_id)
            && self.worker_claim_id.as_deref() == Some(claim_id)
            && self
                .worker_claim_expires_at_unix_nanos
                .is_some_and(|expires_at| now_unix_nanos < expires_at)
            && now_unix_nanos < self.deadline_at_unix_nanos
    }

    pub(crate) const fn execution_started(&self) -> bool {
        self.execution_started_at_unix_nanos.is_some()
    }

    pub(crate) fn request_termination<'a>(
        &self,
        kind: RunTerminationKind,
        requester: Option<&'a Principal>,
        started_at_unix_nanos: i64,
    ) -> Result<RequestedTermination<'a>, Error> {
        if (kind == RunTerminationKind::Canceled) != requester.is_some() {
            return Err(Error::CorruptData(
                "Run termination has inconsistent requester evidence".into(),
            ));
        }
        let deadline_at_unix_nanos = started_at_unix_nanos
            .checked_add(self.shutdown_grace_nanos)
            .ok_or_else(|| Error::CorruptData("Run termination deadline overflowed".into()))?;
        Ok(RequestedTermination {
            termination: RunTermination {
                kind,
                started_at_unix_nanos,
                deadline_at_unix_nanos,
            },
            requester,
            release_worker_claim: kind == RunTerminationKind::Interrupted,
        })
    }
}

pub(crate) struct RequestedTermination<'a> {
    pub(crate) termination: RunTermination,
    pub(crate) requester: Option<&'a Principal>,
    pub(crate) release_worker_claim: bool,
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
