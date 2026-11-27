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
    /// The start mark's instant plus the Run's time limit; `None` before the
    /// Run starts, and a Run with none never times out.
    pub(crate) deadline_at_unix_nanos: Option<i64>,
    /// The worker claim an older release's Run Owner wrote with acceptance;
    /// a Run this release accepts has none (dropped by #2916).
    pub(crate) worker_claim_id: Option<String>,
    /// The Coral Instance that holds the Run's stream, which its start or
    /// a reopen records.
    pub(crate) holder_instance_id: Option<crate::CoralInstanceId>,
    pub(crate) execution_claim_id: Option<String>,
    pub(crate) execution_started_at_unix_nanos: Option<i64>,
    /// A termination an older build recorded. This release records none:
    /// it ends a Run in the chunk that decides its termination.
    pub(crate) termination: Option<RunTermination>,
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
    /// The Run Trace acceptance stored; `None` for a Run accepted before it
    /// was stored, or without an OpenTelemetry layer.
    pub(crate) trace: Option<coral_telemetry::RunTrace>,
    /// The time limit acceptance stored, which a Run stream's start adds
    /// to its own instant; `None` for a Run accepted before it was stored.
    pub(crate) time_limit_nanos: Option<i64>,
    /// When a take gave the Run to a Run Executor, on the clock of the
    /// Coral Instance that served the take.
    pub(crate) taken_at_unix_nanos: Option<i64>,
    /// The kind of Run Executor whose take holds the Run; `None` while no
    /// take does: before the first, after a put-back, or on an older
    /// release's Run Owner's Run. A local child's Run stream reaches only its
    /// parent Coral Instance.
    pub(crate) run_executor_kind: Option<coral_run_executor::Kind>,
    /// The highest Run stream request number committed.
    pub(crate) committed_request_number: Option<u64>,
    pub(crate) last_commit_at_unix_nanos: Option<i64>,
}

impl RunRecord {
    /// The Run Trace identifier its terminal diagnostic names: only for a
    /// Run that failed, timed out or was interrupted.
    pub(crate) fn diagnostic_trace_id(&self) -> Option<coral_telemetry::TraceId> {
        matches!(
            self.state,
            RunState::Failed | RunState::TimedOut | RunState::Interrupted
        )
        .then(|| self.trace.as_ref().map(coral_telemetry::RunTrace::trace_id))
        .flatten()
    }

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

    /// The commit time a chunk on this row records for `now`: never before
    /// acceptance, and never the instant the row carries, so a reread can
    /// tell the chunk landed.
    pub(crate) fn commit_mark(&self, now: i64) -> i64 {
        let mark = now.max(self.accepted_at_unix_nanos);
        if Some(mark) == self.last_commit_at_unix_nanos {
            mark.saturating_add(1)
        } else {
            mark
        }
    }

    /// Whether the Run row still takes the loop's chunks: a running Run that
    /// holds `claim_id` as its loop's claim ([`Self::holds_loop_claim`]).
    /// Never the clock: the loop applies the Run deadline itself, and an
    /// older build's recorded termination intent does not stop the loop
    /// recording what it applied before the intent.
    pub(crate) fn accepts_loop_chunk(&self, claim_id: &str) -> bool {
        self.state == RunState::Running && self.holds_loop_claim(claim_id)
    }

    /// Whether `claim_id` is the execution claim a take gave the Run for a
    /// Run Executor. An older release's Run Owner's Run, which a worker
    /// claim fences, takes no chunk of a loop.
    pub(crate) fn holds_loop_claim(&self, claim_id: &str) -> bool {
        self.execution_claim_id.as_deref() == Some(claim_id)
            && self.run_executor_kind.is_some()
            && self.worker_claim_id.is_none()
    }

    /// Whether the loop holding `claim_id` may still do Program work on
    /// this Run. True when the Run is running, no termination is recorded,
    /// it holds `claim_id` as its loop's claim ([`Self::holds_loop_claim`]),
    /// and the deadline has not passed.
    pub(crate) fn program_work_is_current(&self, claim_id: &str, now_unix_nanos: i64) -> bool {
        self.state == RunState::Running
            && self.termination.is_none()
            && self.holds_loop_claim(claim_id)
            && !self.deadline_passed(now_unix_nanos)
    }

    /// Whether the recorded Run deadline passed at `now_unix_nanos`; never
    /// for a Run with no deadline.
    pub(crate) fn deadline_passed(&self, now_unix_nanos: i64) -> bool {
        self.deadline_at_unix_nanos
            .is_some_and(|deadline| now_unix_nanos >= deadline)
    }

    /// The earlier of `at_unix_nanos` and the Run deadline: `at_unix_nanos`
    /// for a Run with no deadline.
    pub(crate) fn bounded_by_deadline(&self, at_unix_nanos: i64) -> i64 {
        self.deadline_at_unix_nanos
            .map_or(at_unix_nanos, |deadline| deadline.min(at_unix_nanos))
    }

    /// Whether a Run Executor waits for, has taken or holds the Run: it is
    /// running with no worker claim, and started only if a take gave it to a
    /// Run Executor. An older release's Run Owner's Run holds a worker claim
    /// until it ends, unless that release released it when it recorded the
    /// Run's interruption.
    pub(crate) fn runs_on_run_executor(&self) -> bool {
        self.state == RunState::Running
            && self.worker_claim_id.is_none()
            && (!self.execution_started() || self.run_executor_kind.is_some())
    }

    pub(crate) const fn execution_started(&self) -> bool {
        self.execution_started_at_unix_nanos.is_some()
    }

    /// The claim the Run row holds: the one a take gave it; `None` for a
    /// Run no take claimed.
    pub(crate) fn held_claim(&self) -> Option<&str> {
        self.execution_claim_id.as_deref()
    }
}

/// A termination as a writer decides it, for the chunk that ends the Run:
/// the termination and the Principal who asked for a cancellation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestedTermination {
    pub(crate) termination: RunTermination,
    pub(crate) requester: Option<Principal>,
}

impl RequestedTermination {
    /// A termination the Run row records, written again as it is: the row
    /// keeps a recorded cancellation's requester.
    pub(crate) const fn recorded(termination: RunTermination) -> Self {
        Self {
            termination,
            requester: None,
        }
    }

    pub(crate) fn new(
        kind: RunTerminationKind,
        requester: Option<Principal>,
        started_at_unix_nanos: i64,
    ) -> Result<Self, Error> {
        if (kind == RunTerminationKind::Canceled) != requester.is_some() {
            return Err(Error::CorruptData(
                "Run termination has inconsistent requester evidence".into(),
            ));
        }
        Ok(Self {
            termination: RunTermination {
                kind,
                started_at_unix_nanos,
            },
            requester,
        })
    }

    /// A termination no Principal asked for: the deadline, an interruption,
    /// the memory limit, or the CPU limit.
    pub(crate) fn unrequested(
        kind: RunTerminationKind,
        started_at_unix_nanos: i64,
    ) -> Result<Self, Error> {
        Self::new(kind, None, started_at_unix_nanos)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunTerminationKind {
    Canceled,
    TimedOut,
    Interrupted,
    MemoryLimitExceeded,
    CpuLimitExceeded,
}

impl RunTerminationKind {
    /// How a Run in `state` ended, as the reason its Program's `Terminate`
    /// gives: `None` for a running Run, and for one that succeeded or
    /// failed (a limit's failure among them).
    pub(crate) const fn of_ended(state: RunState) -> Option<Self> {
        match state {
            RunState::Canceled => Some(Self::Canceled),
            RunState::TimedOut => Some(Self::TimedOut),
            RunState::Interrupted => Some(Self::Interrupted),
            RunState::Running | RunState::Succeeded | RunState::Failed => None,
        }
    }

    pub(crate) fn outcome(self) -> RunTerminalOutcome {
        match self {
            Self::Canceled => RunTerminalOutcome::Canceled,
            Self::TimedOut => RunTerminalOutcome::TimedOut,
            Self::Interrupted => RunTerminalOutcome::Interrupted,
            Self::MemoryLimitExceeded => RunTerminalOutcome::Failed(RunFailure::new(
                RunFailureCode::MemoryLimitExceeded,
                "Program memory limit exceeded".to_string(),
            )),
            Self::CpuLimitExceeded => RunTerminalOutcome::Failed(RunFailure::new(
                RunFailureCode::CpuLimitExceeded,
                "Program CPU limit exceeded".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunTermination {
    pub(crate) kind: RunTerminationKind,
    pub(crate) started_at_unix_nanos: i64,
}
