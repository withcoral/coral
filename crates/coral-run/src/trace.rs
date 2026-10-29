//! The Run's spans. Acceptance begins the Run Trace with a short
//! `coral.run.accept` root linked to the accepting request; every later
//! phase is a new child of that stored accept span, on whichever Coral
//! Instance runs it, and no span of an earlier phase is reopened.

use std::future::Future;

use coral_telemetry::RunTrace;
use coral_telemetry::{metric, safe};
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::lifecycle::{RunRecord, RunTerminationKind};
use crate::types::HeldRunId;
use crate::{FinalValue, RunState, RunTerminalOutcome};

/// One phase of a Run under its Run Owner.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Phase {
    /// The start mark: the owner's first chunk.
    Start,
    /// The Program, from start until it returns or throws.
    Execute,
    /// The terminal write after the Program ended, or an interruption.
    Complete,
    /// Settling a recorded termination: a cancellation, the deadline or a
    /// shutdown.
    Terminate,
    /// A recovered owner's one chunk.
    Recover,
}

impl Phase {
    const fn metric(self) -> metric::RunPhase {
        match self {
            Self::Start => metric::RunPhase::Start,
            Self::Execute => metric::RunPhase::Execute,
            Self::Complete => metric::RunPhase::Complete,
            Self::Terminate => metric::RunPhase::Terminate,
            Self::Recover => metric::RunPhase::Recover,
        }
    }
}

/// A span whose duration is also a metric: recorded once the work it
/// instruments is over, or once it drops.
pub(crate) struct TimedSpan {
    span: tracing::Span,
    timer: metric::Timer,
}

impl TimedSpan {
    /// The span, for work that runs in it more than once.
    pub(crate) const fn span(&self) -> &tracing::Span {
        &self.span
    }

    /// Runs `work` in the span and times it.
    pub(crate) fn instrument<F: Future>(self, work: F) -> impl Future<Output = F::Output> {
        let Self { span, timer } = self;
        async move {
            let output = work.await;
            drop(timer);
            output
        }
        .instrument(span)
    }
}

/// The `coral.run.accept` span, not yet entered. Its context becomes the
/// Run Trace (`RunTrace::begin`).
pub(crate) fn accept_span() -> tracing::Span {
    tracing::info_span!(parent: None, "coral.run.accept", coral.run.id = Empty)
}

/// A new span for `phase` of the Run `run_id`, a child of the stored accept
/// span, timed as `coral.run.phase.duration`. Without a stored Run Trace it
/// is a new root.
pub(crate) fn phase_span(trace: Option<&RunTrace>, run_id: &HeldRunId, phase: Phase) -> TimedSpan {
    let span = match phase {
        Phase::Start => tracing::info_span!(
            parent: None,
            "coral.run.start",
            coral.run.id = safe::id(run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
        Phase::Execute => {
            tracing::info_span!(parent: None, "coral.run.execute", coral.run.id = safe::id(run_id))
        }
        Phase::Complete => tracing::info_span!(
            parent: None,
            "coral.run.complete",
            coral.run.id = safe::id(run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
        Phase::Terminate => tracing::info_span!(
            parent: None,
            "coral.run.terminate",
            coral.run.id = safe::id(run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
        Phase::Recover => tracing::info_span!(
            parent: None,
            "coral.run.recover",
            coral.run.id = safe::id(run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
    };
    if let Some(trace) = trace {
        trace.adopt(&span);
    }
    TimedSpan {
        span,
        timer: metric::run_phase(phase.metric()),
    }
}

/// A claim ID as a span field. Coral mints each as a UUID; one that does
/// not parse is left off.
pub(crate) fn claim(claim_id: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(claim_id).ok()
}

/// How a Run's committed terminal write ended it, as its end records it.
#[derive(Debug)]
pub(crate) enum RunEnd<'a> {
    /// Ended before its Program started.
    Unstarted(&'a RunTerminalOutcome),
    /// Its Program returned, and the write recorded `outcome`: the Final
    /// Value is rejected when it was over the Run's output budget.
    Returned {
        outcome: &'a RunTerminalOutcome,
        final_value_over_budget: bool,
    },
    /// This Coral Instance ended it for `reason`, stopping its Program if
    /// the Program had started.
    Stopped {
        outcome: &'a RunTerminalOutcome,
        reason: RunTerminationKind,
    },
    /// A recovered Run Owner ended it. Its Program, if it started, ended
    /// with the Coral Instance that ran it, so none is stopped here.
    Recovered(&'a RunTerminalOutcome),
}

impl RunEnd<'_> {
    const fn outcome(&self) -> &RunTerminalOutcome {
        match self {
            Self::Unstarted(outcome)
            | Self::Returned { outcome, .. }
            | Self::Stopped { outcome, .. }
            | Self::Recovered(outcome) => outcome,
        }
    }
}

/// Records the end of `run`, whose terminal write committed at `ended_at`
/// (Unix nanoseconds), on the current phase's span: its state, and for a
/// failure, a timeout or an interruption, its failure code as the error. A
/// success or a cancellation is not an error. Counts the Run's end and its
/// duration from acceptance, the Program it stopped, and the Final Value it
/// kept or rejected.
pub(crate) fn record_run_end(run: &RunRecord, end: &RunEnd<'_>, ended_at: i64) {
    let outcome = end.outcome();
    let span = tracing::Span::current();
    let state = outcome.state();
    span.record("coral.run.status", safe::kind(state.as_storage_str()));
    if let Some(code) = outcome.failure_code() {
        coral_telemetry::record_failure(&span, code);
    }
    match end {
        RunEnd::Returned {
            outcome:
                RunTerminalOutcome::Succeeded {
                    final_value: FinalValue::Present(value),
                },
            ..
        } => metric::output_kept(metric::OutputKind::FinalValue, value.encoded_json().len()),
        RunEnd::Returned {
            final_value_over_budget: true,
            ..
        } => metric::output_rejected(metric::OutputKind::FinalValue),
        RunEnd::Stopped { reason, .. } if run.execution_started() => {
            metric::program_terminated(termination_reason(*reason));
        }
        RunEnd::Unstarted(_)
        | RunEnd::Returned { .. }
        | RunEnd::Stopped { .. }
        | RunEnd::Recovered(_) => {}
    }
    let status = match state {
        RunState::Succeeded => metric::RunStatus::Succeeded,
        RunState::Failed => metric::RunStatus::Failed,
        RunState::Canceled => metric::RunStatus::Canceled,
        RunState::TimedOut => metric::RunStatus::TimedOut,
        RunState::Interrupted => metric::RunStatus::Interrupted,
        RunState::Running => return,
    };
    let elapsed = u64::try_from(ended_at.saturating_sub(run.accepted_at_unix_nanos)).unwrap_or(0);
    metric::run_ended(
        status,
        outcome.failure_code(),
        std::time::Duration::from_nanos(elapsed),
    );
}

const fn termination_reason(kind: RunTerminationKind) -> metric::TerminationReason {
    match kind {
        RunTerminationKind::Canceled => metric::TerminationReason::Canceled,
        RunTerminationKind::TimedOut => metric::TerminationReason::TimedOut,
        RunTerminationKind::Interrupted => metric::TerminationReason::Interrupted,
        RunTerminationKind::MemoryLimitExceeded => metric::TerminationReason::MemoryLimitExceeded,
    }
}

/// The `coral.invocation` span of one admitted Tool Invocation of the Tool
/// at `call_path`, a Tool of `provider`, a child of the current span (the
/// Run's execute phase).
pub(crate) fn invocation_span(
    id: &crate::InvocationId,
    call_path: coral_provider::ResolvedCallPath<'_>,
    provider: &coral_provider::ProviderId,
) -> tracing::Span {
    tracing::info_span!(
        "coral.invocation",
        coral.invocation.id = safe::id(id),
        coral.tool.call_path = safe::id(&call_path),
        coral.provider.id = safe::id(provider),
        coral.provider.protocol = Empty,
        coral.invocation.dispatch_started = Empty,
        coral.invocation.result_kind = Empty,
        coral.invocation.failure.code = Empty,
        error.type = Empty,
    )
}

/// One step of a Tool Invocation on its way to dispatch.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Step {
    Plan,
    /// Readying the Account's credentials, including a token refresh.
    Credentials,
    DispatchFence,
    /// Opening the credentials the Dispatch Fence judged current.
    Authorize,
    Persist,
}

/// A gate a Tool Invocation passes before its Dispatch Fence.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Gate {
    /// The held recheck of the Workspace, Principal, Provider and policy.
    Authority,
    /// Selecting the Account, including a wait on a connection request.
    Account,
    /// The Approval Request, including the wait for its decision.
    Approval,
}

/// A span for `step` of the Invocation whose span is `invocation`.
pub(crate) fn step_span(invocation: &tracing::Span, step: Step) -> tracing::Span {
    match step {
        Step::Plan => tracing::info_span!(parent: invocation, "coral.invocation.plan"),
        Step::Credentials => {
            tracing::info_span!(parent: invocation, "coral.invocation.credentials")
        }
        Step::DispatchFence => {
            tracing::info_span!(parent: invocation, "coral.invocation.dispatch_fence")
        }
        Step::Authorize => tracing::info_span!(parent: invocation, "coral.invocation.authorize"),
        Step::Persist => tracing::info_span!(parent: invocation, "coral.invocation.persist"),
    }
}

/// A span for `gate` of the Invocation whose span is `invocation`, timed as
/// `coral.invocation.gate.duration`.
pub(crate) fn gate_span(invocation: &tracing::Span, gate: Gate) -> TimedSpan {
    let kind = match gate {
        Gate::Authority => metric::GateKind::Authority,
        Gate::Account => metric::GateKind::Account,
        Gate::Approval => metric::GateKind::Approval,
    };
    TimedSpan {
        span: tracing::info_span!(parent: invocation, "coral.invocation.gate", coral.invocation.gate.kind = safe::kind(kind.as_str())),
        timer: metric::invocation_gate(kind),
    }
}

/// Counts one admitted Tool Invocation of `tool`, a Tool of `protocol`,
/// and measures it until [`record_invocation_end`].
pub(crate) fn invocation_started(
    tool: &coral_catalog::MaterializedTool,
    protocol: metric::ProviderProtocol,
) -> metric::InvocationMeasure {
    let effect = if coral_policy::reads_only(tool.hints(), tool.http_method()) {
        metric::ToolEffect::Read
    } else {
        metric::ToolEffect::Write
    };
    metric::invocation_started(protocol, effect)
}

/// Records how a Tool Invocation ended: `kind`, or a host defect for
/// `None`. A complete Upstream Outcome is never an error, whatever its
/// status; a Coral Failure, `indeterminate` and a host defect are.
/// `run_terminated` is not. Its end is counted on `measure`, which an
/// Invocation its Run Owner never admitted does not have.
pub(crate) fn record_invocation_end(
    span: &tracing::Span,
    measure: Option<metric::InvocationMeasure>,
    kind: Option<crate::InvocationResultKind>,
    failure_code: Option<&'static str>,
) {
    use crate::InvocationResultKind;
    if let Some(measure) = measure {
        measure.end(
            match kind {
                Some(InvocationResultKind::UpstreamOutcome) => {
                    metric::InvocationEnd::UpstreamOutcome
                }
                Some(InvocationResultKind::CoralFailure) => metric::InvocationEnd::CoralFailure,
                Some(InvocationResultKind::RunTerminated) => metric::InvocationEnd::RunTerminated,
                Some(InvocationResultKind::Indeterminate) => metric::InvocationEnd::Indeterminate,
                None => metric::InvocationEnd::HostDefect,
            },
            failure_code,
        );
    }
    if let Some(kind) = kind {
        span.record(
            "coral.invocation.result_kind",
            safe::kind(kind.as_storage_str()),
        );
    }
    match kind {
        Some(InvocationResultKind::UpstreamOutcome | InvocationResultKind::RunTerminated) => {}
        Some(InvocationResultKind::CoralFailure) => {
            let code = failure_code.unwrap_or("coral_failure");
            span.record("coral.invocation.failure.code", safe::kind(code));
            coral_telemetry::record_failure(span, code);
        }
        Some(InvocationResultKind::Indeterminate) => {
            coral_telemetry::record_failure(span, "indeterminate");
        }
        None => coral_telemetry::record_failure(span, "host_defect"),
    }
}

/// Links the current span, a request about a Run its caller may see, to the
/// Run's Run Trace. Call it only once the Run is found visible.
pub(crate) fn link_request(trace: Option<&RunTrace>) {
    if let Some(trace) = trace {
        trace.link(&tracing::Span::current());
    }
}
