//! The Run's spans. Acceptance begins the Run Trace with a short
//! `coral.run.accept` root linked to the accepting request; every later
//! phase is a new child of that stored accept span, on whichever Coral
//! Instance runs it, and no span of an earlier phase is reopened.

use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use coral_catalog::ExecutionBinding;

use coral_telemetry::RunTrace;
use coral_telemetry::{metric, safe};
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::invocation::InvocationHostError;
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

/// Counts a Run accepted for `principal`, and the claim written with it,
/// which is uncontested.
pub(crate) fn record_run_accepted(principal: &coral_workspace::Principal) {
    metric::run_accepted(match principal.kind() {
        coral_workspace::PrincipalKind::User => metric::PrincipalKind::User,
        coral_workspace::PrincipalKind::Agent => metric::PrincipalKind::Agent,
    });
    metric::run_claimed(metric::ClaimKind::Accept, metric::ClaimOutcome::Won);
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

/// One Tool Invocation's telemetry: its `coral.invocation` span, its
/// measure from admission, and whether its upstream send began.
#[derive(Debug)]
pub(crate) struct InvocationTrace {
    span: tracing::Span,
    measure: OnceLock<metric::InvocationMeasure>,
    sent: AtomicBool,
}

impl InvocationTrace {
    /// The Invocation of the Tool at `call_path`, a Tool of `provider`: a
    /// span that is a child of the current span (the Run's execute phase).
    pub(crate) fn begin(
        id: &crate::InvocationId,
        call_path: coral_provider::ResolvedCallPath<'_>,
        provider: &coral_provider::ProviderId,
    ) -> Self {
        let span = tracing::info_span!(
            "coral.invocation",
            coral.invocation.id = safe::id(id),
            coral.tool.call_path = safe::id(&call_path),
            coral.provider.id = safe::id(provider),
            coral.provider.protocol = Empty,
            coral.invocation.dispatch_started = Empty,
            coral.invocation.result_kind = Empty,
            coral.invocation.failure.code = Empty,
            error.type = Empty,
        );
        Self {
            span,
            measure: OnceLock::new(),
            sent: AtomicBool::new(false),
        }
    }

    pub(crate) const fn span(&self) -> &tracing::Span {
        &self.span
    }

    /// Counts the Invocation of `tool` as started, once its Run Owner
    /// admitted it, and measures it until [`Self::end`]. One the owner
    /// refuses is never counted.
    pub(crate) fn admitted(&self, tool: &coral_catalog::MaterializedTool) {
        let protocol = match tool.execution_binding() {
            ExecutionBinding::Http(_) => metric::ProviderProtocol::Http,
            ExecutionBinding::Mcp(_) => metric::ProviderProtocol::Mcp,
            ExecutionBinding::Builtin(_) => metric::ProviderProtocol::Builtin,
        };
        let effect = if coral_policy::reads_only(tool.hints(), tool.http_method()) {
            metric::ToolEffect::Read
        } else {
            metric::ToolEffect::Write
        };
        let _first = self
            .measure
            .set(metric::invocation_started(protocol, effect));
    }

    /// Marks the upstream send begun: a stop from now on leaves the
    /// Invocation `indeterminate`.
    pub(crate) fn sending(&self) {
        self.sent.store(true, Ordering::Release);
    }

    /// Records how the Invocation that returned `ended` ended. A complete
    /// Upstream Outcome is never an error, whatever its status; a Coral
    /// Failure, `indeterminate` and a host defect are. `run_terminated` is
    /// not.
    pub(crate) fn end(self, ended: &Result<String, InvocationHostError>) {
        use crate::InvocationResultKind;
        let (kind, failure_code) = match ended {
            Ok(_) | Err(InvocationHostError::Upstream { .. }) => {
                (Some(InvocationResultKind::UpstreamOutcome), None)
            }
            Err(InvocationHostError::Rejected(rejection)) => (
                Some(InvocationResultKind::CoralFailure),
                Some(rejection.code()),
            ),
            Err(InvocationHostError::Fatal(crate::Error::ProgramHostTerminated)) => (
                Some(if self.sent.load(Ordering::Acquire) {
                    InvocationResultKind::Indeterminate
                } else {
                    InvocationResultKind::RunTerminated
                }),
                None,
            ),
            Err(InvocationHostError::Fatal(_)) => (None, None),
        };
        let Self { span, measure, .. } = self;
        if let Some(measure) = measure.into_inner() {
            measure.end(
                match kind {
                    Some(InvocationResultKind::UpstreamOutcome) => {
                        metric::InvocationEnd::UpstreamOutcome
                    }
                    Some(InvocationResultKind::CoralFailure) => metric::InvocationEnd::CoralFailure,
                    Some(InvocationResultKind::RunTerminated) => {
                        metric::InvocationEnd::RunTerminated
                    }
                    Some(InvocationResultKind::Indeterminate) => {
                        metric::InvocationEnd::Indeterminate
                    }
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
                coral_telemetry::record_failure(&span, code);
            }
            Some(InvocationResultKind::Indeterminate) => {
                coral_telemetry::record_failure(&span, "indeterminate");
            }
            None => coral_telemetry::record_failure(&span, "host_defect"),
        }
    }
}

/// One step of a Tool Invocation on its way to dispatch.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Step {
    Plan,
    /// Readying the Account's credentials, including a token refresh.
    Credentials,
    Enforcement,
    /// Opening the credentials the Policy Enforcement Point judged current.
    Authorize,
    Persist,
}

/// A gate a Tool Invocation passes before its Policy Enforcement Point.
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
        Step::Enforcement => {
            tracing::info_span!(parent: invocation, "coral.invocation.policy_enforcement_point")
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

/// Links the current span, a request about a Run its caller may see, to the
/// Run's Run Trace. Call it only once the Run is found visible.
pub(crate) fn link_request(trace: Option<&RunTrace>) {
    if let Some(trace) = trace {
        trace.link(&tracing::Span::current());
    }
}
