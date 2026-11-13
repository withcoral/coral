//! The Program's host on a Run Executor. It numbers the Program's requests
//! and sends them on the Run stream. It settles Emissions and Logs itself,
//! and holds each Invoke, Perform and Complete until its `InvocationResult`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use coral_v8::{
    AuthoredLocation, AuthoredSource, ConsoleLevel, PermitFuture, ProgramExecutionControl,
    ProgramOutputHostError, ToolBodyOutcome, ToolCalls, ToolCompletion, ToolFuture, ToolHostError,
};
use tokio::sync::{mpsc, oneshot};

use super::PROGRAM_LOG_MESSAGE_MAX_BYTES;
use crate::link::HeldRun;
use crate::wire::v1::{
    Complete, Emit, Failure, Finish, FinishEnd, InvocationResult, Invoke, LinkedToolRoot, Location,
    Log, LogLevel, NotData, Perform, Request, RunRequest, Started, Threw, complete, finish,
    invocation_result, request, run_request,
};

/// What Coral's answer settles for one request.
enum Awaiting {
    /// An Invoke's or a Complete's value, as native JSON.
    Value(oneshot::Sender<Result<String, ToolHostError>>),
    /// A Perform's permit.
    Permit(oneshot::Sender<Result<(), ToolHostError>>),
}

/// One Run's Program host, shared by the Program's thread and its stream.
pub(super) struct RunHost {
    state: Mutex<State>,
    changed: Condvar,
    outbound: mpsc::UnboundedSender<RunRequest>,
    control: ProgramExecutionControl,
    value_limits: coral_value::ValueLimits,
    applied_cap_bytes: usize,
    #[cfg(test)]
    on_parked: Option<&'static mpsc::UnboundedSender<u64>>,
    /// Each linked Tool's root and path, to its id.
    linked: HashMap<(LinkedToolRoot, String), String>,
    trace_id: Option<String>,
}

#[derive(Default)]
struct State {
    /// `Started` arrived.
    started: bool,
    /// `Started`'s CPU budget.
    cpu_budget: Option<Duration>,
    /// The Program must stop: `Terminate`, a host failure, or the stream's end.
    stopped: bool,
    /// The Program's output exceeded the Run Output Budget.
    over_budget: bool,
    /// The number of the last request sent.
    last: u64,
    /// The Run Output Budget bytes counted.
    output: usize,
    /// Each request above the committed watermark, with its weight under
    /// the Applied cap: its output bytes, which are zero for a request Coral
    /// answers.
    unconfirmed: BTreeMap<u64, usize>,
    /// The sum of `unconfirmed`'s weights.
    unconfirmed_bytes: usize,
    /// Each request that waits for its `InvocationResult`.
    invocations: HashMap<u64, Awaiting>,
}

impl RunHost {
    pub(super) fn new(
        outbound: mpsc::UnboundedSender<RunRequest>,
        value_limits: coral_value::ValueLimits,
        limits: &super::RunLimits,
        linked: HashMap<(LinkedToolRoot, String), String>,
        trace_id: Option<String>,
    ) -> Self {
        Self {
            state: Mutex::default(),
            changed: Condvar::new(),
            outbound,
            control: ProgramExecutionControl::new(),
            value_limits,
            applied_cap_bytes: limits.applied_cap_bytes,
            #[cfg(test)]
            on_parked: limits.on_parked,
            linked,
            trace_id,
        }
    }

    /// Runs `program` on a thread of its own, which loads it into a fresh
    /// isolate, runs it once `Started` arrives, and sends `Finish`. The
    /// thread frees `unit` and completes the receiver once it has discarded
    /// the isolate.
    pub(super) fn spawn(
        self: &Arc<Self>,
        program: coral_v8::CompiledProgram,
        isolate: coral_v8::IsolateLimits,
        unit: HeldRun,
    ) -> std::io::Result<oneshot::Receiver<()>> {
        let host = self.clone();
        let (exiting, exited) = oneshot::channel();
        // The thread records where its creator does.
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::Builder::new()
            .name("coral-run".to_owned())
            .spawn(move || {
                // Dropped last, after the isolate.
                let _unit = unit;
                let _exiting = exiting;
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let executor = coral_v8::ProgramExecutor::new(isolate);
                let result =
                    executor.execute_with_host_control(&program, host.clone(), &host.control);
                host.finish(host.completion(result));
            })?;
        Ok(exited)
    }

    pub(super) fn started(&self, started: &Started) {
        let mut state = self.state();
        state.started = true;
        state.cpu_budget = started.cpu_budget_ms.map(Duration::from_millis);
        drop(state);
        self.changed.notify_all();
    }

    /// Settles the request `result` answers, and counts its bytes against
    /// the Run Output Budget. `false` when it breaks the protocol: no
    /// outcome, no request waits for it while the Program runs, a permit for
    /// an Invoke or a Complete, or a value or an Upstream Outcome for a
    /// Perform.
    pub(super) fn invocation_result(&self, result: InvocationResult) -> bool {
        /// What an answer gives short of a rejection.
        enum Answer {
            Value(String),
            Permitted,
        }
        /// A waiter and the answer it takes.
        enum Reply {
            Value(
                oneshot::Sender<Result<String, ToolHostError>>,
                Result<String, ToolHostError>,
            ),
            Permit(
                oneshot::Sender<Result<(), ToolHostError>>,
                Result<(), ToolHostError>,
            ),
        }
        let answer = match result.outcome {
            None => return false,
            Some(invocation_result::Outcome::ResultJson(json)) => Ok(Answer::Value(json)),
            Some(invocation_result::Outcome::Permitted(_)) => Ok(Answer::Permitted),
            Some(invocation_result::Outcome::CoralFailure(rejection)) => {
                Err(ToolHostError::Rejected {
                    code: rejection.code.into(),
                    message: rejection.message.into(),
                    trace_id: Some(rejection.trace_id).filter(|id| !id.is_empty()),
                    suggestions: rejection.suggestions,
                })
            }
            Some(invocation_result::Outcome::UpstreamOutcome(upstream)) => {
                Err(ToolHostError::Upstream {
                    code: upstream.code.into(),
                    message: upstream.message.into(),
                    outcome_json: upstream.outcome_json,
                })
            }
            Some(invocation_result::Outcome::Fatal(_)) => Err(ToolHostError::Fatal),
        };
        let mut state = self.state();
        let Some(awaiting) = state.invocations.remove(&result.number) else {
            return state.stopped;
        };
        let reply = match (awaiting, answer) {
            // Answers of the wrong kind. A Perform dispatches nothing, so no
            // upstream answers it.
            (Awaiting::Value(_), Ok(Answer::Permitted))
            | (Awaiting::Permit(_), Ok(Answer::Value(_)) | Err(ToolHostError::Upstream { .. })) => {
                return false;
            }
            (Awaiting::Value(sender), Ok(Answer::Value(json))) => Reply::Value(sender, Ok(json)),
            (Awaiting::Permit(sender), Ok(Answer::Permitted)) => Reply::Permit(sender, Ok(())),
            (Awaiting::Value(sender), Err(error)) => Reply::Value(sender, Err(error)),
            (Awaiting::Permit(sender), Err(error)) => Reply::Permit(sender, Err(error)),
        };
        let bytes = usize::try_from(result.bytes).unwrap_or(usize::MAX);
        let over_budget = !self.fits(&state, bytes);
        state.output = state.output.saturating_add(bytes);
        state.over_budget |= over_budget;
        drop(state);
        // Output over the budget ends the Run whatever the answer was.
        let stops = over_budget
            || matches!(
                reply,
                Reply::Value(_, Err(ToolHostError::Fatal))
                    | Reply::Permit(_, Err(ToolHostError::Fatal))
            );
        if stops {
            // The Program may be running JavaScript that never polls this
            // Tool.
            self.stop();
        }
        // A waiter left unanswered settles as a host failure.
        if !over_budget {
            match reply {
                Reply::Value(sender, result) => drop(sender.send(result)),
                Reply::Permit(sender, result) => drop(sender.send(result)),
            }
        }
        true
    }

    /// Drops what the committed watermark at `number` covers, which may let
    /// the Program's next request go.
    pub(super) fn committed(&self, number: u64) {
        let mut state = self.state();
        let above = state.unconfirmed.split_off(&number.saturating_add(1));
        let covered: usize = std::mem::replace(&mut state.unconfirmed, above)
            .values()
            .sum();
        state.unconfirmed_bytes = state.unconfirmed_bytes.saturating_sub(covered);
        drop(state);
        self.changed.notify_all();
    }

    /// Ends the Program uncatchably, including one that waits for a request.
    pub(super) fn stop(&self) {
        let mut state = self.state();
        state.stopped = true;
        state.invocations.clear();
        drop(state);
        self.changed.notify_all();
        let _installed = self.control.terminate();
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Numbers and sends one of the Program's requests once no more than the
    /// Applied cap is above the committed watermark, and counts its output
    /// `bytes` against the Run Output Budget and the Applied cap. Its number,
    /// or `None` when the Program must stop instead.
    fn send(&self, kind: request::Kind, bytes: usize, awaiting: Option<Awaiting>) -> Option<u64> {
        let mut state = self.below_applied_cap();
        if state.stopped {
            return None;
        }
        if !self.fits(&state, bytes) {
            state.over_budget = true;
            drop(state);
            self.stop();
            return None;
        }
        state.output = state.output.saturating_add(bytes);
        let number = state.last.saturating_add(1);
        state.last = number;
        state.unconfirmed.insert(number, bytes);
        state.unconfirmed_bytes = state.unconfirmed_bytes.saturating_add(bytes);
        if let Some(awaiting) = awaiting {
            state.invocations.insert(number, awaiting);
        }
        self.transmit(number, kind);
        Some(number)
    }

    /// Sends `kind`, an Invoke or a Complete, and settles with the value
    /// Coral answers.
    fn ask(&self, kind: request::Kind) -> ToolFuture {
        let (answer, answered) = oneshot::channel();
        let sent = self.send(kind, 0, Some(Awaiting::Value(answer)));
        Box::pin(async move {
            sent.ok_or(ToolHostError::Fatal)?;
            answered.await.unwrap_or(Err(ToolHostError::Fatal))
        })
    }

    /// [`coral_v8::ProgramHost::invoke`] for a call that the body of
    /// permitted Perform `parent` makes, or the Program's own when `None`.
    fn invoke_as(
        &self,
        parent: Option<u64>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture {
        self.ask(request::Kind::Invoke(Invoke {
            call_path,
            arguments_json,
            location: self.location(location),
            parent,
        }))
    }

    /// [`coral_v8::ProgramHost::perform`] for a call that the body of
    /// permitted Perform `parent` makes, or the Program's own when `None`.
    fn perform_as(
        self: &Arc<Self>,
        parent: Option<u64>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> PermitFuture {
        let (answer, answered) = oneshot::channel();
        let perform = Perform {
            call_path,
            arguments_json,
            location: self.location(location),
            parent,
        };
        let sent = self.send(
            request::Kind::Perform(perform),
            0,
            Some(Awaiting::Permit(answer)),
        );
        let host = self.clone();
        Box::pin(async move {
            let perform = sent.ok_or(ToolHostError::Fatal)?;
            answered.await.unwrap_or(Err(ToolHostError::Fatal))?;
            Ok(Box::new(Performed(Arc::new(BodyCalls { host, perform })))
                as Box<dyn ToolCompletion>)
        })
    }

    /// The state once no more than the Applied cap is above the committed
    /// watermark, or the Program stopped.
    fn below_applied_cap(&self) -> MutexGuard<'_, State> {
        #[cfg(test)]
        let told = std::cell::Cell::new(false);
        self.wait_until(|state| {
            let below = state.unconfirmed_bytes <= self.applied_cap_bytes;
            #[cfg(test)]
            if !below && !state.stopped && !told.replace(true) {
                self.tell_parked(state);
            }
            below
        })
    }

    /// The state once `done` holds or the Program stopped.
    fn wait_until(&self, done: impl Fn(&State) -> bool) -> MutexGuard<'_, State> {
        let mut state = self.state();
        while !done(&state) && !state.stopped {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state
    }

    /// Tells a test the number of the request that parks on the Applied cap,
    /// before it waits.
    #[cfg(test)]
    fn tell_parked(&self, state: &State) {
        if let Some(on_parked) = self.on_parked {
            // A test that waits for none drops its receiver.
            let _unwatched = on_parked.send(state.last.saturating_add(1)).is_err();
        }
    }

    /// Whether `bytes` more output fit in the Run Output Budget.
    fn fits(&self, state: &State, bytes: usize) -> bool {
        state.output.saturating_add(bytes) <= self.value_limits.run_output_bytes()
    }

    /// Numbers and sends `Finish` once the Applied cap lets it go, or at once
    /// when the Program stopped. A Final Value counts against the Run Output
    /// Budget too: one that does not fit ends the Run with the output-budget
    /// end. Nothing is counted after it.
    fn finish(&self, completion: finish::Completion) {
        let mut state = self.below_applied_cap();
        let completion = match completion {
            finish::Completion::ValueJson(json) if !self.fits(&state, json.len()) => {
                finish::Completion::End(FinishEnd::OutputBudget.into())
            }
            completion => completion,
        };
        state.last = state.last.saturating_add(1);
        let finish = Finish {
            completion: Some(completion),
        };
        self.transmit(state.last, request::Kind::Finish(finish));
    }

    /// Sends request `number`. A closed stream stops the Program, so a
    /// failed send needs no answer here.
    fn transmit(&self, number: u64, kind: request::Kind) {
        let request = RunRequest {
            message: Some(run_request::Message::Request(Request {
                number,
                kind: Some(kind),
            })),
        };
        drop(self.outbound.send(request));
    }

    /// Names the snapshot of a linked Tool's call site, as Coral does. A
    /// path with no linked snapshot gives no location rather than a guess.
    fn location(&self, location: Option<AuthoredLocation>) -> Option<Location> {
        let location = location?;
        let linked = match location.source {
            AuthoredSource::Program => None,
            AuthoredSource::PrivateTool { path } => Some((LinkedToolRoot::Private, path)),
            AuthoredSource::SharedTool { path } => Some((LinkedToolRoot::Shared, path)),
        };
        let tool_id = match linked {
            None => String::new(),
            Some(key) => self.linked.get(&key)?.clone(),
        };
        Some(Location {
            tool_id,
            line: location.line,
            column: location.column,
        })
    }

    fn output(&self, kind: request::Kind, bytes: usize) -> Result<(), ProgramOutputHostError> {
        self.send(kind, bytes, None)
            .map(|_number| ())
            .ok_or(ProgramOutputHostError::Fatal)
    }

    /// How the Program ended, as `Finish` carries it. The codes are the Run
    /// failure codes Coral records.
    fn completion(
        &self,
        result: Result<coral_v8::ExecutionOutput, coral_v8::Error>,
    ) -> finish::Completion {
        let failure = |code: &str, message: String, error: Option<&coral_v8::ProgramError>| {
            finish::Completion::Failure(Failure {
                code: code.to_owned(),
                message,
                error_json: error
                    .and_then(|error| serde_json::to_string(error).ok())
                    .unwrap_or_default(),
            })
        };
        let end = |end: FinishEnd| finish::Completion::End(end.into());
        match result {
            Ok(output) => output
                .into_final_value_json()
                .map_or_else(|| end(FinishEnd::Absent), finish::Completion::ValueJson),
            Err(coral_v8::Error::MemoryLimitExceeded) => end(FinishEnd::MemoryLimit),
            Err(coral_v8::Error::CpuLimitExceeded) => end(FinishEnd::CpuBudget),
            Err(coral_v8::Error::ProgramHostTerminated) if self.state().over_budget => {
                end(FinishEnd::OutputBudget)
            }
            Err(coral_v8::Error::ProgramHostTerminated) => end(FinishEnd::HostTerminated),
            Err(coral_v8::Error::ResourceLimitExceeded) => failure(
                "resource_limit_exceeded",
                "Program output exceeds its limit".to_owned(),
                None,
            ),
            Err(coral_v8::Error::ProgramRejected(error)) => {
                failure("program_rejected", error.summary(), Some(&error))
            }
            Err(coral_v8::Error::ProgramOutputRejected(error)) => {
                failure("invalid_program_value", error.summary(), Some(&error))
            }
            Err(coral_v8::Error::InvalidProgramValue(message)) => {
                failure("invalid_program_value", message, None)
            }
            Err(_private_diagnostic) => failure(
                "program_execution_failed",
                "Program execution failed".to_owned(),
                None,
            ),
        }
    }
}

impl coral_v8::ProgramHost for RunHost {
    fn value_limits(&self) -> coral_value::ValueLimits {
        self.value_limits
    }

    fn trace_id(&self) -> Option<String> {
        self.trace_id.clone()
    }

    fn cpu_budget(&self) -> Option<Duration> {
        self.state().cpu_budget
    }

    fn start(&self) -> bool {
        drop(self.outbound.send(RunRequest {
            message: Some(run_request::Message::Loaded(crate::wire::v1::Loaded {})),
        }));
        !self.wait_until(|state| state.started).stopped
    }

    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture {
        self.invoke_as(None, call_path, arguments_json, location)
    }

    fn perform(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> PermitFuture {
        self.perform_as(None, call_path, arguments_json, location)
    }

    fn emit(
        &self,
        value_json: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        // V8 checked the value against the package's value limits.
        let emit = request::Kind::Emit(Emit {
            value_json: value_json.to_owned(),
            location: self.location(location),
        });
        self.output(emit, value_json.len())
    }

    fn log(
        &self,
        level: ConsoleLevel,
        message: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        if message.len() > PROGRAM_LOG_MESSAGE_MAX_BYTES {
            return Err(ProgramOutputHostError::LimitExceeded);
        }
        let level = match level {
            ConsoleLevel::Debug => LogLevel::Debug,
            ConsoleLevel::Info => LogLevel::Info,
            ConsoleLevel::Warn => LogLevel::Warn,
            ConsoleLevel::Error => LogLevel::Error,
        };
        let log = request::Kind::Log(Log {
            level: level.into(),
            message: message.to_owned(),
            location: self.location(location),
        });
        self.output(log, message.len())
    }
}

/// The Tool calls the body of one permitted Perform makes, each its child.
struct BodyCalls {
    host: Arc<RunHost>,
    /// The Perform's request number.
    perform: u64,
}

impl ToolCalls for BodyCalls {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture {
        self.host
            .invoke_as(Some(self.perform), call_path, arguments_json, location)
    }

    fn perform(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> PermitFuture {
        self.host
            .perform_as(Some(self.perform), call_path, arguments_json, location)
    }
}

/// Completes one permitted Perform. One the Program drops uncompleted sends
/// nothing: its `Finish` tells Coral the body never ended.
struct Performed(Arc<BodyCalls>);

impl ToolCompletion for Performed {
    fn body_calls(&self) -> Arc<dyn ToolCalls> {
        self.0.clone()
    }

    fn complete(self: Box<Self>, outcome: ToolBodyOutcome) -> ToolFuture {
        let outcome = match outcome {
            ToolBodyOutcome::Returned(json) => complete::Outcome::ReturnedJson(json),
            ToolBodyOutcome::NotData { field, reason } => {
                complete::Outcome::NotData(NotData { field, reason })
            }
            ToolBodyOutcome::Threw {
                message,
                tool_error,
            } => complete::Outcome::Threw(Threw {
                message,
                tool_error,
            }),
        };
        // Sent here, on the Program's thread, so the body has ended before
        // any later call of its leftover work.
        self.0.host.ask(request::Kind::Complete(Complete {
            perform: self.0.perform,
            outcome: Some(outcome),
        }))
    }
}
