//! The Program's host on a Run Executor. It numbers the Program's requests
//! and sends them on the Run stream. It settles Emissions and Logs itself,
//! and holds each Invoke, Perform and Complete until its `InvocationResult`.
//! It keeps every request above the committed watermark, so a reopened
//! stream gets them again, and the Program never sees the loss.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use coral_v8::{
    AuthoredLocation, AuthoredSource, ConsoleLevel, PermitFuture, ProgramExecutionControl,
    ProgramOutputHostError, ToolBodyOutcome, ToolCalls, ToolCompletion, ToolFuture, ToolHostError,
};
use tokio::sync::{mpsc, oneshot, watch};

use super::PROGRAM_LOG_MESSAGE_MAX_BYTES;
use crate::link::HeldUnit;
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
    /// Turns true once the Program is stopped.
    pub(super) stopping: watch::Sender<bool>,
    control: ProgramExecutionControl,
    value_limits: coral_value::ValueLimits,
    applied_cap_bytes: usize,
    held_cap: usize,
    #[cfg(test)]
    on_parked: Option<&'static mpsc::UnboundedSender<u64>>,
    /// Each linked Tool's root and path, to its id.
    linked: HashMap<(LinkedToolRoot, String), String>,
    trace_id: Option<String>,
}

#[derive(Default)]
struct State {
    /// The open Run stream's side, or `None` while it is lost.
    sink: Option<mpsc::UnboundedSender<RunRequest>>,
    /// The package loaded and `Loaded` was sent.
    loaded: bool,
    /// `Started` arrived.
    started: bool,
    /// `Started`'s CPU budget.
    cpu_budget: Option<Duration>,
    /// The Program must stop: `Terminate`, a host failure, the deadline, or
    /// the stream task's end.
    stopped: bool,
    /// How the host ended the Program, when the Run Output Budget or the
    /// Run's deadline did; the first wins.
    end: Option<FinishEnd>,
    /// The number of the last request sent.
    last: u64,
    /// The Run Output Budget bytes counted.
    output: usize,
    /// Each request above the committed watermark, kept for a resend.
    unconfirmed: BTreeMap<u64, Held>,
    /// The sum of `unconfirmed`'s weights.
    unconfirmed_bytes: usize,
    /// Each request that waits for its `InvocationResult`.
    invocations: HashMap<u64, Awaiting>,
}

/// One request above the committed watermark.
struct Held {
    kind: request::Kind,
    /// Its weight under the Applied cap: its output bytes, which are zero
    /// for `Finish` and for a request Coral answers.
    weight: usize,
    /// Coral answered it, so an answer to its resend repeats that one.
    answered: bool,
}

/// What answered `Open` on a reopened Run stream.
pub(super) enum Resume {
    /// The package again: the Run has not started.
    Package,
    /// The highest request number committed.
    Committed(u64),
}

impl State {
    /// Sends `message` on the open stream, if there is one. A lost stream's
    /// requests wait above the watermark for the next.
    fn transmit(&self, message: run_request::Message) {
        if let Some(sink) = &self.sink {
            drop(sink.send(RunRequest {
                message: Some(message),
            }));
        }
    }

    /// Keeps request `number` above the watermark, and sends it.
    fn hold(&mut self, number: u64, kind: request::Kind, weight: usize) {
        self.transmit(numbered(number, kind.clone()));
        self.unconfirmed_bytes = self.unconfirmed_bytes.saturating_add(weight);
        let held = Held {
            kind,
            weight,
            answered: false,
        };
        self.unconfirmed.insert(number, held);
    }

    /// Drops the requests the committed watermark at `number` covers.
    fn drop_covered(&mut self, number: u64) {
        let above = self.unconfirmed.split_off(&number.saturating_add(1));
        let covered: usize = std::mem::replace(&mut self.unconfirmed, above)
            .values()
            .map(|held| held.weight)
            .sum();
        self.unconfirmed_bytes = self.unconfirmed_bytes.saturating_sub(covered);
    }
}

fn numbered(number: u64, kind: request::Kind) -> run_request::Message {
    run_request::Message::Request(Request {
        number,
        kind: Some(kind),
    })
}

impl RunHost {
    pub(super) fn new(
        outbound: mpsc::UnboundedSender<RunRequest>,
        value_limits: coral_value::ValueLimits,
        limits: &super::RunLimits,
        linked: HashMap<(LinkedToolRoot, String), String>,
        trace_id: Option<String>,
    ) -> Self {
        let state = State {
            sink: Some(outbound),
            ..State::default()
        };
        Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
            stopping: watch::Sender::new(false),
            control: ProgramExecutionControl::new(),
            value_limits,
            applied_cap_bytes: limits.applied_cap_bytes,
            // A cap of zero would hold the first request forever.
            held_cap: limits.held_cap.max(1),
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
        limits: super::RunLimits,
        unit: HeldUnit,
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
                let executor = coral_v8::ProgramExecutor::new(limits.isolate);
                let result =
                    executor.execute_with_host_control(&program, host.clone(), &host.control);
                #[cfg(test)]
                if let Some(stall) = limits.stall
                    && host.state().stopped
                {
                    drop(stall.read());
                }
                host.finish(host.completion(result));
            })?;
        Ok(exited)
    }

    /// Lets the Program run. `true` for the first `Started`; a repeat
    /// changes nothing.
    pub(super) fn started(&self, started: &Started) -> bool {
        let mut state = self.state();
        if std::mem::replace(&mut state.started, true) {
            return false;
        }
        state.cpu_budget = started.cpu_budget_ms.map(Duration::from_millis);
        drop(state);
        self.changed.notify_all();
        true
    }

    /// Moves the Run onto a reopened stream's `sink` once Coral answered
    /// its `Open`: drops what `Committed` covers, sends `Loaded` again for a
    /// package already loaded, then resends every request above the
    /// watermark in order. `false` when a package answers a Run that
    /// started, which breaks the protocol.
    pub(super) fn resume(&self, sink: mpsc::UnboundedSender<RunRequest>, answer: &Resume) -> bool {
        let mut state = self.state();
        match answer {
            Resume::Package if state.started => return false,
            // A package still loading sends `Loaded` on this sink itself.
            Resume::Package => {}
            Resume::Committed(number) => state.drop_covered(*number),
        }
        state.sink = Some(sink);
        if matches!(answer, Resume::Package) && state.loaded {
            state.transmit(run_request::Message::Loaded(crate::wire::v1::Loaded {}));
        }
        for (number, held) in &state.unconfirmed {
            state.transmit(numbered(*number, held.kind.clone()));
        }
        drop(state);
        self.changed.notify_all();
        true
    }

    /// The stream is lost: the Program's requests wait above the watermark.
    pub(super) fn detach(&self) {
        self.state().sink = None;
    }

    /// Ends a Program still running at the Run's deadline; its `Finish`
    /// says the deadline was reached.
    pub(super) fn reach_deadline(&self) {
        let mut state = self.state();
        if state.stopped {
            return;
        }
        state.end.get_or_insert(FinishEnd::DeadlineReached);
        drop(state);
        self.stop();
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
            // Coral may answer a resent request again; its first answer
            // settled it.
            let repeat = state
                .unconfirmed
                .get(&result.number)
                .is_some_and(|held| held.answered);
            return repeat || state.stopped;
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
        if let Some(held) = state.unconfirmed.get_mut(&result.number) {
            held.answered = true;
        }
        let bytes = usize::try_from(result.bytes).unwrap_or(usize::MAX);
        let over_budget = !self.fits(&state, bytes);
        state.output = state.output.saturating_add(bytes);
        if over_budget {
            state.end.get_or_insert(FinishEnd::OutputBudget);
        }
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
        self.state().drop_covered(number);
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
        self.stopping.send_replace(true);
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Numbers and sends one of the Program's requests once the caps let it
    /// go, and counts its output `bytes` against the Run Output Budget and
    /// the Applied cap. Its number, or `None` when the Program must stop
    /// instead.
    fn send(&self, kind: request::Kind, bytes: usize, awaiting: Option<Awaiting>) -> Option<u64> {
        let mut state = self.below_caps();
        if state.stopped {
            return None;
        }
        if !self.fits(&state, bytes) {
            state.end.get_or_insert(FinishEnd::OutputBudget);
            drop(state);
            self.stop();
            return None;
        }
        state.output = state.output.saturating_add(bytes);
        let number = state.last.saturating_add(1);
        state.last = number;
        if let Some(awaiting) = awaiting {
            state.invocations.insert(number, awaiting);
        }
        state.hold(number, kind, bytes);
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

    /// The state once no more than the Applied cap and fewer than the held
    /// cap's requests are above the committed watermark, or the Program
    /// stopped.
    fn below_caps(&self) -> MutexGuard<'_, State> {
        #[cfg(test)]
        let told = std::cell::Cell::new(false);
        self.wait_until(|state| {
            let below = state.unconfirmed_bytes <= self.applied_cap_bytes
                && state.unconfirmed.len() < self.held_cap;
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

    /// Tells a test the number of the request that parks on a cap, before
    /// it waits.
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

    /// Numbers and sends `Finish` once the caps let it go, or at once when
    /// the Program stopped, and keeps it until committed. A Final Value
    /// counts against the Run Output Budget too: one that does not fit ends
    /// the Run with the output-budget end. Nothing is counted after it.
    fn finish(&self, completion: finish::Completion) {
        let mut state = self.below_caps();
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
        let number = state.last;
        state.hold(number, request::Kind::Finish(finish), 0);
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
            Err(coral_v8::Error::ProgramHostTerminated) => {
                end(self.state().end.unwrap_or(FinishEnd::HostTerminated))
            }
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
        let mut state = self.state();
        state.loaded = true;
        state.transmit(run_request::Message::Loaded(crate::wire::v1::Loaded {}));
        drop(state);
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
