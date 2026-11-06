//! The Program's host on a Run Executor. It numbers the Program's requests
//! and sends them on the Run stream. It settles Emissions and Logs itself,
//! and holds each Invoke until its `ToolResult`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use coral_v8::{
    AuthoredLocation, AuthoredSource, ConsoleLevel, ProgramExecutionControl,
    ProgramOutputHostError, ToolFuture, ToolHostError,
};
use tokio::sync::{mpsc, oneshot};

use super::PROGRAM_LOG_MESSAGE_MAX_BYTES;
use crate::wire::v1::{
    Emit, Failure, Finish, FinishEnd, Invoke, Location, Log, LogLevel, Request, RunRequest,
    Started, ToolResult, finish, request, run_request, tool_result,
};

type Answer = oneshot::Sender<Result<String, ToolHostError>>;

/// One Run's Program host, shared by the Program's thread and its stream.
pub(super) struct RunHost {
    state: Mutex<State>,
    changed: Condvar,
    outbound: mpsc::UnboundedSender<RunRequest>,
    control: ProgramExecutionControl,
    value_limits: coral_value::ValueLimits,
    applied_cap_bytes: usize,
    /// Linked Saved Function path to its snapshot's id.
    linked: HashMap<String, String>,
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
    /// the Applied cap: its encoded size.
    unconfirmed: BTreeMap<u64, usize>,
    /// The sum of `unconfirmed`'s weights.
    unconfirmed_bytes: usize,
    /// Each Invoke that waits for its `ToolResult`.
    invocations: HashMap<u64, Answer>,
}

impl RunHost {
    pub(super) fn new(
        outbound: mpsc::UnboundedSender<RunRequest>,
        value_limits: coral_value::ValueLimits,
        applied_cap_bytes: usize,
        linked: HashMap<String, String>,
        trace_id: Option<String>,
    ) -> Self {
        Self {
            state: Mutex::default(),
            changed: Condvar::new(),
            outbound,
            control: ProgramExecutionControl::new(),
            value_limits,
            applied_cap_bytes,
            linked,
            trace_id,
        }
    }

    /// Runs `program` on a thread of its own, which loads it into a fresh
    /// isolate, runs it once `Started` arrives, and sends `Finish`. The
    /// receiver completes once the thread has discarded the isolate.
    pub(super) fn spawn(
        self: &Arc<Self>,
        program: coral_v8::CompiledProgram,
        isolate: coral_v8::IsolateLimits,
    ) -> std::io::Result<oneshot::Receiver<()>> {
        let host = self.clone();
        let (exiting, exited) = oneshot::channel();
        // The thread records where its creator does.
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::Builder::new()
            .name("coral-run".to_owned())
            .spawn(move || {
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let _exiting = exiting;
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

    /// Settles the Invoke `result` answers, and counts its bytes against the
    /// Run Output Budget. `false` when it breaks the protocol: no outcome, or
    /// no Invoke waits for it while the Program runs.
    pub(super) fn tool_result(&self, result: ToolResult) -> bool {
        let mut state = self.state();
        let Some(outcome) = result.outcome else {
            return false;
        };
        let Some(answer) = state.invocations.remove(&result.number) else {
            return state.stopped;
        };
        let bytes = usize::try_from(result.bytes).unwrap_or(usize::MAX);
        let over_budget = !self.fits(&state, bytes);
        state.output = state.output.saturating_add(bytes);
        state.over_budget |= over_budget;
        drop(state);
        let settled = match outcome {
            _ if over_budget => {
                self.stop();
                Err(ToolHostError::Fatal)
            }
            tool_result::Outcome::ResultJson(json) => Ok(json),
            tool_result::Outcome::Rejection(rejection) => Err(ToolHostError::Rejected {
                code: rejection.code.into(),
                message: rejection.message.into(),
                trace_id: Some(rejection.trace_id).filter(|id| !id.is_empty()),
                suggestions: rejection.suggestions,
            }),
            tool_result::Outcome::UpstreamError(upstream) => Err(ToolHostError::Upstream {
                code: upstream.code.into(),
                message: upstream.message.into(),
                outcome_json: upstream.outcome_json,
            }),
            tool_result::Outcome::Fatal(_) => {
                // The Program may be running JavaScript that never polls
                // this Tool.
                self.stop();
                Err(ToolHostError::Fatal)
            }
        };
        drop(answer.send(settled));
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
    /// Applied cap is above the committed watermark, and counts `bytes`
    /// against the Run Output Budget. `false` when the Program must stop
    /// instead.
    fn send(&self, kind: request::Kind, bytes: usize, answer: Option<Answer>) -> bool {
        let weight = kind.encoded_len();
        let mut state = self.state();
        while state.unconfirmed_bytes > self.applied_cap_bytes && !state.stopped {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        if state.stopped {
            return false;
        }
        if !self.fits(&state, bytes) {
            state.over_budget = true;
            drop(state);
            self.stop();
            return false;
        }
        state.output = state.output.saturating_add(bytes);
        let number = state.last.saturating_add(1);
        state.last = number;
        state.unconfirmed.insert(number, weight);
        state.unconfirmed_bytes = state.unconfirmed_bytes.saturating_add(weight);
        if let Some(answer) = answer {
            state.invocations.insert(number, answer);
        }
        self.transmit(number, kind);
        true
    }

    /// Whether `bytes` more output fit in the Run Output Budget.
    fn fits(&self, state: &State, bytes: usize) -> bool {
        state.output.saturating_add(bytes) <= self.value_limits.run_output_bytes()
    }

    /// Numbers and sends `Finish`, which goes at once. A Final Value counts
    /// against the Run Output Budget too: one that does not fit ends the Run
    /// with the output-budget end. Nothing is counted after it.
    fn finish(&self, completion: finish::Completion) {
        let mut state = self.state();
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

    /// Names the snapshot of a Saved Function call site, as Coral does. A
    /// path with no linked snapshot gives no location rather than a guess.
    fn location(&self, location: Option<AuthoredLocation>) -> Option<Location> {
        let location = location?;
        let saved_function_id = match location.source {
            AuthoredSource::Program => String::new(),
            AuthoredSource::SavedFunction { path } => self.linked.get(&path)?.clone(),
        };
        Some(Location {
            saved_function_id,
            line: location.line,
            column: location.column,
        })
    }

    fn output(&self, kind: request::Kind, bytes: usize) -> Result<(), ProgramOutputHostError> {
        if self.send(kind, bytes, None) {
            Ok(())
        } else {
            Err(ProgramOutputHostError::Fatal)
        }
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
        let mut state = self.state();
        while !state.started && !state.stopped {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        !state.stopped
    }

    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture {
        let (answer, answered) = oneshot::channel();
        let invoke = request::Kind::Invoke(Invoke {
            call_path,
            arguments_json,
            location: self.location(location),
        });
        let sent = self.send(invoke, 0, Some(answer));
        Box::pin(async move {
            if !sent {
                return Err(ToolHostError::Fatal);
            }
            answered.await.unwrap_or(Err(ToolHostError::Fatal))
        })
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
