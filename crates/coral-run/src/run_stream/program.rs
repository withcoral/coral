//! The Program of a Run stream: the Run Executor's requests, read in order
//! on a blocking thread and made as calls on the Run's Program host, which
//! the in-process isolate calls too. Each request reaches the owner with its
//! number; the answers go back on the stream.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use coral_catalog::AuthoredToolKind;
use coral_run_executor::wire::v1::{
    CoralFailure, Fatal, Finish, FinishEnd, InvocationResult, Location, LogLevel, Permitted,
    RunRequest, Started, UpstreamOutcome, complete, finish, invocation_result, request,
    run_request, run_response,
};
use coral_v8::{
    AuthoredLocation, AuthoredSource, ConsoleLevel, ProgramHost, ToolBodyOutcome, ToolCalls,
    ToolCompletion, ToolHostError,
};
use tokio_stream::StreamExt as _;

mod resume;

use super::{StreamLink, StreamRun};
use crate::engine::{self, ExecutionCompletion};
use crate::owner::{Message, OwnerHandle};
use crate::program_output::ProgramOutputWriter;
use crate::types::AcceptedRun;
use crate::{FinalValue, RunFailure, RunFailureCode, RunModule};

/// The Run Executor's messages on one Run stream.
pub(crate) type Requests = Pin<Box<dyn tokio_stream::Stream<Item = RunRequest> + Send>>;

/// Starts the stream's Program once its start landed: `Started` goes out,
/// and the Run Executor's requests are read until `Finish`, the stream's
/// end, or a message that breaks the protocol.
pub(crate) fn start(
    module: &RunModule,
    accepted: &AcceptedRun,
    stream: &Arc<StreamRun>,
    handle: &OwnerHandle,
    invoker: crate::invocation::ToolInvoker,
    deadline_at_unix_nanos: i64,
) {
    let Some(requests) = stream
        .requests
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    else {
        return;
    };
    let now = module.now().unwrap_or(deadline_at_unix_nanos);
    let cpu_budget = module.cpu_budget.and_then(|budget| {
        accepted
            .time_limit_nanos
            .and_then(|limit| budget.below(crate::nanos_duration(limit)))
    });
    stream.link.send(run_response::Message::Started(Started {
        deadline_ms: u64::try_from(deadline_at_unix_nanos.saturating_sub(now) / 1_000_000)
            .unwrap_or(0),
        cpu_budget_ms: cpu_budget
            .map(|budget| u64::try_from(budget.as_millis()).unwrap_or(u64::MAX)),
    }));
    let span = crate::trace::phase_span(
        accepted.trace.as_ref(),
        &accepted.held_id(),
        crate::trace::Phase::Execute,
    );
    // The execute span lasts as long as the Program; each Tool Invocation's
    // span is its child.
    let entered = span.span().clone().entered();
    let host = engine::host::program_host(
        accepted.linked_snapshots.clone(),
        invoker,
        ProgramOutputWriter::new(handle.clone(), accepted.value_limits),
        accepted.value_limits,
        cpu_budget,
        handle.stop(),
    );
    let mut reader = Reader {
        runtime: tokio::runtime::Handle::current(),
        host,
        owner: handle.clone(),
        link: stream.link.clone(),
        sources: accepted
            .linked_snapshots
            .iter()
            .filter_map(|(call_path, tool)| {
                let path = tool.kind.relative_path(call_path)?.to_owned();
                let source = match tool.kind {
                    AuthoredToolKind::Private => AuthoredSource::PrivateTool { path },
                    AuthoredToolKind::Shared => AuthoredSource::SharedTool { path },
                };
                Some((tool.binding.definition().to_owned(), source))
            })
            .collect(),
        performed: Arc::default(),
        completed: std::collections::HashSet::new(),
        last: 0,
        covered: 0,
    };
    let resume = stream
        .resume
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    if let Some(resume) = resume {
        reader.resume(resume, &accepted.linked_snapshots);
    }
    drop(entered);
    let value_limits = accepted.value_limits;
    let handle = handle.clone();
    tokio::task::spawn_blocking(move || {
        let _execute = span;
        let message = match reader.read(requests, value_limits) {
            // `Finish` reached the owner, or the reader detached it before
            // its calls dropped.
            Read::Finished | Read::Detached => return,
            Read::Violated => {
                stream_violated(&handle);
                Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated)
            }
        };
        handle.send(message);
    });
}

fn stream_violated(handle: &OwnerHandle) {
    tracing::warn!(
        coral.run.id = coral_telemetry::safe::id(handle.held_id()),
        "the Run Executor broke the Run stream's protocol; the Run is interrupted"
    );
}

/// How reading the requests ended.
enum Read {
    /// `Finish` reached the owner.
    Finished,
    /// The stream ended, or its server retired.
    Detached,
    /// A message broke the protocol.
    Violated,
}

/// A permitted Perform, until `Complete`: what completes it, and its body's
/// calls.
struct Performed {
    completion: Box<dyn ToolCompletion>,
    body: Arc<dyn ToolCalls>,
}

/// The body a request names as its parent.
enum Body {
    Running(Arc<dyn ToolCalls>),
    /// A Perform that completed.
    Ended,
    /// A number that is no permitted Perform.
    Unknown,
}

struct Reader {
    runtime: tokio::runtime::Handle,
    host: Arc<engine::host::RunProgramHost>,
    owner: OwnerHandle,
    link: Arc<StreamLink>,
    /// The Program and each linked Tool's source, by the Tool's id.
    sources: HashMap<String, AuthoredSource>,
    performed: Arc<Mutex<HashMap<u64, Performed>>>,
    /// The Performs a `Complete` ended: a later call their bodies make is
    /// refused, not a broken protocol.
    completed: std::collections::HashSet<u64>,
    /// The last request number read.
    last: u64,
    /// The watermark a reopened stream started from: a resent request at
    /// or below it is committed already.
    covered: u64,
}

impl Reader {
    fn read(mut self, mut requests: Requests, value_limits: coral_value::ValueLimits) -> Read {
        let mut retired = self.link.retired.subscribe();
        loop {
            let next = self.runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = retired.wait_for(|retired| *retired) => None,
                    request = requests.next() => request,
                }
            });
            let Some(RunRequest {
                message: Some(run_request::Message::Request(request)),
            }) = next
            else {
                return if next.is_some() {
                    self.violate()
                } else {
                    // Before the open Performs drop: their bodies run on.
                    self.owner.detach();
                    Read::Detached
                };
            };
            if request.number <= self.covered {
                continue;
            }
            // The Run Executor holds no more than its held cap above the
            // watermark, which never runs ahead of this one.
            let held_cap = u64::try_from(coral_run_executor::HELD_CAP_REQUESTS).unwrap_or(u64::MAX);
            if request.number != self.last + 1
                || request.number > self.link.watermark.load(Ordering::SeqCst) + held_cap
            {
                return self.violate();
            }
            self.last = request.number;
            let number = request.number;
            if let Some(request::Kind::Finish(end)) = request.kind {
                let Some(completion) = completion(end, value_limits) else {
                    return self.violate();
                };
                self.link.finished.store(true, Ordering::SeqCst);
                self.owner.send(Message::Request {
                    number,
                    message: Some(Box::new(Message::ProgramEnded(completion))),
                });
                return Read::Finished;
            }
            // The call the request makes takes the number; one it does not
            // take leaves nothing for the owner to record.
            self.owner.hand_request(number);
            let read = match request.kind {
                Some(request::Kind::Finish(_)) | None => None,
                Some(request::Kind::Invoke(invoke)) => self.invoke(number, invoke),
                Some(request::Kind::Perform(perform)) => self.perform(number, perform),
                Some(request::Kind::Complete(done)) => self.complete(number, done),
                Some(request::Kind::Emit(emit)) => {
                    let location = self.location(emit.location);
                    output(self.host.emit(&emit.value_json, location))
                }
                Some(request::Kind::Log(log)) => {
                    let level = match log.level() {
                        LogLevel::Debug => Some(ConsoleLevel::Debug),
                        LogLevel::Info => Some(ConsoleLevel::Info),
                        LogLevel::Warn => Some(ConsoleLevel::Warn),
                        LogLevel::Error => Some(ConsoleLevel::Error),
                        LogLevel::Unspecified => None,
                    };
                    let location = self.location(log.location);
                    level.and_then(|level| output(self.host.log(level, &log.message, location)))
                }
            };
            drop(self.owner.take_request());
            if read.is_none() {
                return self.violate();
            }
        }
    }

    /// Starts the Invoke the Program or a permitted Perform's body made.
    fn invoke(&self, number: u64, invoke: coral_run_executor::wire::v1::Invoke) -> Option<()> {
        let location = self.location(invoke.location);
        let future = match invoke.parent.map(|parent| self.body(parent)) {
            Some(Body::Running(body)) => {
                body.invoke(invoke.call_path, invoke.arguments_json, location)
            }
            Some(Body::Ended) => {
                let refused = self.parent_ended();
                Box::pin(async move { Err(refused) })
            }
            Some(Body::Unknown) => return None,
            None => self
                .host
                .clone()
                .invoke(invoke.call_path, invoke.arguments_json, location),
        };
        self.answer(number, future);
        Some(())
    }

    /// Starts the Perform the Program or a permitted Perform's body made.
    fn perform(&self, number: u64, perform: coral_run_executor::wire::v1::Perform) -> Option<()> {
        let location = self.location(perform.location);
        let permit = match perform.parent.map(|parent| self.body(parent)) {
            Some(Body::Running(body)) => {
                body.perform(perform.call_path, perform.arguments_json, location)
            }
            Some(Body::Ended) => {
                let refused = self.parent_ended();
                Box::pin(async move { Err(refused) })
            }
            Some(Body::Unknown) => return None,
            None => self
                .host
                .clone()
                .perform(perform.call_path, perform.arguments_json, location),
        };
        self.permit(number, permit);
        Some(())
    }

    fn violate(&self) -> Read {
        self.link.violated.store(true, Ordering::SeqCst);
        Read::Violated
    }

    fn location(&self, location: Option<Location>) -> Option<AuthoredLocation> {
        let location = location?;
        let source = if location.tool_id.is_empty() {
            AuthoredSource::Program
        } else {
            self.sources.get(&location.tool_id)?.clone()
        };
        Some(AuthoredLocation {
            source,
            line: location.line,
            column: location.column,
        })
    }

    /// The body of Perform `perform`, which a request names as its parent.
    fn body(&self, perform: u64) -> Body {
        let performed = self
            .performed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match performed.get(&perform) {
            Some(performed) => Body::Running(performed.body.clone()),
            // A completed Perform's body ended: its later calls are refused.
            // So did one from before a reopen that is no longer open.
            None if self.completed.contains(&perform) || perform <= self.covered => Body::Ended,
            None => Body::Unknown,
        }
    }

    /// The answer to a call whose parent's body ended, as in process.
    fn parent_ended(&self) -> ToolHostError {
        let failure = crate::CoralFailure::ParentInvocationEnded;
        ToolHostError::Rejected {
            code: failure.code().into(),
            message: failure.message().into(),
            trace_id: self.host.trace_id(),
            suggestions: Vec::new(),
        }
    }

    /// Sends request `number` the answer `future` settles to.
    fn answer(&self, number: u64, future: coral_v8::ToolFuture) {
        let link = self.link.clone();
        self.runtime.spawn(async move {
            send_result(&link, number, future.await);
        });
    }

    /// Sends Perform `number` its permit once the call is decided, keeping
    /// what completes it.
    fn permit(&self, number: u64, permit: coral_v8::PermitFuture) {
        let link = self.link.clone();
        let performed = self.performed.clone();
        self.runtime.spawn(async move {
            let outcome = match permit.await {
                Ok(completion) => {
                    performed
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(
                            number,
                            Performed {
                                body: completion.body_calls(),
                                completion,
                            },
                        );
                    invocation_result::Outcome::Permitted(Permitted {})
                }
                Err(error) => failure(error),
            };
            send_outcome(&link, number, 0, outcome);
        });
    }

    /// Completes the Perform `done` names; `None` when it names none still
    /// running.
    fn complete(
        &mut self,
        number: u64,
        done: coral_run_executor::wire::v1::Complete,
    ) -> Option<()> {
        let completion = self
            .performed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&done.perform)?
            .completion;
        self.completed.insert(done.perform);
        let outcome = match done.outcome? {
            complete::Outcome::ReturnedJson(json) => ToolBodyOutcome::Returned(json),
            complete::Outcome::NotData(not_data) => ToolBodyOutcome::NotData {
                field: not_data.field,
                reason: not_data.reason,
            },
            complete::Outcome::Threw(threw) => ToolBodyOutcome::Threw {
                message: threw.message,
                tool_error: threw.tool_error,
            },
        };
        self.answer(number, completion.complete(outcome));
        Some(())
    }
}

/// An Emission or a Log the Run Executor sent: one the owner refuses for
/// its value or the budget breaks the protocol, since the Run Executor
/// checks both first; one it no longer takes is not recorded.
fn output(written: Result<(), coral_v8::ProgramOutputHostError>) -> Option<()> {
    match written {
        Ok(()) | Err(coral_v8::ProgramOutputHostError::Fatal) => Some(()),
        Err(
            coral_v8::ProgramOutputHostError::InvalidValue
            | coral_v8::ProgramOutputHostError::LimitExceeded,
        ) => None,
    }
}

/// Answers request `number` with the value or the error its call ended with.
fn send_result(link: &StreamLink, number: u64, result: Result<String, ToolHostError>) {
    let (bytes, outcome) = match result {
        Ok(json) => (json.len(), invocation_result::Outcome::ResultJson(json)),
        Err(error) => (0, failure(error)),
    };
    send_outcome(link, number, bytes, outcome);
}

/// Answers request `number` with `outcome`, which counts `bytes`.
fn send_outcome(link: &StreamLink, number: u64, bytes: usize, outcome: invocation_result::Outcome) {
    link.send(run_response::Message::InvocationResult(InvocationResult {
        number,
        bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
        outcome: Some(outcome),
    }));
}

fn failure(error: ToolHostError) -> invocation_result::Outcome {
    match error {
        ToolHostError::Rejected {
            code,
            message,
            trace_id,
            suggestions,
        } => invocation_result::Outcome::CoralFailure(CoralFailure {
            code: code.into_owned(),
            message: message.into_owned(),
            trace_id: trace_id.unwrap_or_default(),
            suggestions,
        }),
        ToolHostError::Upstream {
            code,
            message,
            outcome_json,
        } => invocation_result::Outcome::UpstreamOutcome(UpstreamOutcome {
            code: code.into_owned(),
            message: message.into_owned(),
            outcome_json,
        }),
        ToolHostError::Fatal => invocation_result::Outcome::Fatal(Fatal {}),
    }
}

/// How the Program ended, as `finish` says; `None` when it breaks the
/// protocol. The Run's deadline, when reached, is the owner's to record.
pub(super) fn completion(
    finish: Finish,
    limits: coral_value::ValueLimits,
) -> Option<ExecutionCompletion> {
    Some(match finish.completion? {
        finish::Completion::ValueJson(json) => engine::final_value(json, limits),
        finish::Completion::Failure(failure) => {
            let code = RunFailureCode::from_storage_str(&failure.code).ok()?;
            let mut run_failure = RunFailure::new(code, failure.message);
            if !failure.error_json.is_empty() {
                run_failure =
                    run_failure.with_error(serde_json::from_str(&failure.error_json).ok()?);
            }
            ExecutionCompletion::Failed(run_failure)
        }
        finish::Completion::End(end) => match FinishEnd::try_from(end).ok()? {
            FinishEnd::Unspecified => return None,
            FinishEnd::Absent => ExecutionCompletion::Succeeded(FinalValue::Absent),
            FinishEnd::HostTerminated | FinishEnd::DeadlineReached => {
                ExecutionCompletion::ProgramHostTerminated
            }
            FinishEnd::MemoryLimit => ExecutionCompletion::MemoryLimitExceeded,
            FinishEnd::CpuBudget => ExecutionCompletion::CpuLimitExceeded,
            FinishEnd::OutputBudget => engine::output_over_budget(),
        },
    })
}
