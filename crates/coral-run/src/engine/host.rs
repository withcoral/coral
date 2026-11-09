//! V8 host callbacks and the synchronous/asynchronous execution bridge.

use std::collections::HashMap;
use std::sync::Arc;

use coral_catalog::AuthoredToolKind;
use tracing::Instrument as _;
use tracing::instrument::WithSubscriber as _;

use crate::execution::ExecutionStop;
use crate::invocation::{InvocationHostError, PerformedCall, ToolBody, ToolInvoker};
use crate::program_output::{ProgramOutputWriteError, ProgramOutputWriter};
use crate::{
    CoralFailure, ProgramLogLevel, Rejection, SourceDocument, SourceLocation, ValueLimits,
};

use super::{ExecutionCompletion, ProgramEngine, ProgramPackage};

#[expect(
    clippy::too_many_arguments,
    reason = "each is one part of the Run that the Program's host serves"
)]
pub(crate) async fn execute(
    engine: Arc<dyn ProgramEngine>,
    package: ProgramPackage,
    linked_snapshots: HashMap<String, crate::types::LinkedTool>,
    invoker: ToolInvoker,
    output: ProgramOutputWriter,
    value_limits: ValueLimits,
    cpu_budget: Option<std::time::Duration>,
    stop: Arc<ExecutionStop>,
) -> ExecutionCompletion {
    let host = Arc::new(RunProgramHost {
        runtime: tokio::runtime::Handle::current(),
        span: tracing::Span::current(),
        trace_id: invoker.trace_id(),
        value_limits,
        cpu_budget,
        stop: stop.clone(),
        linked_snapshots,
        invoker: Arc::new(invoker),
        output,
    });
    // The Program's thread records its telemetry where this task does. Where
    // this task has no dispatcher, it is left to find the global one, which
    // may be set by then.
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    tokio::task::spawn_blocking(move || {
        if dispatch.is::<tracing::subscriber::NoSubscriber>() {
            engine.execute(package, host, stop)
        } else {
            tracing::dispatcher::with_default(&dispatch, || engine.execute(package, host, stop))
        }
    })
    .await
    .unwrap_or(ExecutionCompletion::ProgramHostTerminated)
}

struct RunProgramHost {
    runtime: tokio::runtime::Handle,
    /// The Run's execute span: each Tool Invocation's span is its child.
    span: tracing::Span,
    /// The Run Trace identifier each Coral Failure names to the Program.
    trace_id: Option<String>,
    value_limits: ValueLimits,
    /// The Run's CPU budget, below its time limit; `None` leaves the Program
    /// to its deadline.
    cpu_budget: Option<std::time::Duration>,
    stop: Arc<ExecutionStop>,
    /// Each linked Private or Shared Tool's complete Call Path to the
    /// definition the Run retained.
    linked_snapshots: HashMap<String, crate::types::LinkedTool>,
    /// What the Program's own calls and its modules' top-level calls go
    /// through.
    invoker: Arc<ToolInvoker>,
    output: ProgramOutputWriter,
}

impl RunProgramHost {
    /// Names the Run's retained source for one V8 call site. A Private or
    /// Shared Tool path without a retained snapshot gives no location rather
    /// than a guess.
    fn source_location(
        &self,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> Option<SourceLocation> {
        let location = location?;
        let document = match location.source {
            coral_v8::AuthoredSource::Program => SourceDocument::Program,
            coral_v8::AuthoredSource::PrivateTool { path } => SourceDocument::PrivateTool {
                private_tool_id: self
                    .linked_snapshots
                    .get(&AuthoredToolKind::Private.call_path(&path))?
                    .binding
                    .definition()
                    .to_owned(),
            },
            coral_v8::AuthoredSource::SharedTool { path } => SourceDocument::SharedTool {
                shared_tool_id: self
                    .linked_snapshots
                    .get(&AuthoredToolKind::Shared.call_path(&path))?
                    .binding
                    .definition()
                    .to_owned(),
            },
        };
        Some(SourceLocation {
            document,
            line: location.line,
            column: location.column,
        })
    }
}

impl RunProgramHost {
    /// What the Program receives for an Invocation's end. A host defect
    /// stops the Program: V8 may still be running noncooperative JavaScript,
    /// rather than polling this Tool's completion.
    fn host_error(&self, error: InvocationHostError) -> coral_v8::ToolHostError {
        match error {
            InvocationHostError::Rejected(failure) => coral_v8::ToolHostError::Rejected {
                code: failure.code(),
                message: failure.message(),
                trace_id: self.trace_id.clone(),
                suggestions: failure.suggestions().to_vec(),
            },
            InvocationHostError::Upstream {
                rejection,
                outcome_json,
            } => coral_v8::ToolHostError::Upstream {
                code: rejection.code(),
                message: rejection.message(),
                outcome_json,
            },
            InvocationHostError::Fatal(_private_error) => {
                self.stop.request();
                coral_v8::ToolHostError::Fatal
            }
        }
    }
}

/// Completes one permitted Private Tool call with how its body ended. One
/// the Program drops uncompleted (it ended while the body still waited)
/// abandons its call.
struct RunToolCompletion {
    host: Arc<RunProgramHost>,
    /// The call, until its body's outcome completes it.
    performed: Option<PerformedCall>,
    body_calls: Arc<RunBodyCalls>,
}

/// The Tool calls one permitted call's body makes, each the call's child.
struct RunBodyCalls {
    host: Arc<RunProgramHost>,
    body: Arc<ToolBody>,
}

impl coral_v8::ToolCalls for RunBodyCalls {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        self.host
            .clone()
            .invoke_as(Some(&self.body), call_path, arguments_json, location)
    }

    fn perform(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::PermitFuture {
        self.host
            .clone()
            .perform_as(Some(&self.body), call_path, arguments_json, location)
    }
}

impl Drop for RunToolCompletion {
    fn drop(&mut self) {
        if let Some(performed) = self.performed.take() {
            performed.abandon();
        }
    }
}

impl coral_v8::ToolCompletion for RunToolCompletion {
    fn body_calls(&self) -> Arc<dyn coral_v8::ToolCalls> {
        self.body_calls.clone()
    }

    fn complete(mut self: Box<Self>, outcome: coral_v8::ToolBodyOutcome) -> coral_v8::ToolFuture {
        let Some(performed) = self.performed.take() else {
            return Box::pin(async { Err(coral_v8::ToolHostError::Fatal) });
        };
        let host = self.host.clone();
        let runtime = host.runtime.clone();
        let span = host.span.clone();
        // Here, on the Program's thread, so the body has ended before any
        // later call of its leftover work reaches Coral.
        let completes = performed.complete(outcome);
        let completion = runtime.spawn(
            async move {
                let _panic_guard = StopPanickingInvocation(host.stop.clone());
                completes.await.map_err(|error| host.host_error(error))
            }
            .instrument(span)
            .with_current_subscriber(),
        );
        Box::pin(async move {
            completion
                .await
                .unwrap_or(Err(coral_v8::ToolHostError::Fatal))
        })
    }
}

// A task panic must stop even noncooperative JavaScript, which may never poll
// the JoinHandle and discover the host defect itself.
struct StopPanickingInvocation(Arc<ExecutionStop>);

impl Drop for StopPanickingInvocation {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.request();
        }
    }
}

impl RunProgramHost {
    /// [`coral_v8::ProgramHost::invoke`] for a call that `body` makes, or
    /// the Program's own when `None`.
    fn invoke_as(
        self: Arc<Self>,
        body: Option<&Arc<ToolBody>>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        let source_location = self.source_location(location);
        let call = self.invoker.start(body);
        let runtime = self.runtime.clone();
        let span = self.span.clone();
        let trace_id = self.trace_id.clone();
        // Start independently of V8 polling. Run termination owns cancellation
        // and durable child shutdown even if execution drops this JoinHandle.
        let invocation = runtime.spawn(
            async move {
                let _panic_guard = StopPanickingInvocation(self.stop.clone());
                let call = call.map_err(|error| self.host_error(error))?;
                let arguments = serde_json::from_str(&arguments_json).map_err(|_error| {
                    coral_v8::ToolHostError::Rejected {
                        code: CoralFailure::InvalidInput.code(),
                        message: CoralFailure::InvalidInput.message().into(),
                        trace_id: trace_id.clone(),
                        suggestions: Vec::new(),
                    }
                })?;
                call.invoke(&call_path, arguments, source_location)
                    .await
                    .map_err(|error| self.host_error(error))
            }
            .instrument(span)
            // Records its telemetry where the Program's thread does.
            .with_current_subscriber(),
        );
        Box::pin(async move {
            invocation
                .await
                .unwrap_or(Err(coral_v8::ToolHostError::Fatal))
        })
    }

    /// [`coral_v8::ProgramHost::perform`] for a call that `body` makes, or
    /// the Program's own when `None`.
    fn perform_as(
        self: Arc<Self>,
        body: Option<&Arc<ToolBody>>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::PermitFuture {
        let source_location = self.source_location(location);
        let call = self.invoker.start(body);
        let runtime = self.runtime.clone();
        let span = self.span.clone();
        let permit = runtime.spawn(
            async move {
                let _panic_guard = StopPanickingInvocation(self.stop.clone());
                let call = call.map_err(|error| self.host_error(error))?;
                // Only the linker performs a call, and only to a Tool the
                // Run linked.
                let Some((relative_path, linked)) =
                    self.linked_snapshots.get(&call_path).and_then(|linked| {
                        Some((linked.kind.relative_path(&call_path)?, linked.clone()))
                    })
                else {
                    return Err(self.host_error(InvocationHostError::Rejected(
                        Rejection::tool_not_found(&call_path, Vec::new()),
                    )));
                };
                let arguments = serde_json::from_str(&arguments_json).map_err(|_error| {
                    self.host_error(InvocationHostError::Rejected(
                        CoralFailure::InvalidInput.into(),
                    ))
                })?;
                match call
                    .perform(relative_path, linked, arguments, source_location)
                    .await
                {
                    Ok(performed) => Ok(Box::new(RunToolCompletion {
                        body_calls: Arc::new(RunBodyCalls {
                            host: self.clone(),
                            body: performed.body(),
                        }),
                        host: self.clone(),
                        performed: Some(performed),
                    })
                        as Box<dyn coral_v8::ToolCompletion>),
                    Err(error) => Err(self.host_error(error)),
                }
            }
            .instrument(span)
            // Records its telemetry where the Program's thread does.
            .with_current_subscriber(),
        );
        Box::pin(async move { permit.await.unwrap_or(Err(coral_v8::ToolHostError::Fatal)) })
    }
}

impl coral_v8::ProgramHost for RunProgramHost {
    fn value_limits(&self) -> ValueLimits {
        self.value_limits
    }

    fn cpu_budget(&self) -> Option<std::time::Duration> {
        self.cpu_budget
    }

    fn trace_id(&self) -> Option<String> {
        self.trace_id.clone()
    }

    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        self.invoke_as(None, call_path, arguments_json, location)
    }

    fn perform(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::PermitFuture {
        self.perform_as(None, call_path, arguments_json, location)
    }

    fn emit(
        &self,
        value_json: &str,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> Result<(), coral_v8::ProgramOutputHostError> {
        self.runtime
            .block_on(self.output.emit(value_json, self.source_location(location)))
            .map_err(map_program_output_error)
    }

    fn log(
        &self,
        level: coral_v8::ConsoleLevel,
        message: &str,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> Result<(), coral_v8::ProgramOutputHostError> {
        let level = match level {
            coral_v8::ConsoleLevel::Debug => ProgramLogLevel::Debug,
            coral_v8::ConsoleLevel::Info => ProgramLogLevel::Info,
            coral_v8::ConsoleLevel::Warn => ProgramLogLevel::Warn,
            coral_v8::ConsoleLevel::Error => ProgramLogLevel::Error,
        };
        self.runtime
            .block_on(
                self.output
                    .log(level, message, self.source_location(location)),
            )
            .map_err(map_program_output_error)
    }
}

fn map_program_output_error(error: ProgramOutputWriteError) -> coral_v8::ProgramOutputHostError {
    match error {
        ProgramOutputWriteError::InvalidValue => coral_v8::ProgramOutputHostError::InvalidValue,
        ProgramOutputWriteError::LimitExceeded => coral_v8::ProgramOutputHostError::LimitExceeded,
        ProgramOutputWriteError::Fatal(_private_error) => coral_v8::ProgramOutputHostError::Fatal,
    }
}
