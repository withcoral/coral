//! V8 host callbacks and the synchronous/asynchronous execution bridge.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::Instrument as _;

use crate::execution::ExecutionStop;
use crate::invocation::{InvocationHostError, ToolInvoker};
use crate::program_output::{ProgramOutputWriteError, ProgramOutputWriter};
use crate::{CoralFailure, ProgramLogLevel, SourceDocument, SourceLocation, ValueLimits};

use super::{ExecutionCompletion, ProgramEngine, ProgramPackage};

pub(crate) async fn execute(
    engine: Arc<dyn ProgramEngine>,
    package: ProgramPackage,
    linked_snapshots: HashMap<String, String>,
    invoker: ToolInvoker,
    output: ProgramOutputWriter,
    value_limits: ValueLimits,
    stop: Arc<ExecutionStop>,
) -> ExecutionCompletion {
    let host = Arc::new(RunProgramHost {
        runtime: tokio::runtime::Handle::current(),
        span: tracing::Span::current(),
        trace_id: invoker.trace_id(),
        value_limits,
        stop: stop.clone(),
        linked_snapshots,
        invoker,
        output,
    });
    tokio::task::spawn_blocking(move || engine.execute(package, host, stop))
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
    stop: Arc<ExecutionStop>,
    /// Linked Saved Function path to the identity of the Run's snapshot.
    linked_snapshots: HashMap<String, String>,
    invoker: ToolInvoker,
    output: ProgramOutputWriter,
}

impl RunProgramHost {
    /// Names the Run's retained source for one V8 call site. A Saved Function
    /// path without a retained snapshot gives no location rather than a guess.
    fn source_location(
        &self,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> Option<SourceLocation> {
        let location = location?;
        let document = match location.source {
            coral_v8::AuthoredSource::Program => SourceDocument::Program,
            coral_v8::AuthoredSource::SavedFunction { path } => SourceDocument::SavedFunction {
                saved_function_id: self.linked_snapshots.get(&path)?.clone(),
            },
        };
        Some(SourceLocation {
            document,
            line: location.line,
            column: location.column,
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

impl coral_v8::ProgramHost for RunProgramHost {
    fn value_limits(&self) -> ValueLimits {
        self.value_limits
    }

    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        let source_location = self.source_location(location);
        let runtime = self.runtime.clone();
        let span = self.span.clone();
        let trace_id = self.trace_id.clone();
        // Start independently of V8 polling. Run termination owns cancellation
        // and durable child shutdown even if execution drops this JoinHandle.
        let invocation = runtime.spawn(
            async move {
                let _panic_guard = StopPanickingInvocation(self.stop.clone());
                let arguments = serde_json::from_str(&arguments_json).map_err(|_error| {
                    coral_v8::ToolHostError::Rejected {
                        code: CoralFailure::InvalidInput.code(),
                        message: CoralFailure::InvalidInput.message().into(),
                        trace_id: trace_id.clone(),
                    }
                })?;
                self.invoker
                    .invoke(&call_path, arguments, source_location)
                    .await
                    .map_err(|error| match error {
                        InvocationHostError::Rejected(failure) => {
                            coral_v8::ToolHostError::Rejected {
                                code: failure.code(),
                                message: failure.message(),
                                trace_id: trace_id.clone(),
                            }
                        }
                        InvocationHostError::Upstream {
                            rejection,
                            outcome_json,
                        } => coral_v8::ToolHostError::Upstream {
                            code: rejection.code(),
                            message: rejection.message(),
                            outcome_json,
                        },
                        InvocationHostError::Fatal(_private_error) => {
                            // V8 may still be running noncooperative JavaScript,
                            // rather than polling this Tool's completion.
                            self.stop.request();
                            coral_v8::ToolHostError::Fatal
                        }
                    })
            }
            .instrument(span),
        );
        Box::pin(async move {
            invocation
                .await
                .unwrap_or(Err(coral_v8::ToolHostError::Fatal))
        })
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
