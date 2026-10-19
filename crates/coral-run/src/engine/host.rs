//! V8 host callbacks and the synchronous/asynchronous execution bridge.

use std::collections::HashMap;
use std::sync::Arc;

use crate::emission_approval::EmissionApprovals;
use crate::execution::ExecutionStop;
use crate::invocation::{InvocationHostError, OperationInvoker};
use crate::middleware::MiddlewareRecorder;
use crate::program_output::{ProgramOutputWriteError, ProgramOutputWriter};
use crate::{CoralFailure, ProgramLogLevel, SourceDocument, SourceLocation, ValueLimits};

use super::{ExecutionCompletion, ProgramEngine, ProgramPackage};

/// The Run-scoped writers through which one execution's host records evidence.
pub(crate) struct RunEvidence {
    pub(crate) invoker: OperationInvoker,
    pub(crate) output: ProgramOutputWriter,
    pub(crate) middleware: MiddlewareRecorder,
    pub(crate) emission_approvals: EmissionApprovals,
}

pub(crate) async fn execute(
    engine: Arc<dyn ProgramEngine>,
    package: ProgramPackage,
    linked_snapshots: HashMap<String, String>,
    evidence: RunEvidence,
    value_limits: ValueLimits,
    stop: Arc<ExecutionStop>,
) -> ExecutionCompletion {
    let RunEvidence {
        invoker,
        output,
        middleware,
        emission_approvals,
    } = evidence;
    let host = Arc::new(RunProgramHost {
        runtime: tokio::runtime::Handle::current(),
        value_limits,
        stop: stop.clone(),
        linked_snapshots,
        invoker,
        output,
        middleware,
        emission_approvals,
    });
    tokio::task::spawn_blocking(move || engine.execute(package, host, stop))
        .await
        .unwrap_or(ExecutionCompletion::ProgramHostTerminated)
}

struct RunProgramHost {
    runtime: tokio::runtime::Handle,
    value_limits: ValueLimits,
    stop: Arc<ExecutionStop>,
    /// Linked Saved Function path to the identity of the Run's snapshot.
    linked_snapshots: HashMap<String, String>,
    invoker: OperationInvoker,
    output: ProgramOutputWriter,
    middleware: MiddlewareRecorder,
    emission_approvals: EmissionApprovals,
}

impl RunProgramHost {
    fn host_rejection(&self, error: InvocationHostError) -> coral_v8::OperationHostError {
        match error {
            InvocationHostError::Rejected(failure) => coral_v8::OperationHostError::Rejected {
                code: failure.code(),
                message: failure.message(),
                trace_id: None,
            },
            InvocationHostError::Fatal(_private_error) => {
                // V8 may still be running noncooperative JavaScript, rather
                // than polling this Operation's completion.
                self.stop.request();
                coral_v8::OperationHostError::Fatal
            }
        }
    }

    fn emission_rejection(&self, error: ProgramOutputWriteError) -> coral_v8::OperationHostError {
        self.host_rejection(match error {
            ProgramOutputWriteError::Rejected(failure) => InvocationHostError::Rejected(failure),
            ProgramOutputWriteError::LimitExceeded => {
                InvocationHostError::Rejected(CoralFailure::RunOutputTooLarge)
            }
            ProgramOutputWriteError::InvalidValue => {
                InvocationHostError::Rejected(CoralFailure::InvalidInput)
            }
            ProgramOutputWriteError::Fatal(error) => InvocationHostError::Fatal(error),
        })
    }

    fn middleware_evidence<T>(
        &self,
        result: Result<T, crate::Error>,
    ) -> Result<T, coral_v8::ProgramOutputHostError> {
        result.map_err(|_private_error| {
            self.stop.request();
            coral_v8::ProgramOutputHostError::Fatal
        })
    }
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
    ) -> coral_v8::OperationFuture {
        self.invoke_with_origin(
            call_path,
            arguments_json,
            coral_v8::InvocationOrigin::Program { policy: None },
            location,
        )
    }

    fn invoke_with_origin(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        origin: coral_v8::InvocationOrigin,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::OperationFuture {
        let source_location = self.source_location(location);
        let runtime = self.runtime.clone();
        // Start independently of V8 polling. Run termination owns cancellation
        // and durable child shutdown even if execution drops this JoinHandle.
        let invocation = runtime.spawn(async move {
            let _panic_guard = StopPanickingInvocation(self.stop.clone());
            let arguments = serde_json::from_str(&arguments_json).map_err(|_error| {
                coral_v8::OperationHostError::Rejected {
                    code: CoralFailure::InvalidInput.code(),
                    message: CoralFailure::InvalidInput.message(),
                    trace_id: None,
                }
            })?;
            self.invoker
                .invoke(&call_path, arguments, origin, source_location)
                .await
                .map_err(|error| self.host_rejection(error))
        });
        Box::pin(async move {
            invocation
                .await
                .unwrap_or(Err(coral_v8::OperationHostError::Fatal))
        })
    }

    fn operation_policy(self: Arc<Self>, call_path: String) -> coral_v8::OperationFuture {
        let runtime = self.runtime.clone();
        let lookup = runtime.spawn(async move {
            let _panic_guard = StopPanickingInvocation(self.stop.clone());
            let policy = self
                .invoker
                .original_policy(&call_path)
                .await
                .map_err(|error| self.host_rejection(error))?;
            Ok(format!(
                "\"{}\"",
                crate::invocation::operation_policy(policy).as_str()
            ))
        });
        Box::pin(async move {
            lookup
                .await
                .unwrap_or(Err(coral_v8::OperationHostError::Fatal))
        })
    }

    fn run_id(&self) -> Option<String> {
        Some(self.invoker.run_id().to_string())
    }

    fn middleware_started(
        &self,
        start: &coral_v8::MiddlewareExecutionStart,
    ) -> Result<String, coral_v8::ProgramOutputHostError> {
        self.middleware_evidence(self.runtime.block_on(self.middleware.started(start)))
    }

    fn middleware_continued(
        &self,
        execution_id: &str,
    ) -> Result<(), coral_v8::ProgramOutputHostError> {
        self.middleware_evidence(
            self.runtime
                .block_on(self.middleware.continued(execution_id)),
        )
    }

    fn middleware_ended(
        &self,
        execution_id: &str,
        outcome: coral_v8::MiddlewareExecutionOutcome,
    ) -> Result<(), coral_v8::ProgramOutputHostError> {
        self.middleware_evidence(
            self.runtime
                .block_on(self.middleware.ended(execution_id, &outcome)),
        )
    }

    fn emit(
        &self,
        value_json: &str,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> Result<(), coral_v8::ProgramOutputHostError> {
        self.runtime
            .block_on(
                self.output
                    .emit(value_json, self.source_location(location), None),
            )
            .map_err(|error| map_program_output_error(&error))
    }

    fn emit_with_policy(
        self: Arc<Self>,
        value_json: String,
        policy: coral_v8::OperationPolicy,
        location: Option<coral_v8::AuthoredLocation>,
    ) -> coral_v8::OperationFuture {
        let source_location = self.source_location(location);
        let runtime = self.runtime.clone();
        // A human decides off the V8 thread; the Program's Promise settles after
        // the durable outcome, as an Invocation's does.
        let emission = runtime.spawn(async move {
            let _panic_guard = StopPanickingInvocation(self.stop.clone());
            match policy {
                coral_v8::OperationPolicy::Disabled => {
                    return Err(self.host_rejection(InvocationHostError::Rejected(
                        CoralFailure::OperationDisabled,
                    )));
                }
                coral_v8::OperationPolicy::Enabled => {
                    self.output
                        .emit(&value_json, source_location, None)
                        .await
                        .map_err(|error| self.emission_rejection(error))?;
                }
                coral_v8::OperationPolicy::ApprovalRequired => {
                    let (value, approval) = self
                        .emission_approvals
                        .wait_for_approval(value_json, source_location.as_ref())
                        .await
                        .map_err(|error| self.host_rejection(error))?;
                    self.output
                        .emit_approved(value, source_location, &approval)
                        .await
                        .map_err(|error| self.emission_rejection(error))?;
                }
            }
            Ok("null".to_string())
        });
        Box::pin(async move {
            emission
                .await
                .unwrap_or(Err(coral_v8::OperationHostError::Fatal))
        })
    }

    fn middleware_emit(
        &self,
        value_json: &str,
        execution_id: &str,
    ) -> Result<(), coral_v8::ProgramOutputHostError> {
        self.runtime
            .block_on(self.output.emit(value_json, None, Some(execution_id)))
            .map_err(|error| map_program_output_error(&error))
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
            .map_err(|error| map_program_output_error(&error))
    }
}

fn map_program_output_error(error: &ProgramOutputWriteError) -> coral_v8::ProgramOutputHostError {
    match error {
        ProgramOutputWriteError::InvalidValue => coral_v8::ProgramOutputHostError::InvalidValue,
        ProgramOutputWriteError::LimitExceeded => coral_v8::ProgramOutputHostError::LimitExceeded,
        ProgramOutputWriteError::Rejected(_) | ProgramOutputWriteError::Fatal(_) => {
            coral_v8::ProgramOutputHostError::Fatal
        }
    }
}
