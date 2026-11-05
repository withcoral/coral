use std::time::{Duration, Instant};

use coral_telemetry::metric::CompilerRole;
use coral_v8::{
    CompileFailure, CompiledProgram, CoralDeclarations, IsolateLimits, LinkedSavedFunctionSource,
    ProgramCompiler, SavedFunctionCallPaths, SavedFunctionCompilation, SavedFunctionReference,
    ToolReferences, UnknownToolCall,
};

use crate::{CompileError, CompilerClient, Diagnostics, Unavailable};

/// The compiler client's in-process adapter: one warm Program Compiler on its
/// own thread, which measures each request by the caller it serves.
pub struct InProcessCompiler {
    /// Program Admission's handle.
    run: ProgramCompiler,
    /// Saved Function save's handle on the same thread and queue.
    authoring: ProgramCompiler,
}

impl InProcessCompiler {
    /// Starts the warm compiler within the deployment's compiler heap budget.
    ///
    /// # Errors
    ///
    /// Returns an initialization or memory-limit error when V8 or the packaged
    /// compiler cannot start.
    pub fn new(isolate_limits: IsolateLimits) -> Result<Self, coral_v8::Error> {
        let run = ProgramCompiler::new(isolate_limits, CompilerRole::Run)?;
        let authoring = run.with_role(CompilerRole::Authoring);
        Ok(Self { run, authoring })
    }
}

impl CompilerClient for InProcessCompiler {
    fn program_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        self.run
            .program_call_paths(deadline(time_left), source)
            .map_err(compile_error)
    }

    fn unknown_tool_calls(
        &self,
        time_left: Duration,
        tools: &ToolReferences,
        callable: &[String],
    ) -> Result<Vec<UnknownToolCall>, CompileError> {
        self.run
            .unknown_tool_calls(deadline(time_left), tools, callable)
            .map_err(compile_error)
    }

    fn dependency_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileError> {
        self.run
            .saved_function_dependency_paths(deadline(time_left), source)
            .map_err(compile_error)
    }

    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
    ) -> Result<CompiledProgram, CompileError> {
        let deadline = deadline(time_left);
        if saved_functions.is_empty() {
            self.run.compile(deadline, source)
        } else {
            self.run
                .compile_with_saved_functions(deadline, source, saved_functions)
        }
        .map_err(compile_error)
    }

    fn call_paths(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        self.authoring
            .saved_function_call_paths(deadline(time_left), source)
            .map_err(compile_error)
    }

    fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<SavedFunctionCompilation, CompileError> {
        let deadline = deadline(time_left);
        match declarations {
            Some(declarations) => {
                self.authoring
                    .prepare_typed_saved_function(deadline, source, declarations)
            }
            None => self.authoring.prepare_saved_function(deadline, source),
        }
        .map_err(compile_error)
    }
}

fn deadline(time_left: Duration) -> Instant {
    Instant::now() + time_left
}

fn compile_error(error: coral_v8::Error) -> CompileError {
    match error {
        coral_v8::Error::MemoryLimitExceeded => CompileError::MemoryLimit,
        coral_v8::Error::InvalidSavedFunctionReference {
            message,
            line,
            column,
        } => CompileError::Diagnostics(Diagnostics::Reference {
            message,
            line,
            column,
        }),
        other => {
            let kind = other.code();
            match CompileFailure::from(other) {
                CompileFailure::Rejected(message) => {
                    CompileError::Diagnostics(Diagnostics::Message { kind, message })
                }
                CompileFailure::TimedOut => CompileError::TimedOut,
                CompileFailure::Busy => CompileError::Unavailable(Unavailable::Busy),
                CompileFailure::Stopped => CompileError::Unavailable(Unavailable::Stopped),
                CompileFailure::Failed(failed) => {
                    CompileError::Unavailable(Unavailable::Failed(failed))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use coral_test_support::TEST_COMPILATION_TIMEOUT;

    use super::*;

    #[test]
    fn errors_keep_the_failure_and_kind_each_caller_reported_before() {
        type Case = (fn() -> coral_v8::Error, &'static str);
        let errors: [Case; 14] = [
            (|| coral_v8::Error::CompilerBusy, "compiler_busy"),
            (
                || coral_v8::Error::CompilationTimedOut,
                "compilation_timed_out",
            ),
            (
                || coral_v8::Error::CompilerUnavailable,
                "compiler_unavailable",
            ),
            // Only starting a compiler fails this way, never a call, so the
            // client reports it as stopped rather than as `compiler_thread`.
            (
                || coral_v8::Error::CompilerThread(std::io::Error::other("no thread")),
                "compiler_unavailable",
            ),
            (
                || coral_v8::Error::Compilation {
                    diagnostics: Vec::new(),
                },
                "compilation",
            ),
            (
                || coral_v8::Error::InvalidProgramBody,
                "invalid_program_body",
            ),
            (
                || coral_v8::Error::InvalidSavedFunction("no default export".into()),
                "invalid_saved_function",
            ),
            (
                || coral_v8::Error::SourceTooDeeplyNested,
                "source_too_deeply_nested",
            ),
            (
                || coral_v8::Error::ForbiddenCapability("eval".into()),
                "forbidden_capability",
            ),
            (
                || coral_v8::Error::InvalidSavedFunctionReference {
                    message: "computed access".into(),
                    line: 2,
                    column: 7,
                },
                "invalid_saved_function_reference",
            ),
            (
                || coral_v8::Error::MemoryLimitExceeded,
                "memory_limit_exceeded",
            ),
            (|| coral_v8::Error::Engine("private".into()), "engine"),
            (
                || coral_v8::Error::CompilerProtocol("private".into()),
                "compiler_protocol",
            ),
            (
                || coral_v8::Error::InvalidSourceMap("private".into()),
                "invalid_source_map",
            ),
        ];
        for (error, kind) in errors {
            let client = compile_error(error());
            assert_eq!(client.code(), kind, "{:?}", error());
            assert_eq!(
                CompileFailure::from(client),
                CompileFailure::from(error()),
                "{:?}",
                error()
            );
        }
    }

    #[test]
    fn a_call_with_no_time_left_times_out() {
        let compiler = InProcessCompiler::new(IsolateLimits::default()).expect("compiler");
        assert_eq!(
            compiler
                .compile(Duration::ZERO, "return 42;", &[])
                .map(drop),
            Err(CompileError::TimedOut)
        );
        compiler
            .compile(TEST_COMPILATION_TIMEOUT, "return 42;", &[])
            .expect("a call with time left compiles");
    }
}
