use std::time::{Duration, Instant};

use coral_telemetry::metric::CompilerRole;
use coral_telemetry::safe;
use coral_v8::{
    AnalyzedSource, CompileFailure, CompiledProgram, CoralDeclarations, IsolateLimits,
    LinkedSavedFunctionSource, ProgramCompiler, SavedFunctionCompilation, SourceReferences,
    UnknownToolCall,
};

use crate::{
    AnalysisError, CompileError, CompilerClient, Diagnostics, Unavailable, tool_check_deadline,
};

/// The compiler client's in-process adapter: one warm Program Compiler on its
/// own thread, which measures each request by the caller it serves. Analysis
/// serves both callers and is measured as Program Admission's.
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

    /// The called chains of `source` that name no Tool among `callable`,
    /// or none when the check fails or has no time left before the reserve.
    fn unknown_tool_calls(
        &self,
        deadline: Instant,
        source: &str,
        callable: &[String],
    ) -> Vec<UnknownToolCall> {
        let Some(check_deadline) = tool_check_deadline(deadline) else {
            tracing::warn!(
                "The compiler has no time left to check the Program's Call Paths; they are checked only when called"
            );
            return Vec::new();
        };
        match self
            .run
            .unknown_tool_calls(check_deadline, source, callable)
        {
            Ok(unknown) => unknown,
            Err(error) => {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "The compiler could not check the Program's Call Paths; they are checked only when called"
                );
                Vec::new()
            }
        }
    }
}

impl CompilerClient for InProcessCompiler {
    fn analyze(
        &self,
        time_left: Duration,
        sources: &[AnalyzedSource],
    ) -> Result<Vec<SourceReferences>, AnalysisError> {
        let deadline = deadline(time_left);
        // One request per source, so a source that faults the isolate has it
        // rebuilt before the next.
        sources
            .iter()
            .enumerate()
            .map(|(index, source)| {
                self.run
                    .analyze(deadline, source)
                    .map_err(|error| AnalysisError {
                        index,
                        error: compile_error(error),
                    })
            })
            .collect()
    }

    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError> {
        let deadline = deadline(time_left);
        if let Some(callable) = callable {
            let unknown = self.unknown_tool_calls(deadline, source, callable);
            if !unknown.is_empty() {
                return Err(CompileError::Diagnostics(Diagnostics::UnknownTools(
                    unknown,
                )));
            }
        }
        if saved_functions.is_empty() {
            self.run.compile(deadline, source)
        } else {
            self.run
                .compile_with_saved_functions(deadline, source, saved_functions)
        }
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
        let errors: [Case; 15] = [
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
                || coral_v8::Error::InvalidFunctionSave("names no function".into()),
                "invalid_function_save",
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

    // Catches: an analysis that loses which source failed, or that answers
    // out of order.
    #[test]
    fn analysis_answers_each_source_in_order_and_names_the_first_that_fails() {
        let compiler = InProcessCompiler::new(IsolateLimits::default()).expect("compiler");
        let program = AnalyzedSource::Program(
            "await coral.providers.github.repos.get({});\nreturn coral.functions.math.add();"
                .to_owned(),
        );
        let module = AnalyzedSource::SavedFunction(
            "export default () => coral.functions.math.double();".to_owned(),
        );
        let computed = AnalyzedSource::SavedFunction(
            "const key = 'double'; export default () => coral.functions.math[key]();".to_owned(),
        );
        let analyzed = compiler
            .analyze(TEST_COMPILATION_TIMEOUT, &[program.clone(), module.clone()])
            .expect("analyze both sources");
        assert_eq!(
            analyzed
                .iter()
                .map(|references| (
                    references
                        .functions()
                        .iter()
                        .map(coral_v8::SavedFunctionReference::path)
                        .collect::<Vec<_>>(),
                    references.tool_call_paths().to_vec(),
                ))
                .collect::<Vec<_>>(),
            [
                (
                    vec!["math.add"],
                    vec![
                        "coral.providers.github.repos".to_owned(),
                        "coral.providers.github.repos.get".to_owned(),
                    ],
                ),
                (vec!["math.double"], Vec::new()),
            ]
        );
        let failed = compiler.analyze(TEST_COMPILATION_TIMEOUT, &[program, computed, module]);
        assert!(
            matches!(
                &failed,
                Err(AnalysisError {
                    index: 1,
                    error: CompileError::Diagnostics(Diagnostics::Reference { .. }),
                })
            ),
            "{failed:?}"
        );
    }

    // Catches: a Tool check that runs without a callable list, or blocks a
    // Program whose every call names a Tool. The unknown-Tool rule itself is
    // tested in coral-v8.
    #[test]
    fn compile_checks_the_called_tools_only_against_a_callable_list() {
        let compiler = InProcessCompiler::new(IsolateLimits::default()).expect("compiler");
        let source = "await coral.providers.github.repos.get({});\n\
            return await coral.providers.notion.retrievePage({});";
        let get = "coral.providers.github.repos.get".to_owned();
        let page = "coral.providers.notion.retrievePage".to_owned();
        let cases = [
            (None, false),
            (Some(vec![get.clone(), page]), false),
            (Some(vec![get]), true),
        ];
        for (callable, refused) in cases {
            let outcome =
                compiler.compile(TEST_COMPILATION_TIMEOUT, source, &[], callable.as_deref());
            if refused {
                assert!(
                    matches!(
                        &outcome,
                        Err(CompileError::Diagnostics(Diagnostics::UnknownTools(calls)))
                            if !calls.is_empty()
                    ),
                    "{callable:?}: {:?}",
                    outcome.map(drop)
                );
            } else {
                outcome.unwrap_or_else(|error| panic!("{callable:?}: {error:?}"));
            }
        }
    }

    #[test]
    fn a_call_with_no_time_left_times_out() {
        let compiler = InProcessCompiler::new(IsolateLimits::default()).expect("compiler");
        assert_eq!(
            compiler
                .compile(Duration::ZERO, "return 42;", &[], None)
                .map(drop),
            Err(CompileError::TimedOut)
        );
        compiler
            .compile(TEST_COMPILATION_TIMEOUT, "return 42;", &[], None)
            .expect("a call with time left compiles");
    }
}
