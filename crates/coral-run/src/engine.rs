//! Internal compiler and execution adapters for Run orchestration.
//!
//! [`execute`] takes an accepted package, Run-bound invoker and output writer,
//! accepted value limits, and stop control. It runs the selected [`ProgramEngine`]
//! on a blocking worker and returns [`ExecutionCompletion`], not a durable Run
//! outcome. It requires a Tokio runtime; the Run Owner that hosts it retains
//! the claim, the lease, child shutdown, and the final outcome transaction.
//!
//! The private host bridge owns Tool task spawning, JSON and console
//! projection, safe host-error translation, synchronous output calls, and
//! stop-on-panic behavior. Invocation tasks start independently of V8 polling.
//! The invoker and the output writer ask the Run Owner, which answers at
//! Applied, or at Recorded while the unrecorded output exceeds the Applied cap;
//! a known Invocation failure is recorded before its Promise settles.
//! Fatal Invocation errors and task panics request stop even while JavaScript
//! does not yield. Host defects expose no private diagnostics; output errors
//! retain their invalid-value, resource-limit, or uncatchable fatal
//! classification.
//!
//! The existing scripted/V8 engine seam stays internal. V8 owns isolate mechanics
//! and authored diagnostics; its compiler-only and execution isolates stay separate.
//! Run retains admission, authority, persistence, and terminal outcome selection.
//! For example, changing console-level projection or panic termination changes
//! `engine/host.rs`, not the main Run orchestration module.

mod host;
pub(crate) use host::execute;

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use coral_functions::{SavedFunctionPath, SavedFunctionSelection};
use coral_run_executor::{CompileError, CompilerClient, Diagnostics, time_left};
use coral_v8::{BuiltinTools, CompileFailure};

use crate::execution::ExecutionStop;
use crate::{
    EMITTED_JAVASCRIPT_MAX_BYTES, Error, FinalValue, ProgramValue, RunFailure, RunFailureCode,
    SOURCE_MAP_MAX_BYTES,
};

#[derive(Clone, Debug)]
pub(crate) struct SavedFunctionReference {
    pub(crate) path: SavedFunctionPath,
    pub(crate) line: u64,
    pub(crate) column: u64,
}

/// What one Program body names: the Saved Functions admission links and,
/// from an engine that finds them, the Tool chains admission checks.
pub(crate) struct ProgramReferences {
    pub(crate) functions: Vec<SavedFunctionReference>,
    pub(crate) tools: Option<coral_v8::ToolReferences>,
}

pub(crate) trait ProgramEngine: Send + Sync {
    fn program_saved_function_references(
        &self,
        _deadline: Instant,
        _source: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileFailure> {
        Ok(Vec::new())
    }

    /// The Saved Functions and Tools one Program body names, from one
    /// analysis. An engine that finds no Tool chains names its Saved
    /// Functions alone, and admission checks no Call Path.
    fn program_references(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<ProgramReferences, CompileFailure> {
        Ok(ProgramReferences {
            functions: self.program_saved_function_references(deadline, source)?,
            tools: None,
        })
    }

    /// The called chains of `tools` that name none of the `callable`
    /// Call Paths.
    fn unknown_tool_calls(
        &self,
        _deadline: Instant,
        _tools: &coral_v8::ToolReferences,
        _callable: &[String],
    ) -> Result<Vec<coral_v8::UnknownToolCall>, CompileFailure> {
        Ok(Vec::new())
    }

    fn saved_function_dependency_references(
        &self,
        _deadline: Instant,
        _path: &SavedFunctionPath,
        _source: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileFailure> {
        Ok(Vec::new())
    }

    /// The Private Tools and Tools one linked module at `path` names, from
    /// one analysis. An engine that finds no Tool chains names its Private
    /// Tools alone, and admission checks no Call Path in it.
    fn saved_function_references(
        &self,
        deadline: Instant,
        path: &SavedFunctionPath,
        source: &str,
    ) -> Result<ProgramReferences, CompileFailure> {
        Ok(ProgramReferences {
            functions: self.saved_function_dependency_references(deadline, path, source)?,
            tools: None,
        })
    }

    fn compile(&self, deadline: Instant, source: &str) -> Result<ProgramPackage, CompileFailure>;

    /// Compiles `source` with the Saved Functions it links and with
    /// `builtin_tools`, the Workspace's Built-in Tools, on `coral`.
    fn compile_with_saved_functions(
        &self,
        deadline: Instant,
        source: &str,
        linked: &[SavedFunctionSelection],
        _builtin_tools: &BuiltinTools,
    ) -> Result<ProgramPackage, CompileFailure> {
        if linked.is_empty() {
            self.compile(deadline, source)
        } else {
            Err(CompileFailure::Rejected(
                "scripted Program compiler does not support Private Tools".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        stop: Arc<ExecutionStop>,
    ) -> ExecutionCompletion;
}

pub(crate) trait Clock: Send + Sync {
    fn now_unix_nanos(&self) -> Result<i64, String>;
}

pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_nanos(&self) -> Result<i64, String> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?;
        i64::try_from(elapsed.as_nanos())
            .map_err(|_error| "current time exceeds durable nanosecond range".to_string())
    }
}

pub(crate) struct V8ProgramEngine {
    compiler: Arc<dyn CompilerClient>,
    executor: coral_v8::ProgramExecutor,
}

impl V8ProgramEngine {
    /// Compiles through the process's one warm `compiler` and runs each
    /// Program in a fresh isolate within `isolate_limits`.
    pub(crate) fn with_compiler(
        compiler: Arc<dyn CompilerClient>,
        isolate_limits: coral_v8::IsolateLimits,
    ) -> Self {
        Self {
            compiler,
            executor: coral_v8::ProgramExecutor::new(isolate_limits),
        }
    }

    /// An engine with a warm compiler of its own, for tests.
    #[cfg(test)]
    pub(crate) fn new(isolate_limits: coral_v8::IsolateLimits) -> Result<Self, Error> {
        let compiler = coral_run_executor::InProcessCompiler::new(isolate_limits)
            .map_err(|error| Error::ProgramRuntimeUnavailable(error.to_string()))?;
        Ok(Self::with_compiler(Arc::new(compiler), isolate_limits))
    }
}

impl ProgramEngine for V8ProgramEngine {
    fn program_references(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<ProgramReferences, CompileFailure> {
        let source_name = "Program";
        let named = self
            .compiler
            .program_references(time_left(deadline), source)
            .map_err(|error| compile_failure_for_source(error, source_name))?;
        Ok(ProgramReferences {
            functions: parse_saved_function_references(named.functions().to_vec(), source_name)?,
            tools: Some(named.tools().clone()),
        })
    }

    fn unknown_tool_calls(
        &self,
        deadline: Instant,
        tools: &coral_v8::ToolReferences,
        callable: &[String],
    ) -> Result<Vec<coral_v8::UnknownToolCall>, CompileFailure> {
        Ok(self
            .compiler
            .unknown_tool_calls(time_left(deadline), tools, callable)?)
    }

    fn saved_function_references(
        &self,
        deadline: Instant,
        path: &SavedFunctionPath,
        source: &str,
    ) -> Result<ProgramReferences, CompileFailure> {
        let source_name = format!("Private Tool coral.private.{}", path.as_str());
        let named = self
            .compiler
            .call_paths(time_left(deadline), source)
            .map_err(|error| compile_failure_for_source(error, &source_name))?;
        Ok(ProgramReferences {
            functions: parse_saved_function_references(named.functions().to_vec(), &source_name)?,
            tools: Some(named.tools().clone()),
        })
    }

    fn compile(&self, deadline: Instant, source: &str) -> Result<ProgramPackage, CompileFailure> {
        self.compile_with_saved_functions(deadline, source, &[], &BuiltinTools::default())
    }

    fn compile_with_saved_functions(
        &self,
        deadline: Instant,
        source: &str,
        linked: &[SavedFunctionSelection],
        builtin_tools: &BuiltinTools,
    ) -> Result<ProgramPackage, CompileFailure> {
        let sources = linked
            .iter()
            .map(|snapshot| {
                coral_v8::LinkedSavedFunctionSource::new(
                    snapshot.path().segments(),
                    snapshot.source().as_str().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let compiled =
            self.compiler
                .compile(time_left(deadline), source, &sources, builtin_tools)?;
        package_from_compiled(&compiled)
    }

    fn execute(
        &self,
        package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        stop: Arc<ExecutionStop>,
    ) -> ExecutionCompletion {
        let limits = host.value_limits();
        let program = coral_v8::CompiledProgram::restore(package.javascript, package.source_map);
        match self
            .executor
            .execute_with_host_control(&program, host, stop.v8_control())
        {
            Ok(output) => match output.into_final_value_json() {
                None => ExecutionCompletion::Succeeded(FinalValue::Absent),
                Some(value) => match ProgramValue::with_limits(value, limits) {
                    Ok(value) => ExecutionCompletion::Succeeded(FinalValue::Present(value)),
                    Err(error) => {
                        coral_telemetry::metric::output_rejected(
                            coral_telemetry::metric::OutputKind::FinalValue,
                        );
                        ExecutionCompletion::Failed(RunFailure::new(
                            RunFailureCode::InvalidProgramValue,
                            error.to_string(),
                        ))
                    }
                },
            },
            Err(coral_v8::Error::MemoryLimitExceeded) => ExecutionCompletion::MemoryLimitExceeded,
            Err(coral_v8::Error::CpuLimitExceeded) => ExecutionCompletion::CpuLimitExceeded,
            Err(coral_v8::Error::ResourceLimitExceeded) => {
                ExecutionCompletion::Failed(RunFailure::new(
                    RunFailureCode::ResourceLimitExceeded,
                    "Program output exceeds its limit".to_string(),
                ))
            }
            Err(coral_v8::Error::ProgramHostTerminated) => {
                ExecutionCompletion::ProgramHostTerminated
            }
            Err(coral_v8::Error::ProgramRejected(diagnostic)) => ExecutionCompletion::Failed(
                RunFailure::new(RunFailureCode::ProgramRejected, diagnostic.summary())
                    .with_error(*diagnostic),
            ),
            Err(coral_v8::Error::ProgramOutputRejected(error)) => ExecutionCompletion::Failed(
                RunFailure::new(RunFailureCode::InvalidProgramValue, error.summary())
                    .with_error(*error),
            ),
            Err(coral_v8::Error::InvalidProgramValue(message)) => ExecutionCompletion::Failed(
                RunFailure::new(RunFailureCode::InvalidProgramValue, message),
            ),
            Err(_private_diagnostic) => ExecutionCompletion::Failed(RunFailure::new(
                RunFailureCode::ProgramExecutionFailed,
                "Program execution failed".to_string(),
            )),
        }
    }
}

fn parse_saved_function_references(
    references: Vec<coral_v8::SavedFunctionReference>,
    source_name: &str,
) -> Result<Vec<SavedFunctionReference>, String> {
    references
        .into_iter()
        .map(|reference| {
            let path = SavedFunctionPath::parse(reference.path()).map_err(|error| {
                format!(
                    "{source_name}:{}:{}: {error}",
                    reference.line(),
                    reference.column()
                )
            })?;
            Ok(SavedFunctionReference {
                path,
                line: reference.line(),
                column: reference.column(),
            })
        })
        .collect()
}

fn compile_failure_for_source(error: CompileError, source_name: &str) -> CompileFailure {
    match error {
        CompileError::Diagnostics(Diagnostics::Reference {
            message,
            line,
            column,
        }) => CompileFailure::Rejected(format!("{source_name}:{line}:{column}: {message}")),
        other => other.into(),
    }
}

fn package_from_compiled(
    compiled: &coral_v8::CompiledProgram,
) -> Result<ProgramPackage, CompileFailure> {
    let diagnostics_json = serde_json::to_string(compiled.diagnostics())
        .map_err(|_error| CompileFailure::Failed("compiler_diagnostics_encoding"))?;
    let profile = compiled.profile();
    Ok(ProgramPackage::new(
        compiled.javascript().to_string(),
        compiled.source_map().to_string(),
        diagnostics_json,
        profile.id().to_string(),
        profile.compiler_version().to_string(),
        profile.javascript_target().to_string(),
    ))
}

#[derive(Debug, Clone)]
pub(crate) struct ProgramPackage {
    pub(crate) javascript: String,
    pub(crate) source_map: String,
    pub(crate) diagnostics_json: String,
    pub(crate) typescript_profile_id: String,
    pub(crate) compiler_version: String,
    pub(crate) javascript_target: String,
}

impl ProgramPackage {
    pub(crate) fn new(
        javascript: String,
        source_map: String,
        diagnostics_json: String,
        typescript_profile_id: String,
        compiler_version: String,
        javascript_target: String,
    ) -> Self {
        Self {
            javascript,
            source_map,
            diagnostics_json,
            typescript_profile_id,
            compiler_version,
            javascript_target,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        validate_package_text(
            "emitted JavaScript",
            &self.javascript,
            EMITTED_JAVASCRIPT_MAX_BYTES,
        )?;
        validate_package_text("source map", &self.source_map, SOURCE_MAP_MAX_BYTES)?;
        coral_v8::validate_source_map(&self.source_map)
            .map_err(|error| Error::ProgramAdmission(error.to_string()))?;
        if self.diagnostics_json.is_empty()
            || self.typescript_profile_id.is_empty()
            || self.compiler_version.is_empty()
            || self.javascript_target.is_empty()
        {
            return Err(Error::CorruptData(
                "Program package has missing compiler provenance".to_string(),
            ));
        }
        let _: serde_json::Value =
            serde_json::from_str(&self.diagnostics_json).map_err(|error| {
                Error::CorruptData(format!("Program package has invalid diagnostics: {error}"))
            })?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn scripted(marker: &str) -> Self {
        Self::new(
            marker.to_string(),
            r#"{"version":3,"file":"program.js","sources":[],"names":[],"mappings":""}"#
                .to_string(),
            "[]".to_string(),
            "scripted-test-profile".to_string(),
            "test".to_string(),
            "test".to_string(),
        )
    }
}

fn validate_package_text(
    component: &'static str,
    value: &str,
    maximum_bytes: usize,
) -> Result<(), Error> {
    if value.len() > maximum_bytes {
        return Err(Error::PackageTooLarge {
            component,
            maximum_bytes,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExecutionCompletion {
    Succeeded(FinalValue),
    Failed(RunFailure),
    ProgramHostTerminated,
    MemoryLimitExceeded,
    CpuLimitExceeded,
}
