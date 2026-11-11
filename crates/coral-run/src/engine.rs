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

use coral_catalog::AuthoredToolKind;
use coral_run_executor::{AnalysisError, CompileError, CompilerClient, Diagnostics, time_left};
use coral_tools::{AuthoredToolPath, LinkedToolReference, LinkedToolSelection};
use coral_v8::{BuiltinTools, CompileFailure};

use crate::execution::ExecutionStop;
use crate::{
    EMITTED_JAVASCRIPT_MAX_BYTES, Error, FinalValue, ProgramValue, RunFailure, RunFailureCode,
    SOURCE_MAP_MAX_BYTES,
};

/// One source admission analyzes.
#[derive(Clone, Debug)]
pub(crate) enum AdmissionSource {
    /// The submitted Program body.
    Program(String),
    /// One Private or Shared Tool module of the closure.
    Linked {
        path: AuthoredToolPath,
        source: String,
    },
}

impl AdmissionSource {
    /// How a rejection names the source.
    fn name(&self) -> String {
        match self {
            Self::Program(_) => "Program".to_string(),
            Self::Linked { path, .. } => path.source_name(),
        }
    }
}

/// What one source names: the Private and Shared Tools admission links and
/// the Tool Call Paths it checks.
#[derive(Clone, Debug, Default)]
pub(crate) struct AdmissionReferences {
    pub(crate) linked: Vec<LinkedToolReference>,
    pub(crate) tool_call_paths: Vec<String>,
}

impl AdmissionReferences {
    /// The references of an engine that finds no Tool Call Paths: it names
    /// its linked Tools alone, and admission checks no Call Path.
    #[cfg(test)]
    pub(crate) const fn linking(linked: Vec<LinkedToolReference>) -> Self {
        Self {
            linked,
            tool_call_paths: Vec::new(),
        }
    }
}

/// Why an engine compiled no package.
#[derive(Debug)]
pub(crate) enum PackageFailure {
    /// One source calls these Tools, which name nothing the caller can
    /// call: the Program body, or the linked Tool at index `linked` of
    /// those compiled with it.
    UnknownTools {
        linked: Option<usize>,
        calls: Vec<coral_v8::UnknownToolCall>,
    },
    /// Any other failure.
    Compile(CompileFailure),
}

impl From<CompileFailure> for PackageFailure {
    fn from(failure: CompileFailure) -> Self {
        Self::Compile(failure)
    }
}

pub(crate) trait ProgramEngine: Send + Sync {
    /// Blocks until a compiler can take a call, for at most the compiler
    /// client's own bound. The compilation deadline starts after it.
    /// `Some` is an engine that holds room for this admission's first call,
    /// which compiles through it; `None`: this engine as it is.
    fn ready(&self) -> Result<Option<Arc<dyn ProgramEngine>>, CompileFailure> {
        Ok(None)
    }

    /// What each source names: one answer per source, in the order of
    /// `sources`. The first source that fails stops the analysis, and its
    /// failure names the source. An engine that finds nothing names nothing,
    /// and admission links and checks nothing.
    fn analyze(
        &self,
        _deadline: Instant,
        sources: &[AdmissionSource],
    ) -> Result<Vec<AdmissionReferences>, CompileFailure> {
        Ok(vec![AdmissionReferences::default(); sources.len()])
    }

    /// `source` lowered with the Private and Shared Tools it links, in
    /// initialization order, and with `builtin_tools`, the Workspace's
    /// Built-in Tools, on `coral`, checked first against the `callable`
    /// Call Paths when there are any.
    fn compile(
        &self,
        deadline: Instant,
        source: &str,
        linked: &[LinkedToolSelection],
        builtin_tools: &BuiltinTools,
        callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure>;

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
    fn ready(&self) -> Result<Option<Arc<dyn ProgramEngine>>, CompileFailure> {
        let held = self.compiler.ready().map_err(CompileFailure::from)?;
        Ok(held.map(|compiler| {
            Arc::new(Self {
                compiler,
                executor: self.executor,
            }) as Arc<dyn ProgramEngine>
        }))
    }

    fn analyze(
        &self,
        deadline: Instant,
        sources: &[AdmissionSource],
    ) -> Result<Vec<AdmissionReferences>, CompileFailure> {
        let analyzed = sources
            .iter()
            .map(|source| match source {
                AdmissionSource::Program(body) => coral_v8::AnalyzedSource::Program(body.clone()),
                AdmissionSource::Linked { source, .. } => {
                    coral_v8::AnalyzedSource::AuthoredTool(source.clone())
                }
            })
            .collect::<Vec<_>>();
        let references = self
            .compiler
            .analyze(time_left(deadline), &analyzed)
            .map_err(|AnalysisError { index, error }| match sources.get(index) {
                Some(source) => compile_failure_for_source(error, &source.name()),
                None => CompileFailure::Failed("compiler_protocol"),
            })?;
        if references.len() != sources.len() {
            return Err(CompileFailure::Failed("compiler_protocol"));
        }
        sources
            .iter()
            .zip(references)
            .map(|(source, references)| admission_references(&references, &source.name()))
            .collect()
    }

    fn compile(
        &self,
        deadline: Instant,
        source: &str,
        linked: &[LinkedToolSelection],
        builtin_tools: &BuiltinTools,
        callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure> {
        let sources = linked
            .iter()
            .map(|linked| {
                let source = match linked.kind() {
                    AuthoredToolKind::Private => coral_v8::LinkedToolSource::private,
                    AuthoredToolKind::Shared => coral_v8::LinkedToolSource::shared,
                };
                source(
                    linked.path().segments(),
                    linked.source().as_str().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let compiled = self
            .compiler
            .compile(
                time_left(deadline),
                source,
                &sources,
                builtin_tools,
                callable,
            )
            .map_err(|error| match error {
                CompileError::Diagnostics(Diagnostics::UnknownTools { linked, calls }) => {
                    PackageFailure::UnknownTools { linked, calls }
                }
                other => PackageFailure::Compile(other.into()),
            })?;
        Ok(package_from_compiled(&compiled)?)
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
            // Nothing is left that could settle what the Program awaits: no
            // Tool call is in flight, and no other event source exists.
            Err(coral_v8::Error::PendingProgram) => ExecutionCompletion::Failed(RunFailure::new(
                RunFailureCode::ProgramExecutionFailed,
                "Program awaited a Promise that nothing can settle".to_string(),
            )),
            Err(_private_diagnostic) => ExecutionCompletion::Failed(RunFailure::new(
                RunFailureCode::ProgramExecutionFailed,
                "Program execution failed".to_string(),
            )),
        }
    }
}

/// What the compiler found `source_name` to name, with each path parsed.
fn admission_references(
    named: &coral_v8::SourceReferences,
    source_name: &str,
) -> Result<AdmissionReferences, CompileFailure> {
    let linked = LinkedToolReference::from_compiler(named.private_tools(), named.shared_tools())
        .map_err(|invalid| {
            format!(
                "{source_name}:{}:{}: {}",
                invalid.line, invalid.column, invalid.error
            )
        })?;
    Ok(AdmissionReferences {
        linked,
        tool_call_paths: named.tool_call_paths().to_vec(),
    })
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
