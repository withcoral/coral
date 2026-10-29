//! Secret-free TypeScript compilation and JavaScript execution for Coral.
//!
//! This private Adapter crate gives `coral-run` narrow compiler and execution
//! Interfaces. [`ProgramCompiler::compile`] lowers one TypeScript async-function
//! body; [`ProgramCompiler::prepare_saved_function`] emits one self-contained
//! callable declaration; linked compilation packages one exact current Saved
//! Function source closure. [`ProgramExecutor::execute`] runs emitted JavaScript in a
//! fresh V8 isolate. Run execution installs one narrow [`ProgramHost`] through
//! [`ProgramExecutor::execute_with_host`]. Compiler and execution
//! isolates never share mutable state.
//!
//! The compiler isolate receives packaged compiler assets and virtual source
//! only. Execution starts from a fresh ECMAScript context with no Node, browser,
//! package, filesystem, environment, timer, worker, direct-network,
//! binary-buffer, shared-memory, WebAssembly, or dynamic-code authority. Domain authority,
//! credentials, persistence, and process environment never cross this Adapter.

mod compiler;
mod memory;
mod profile;
mod program_error;
mod runtime;
mod source_map;
mod v8_support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

pub use compiler::{COMPILATION_TIMEOUT, ProgramCompiler};
pub use memory::IsolateLimits;
pub use profile::{TYPESCRIPT_PROFILE, TYPESCRIPT_PROFILE_ID, TypeScriptProfile};
pub use program_error::{ProgramError, ProgramErrorFrame, ProgramToolError};
pub use runtime::ProgramExecutor;

/// Validates one retained authored source map against the pinned profile rules.
///
/// This is the package-admission counterpart to runtime stack projection. It
/// accepts only a regular map for Coral's private emitted file, with safe
/// authored source labels and no embedded source text.
///
/// # Errors
///
/// Returns [`Error::InvalidSourceMap`] when the map is malformed or violates
/// the profile.
pub fn validate_source_map(source_map: &str) -> Result<(), Error> {
    source_map::decode_authored(source_map).map(drop)
}

/// One-use cross-thread termination control for a fresh Program isolate.
///
/// Termination is sticky: a request made before the executor installs its V8
/// isolate handle is applied as soon as execution is ready to begin. The handle
/// exposes no isolate state or Program-visible cancellation signal.
#[derive(Clone, Default)]
pub struct ProgramExecutionControl {
    state: Arc<ProgramExecutionControlState>,
}

#[derive(Default)]
struct ProgramExecutionControlState {
    requested: AtomicBool,
    isolate: Mutex<Option<(v8::IsolateHandle, std::thread::Thread)>>,
}

impl ProgramExecutionControl {
    /// Builds an independent control for one Program execution.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests uncatchable termination of the controlled Program execution.
    ///
    /// Returns whether an isolate was already installed. A `false` result still
    /// records the request and prevents later Program JavaScript from running.
    #[must_use]
    pub fn terminate(&self) -> bool {
        self.state.requested.store(true, Ordering::SeqCst);
        self.state
            .isolate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|(handle, thread)| {
                thread.unpark();
                handle.terminate_execution()
            })
    }

    // Hold the same lock as terminate until its V8 call completes. A timed-out
    // compiler request must never terminate the next user of a warm isolate.
    pub(crate) fn uninstall(&self) {
        *self
            .state
            .isolate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.state.requested.load(Ordering::SeqCst)
    }

    pub(crate) fn install(&self, handle: &v8::IsolateHandle) {
        let requested = self.state.requested.load(Ordering::SeqCst);
        *self
            .state
            .isolate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((handle.clone(), std::thread::current()));
        if requested || self.state.requested.load(Ordering::SeqCst) {
            let _terminated = handle.terminate_execution();
        }
    }
}

/// One independently progressing Tool, settled only after durable evidence.
pub type ToolFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolHostError>> + Send>>;

/// Host seam used by one fresh execution isolate.
///
/// Tools start without blocking JavaScript and their Promises settle only
/// after durable Invocation completion. Emissions and Logs block until durable.
/// A fatal host failure terminates the isolate without a catchable result.
/// Domain authority and persistence stay behind this seam.
pub trait ProgramHost: Send + Sync {
    /// Effective value limits retained by the owning Run.
    fn value_limits(&self) -> coral_value::ValueLimits {
        coral_value::ValueLimits::default()
    }

    /// Invokes one Tool by complete Call Path and native-JSON arguments.
    ///
    /// `location` is the authored call site captured before the call returns
    /// to JavaScript, or `None` when no authored frame made the call.
    ///
    /// # Errors
    ///
    /// Returns a catchable rejection only after any required terminal evidence
    /// is durable. A fatal host failure terminates Program execution without
    /// settling the Tool Promise.
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture;

    /// Durably appends one native-JSON Program Emission made at `location`.
    ///
    /// # Errors
    ///
    /// Invalid and oversized values remain catchable by the Program. A fatal
    /// host failure terminates execution without resolving the Emission Promise.
    fn emit(
        &self,
        value_json: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError>;

    /// Durably appends one bounded Program Log made at `location` before
    /// returning `undefined`.
    ///
    /// # Errors
    ///
    /// A limit failure throws a catchable `RangeError`; a fatal host failure
    /// terminates Program execution.
    fn log(
        &self,
        level: ConsoleLevel,
        message: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError>;
}

/// The authored source that holds one [`AuthoredLocation`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthoredSource {
    /// The submitted Program body.
    Program,
    /// One Saved Function linked into the package, by its path relative to
    /// `coral.functions`.
    SavedFunction {
        /// Dotted path, such as `debug.fetch`.
        path: String,
    },
}

/// The innermost authored frame that called a Program host binding.
///
/// Line and column are one-based positions in the exact authored source that
/// the package was compiled from. The column is where V8 places the call: the
/// called member name of a property call such as `console.log(...)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoredLocation {
    /// Program or linked Saved Function source.
    pub source: AuthoredSource,
    /// One-based authored line.
    pub line: u32,
    /// One-based authored column in UTF-16 code units.
    pub column: u32,
}

/// Failure returned by the private Program Tool host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolHostError {
    /// Reject with a known, safe Coral Failure after durable completion.
    Rejected {
        /// Domain-owned stable `snake_case` code, not an upstream error code.
        code: &'static str,
        /// Safe domain message, never an upstream or host diagnostic.
        message: std::borrow::Cow<'static, str>,
        /// Optional lowercase W3C trace identifier; grants no authority.
        trace_id: Option<String>,
    },
    /// Reject with a complete upstream outcome that is not a declared success,
    /// after it is durable. The Program reads its `status`, `data` and
    /// `metadata` from the `ToolError`.
    Upstream {
        /// Domain-owned stable `snake_case` code, never a Coral Failure code.
        code: &'static str,
        /// Safe domain message, never an upstream diagnostic.
        message: &'static str,
        /// The encoded Tool Result the Program does not receive.
        outcome_json: String,
    },
    /// Terminate Program execution without settling the Tool Promise.
    Fatal,
}

/// Program Log levels crossing the V8 Adapter seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsoleLevel {
    /// `console.debug`.
    Debug,
    /// `console.info` or `console.log`.
    Info,
    /// `console.warn`.
    Warn,
    /// `console.error`.
    Error,
}

/// Failure returned by a Program output host callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramOutputHostError {
    /// The serialized Emission is not an interoperable Program Value.
    InvalidValue,
    /// One bounded Program output or Log limit was crossed.
    LimitExceeded,
    /// Durable host work failed and execution must stop without a catchable result.
    Fatal,
}

/// Result produced by one fulfilled Program entry Promise.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionOutput {
    final_value_json: Option<String>,
}

impl ExecutionOutput {
    pub(crate) const fn new(final_value_json: Option<String>) -> Self {
        Self { final_value_json }
    }

    /// Returns the native JSON encoding, or `None` only when the Program
    /// returned JavaScript `undefined`.
    #[must_use]
    pub fn final_value_json(&self) -> Option<&str> {
        self.final_value_json.as_deref()
    }

    /// Consumes the output and returns its optional native JSON encoding.
    #[must_use]
    pub fn into_final_value_json(self) -> Option<String> {
        self.final_value_json
    }
}

/// A Program package emitted by the pinned compiler profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledProgram {
    javascript: String,
    source_map: String,
    diagnostics: Vec<CompilerDiagnostic>,
}

impl CompiledProgram {
    pub(crate) fn new(
        javascript: String,
        source_map: String,
        diagnostics: Vec<CompilerDiagnostic>,
    ) -> Self {
        Self {
            javascript,
            source_map,
            diagnostics,
        }
    }

    /// Restores emitted code that a durable Run package previously accepted.
    ///
    /// Compilation diagnostics are authoring evidence and are not required to
    /// execute the already-emitted JavaScript.
    #[must_use]
    pub fn restore(javascript: String, source_map: String) -> Self {
        Self::new(javascript, source_map, Vec::new())
    }

    /// Returns the JavaScript executed by V8.
    #[must_use]
    pub fn javascript(&self) -> &str {
        &self.javascript
    }

    /// Returns the source map emitted with the JavaScript.
    #[must_use]
    pub fn source_map(&self) -> &str {
        &self.source_map
    }

    /// Returns non-blocking diagnostics produced during syntax lowering.
    #[must_use]
    pub fn diagnostics(&self) -> &[CompilerDiagnostic] {
        &self.diagnostics
    }

    /// Returns the exact profile that emitted this package.
    #[must_use]
    pub const fn profile(&self) -> TypeScriptProfile {
        TYPESCRIPT_PROFILE
    }
}

/// One callable-rooted field projected from an emitted Saved Function declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SavedFunctionDeclarationField {
    path: String,
    text: String,
}

impl SavedFunctionDeclarationField {
    /// Returns the readable JavaScript path rooted at `input` or `result`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns names, documentation, and type evidence associated with the path.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Authoring output retained for one accepted Saved Function source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedFunctionCompilation {
    declaration: String,
    declaration_field_projection: Result<Vec<SavedFunctionDeclarationField>, String>,
    diagnostics: Vec<CompilerDiagnostic>,
    references: Vec<SavedFunctionReference>,
}

impl SavedFunctionCompilation {
    pub(crate) const fn new(
        declaration: String,
        declaration_field_projection: Result<Vec<SavedFunctionDeclarationField>, String>,
        diagnostics: Vec<CompilerDiagnostic>,
        references: Vec<SavedFunctionReference>,
    ) -> Self {
        Self {
            declaration,
            declaration_field_projection,
            diagnostics,
            references,
        }
    }

    /// Returns the exact self-contained emitted declaration.
    #[must_use]
    pub fn declaration(&self) -> &str {
        &self.declaration
    }

    /// Returns callable-rooted declaration fields or a private projection fault.
    ///
    /// # Errors
    ///
    /// Returns a stable private reason when traversal could not prepare fields.
    pub fn declaration_fields(&self) -> Result<&[SavedFunctionDeclarationField], &str> {
        self.declaration_field_projection
            .as_deref()
            .map_err(String::as_str)
    }

    /// Returns strict semantic diagnostics that do not block ordinary authoring.
    #[must_use]
    pub fn diagnostics(&self) -> &[CompilerDiagnostic] {
        &self.diagnostics
    }

    /// Returns directly named runtime Saved Function references in path order.
    #[must_use]
    pub fn references(&self) -> &[SavedFunctionReference] {
        &self.references
    }
}

/// One statically named runtime Saved Function reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SavedFunctionReference {
    path: String,
    line: u64,
    column: u64,
}

impl SavedFunctionReference {
    /// Returns the dotted path relative to `coral.functions`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the one-based authored source line.
    #[must_use]
    pub const fn line(&self) -> u64 {
        self.line
    }

    /// Returns the one-based authored source column.
    #[must_use]
    pub const fn column(&self) -> u64 {
        self.column
    }
}

/// One exact Saved Function source supplied for linked compilation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkedSavedFunctionSource {
    path_segments: Vec<String>,
    source: String,
}

impl LinkedSavedFunctionSource {
    /// Builds one validated-domain source input for the compiler Adapter.
    #[must_use]
    pub const fn new(path_segments: Vec<String>, source: String) -> Self {
        Self {
            path_segments,
            source,
        }
    }
}

/// The Call Paths one Saved Function module names directly, as its typed
/// preparation needs them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedFunctionCallPaths {
    functions: Vec<SavedFunctionReference>,
    tools: ToolReferences,
}

impl SavedFunctionCallPaths {
    /// Returns the directly named `coral.functions` paths.
    #[must_use]
    pub fn functions(&self) -> &[SavedFunctionReference] {
        &self.functions
    }

    /// Returns the literal `coral.providers` and `coral.workspace` chains.
    #[must_use]
    pub const fn tools(&self) -> &ToolReferences {
        &self.tools
    }
}

/// The literal `coral.providers` and `coral.workspace` chains one source
/// names. Aliased roots are not followed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolReferences {
    call_paths: Vec<String>,
    chains: Vec<ToolChain>,
}

impl ToolReferences {
    /// Returns every chain prefix that could be a Tool Call Path, such
    /// as `coral.providers.github.repos.get`, so `get.call(...)` still names
    /// `get`.
    #[must_use]
    pub fn call_paths(&self) -> &[String] {
        &self.call_paths
    }
}

/// One literal chain, relative to `coral`, where it starts in the source, and
/// whether it is called.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ToolChain {
    segments: Vec<String>,
    start: u64,
    called: bool,
}

/// One declaration that types a Call Path under `coral.providers`,
/// `coral.workspace` or `coral.functions`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallableDeclaration {
    path_segments: Vec<String>,
    declaration: String,
}

impl CallableDeclaration {
    /// Builds one declaration for its path segments: relative to `coral` for
    /// a Tool, such as `providers.github.repos.get`, and to
    /// `coral.functions` for a Saved Function.
    ///
    /// The declaration is self-contained TypeScript with one
    /// `export default` callable, as describe and Saved Function save emit it.
    #[must_use]
    pub const fn new(path_segments: Vec<String>, declaration: String) -> Self {
        Self {
            path_segments,
            declaration,
        }
    }
}

/// The declarations that type the `coral` namespace for one Saved Function
/// save.
///
/// A root without declarations stays `any`. A typed root holds only the
/// supplied paths: any other path the source calls is a diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoralDeclarations {
    declaration_max_bytes: usize,
    tools: Option<TypedTools>,
    functions: Option<Vec<CallableDeclaration>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct TypedTools {
    chains: Vec<ToolChain>,
    declarations: Vec<CallableDeclaration>,
}

impl CoralDeclarations {
    /// Types nothing yet. A typed Saved Function declaration above
    /// `declaration_max_bytes` is saved untyped.
    #[must_use]
    pub const fn new(declaration_max_bytes: usize) -> Self {
        Self {
            declaration_max_bytes,
            tools: None,
            functions: None,
        }
    }

    /// Types `coral.providers` and `coral.workspace` from the Tools the
    /// caller can call. `references` are what
    /// [`ProgramCompiler::saved_function_call_paths`] found in the same
    /// source.
    #[must_use]
    pub fn with_tools(
        mut self,
        references: &ToolReferences,
        declarations: Vec<CallableDeclaration>,
    ) -> Self {
        self.tools = Some(TypedTools {
            chains: references.chains.clone(),
            declarations,
        });
        self
    }

    /// Types `coral.functions` from stored Saved Function declarations.
    #[must_use]
    pub fn with_functions(mut self, functions: Vec<CallableDeclaration>) -> Self {
        self.functions = Some(functions);
        self
    }
}

/// TypeScript compiler diagnostic category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticCategory {
    /// Informational compiler message.
    Message,
    /// Non-blocking warning.
    Warning,
    /// Blocking syntax or emission error.
    Error,
    /// Non-blocking authoring suggestion.
    Suggestion,
}

/// One safe structured diagnostic from the TypeScript compiler.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct CompilerDiagnostic {
    category: DiagnosticCategory,
    code: u64,
    message: String,
    line: Option<u64>,
    column: Option<u64>,
}

impl CompilerDiagnostic {
    /// Returns the diagnostic category.
    #[must_use]
    pub const fn category(&self) -> DiagnosticCategory {
        self.category
    }

    /// Returns the official TypeScript diagnostic code.
    #[must_use]
    pub const fn code(&self) -> u64 {
        self.code
    }

    /// Returns the flattened diagnostic message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the one-based virtual-source line when available.
    #[must_use]
    pub const fn line(&self) -> Option<u64> {
        self.line
    }

    /// Returns the one-based virtual-source column when available.
    #[must_use]
    pub const fn column(&self) -> Option<u64> {
        self.column
    }
}

/// Compiler or runtime Adapter failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// V8 or its ICU data could not initialize.
    #[error("V8 initialization failed: {0}")]
    Initialization(String),
    /// The dedicated compiler thread could not start.
    #[error("TypeScript compiler thread could not start: {0}")]
    CompilerThread(#[source] std::io::Error),
    /// The compiler worker stopped before returning a result.
    #[error("TypeScript compiler is unavailable")]
    CompilerUnavailable,
    /// The bounded compiler queue cannot accept more work.
    #[error("TypeScript compiler is busy")]
    CompilerBusy,
    /// The compiler deadline expired; active compilation is terminated.
    #[error("TypeScript compilation timed out")]
    CompilationTimedOut,
    /// The compiler returned blocking diagnostics.
    #[error("TypeScript compilation failed")]
    Compilation {
        /// Blocking diagnostics returned by the official compiler.
        diagnostics: Vec<CompilerDiagnostic>,
    },
    /// Source changed the shape of Coral's private async entry wrapper.
    #[error("Program source is not one async-function body")]
    InvalidProgramBody,
    /// Saved Function source did not define one import-free default callable.
    #[error("Saved Function authoring failed: {0}")]
    InvalidSavedFunction(String),
    /// Program or Saved Function source requested a forbidden authoring capability.
    #[error("TypeScript source uses a forbidden capability: {0}")]
    ForbiddenCapability(String),
    /// Program source did not directly name supported Saved Function paths.
    #[error("Saved Function reference analysis failed at {line}:{column}: {message}")]
    InvalidSavedFunctionReference {
        /// Clear safe reason.
        message: String,
        /// One-based authored source line.
        line: u64,
        /// One-based authored source column.
        column: u64,
    },
    /// The private compiler bridge returned an invalid payload.
    #[error("TypeScript compiler protocol failed: {0}")]
    CompilerProtocol(String),
    /// TypeScript emitted a source map outside Coral's pinned rules.
    #[error("TypeScript source map is invalid: {0}")]
    InvalidSourceMap(String),
    /// V8 failed to compile or evaluate private or Program JavaScript.
    #[error("V8 execution failed: {0}")]
    Engine(String),
    /// The Program's entry Promise rejected with an authored-location stack.
    #[error("{0}")]
    ProgramRejected(Box<ProgramError>),
    /// Existing JavaScript error thrown while publishing the Final Value.
    #[error("{0}")]
    ProgramOutputRejected(Box<ProgramError>),
    /// A Program output exceeded an accepted finite bound.
    #[error("Program output exceeds its limit")]
    ResourceLimitExceeded,
    /// The isolate exceeded its configured heap budget.
    #[error("V8 isolate memory limit exceeded")]
    MemoryLimitExceeded,
    /// The configured heap budgets cannot safely initialize bounded isolates.
    #[error("isolate heap budgets must be at least 8 MiB and leave room for termination")]
    InvalidIsolateLimits,
    /// Pure ECMAScript microtasks could not settle the entry Promise.
    #[error("Program Promise remained pending")]
    PendingProgram,
    /// The Program host failed before it could durably settle host work.
    #[error("Program host terminated execution")]
    ProgramHostTerminated,
    /// An Emission or Final Value was not a JSON-compatible Program Value.
    #[error("Program published an invalid Program Value: {0}")]
    InvalidProgramValue(String),
}

/// Formats safe compiler diagnostics with their authored line and column.
#[must_use]
pub fn format_compiler_error(error: Error) -> String {
    let message = match error {
        Error::Compilation { diagnostics } => diagnostics
            .into_iter()
            .map(|diagnostic| {
                let location = match (diagnostic.line(), diagnostic.column()) {
                    (Some(line), Some(column)) => format!(" at {line}:{column}"),
                    _ => String::new(),
                };
                format!(
                    "TypeScript {}{location}: {}",
                    diagnostic.code(),
                    diagnostic.message()
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => other.to_string(),
    };
    bounded_diagnostic_message(&message)
}

/// Bounds safe authoring diagnostics consistently for callers and retained failures.
/// A suffix explicitly identifies omitted diagnostics. This does not sanitize host errors.
#[must_use]
pub fn bounded_diagnostic_message(message: &str) -> String {
    const MAX_BYTES: usize = 4 * 1024;
    const SUFFIX: &str = "\n[diagnostics truncated]";
    if message.len() <= MAX_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_BYTES - SUFFIX.len();
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{SUFFIX}", message.get(..end).unwrap_or_default())
}
