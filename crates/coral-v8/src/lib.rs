//! Secret-free TypeScript compilation and JavaScript execution for Coral.
//!
//! This private Adapter crate gives `coral-run` narrow compiler and execution
//! Interfaces. [`ProgramCompiler::compile`] lowers one TypeScript async-function
//! body; [`ProgramCompiler::prepare_authored_tool`] emits one self-contained
//! callable declaration; linked compilation packages one exact current Private
//! Tool source closure. [`ProgramExecutor::execute`] runs emitted JavaScript in a
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
mod cpu;
mod memory;
mod profile;
mod program_error;
mod runtime;
mod source_map;
/// Test support: what a test reads about this process's compilers.
#[cfg(feature = "test-support")]
pub mod testing;
mod v8_support;

// `tests/execution_usage.rs` uses `tracing`; the unit tests do not.
#[cfg(test)]
use tracing as _;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

pub use compiler::ProgramCompiler;
pub use cpu::cpu_budget_supported;
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

/// One call to a Tool that Coral performs by running its body in the
/// Program, settled once the call is permitted: with what completes it, or
/// with a rejection, after which the body never runs.
pub type PermitFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Box<dyn ToolCompletion>, ToolHostError>> + Send>,
>;

/// How the body of a permitted Tool call ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolBodyOutcome {
    /// It returned a value, here as native JSON. A body that returns
    /// `undefined` returns `null`.
    Returned(String),
    /// It returned a value that is not data, such as a `bigint`, a `Date`,
    /// `NaN`, a cycle, or a function (ADR 0034).
    NotData {
        /// Where the value is, as a path from `result`.
        field: String,
        /// Why it is not data, bounded text the bootstrap wrote; empty when
        /// it gave none.
        reason: String,
    },
    /// It threw. `message` is the thrown Error's message, or the thrown
    /// value as text. `tool_error` holds the code of a Tool Error that a Tool
    /// call gave the body, which reaches the caller unchanged.
    Threw {
        /// Bounded text the Program wrote.
        message: String,
        /// The thrown Tool Error's code, when the body threw one.
        tool_error: Option<String>,
    },
}

/// Completes one permitted Tool call that Coral performs in the Program.
pub trait ToolCompletion: Send {
    /// The Tool calls this call's body makes, bound to this call. Each call
    /// the body starts reaches them, across every `await` and helper, and
    /// never through a stack frame; so does a call the body started work
    /// for that runs after the body ended. The Program's own calls and its
    /// modules' top-level calls reach the [`ProgramHost`] instead.
    fn body_calls(&self) -> Arc<dyn ToolCalls>;

    /// Records how the call's body ended and settles once that is durable:
    /// with the native-JSON value its caller receives, or a rejection. A
    /// body that threw a Tool Error rejects its caller with that error
    /// whatever rejection this settles with.
    fn complete(self: Box<Self>, outcome: ToolBodyOutcome) -> ToolFuture;
}

/// The Tool calls that the body of one permitted call makes, as
/// [`ProgramHost::invoke`] and [`ProgramHost::perform`] are the Program's.
pub trait ToolCalls: Send + Sync {
    /// Invokes one Tool for the body, as [`ProgramHost::invoke`] does.
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture;

    /// Starts one performed call for the body, as [`ProgramHost::perform`]
    /// does.
    fn perform(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> PermitFuture;
}

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

    /// The trace a Tool rejection decided before [`Self::invoke`] names, as
    /// the host's own rejections do.
    fn trace_id(&self) -> Option<String> {
        None
    }

    /// The CPU time the Program may use on its own thread before execution
    /// ends with [`Error::CpuLimitExceeded`]. `None` runs it without a budget.
    fn cpu_budget(&self) -> Option<std::time::Duration> {
        None
    }

    /// Called once the Program's script is loaded into its fresh isolate,
    /// before any of its JavaScript runs, and returns when it may run.
    /// `false` ends it unrun with [`Error::ProgramHostTerminated`]. The CPU
    /// budget is read after it returns.
    fn start(&self) -> bool {
        true
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

    /// Starts one call to a Tool that Coral performs by running its body in
    /// this Program, by complete Call Path and native-JSON arguments: the
    /// one object it was called with. It settles once the call is
    /// permitted, and only then does the body run; its outcome completes the
    /// call through the [`ToolCompletion`] this returns.
    ///
    /// A host that performs no such Tool refuses every call.
    ///
    /// # Errors
    ///
    /// Returns a catchable rejection only after any required terminal evidence
    /// is durable. A fatal host failure terminates Program execution without
    /// settling the Tool Promise.
    fn perform(
        self: Arc<Self>,
        _call_path: String,
        _arguments_json: String,
        _location: Option<AuthoredLocation>,
    ) -> PermitFuture {
        Box::pin(async {
            Err(ToolHostError::Rejected {
                code: "tool_unavailable".into(),
                message: "Tool is unavailable".into(),
                trace_id: None,
                suggestions: Vec::new(),
            })
        })
    }

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
    /// One Private Tool linked into the package, by its path relative to
    /// `coral.private`.
    PrivateTool {
        /// Dotted path, such as `debug.fetch`.
        path: String,
    },
    /// One Shared Tool linked into the package, by its path relative to
    /// `coral.shared`.
    SharedTool {
        /// Dotted path, such as `mail.remind`.
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
    /// Program or linked Authored Tool source.
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
        code: std::borrow::Cow<'static, str>,
        /// Safe domain message, never an upstream or host diagnostic.
        message: std::borrow::Cow<'static, str>,
        /// Optional lowercase W3C trace identifier; grants no authority.
        trace_id: Option<String>,
        /// Call Paths the caller can call that are near the one it named,
        /// nearest first; empty for every failure but `tool_not_found`.
        suggestions: Vec<String>,
    },
    /// Reject with a complete upstream outcome that is not a declared success,
    /// after it is durable. The Program reads its `status`, `data` and
    /// `metadata` from the `ToolError`.
    Upstream {
        /// Domain-owned stable `snake_case` code, never a Coral Failure code.
        code: std::borrow::Cow<'static, str>,
        /// Safe domain message, never an upstream diagnostic.
        message: std::borrow::Cow<'static, str>,
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
    /// A package with its non-blocking diagnostics, as the compiler emitted it.
    #[must_use]
    pub const fn new(
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

    /// The JavaScript, source map and diagnostics, without a copy.
    #[must_use]
    pub fn into_parts(self) -> (String, String, Vec<CompilerDiagnostic>) {
        (self.javascript, self.source_map, self.diagnostics)
    }
}

/// One callable-rooted field projected from an emitted Private Tool declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AuthoredToolDeclarationField {
    path: String,
    text: String,
}

impl AuthoredToolDeclarationField {
    /// The field at `path` with its `text`.
    #[must_use]
    pub const fn new(path: String, text: String) -> Self {
        Self { path, text }
    }

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

/// Authoring output retained for one accepted Authored Tool source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredToolCompilation {
    declaration: String,
    declaration_field_projection: Result<Vec<AuthoredToolDeclarationField>, String>,
    diagnostics: Vec<CompilerDiagnostic>,
    references: Vec<LinkedToolReference>,
    shared_references: Vec<LinkedToolReference>,
    tools: ToolReferences,
    not_data: Option<String>,
}

impl AuthoredToolCompilation {
    /// The authoring output as the compiler emitted it.
    #[must_use]
    pub const fn new(
        declaration: String,
        declaration_field_projection: Result<Vec<AuthoredToolDeclarationField>, String>,
        diagnostics: Vec<CompilerDiagnostic>,
        references: Vec<LinkedToolReference>,
        shared_references: Vec<LinkedToolReference>,
        tools: ToolReferences,
        not_data: Option<String>,
    ) -> Self {
        Self {
            declaration,
            declaration_field_projection,
            diagnostics,
            references,
            shared_references,
            tools,
            not_data,
        }
    }

    /// Returns the literal Tool chains under `coral` that the prepared
    /// source names, from the same compile as its declaration.
    #[must_use]
    pub const fn tools(&self) -> &ToolReferences {
        &self.tools
    }

    /// Where the declaration's arguments or result hold something other
    /// than data, such as "`input.callback` is a function"; `None` when
    /// they are all data. `any` and `unknown` say nothing, so they pass.
    #[must_use]
    pub fn not_data(&self) -> Option<&str> {
        self.not_data.as_deref()
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
    pub fn declaration_fields(&self) -> Result<&[AuthoredToolDeclarationField], &str> {
        self.declaration_field_projection
            .as_deref()
            .map_err(String::as_str)
    }

    /// Returns strict semantic diagnostics that do not block ordinary authoring.
    #[must_use]
    pub fn diagnostics(&self) -> &[CompilerDiagnostic] {
        &self.diagnostics
    }

    /// Returns directly named runtime Private Tool references in path order.
    #[must_use]
    pub fn references(&self) -> &[LinkedToolReference] {
        &self.references
    }

    /// Returns directly named runtime Shared Tool references, relative to
    /// `coral.shared`, in path order.
    #[must_use]
    pub fn shared_references(&self) -> &[LinkedToolReference] {
        &self.shared_references
    }
}

/// One statically named runtime Private Tool reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LinkedToolReference {
    path: String,
    line: u64,
    column: u64,
}

impl LinkedToolReference {
    /// The reference to `path` at its authored `line` and `column`.
    #[must_use]
    pub const fn new(path: String, line: u64, column: u64) -> Self {
        Self { path, line, column }
    }

    /// Returns the dotted path relative to `coral.private`.
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

/// The member of `coral` that holds one linked Tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LinkedToolRoot {
    /// `coral.private`: the caller's own Private Tools.
    Private,
    /// `coral.shared`: the Workspace's Shared Tools.
    Shared,
}

/// One exact Private or Shared Tool source supplied for linked compilation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkedToolSource {
    root: LinkedToolRoot,
    path_segments: Vec<String>,
    source: String,
}

impl LinkedToolSource {
    /// Builds one validated-domain Private Tool source input for the
    /// compiler Adapter, at `path_segments` under `coral.private`.
    #[must_use]
    pub const fn private(path_segments: Vec<String>, source: String) -> Self {
        Self {
            root: LinkedToolRoot::Private,
            path_segments,
            source,
        }
    }

    /// Builds one validated-domain Shared Tool source input for the
    /// compiler Adapter, at `path_segments` under `coral.shared`.
    #[must_use]
    pub const fn shared(path_segments: Vec<String>, source: String) -> Self {
        Self {
            root: LinkedToolRoot::Shared,
            path_segments,
            source,
        }
    }

    /// The member of `coral` that holds it.
    #[must_use]
    pub const fn root(&self) -> LinkedToolRoot {
        self.root
    }

    /// The path under its root.
    #[must_use]
    pub fn path_segments(&self) -> &[String] {
        &self.path_segments
    }

    /// The module source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// One source for [`ProgramCompiler::analyze`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnalyzedSource {
    /// A Program body.
    Program(String),
    /// An Authored Tool module: a Private or a Shared Tool.
    AuthoredTool(String),
}

/// What one Program body or Authored Tool module names directly: the
/// Private and Shared Tools that link or type it, and the Tools it may call.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceReferences {
    private_tools: Vec<LinkedToolReference>,
    shared_tools: Vec<LinkedToolReference>,
    tool_call_paths: Vec<String>,
}

impl SourceReferences {
    /// The `private_tools`, `shared_tools` and `tool_call_paths` one source
    /// names.
    #[must_use]
    pub const fn new(
        private_tools: Vec<LinkedToolReference>,
        shared_tools: Vec<LinkedToolReference>,
        tool_call_paths: Vec<String>,
    ) -> Self {
        Self {
            private_tools,
            shared_tools,
            tool_call_paths,
        }
    }

    /// Returns the directly named `coral.private` paths.
    #[must_use]
    pub fn private_tools(&self) -> &[LinkedToolReference] {
        &self.private_tools
    }

    /// Returns the directly named `coral.shared` paths.
    #[must_use]
    pub fn shared_tools(&self) -> &[LinkedToolReference] {
        &self.shared_tools
    }

    /// Returns every prefix of a literal Tool chain under `coral` that could
    /// be a Tool Call Path, such as `coral.providers.github.repos.get`, so
    /// `get.call(...)` still names `get`. Aliased roots are not followed.
    #[must_use]
    pub fn tool_call_paths(&self) -> &[String] {
        &self.tool_call_paths
    }
}

/// The literal Tool chains under `coral` that one Authored Tool module names,
/// as its preparation found them. Aliased roots are not followed.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolReferences {
    chains: Vec<ToolChain>,
}

impl ToolReferences {
    /// The references `chains` make.
    #[must_use]
    pub const fn new(chains: Vec<ToolChain>) -> Self {
        Self { chains }
    }

    /// Returns each literal chain, as preparation found it.
    #[must_use]
    pub fn chains(&self) -> &[ToolChain] {
        &self.chains
    }

    /// Returns each Call Path a literal chain calls, such as
    /// `coral.providers.github.repos.get`, once each, in path order.
    #[must_use]
    pub fn called_paths(&self) -> Vec<String> {
        let mut called = self
            .chains
            .iter()
            .filter(|chain| chain.called)
            .map(|chain| format!("coral.{}", chain.segments.join(".")))
            .collect::<Vec<_>>();
        called.sort();
        called.dedup();
        called
    }

    /// Returns the literal start of each chain whose Tool path a computed
    /// segment builds at runtime, such as `coral.providers.github` for
    /// `coral.providers.github[name]()`, once each, in path order. The
    /// Tools such a chain calls are known only when it runs.
    #[must_use]
    pub fn runtime_built_paths(&self) -> Vec<String> {
        let mut built = self
            .chains
            .iter()
            .filter(|chain| chain.computed)
            .map(|chain| format!("coral.{}", chain.segments.join(".")))
            .collect::<Vec<_>>();
        built.sort();
        built.dedup();
        built
    }
}

/// One literal chain, relative to `coral`, whether it is called, and whether
/// it was cut before a computed segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ToolChain {
    segments: Vec<String>,
    called: bool,
    #[serde(default)]
    computed: bool,
}

impl ToolChain {
    /// The chain of `segments` under `coral`, whether it is `called`, and
    /// whether it was cut before a `computed` segment.
    #[must_use]
    pub const fn new(segments: Vec<String>, called: bool, computed: bool) -> Self {
        Self {
            segments,
            called,
            computed,
        }
    }

    /// Returns the segments relative to `coral`.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Returns whether the source calls the chain.
    #[must_use]
    pub const fn called(&self) -> bool {
        self.called
    }

    /// Returns whether the chain was cut before a computed segment.
    #[must_use]
    pub const fn computed(&self) -> bool {
        self.computed
    }
}

/// A called literal Tool chain under `coral` that names
/// no Tool the caller can call, and where the source names it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnknownToolCall {
    call_path: String,
    line: u64,
    column: u64,
}

impl UnknownToolCall {
    /// The unknown `call_path` at its authored `line` and `column`.
    #[must_use]
    pub const fn new(call_path: String, line: u64, column: u64) -> Self {
        Self {
            call_path,
            line,
            column,
        }
    }

    /// The complete chain as written, such as
    /// `coral.providers.notion.retrievePage`.
    #[must_use]
    pub fn call_path(&self) -> &str {
        &self.call_path
    }

    /// The one-based authored source line.
    #[must_use]
    pub const fn line(&self) -> u64 {
        self.line
    }

    /// The one-based authored source column.
    #[must_use]
    pub const fn column(&self) -> u64 {
        self.column
    }
}

/// One declaration that types a Call Path under `coral`: a Tool, a Private
/// Tool at `coral.private` or a Shared Tool at `coral.shared`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallableDeclaration {
    path_segments: Vec<String>,
    declaration: String,
}

impl CallableDeclaration {
    /// Builds one declaration for its path segments: relative to `coral` for
    /// a Tool, such as `providers.github.repos.get`, to `coral.private` for
    /// a Private Tool and to `coral.shared` for a Shared Tool.
    ///
    /// The declaration is self-contained TypeScript with one
    /// `export default` callable, as describe and an Authored Tool save emit it.
    #[must_use]
    pub const fn new(path_segments: Vec<String>, declaration: String) -> Self {
        Self {
            path_segments,
            declaration,
        }
    }

    /// Returns the path segments.
    #[must_use]
    pub fn path_segments(&self) -> &[String] {
        &self.path_segments
    }

    /// Returns the declaration.
    #[must_use]
    pub fn declaration(&self) -> &str {
        &self.declaration
    }
}

/// The Built-in Tools a Program calls directly on `coral`, by Tool Key, such
/// as `search` for `coral.search`. A Program is compiled with them, so it
/// calls exactly the ones it was admitted with, and an Authored Tool is typed
/// with them.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct BuiltinTools(Vec<String>);

impl BuiltinTools {
    /// The Built-in Tools with these keys.
    #[must_use]
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(keys.into_iter().map(Into::into).collect())
    }

    /// Returns the Tool Keys.
    #[must_use]
    pub fn keys(&self) -> &[String] {
        &self.0
    }

    /// The keys as the compiler reads them.
    pub(crate) fn json(&self) -> std::sync::Arc<str> {
        serde_json::Value::from(self.0.clone()).to_string().into()
    }
}

/// The declarations that type the `coral` namespace for one Authored Tool
/// save.
///
/// A root without declarations stays `any`. A typed root holds only the
/// supplied paths: any other path the source calls is a diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoralDeclarations {
    declaration_max_bytes: usize,
    tools: Option<Vec<CallableDeclaration>>,
    private_tools: Option<Vec<CallableDeclaration>>,
    shared_tools: Option<Vec<CallableDeclaration>>,
}

impl CoralDeclarations {
    /// Types nothing yet. A typed Private Tool declaration above
    /// `declaration_max_bytes` is saved untyped.
    #[must_use]
    pub const fn new(declaration_max_bytes: usize) -> Self {
        Self {
            declaration_max_bytes,
            tools: None,
            private_tools: None,
            shared_tools: None,
        }
    }

    /// Types `coral.providers` and the Built-in Tools on `coral` from the
    /// Tools the caller can call, such as those of the
    /// [`SourceReferences::tool_call_paths`] that the source names.
    #[must_use]
    pub fn with_tools(mut self, tools: Vec<CallableDeclaration>) -> Self {
        self.tools = Some(tools);
        self
    }

    /// Types `coral.private` from stored Private Tool declarations.
    #[must_use]
    pub fn with_private_tools(mut self, private_tools: Vec<CallableDeclaration>) -> Self {
        self.private_tools = Some(private_tools);
        self
    }

    /// Types `coral.shared` from the current Shared Tool declarations,
    /// each by its path relative to `coral.shared`.
    #[must_use]
    pub fn with_shared_tools(mut self, shared_tools: Vec<CallableDeclaration>) -> Self {
        self.shared_tools = Some(shared_tools);
        self
    }

    /// Returns the size above which a typed declaration is saved untyped.
    #[must_use]
    pub const fn declaration_max_bytes(&self) -> usize {
        self.declaration_max_bytes
    }

    /// Returns the declarations that type the Tools, if any do.
    #[must_use]
    pub fn tools(&self) -> Option<&[CallableDeclaration]> {
        self.tools.as_deref()
    }

    /// Returns the declarations that type `coral.private`, if any do.
    #[must_use]
    pub fn private_tools(&self) -> Option<&[CallableDeclaration]> {
        self.private_tools.as_deref()
    }

    /// Returns the declarations that type `coral.shared`, if any do.
    #[must_use]
    pub fn shared_tools(&self) -> Option<&[CallableDeclaration]> {
        self.shared_tools.as_deref()
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
    /// The diagnostic as the compiler reported it.
    #[must_use]
    pub const fn new(
        category: DiagnosticCategory,
        code: u64,
        message: String,
        line: Option<u64>,
        column: Option<u64>,
    ) -> Self {
        Self {
            category,
            code,
            message,
            line,
            column,
        }
    }

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
    /// The compiler had no room for the work. Nothing produces it since
    /// the queue lost its bound (#2961 removes it).
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
    /// Authored Tool source did not define one import-free default callable.
    /// The caller says which Tool's authoring failed.
    #[error("{0}")]
    InvalidAuthoredTool(String),
    /// Source nesting overflowed the compiler's stack.
    #[error("TypeScript source is nested too deeply to compile")]
    SourceTooDeeplyNested,
    /// Program or Authored Tool source requested a forbidden authoring capability.
    #[error("TypeScript source uses a forbidden capability: {0}")]
    ForbiddenCapability(String),
    /// Source did not directly name supported Private or Shared Tool paths.
    #[error("Tool reference analysis failed at {line}:{column}: {message}")]
    InvalidLinkedToolReference {
        /// Clear safe reason.
        message: String,
        /// One-based authored source line.
        line: u64,
        /// One-based authored source column.
        column: u64,
    },
    /// A literal `coral.addCustomTool` or `coral.updateCustomTool` call did not name a
    /// function its source declares at the top level. The message starts with the
    /// authored source and location.
    #[error("{0}")]
    InvalidPrivateToolSave(String),
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
    /// The Program used more CPU time than its host's budget.
    #[error("Program CPU budget exceeded")]
    CpuLimitExceeded,
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

impl Error {
    /// The [`Self::code`] of each error the source's author caused, which
    /// [`CompileFailure`] reports as [`CompileFailure::Rejected`]. This list
    /// decides that, so a peer that reads a rejection's code back checks it
    /// against the same set.
    pub const REJECTION_CODES: [&'static str; 8] = [
        "compilation",
        "invalid_program_body",
        "invalid_authored_tool",
        "source_too_deeply_nested",
        "forbidden_capability",
        "invalid_linked_tool_reference",
        "invalid_private_tool_save",
        "memory_limit_exceeded",
    ];
    /// The [`Self::code`] of each error a compile fails with for a reason
    /// the source did not cause. The other errors that
    /// [`CompileFailure::Failed`] carries come from running a Program.
    pub const COMPILER_FAILURE_CODES: [&'static str; 4] = [
        "initialization",
        "compiler_protocol",
        "invalid_source_map",
        "engine",
    ];

    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Initialization { .. } => "initialization",
            Self::CompilerThread { .. } => "compiler_thread",
            Self::CompilerUnavailable { .. } => "compiler_unavailable",
            Self::CompilerBusy { .. } => "compiler_busy",
            Self::CompilationTimedOut { .. } => "compilation_timed_out",
            Self::Compilation { .. } => "compilation",
            Self::InvalidProgramBody { .. } => "invalid_program_body",
            Self::InvalidAuthoredTool { .. } => "invalid_authored_tool",
            Self::SourceTooDeeplyNested { .. } => "source_too_deeply_nested",
            Self::ForbiddenCapability { .. } => "forbidden_capability",
            Self::InvalidLinkedToolReference { .. } => "invalid_linked_tool_reference",
            Self::InvalidPrivateToolSave { .. } => "invalid_private_tool_save",
            Self::CompilerProtocol { .. } => "compiler_protocol",
            Self::InvalidSourceMap { .. } => "invalid_source_map",
            Self::Engine { .. } => "engine",
            Self::ProgramRejected { .. } => "program_rejected",
            Self::ProgramOutputRejected { .. } => "program_output_rejected",
            Self::ResourceLimitExceeded { .. } => "resource_limit_exceeded",
            Self::MemoryLimitExceeded { .. } => "memory_limit_exceeded",
            Self::CpuLimitExceeded { .. } => "cpu_limit_exceeded",
            Self::InvalidIsolateLimits { .. } => "invalid_isolate_limits",
            Self::PendingProgram { .. } => "pending_program",
            Self::ProgramHostTerminated { .. } => "program_host_terminated",
            Self::InvalidProgramValue { .. } => "invalid_program_value",
        }
    }
}

/// Why a compile request returned no result.
///
/// Only [`Self::Rejected`] judges the source. The others say the compiler could
/// not answer, so they give the source's author nothing to fix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompileFailure {
    /// The source was rejected, with a safe bounded diagnostic for its author.
    Rejected(String),
    /// The compiler had no room for the request: its bounded queue is
    /// full, or a Run Executor's pull stream has spent its room.
    Busy,
    /// The compile deadline expired.
    TimedOut,
    /// The compiler worker stopped before it answered.
    Stopped,
    /// The compiler failed for a reason the source did not cause, with the
    /// failure's bounded kind and none of its private text.
    Failed(&'static str),
    /// No compiler could take the request within the wait for one.
    NoCompiler,
}

impl CompileFailure {
    /// What the caller reads when no compiler could take its request.
    pub const NO_COMPILER_MESSAGE: &str = "No program compiler is available. Try again later.";
}

impl From<Error> for CompileFailure {
    fn from(error: Error) -> Self {
        match error {
            Error::CompilerBusy => Self::Busy,
            Error::CompilationTimedOut => Self::TimedOut,
            Error::CompilerUnavailable | Error::CompilerThread(_) => Self::Stopped,
            authored if Error::REJECTION_CODES.contains(&authored.code()) => {
                Self::Rejected(format_compiler_error(authored))
            }
            other => Self::Failed(other.code()),
        }
    }
}

impl From<String> for CompileFailure {
    fn from(message: String) -> Self {
        Self::Rejected(bounded_diagnostic_message(&message))
    }
}

/// Formats safe compiler diagnostics with their authored line and column.
fn format_compiler_error(error: Error) -> String {
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
