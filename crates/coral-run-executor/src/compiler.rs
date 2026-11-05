use std::time::{Duration, Instant};

use coral_v8::{
    CompileFailure, CompiledProgram, CoralDeclarations, LinkedSavedFunctionSource,
    SavedFunctionCallPaths, SavedFunctionCompilation, SavedFunctionReference, ToolReferences,
    UnknownToolCall,
};

/// The calls Program Admission and Saved Function save make of one warm
/// Program Compiler, one for each compiler request.
///
/// Each call carries `time_left`, the time left in its caller's
/// [`coral_v8::COMPILATION_TIMEOUT`]. A call ends [`CompileError::TimedOut`]
/// once that time runs out, while it waits or while it compiles. A call
/// carries source and no authority.
pub trait CompilerClient: Send + Sync {
    /// The Saved Functions and Tools that a Program body names directly.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased `coral.functions` access and malformed
    /// wrapper escapes.
    fn program_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError>;

    /// The called chains of `tools` that name none of the `callable` Call
    /// Paths.
    ///
    /// # Errors
    ///
    /// Returns a timeout or an unavailable compiler.
    fn unknown_tool_calls(
        &self,
        time_left: Duration,
        tools: &ToolReferences,
        callable: &[String],
    ) -> Result<Vec<UnknownToolCall>, CompileError>;

    /// The Saved Functions that one Saved Function module names directly.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased access and malformed module source.
    fn dependency_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileError>;

    /// A Program body lowered with the Saved Functions it links, which
    /// initialize in the given order.
    ///
    /// # Errors
    ///
    /// Returns blocking diagnostics, invalid linked source, the memory limit,
    /// a timeout, or an unavailable compiler.
    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
    ) -> Result<CompiledProgram, CompileError>;

    /// The Saved Functions and Tools that one Saved Function module names
    /// directly, to type it.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased `coral.functions` access and malformed
    /// module source.
    fn call_paths(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError>;

    /// One Saved Function's declaration and advisory diagnostics, typed by
    /// `declarations` when there are any.
    ///
    /// # Errors
    ///
    /// Returns a blocking authoring diagnostic, the memory limit, a timeout,
    /// or an unavailable compiler.
    fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<SavedFunctionCompilation, CompileError>;
}

/// The time left until `deadline`, which a call carries.
#[must_use]
pub fn time_left(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Why a compiler call returned no result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompileError {
    /// The call's time ran out while it waited or compiled.
    TimedOut,
    /// The compiler rejected the source, with safe diagnostics for its author.
    Diagnostics(Diagnostics),
    /// Compiling the source passed the compiler's heap limit.
    MemoryLimit,
    /// No compiler answered the call.
    Unavailable(Unavailable),
}

/// Safe diagnostics for a rejected source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Diagnostics {
    /// A `coral.functions` access that names no supported Saved Function
    /// path, at its authored location.
    Reference {
        /// The safe reason.
        message: String,
        /// One-based authored source line.
        line: u64,
        /// One-based authored source column.
        column: u64,
    },
    /// Any other rejection, as one bounded message.
    Message(String),
}

/// Why no compiler answered a call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unavailable {
    /// The compiler's queue had no room.
    Busy,
    /// The compiler stopped before it answered.
    Stopped,
    /// The compiler failed for a reason the source did not cause, with the
    /// failure's bounded kind and none of its private text.
    Failed(&'static str),
}

impl CompileError {
    /// The error's stable kind, for an event's `error.type`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::TimedOut => "compilation_timed_out",
            Self::Diagnostics(_) => "diagnostics",
            Self::MemoryLimit => "memory_limit_exceeded",
            Self::Unavailable(Unavailable::Busy) => "compiler_busy",
            Self::Unavailable(Unavailable::Stopped) => "compiler_unavailable",
            Self::Unavailable(Unavailable::Failed(kind)) => kind,
        }
    }
}

/// The failure, code and message each caller reported before the client.
impl From<CompileError> for CompileFailure {
    fn from(error: CompileError) -> Self {
        match error {
            CompileError::TimedOut => Self::TimedOut,
            CompileError::Diagnostics(Diagnostics::Message(message)) => Self::Rejected(message),
            CompileError::Diagnostics(Diagnostics::Reference {
                message,
                line,
                column,
            }) => coral_v8::Error::InvalidSavedFunctionReference {
                message,
                line,
                column,
            }
            .into(),
            CompileError::MemoryLimit => coral_v8::Error::MemoryLimitExceeded.into(),
            CompileError::Unavailable(Unavailable::Busy) => Self::Busy,
            CompileError::Unavailable(Unavailable::Stopped) => Self::Stopped,
            CompileError::Unavailable(Unavailable::Failed(kind)) => Self::Failed(kind),
        }
    }
}
