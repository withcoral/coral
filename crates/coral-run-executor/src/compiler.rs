use std::time::{Duration, Instant};

use coral_v8::{
    AuthoredToolCompilation, BuiltinTools, CompileFailure, CompiledProgram, CoralDeclarations,
    LinkedToolSource, NamedCallPaths, ToolReferences, UnknownToolCall,
};

/// The calls Program Admission and Private Tool save make of one warm
/// Program Compiler, one for each compiler request.
///
/// Each call carries `time_left`, the time left until its caller's
/// compilation deadline. A call ends [`CompileError::TimedOut`] once that
/// time runs out, while it waits or while it compiles. A call
/// carries source and no authority.
pub trait CompilerClient: Send + Sync {
    /// The Private and Shared Tools and the Tools that a Program body names
    /// directly.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased access and malformed wrapper escapes.
    fn program_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<NamedCallPaths, CompileError>;

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

    /// The Private and Shared Tools and the Tools that one linked module
    /// names directly, for Program Admission to link and check.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased access and malformed module source.
    fn module_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<NamedCallPaths, CompileError>;

    /// A Program body lowered with the Private and Shared Tools it links, which
    /// initialize in the given order, and with `builtin_tools` on `coral`.
    ///
    /// # Errors
    ///
    /// Returns blocking diagnostics, invalid linked source, the memory limit,
    /// a timeout, or an unavailable compiler.
    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        private_tools: &[LinkedToolSource],
        builtin_tools: &BuiltinTools,
    ) -> Result<CompiledProgram, CompileError>;

    /// The Private and Shared Tools and the Tools that one Authored Tool
    /// module names directly, for authoring to type and review it.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased access and malformed module source.
    fn call_paths(&self, time_left: Duration, source: &str)
    -> Result<NamedCallPaths, CompileError>;

    /// One Authored Tool's declaration and advisory diagnostics, with
    /// `builtin_tools` on `coral` and typed by `declarations` when there are
    /// any.
    ///
    /// # Errors
    ///
    /// Returns a blocking authoring diagnostic, the memory limit, a timeout,
    /// or an unavailable compiler.
    fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        builtin_tools: &BuiltinTools,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<AuthoredToolCompilation, CompileError>;
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
    /// A `coral.private` access that names no supported Private Tool
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
    Message {
        /// The rejection's stable kind, for an event's `error.type`.
        kind: &'static str,
        /// The bounded message.
        message: String,
    },
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
            Self::Diagnostics(Diagnostics::Reference { .. }) => "invalid_linked_tool_reference",
            Self::MemoryLimit => "memory_limit_exceeded",
            Self::Unavailable(Unavailable::Busy) => "compiler_busy",
            Self::Unavailable(Unavailable::Stopped) => "compiler_unavailable",
            Self::Diagnostics(Diagnostics::Message { kind, .. })
            | Self::Unavailable(Unavailable::Failed(kind)) => kind,
        }
    }
}

/// The failure, code and message each caller reported before the client.
impl From<CompileError> for CompileFailure {
    fn from(error: CompileError) -> Self {
        match error {
            CompileError::TimedOut => Self::TimedOut,
            // Bound here: any adapter can build the public `Message` variant.
            CompileError::Diagnostics(Diagnostics::Message { message, .. }) => {
                Self::Rejected(coral_v8::bounded_diagnostic_message(&message))
            }
            CompileError::Diagnostics(Diagnostics::Reference {
                message,
                line,
                column,
            }) => coral_v8::Error::InvalidLinkedToolReference {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_from_any_adapter_keeps_the_diagnostic_bound() {
        let message = "x".repeat(8 * 1024);
        let error = CompileError::Diagnostics(Diagnostics::Message {
            kind: "compilation",
            message: message.clone(),
        });
        assert_eq!(
            CompileFailure::from(error),
            CompileFailure::Rejected(coral_v8::bounded_diagnostic_message(&message))
        );
    }
}
