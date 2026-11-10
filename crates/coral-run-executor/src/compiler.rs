use std::time::{Duration, Instant};

use coral_v8::{
    AnalyzedSource, CompileFailure, CompiledProgram, CoralDeclarations, LinkedSavedFunctionSource,
    SavedFunctionCompilation, SourceReferences, UnknownToolCall,
};

/// The compilation time the Tool check always leaves, so the check never
/// makes a Program time out that would have compiled without it.
const TOOL_CHECK_COMPILATION_RESERVE: Duration = Duration::from_secs(1);

/// The longest each part of the Tool check may take: reading which
/// candidate Call Paths the caller can call, and checking the Program's
/// calls against them in the compiler.
const TOOL_CHECK_BUDGET: Duration = Duration::from_millis(500);

/// The latest one part of the Tool check for a compile due by
/// `compilation_deadline` may run, starting now, or `None` when no time is
/// left for it. A part that runs past it leaves the calls to be checked when
/// called.
#[must_use]
pub fn tool_check_deadline(compilation_deadline: Instant) -> Option<Instant> {
    let now = Instant::now();
    compilation_deadline
        .checked_sub(TOOL_CHECK_COMPILATION_RESERVE)
        .map(|latest| latest.min(now + TOOL_CHECK_BUDGET))
        .filter(|deadline| *deadline > now)
}

/// The calls Program Admission and Saved Function save make of one warm
/// Program Compiler.
///
/// Admission analyzes the Program, then each level of the Saved Function
/// graph it links, then compiles. A save analyzes the module, then prepares
/// it. Each call carries `time_left`, the time left until its caller's
/// compilation deadline. A call ends [`CompileError::TimedOut`] once that
/// time runs out, while it waits or while it compiles. A call carries source
/// and no authority.
pub trait CompilerClient: Send + Sync {
    /// The Saved Functions and Tool Call Paths each source names directly,
    /// in the order of `sources`.
    ///
    /// # Errors
    ///
    /// The sources are analyzed in order, and the first that fails stops the
    /// call. Its error rejects computed or aliased `coral.functions` access,
    /// a Program body with malformed wrapper escapes, or a Saved Function
    /// module with syntax errors, or it is a timeout or an unavailable
    /// compiler.
    fn analyze(
        &self,
        time_left: Duration,
        sources: &[AnalyzedSource],
    ) -> Result<Vec<SourceReferences>, AnalysisError>;

    /// A Program body lowered with the Saved Functions it links, which
    /// initialize in the given order.
    ///
    /// With `callable`, the complete Call Paths the caller can call, the
    /// Program's called literal Tool chains are checked first, by the rule
    /// of the unknown-Tool diagnostic. The check fails open: when it fails,
    /// or runs past [`tool_check_deadline`], the Program compiles unchecked.
    ///
    /// # Errors
    ///
    /// Returns the called chains that name no callable Tool, blocking
    /// diagnostics, invalid linked source, the memory limit, a timeout, or
    /// an unavailable compiler.
    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError>;

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

/// Why [`CompilerClient::analyze`] stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnalysisError {
    /// The index in `sources` of the source it stopped at.
    pub index: usize,
    /// Why.
    pub error: CompileError,
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
    /// Called literal Tool chains that name no Tool the caller can call,
    /// in source order.
    UnknownTools(Vec<UnknownToolCall>),
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
    /// The compiler had no room for the call: a [`crate::PullStreamCompiler`]
    /// whose stream's room is spent.
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
            Self::Diagnostics(Diagnostics::Reference { .. }) => "invalid_saved_function_reference",
            Self::Diagnostics(Diagnostics::UnknownTools(_)) => "unknown_tool_calls",
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
            }) => coral_v8::Error::InvalidSavedFunctionReference {
                message,
                line,
                column,
            }
            .into(),
            // Program Admission writes this failure itself, naming each call.
            CompileError::Diagnostics(Diagnostics::UnknownTools(_)) => {
                Self::Rejected("Program calls Tools that you cannot call".to_string())
            }
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

    // Catches: a Tool check that outlives its budget, or eats into the
    // compilation time it must leave.
    #[test]
    fn a_tool_check_keeps_its_budget_and_leaves_the_compilation_reserve() {
        let started = Instant::now();
        let far = started + Duration::from_secs(10);
        let checked = tool_check_deadline(far).expect("time left to check");
        assert!(checked > started && checked <= Instant::now() + TOOL_CHECK_BUDGET);

        // Anchored just before the call: the reserve, not the budget, ends this
        // check, unless the call starts 400 ms late.
        let near = Instant::now() + TOOL_CHECK_COMPILATION_RESERVE + Duration::from_millis(400);
        assert_eq!(
            tool_check_deadline(near),
            near.checked_sub(TOOL_CHECK_COMPILATION_RESERVE)
        );

        let inside_reserve = Instant::now() + TOOL_CHECK_COMPILATION_RESERVE / 2;
        assert_eq!(tool_check_deadline(inside_reserve), None);
    }

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
