use std::sync::Arc;
use std::time::{Duration, Instant};

use coral_v8::{
    AnalyzedSource, AuthoredToolCompilation, BuiltinTools, CompileFailure, CompiledProgram,
    CoralDeclarations, LinkedToolSource, SourceReferences, UnknownToolCall,
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

/// The calls Program Admission and Private Tool save make of one warm
/// Program Compiler.
///
/// Admission analyzes the Program, then each level of the graph of Private
/// and Shared Tools it links, then compiles. An Authored Tool save or publish
/// analyzes the module, then prepares it. Each call carries `time_left`, the
/// time left until its caller's compilation deadline. A call ends
/// [`CompileError::TimedOut`] once that time runs out while it compiles. A
/// call that no compiler took within it fails as [`Unavailable::NoCompiler`]
/// from a pull stream, and as [`CompileError::TimedOut`] from the queue of a
/// compiler in this process. A call carries source and no authority. Each
/// call is async, so its caller holds no async worker while it waits for
/// room or for its answer.
#[tonic::async_trait]
pub trait CompilerClient: Send + Sync {
    /// Waits until a compiler can take a call, for at most the client's
    /// own bound. A caller starts its compilation deadline after it, so the
    /// wait for a compiler takes none of the compile's time.
    ///
    /// `Some` is a client that holds one unit of room for this caller: its
    /// first call spends it, so no other caller takes it meanwhile, and
    /// dropped before any call, it frees the unit. The caller makes its
    /// calls through it. `None`: this client is ready as
    /// it is, as a compiler in this process always is.
    ///
    /// # Errors
    ///
    /// Returns [`Unavailable::NoCompiler`] when none can take a call by the
    /// bound.
    async fn ready(&self) -> Result<Option<Arc<dyn CompilerClient>>, CompileError> {
        Ok(None)
    }

    /// The Private and Shared Tools and the Tool Call Paths each source names
    /// directly, in the order of `sources`.
    ///
    /// # Errors
    ///
    /// The sources are analyzed in order, and the first that fails stops the
    /// call. Its error rejects computed or aliased `coral.private` or
    /// `coral.shared` access, a Program body with malformed wrapper escapes,
    /// or an Authored Tool module with syntax errors, or it is a timeout or
    /// an unavailable compiler.
    async fn analyze(
        &self,
        time_left: Duration,
        sources: &[AnalyzedSource],
    ) -> Result<Vec<SourceReferences>, AnalysisError>;

    /// A Program body lowered with the Private and Shared Tools it links, which
    /// initialize in the given order, and with `builtin_tools` on `coral`.
    ///
    /// With `callable`, the complete Call Paths the caller can call, the
    /// called literal Tool chains of the Program and then of each linked
    /// Tool are checked first, by the rule of the unknown-Tool diagnostic.
    /// A stored source is checked as the Program is, so an old root fails
    /// at admission, not when called. The check fails open: when it fails,
    /// or runs past [`tool_check_deadline`], the Program compiles unchecked.
    ///
    /// # Errors
    ///
    /// Returns the called chains that name no callable Tool, blocking
    /// diagnostics, invalid linked source, the memory limit, a timeout, or
    /// an unavailable compiler.
    async fn compile(
        &self,
        time_left: Duration,
        source: &str,
        linked_tools: &[LinkedToolSource],
        builtin_tools: &BuiltinTools,
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError>;

    /// One Authored Tool's declaration and advisory diagnostics, with
    /// `builtin_tools` on `coral` and typed by `declarations` when there are
    /// any.
    ///
    /// # Errors
    ///
    /// Returns a blocking authoring diagnostic, the memory limit, a timeout,
    /// or an unavailable compiler.
    async fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        builtin_tools: &BuiltinTools,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<AuthoredToolCompilation, CompileError>;
}

/// A call whose compiler stopped before it answered.
pub(crate) const STOPPED: CompileError = CompileError::Unavailable(Unavailable::Stopped);

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
    /// The call's time ran out while it compiled, or while it waited in the
    /// queue of a compiler in this process.
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
    /// Called literal Tool chains of one source that name no Tool the
    /// caller can call, in source order.
    UnknownTools {
        /// The source: the index of the linked Tool among those compiled
        /// with the Program, or `None` for the Program body.
        linked: Option<usize>,
        /// The calls.
        calls: Vec<UnknownToolCall>,
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
    /// The compiler had no room for the call: its queue is full, or a
    /// [`crate::PullStreamCompiler`]'s stream has spent its room.
    Busy,
    /// The compiler stopped before it answered.
    Stopped,
    /// The compiler failed for a reason the source did not cause, with the
    /// failure's bounded kind and none of its private text.
    Failed(&'static str),
    /// No compiler could take the call within the wait for one.
    NoCompiler,
}

impl CompileError {
    /// The error's stable kind, for an event's `error.type`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::TimedOut => "compilation_timed_out",
            Self::Diagnostics(Diagnostics::Reference { .. }) => "invalid_linked_tool_reference",
            Self::Diagnostics(Diagnostics::UnknownTools { .. }) => "unknown_tool_calls",
            Self::MemoryLimit => "memory_limit_exceeded",
            Self::Unavailable(Unavailable::Busy) => "compiler_busy",
            Self::Unavailable(Unavailable::Stopped | Unavailable::NoCompiler) => {
                "compiler_unavailable"
            }
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
            // Program Admission writes this failure itself, naming each call.
            CompileError::Diagnostics(Diagnostics::UnknownTools { .. }) => {
                Self::Rejected("Program calls Tools that you cannot call".to_string())
            }
            CompileError::MemoryLimit => coral_v8::Error::MemoryLimitExceeded.into(),
            CompileError::Unavailable(Unavailable::Busy) => Self::Busy,
            CompileError::Unavailable(Unavailable::Stopped) => Self::Stopped,
            CompileError::Unavailable(Unavailable::Failed(kind)) => Self::Failed(kind),
            CompileError::Unavailable(Unavailable::NoCompiler) => Self::NoCompiler,
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
