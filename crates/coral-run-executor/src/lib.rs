//! The Run Executor link, from both of its sides.
//!
//! On Coral's side it holds the compiler client that Program Admission and
//! Private Tool save share. [`CompilerClient`] is the one interface.
//! [`PullStreamCompiler`] sends each call down the held pull stream with the
//! most room, to a Run Executor, and waits for one when none has room.
//! [`InProcessCompiler`] runs one warm Program Compiler on its own thread in
//! this process, for tests.
//!
//! On the Run Executor's side, [`run`] warms its compiler, dials Coral, holds
//! the pull stream that the [`wire`] contract defines, and serves the
//! compiles that ride it, and one Run stream for each Run Coral takes for it.
//! Each Run's Program runs on a thread of its own. On Coral's side,
//! [`LinkServer`] listens for Run Executors, admits them by [`Credentials`],
//! and keeps the pull streams it holds in [`PullStreams`].
//! [`LocalRunExecutors`] starts the Run Executors that run as Coral's own
//! child processes.

mod compile_wire;
mod compiler;
mod in_process;
mod lease_repository;
mod link;
mod local;
mod pull;
mod pull_compiler;
mod run;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use compile_wire::ProtocolError;
pub use compiler::{
    AnalysisError, CompileError, CompilerClient, Diagnostics, Unavailable, time_left,
    tool_check_deadline,
};
pub use coral_v8::BuiltinTools;
pub use in_process::InProcessCompiler;
pub use link::{
    AddressError, CoralAddress, MESSAGE_MAX_BYTES, PROTOCOL_LEVEL, RunError, RunExecutorConfig,
    Token, TokenError, run,
};
pub use local::{ChildCommand, LocalChild, LocalRunExecutors, ProcessStarter};
pub use pull::{
    Credentials, HeldStream, Kind, LinkServer, LocalCredential, PullStreams, Refused, StreamId,
};
pub use pull_compiler::PullStreamCompiler;
pub use run::{APPLIED_CAP_BYTES, PROGRAM_LOG_MESSAGE_MAX_BYTES, RunLimits};

/// The link's wire contract, generated from `proto/`.
#[allow(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    reason = "tonic-generated code uses allow attributes"
)]
pub mod wire {
    /// Version one.
    #[allow(
        missing_docs,
        clippy::all,
        clippy::pedantic,
        reason = "protoc-generated wire bindings"
    )]
    pub mod v1 {
        include!("generated/coral.run_executor.v1.rs");
    }
}
