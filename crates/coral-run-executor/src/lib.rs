//! The Run Executor link, from both of its sides.
//!
//! On Coral's side it holds the compiler client that Program Admission and
//! Private Tool save share. [`CompilerClient`] is the one interface.
//! [`PullStreamCompiler`] sends each call down the held pull stream with the
//! most room, to a Run Executor; when none has room, it relays the call
//! through the database to any Coral Instance with room.
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
mod relay;
mod relay_repository;
mod run;
mod served;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod time;

pub use compile_wire::ProtocolError;
pub use compile_wire::package_sha256;
pub use compiler::{
    AnalysisError, CompileError, CompilerClient, Diagnostics, Unavailable, time_left,
    tool_check_deadline,
};
pub use coral_v8::{BuiltinTools, IsolateLimits};
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
pub use run::{APPLIED_CAP_BYTES, HELD_CAP_REQUESTS, PROGRAM_LOG_MESSAGE_MAX_BYTES, RunLimits};
pub use served::{Closing, RunStreamServer, SHUTTING_DOWN, ServedRunStream};

/// The Run Executor rows the Run sweeper reads, holds and deletes. A read
/// runs on a database or a transaction, and holds nothing: [`leases::hold`]
/// and [`leases::delete`] join the caller's transaction and act only at the
/// lease a read returned.
pub mod leases {
    pub use crate::lease_repository::{RunExecutorLease, delete, find, hold, lapsed, local};
}

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

    use coral_telemetry::metric::OutputKind as MetricKind;
    use v1::OutputKind;

    /// The kind a Run Executor reports; `Err` for a Tool result, which Coral
    /// counts itself.
    impl TryFrom<MetricKind> for OutputKind {
        type Error = MetricKind;

        fn try_from(kind: MetricKind) -> Result<Self, MetricKind> {
            match kind {
                MetricKind::Emission => Ok(Self::Emission),
                MetricKind::Log => Ok(Self::Log),
                MetricKind::FinalValue => Ok(Self::FinalValue),
                MetricKind::Result => Err(kind),
            }
        }
    }

    /// The kind Coral's metrics count; `Err` when it breaks the protocol.
    impl TryFrom<OutputKind> for MetricKind {
        type Error = OutputKind;

        fn try_from(kind: OutputKind) -> Result<Self, OutputKind> {
            match kind {
                OutputKind::Emission => Ok(Self::Emission),
                OutputKind::Log => Ok(Self::Log),
                OutputKind::FinalValue => Ok(Self::FinalValue),
                OutputKind::Unspecified => Err(kind),
            }
        }
    }
}
