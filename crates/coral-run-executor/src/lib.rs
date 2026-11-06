//! The Run Executor link, from both of its sides.
//!
//! On Coral's side it holds the compiler client that Program Admission and
//! Saved Function save share. [`CompilerClient`] is the one interface.
//! [`InProcessCompiler`] is its first adapter: one warm Program Compiler on
//! its own thread in this process. Bootstrap builds one and gives it to both
//! callers.
//!
//! On the Run Executor's side, [`run`] dials Coral and holds the pull stream
//! that the [`wire`] contract defines, and one Run stream for each Run Coral
//! takes for it. Each Run's Program runs on a thread of its own.

mod compiler;
mod in_process;
mod link;
mod run;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use compiler::{CompileError, CompilerClient, Diagnostics, Unavailable, time_left};
pub use in_process::InProcessCompiler;
pub use link::{
    AddressError, CoralAddress, PROTOCOL_LEVEL, RunExecutorConfig, Token, TokenError, run,
};
pub use run::{APPLIED_CAP_BYTES, PROGRAM_LOG_MESSAGE_MAX_BYTES, RunLimits, package_digest};

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
