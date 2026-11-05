//! Coral's side of the Run Executor link (ADR 0027). It holds the compiler
//! client that Program Admission and Saved Function save share.
//!
//! [`CompilerClient`] is the one interface. [`InProcessCompiler`] is its first
//! adapter: one warm Program Compiler on its own thread in this process.
//! Bootstrap builds one and gives it to both callers.

mod compiler;
mod in_process;

pub use compiler::{CompileError, CompilerClient, Diagnostics, Unavailable, time_left};
pub use in_process::InProcessCompiler;
