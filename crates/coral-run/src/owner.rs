//! The Run Owner (ADR 0018): one in-process actor per nonterminal Run, created
//! with the Run's acceptance and the only writer of its Run Ledger on every
//! path the Program takes.
//!
//! The loop is [`state::apply`], a pure function from the owner's memory and
//! one [`handle::Message`] to [`state::Effect`]s, with no I/O: it decides the
//! Run Output Budget against the exact in-memory total, the Applied cap, how
//! Events and row work fill chunks, and what a lost commit means. The hosting
//! task in [`host`] drains the mailbox in arrival order, calls `apply`, and
//! runs the effects: it holds the chunk commit, the Program execution and the
//! terminal write, and settles a recorded termination when the owner retires.
//!
//! Row work that must join a chunk (an Invocation's admission, its held
//! rechecks, the Dispatch Fence, its completion) implements [`ChunkWork`]: it
//! runs inside the commit transaction after the Run row is held, and learns
//! the chunk's fate once the transaction ends. A chunk commit is one short
//! transaction: `hold_run`, the row work, the Events, the head update that
//! asserts the owner's claim and renews its lease.

mod chunk;
mod handle;
mod host;
mod state;

pub(crate) use chunk::ChunkWork;
#[cfg(test)]
pub(crate) use chunk::{CHUNK_WORK_CAP, FENCE_BATCH_CAP};
#[cfg(test)]
pub(crate) use handle::{CommitRejection, Message, Reply};
pub(crate) use handle::{OwnerHandle, Owners};
pub(crate) use host::host;
#[cfg(test)]
pub(crate) use state::{Effect, OwnerLimits, OwnerState, Phase, apply};
