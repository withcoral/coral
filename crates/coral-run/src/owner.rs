//! The Run Owner (ADR 0018): one in-process actor per nonterminal Run, created
//! with the Run's acceptance and the only writer of its Run Ledger while the
//! Run is nonterminal.
//!
//! The loop is [`state::apply`], a pure function from the owner's memory and
//! one [`handle::Message`] to [`state::Effect`]s, with no I/O: it decides the
//! Run Output Budget against the exact in-memory total, the Applied cap, how
//! Events and row work fill chunks, the outside requests (a decision on an
//! Approval Request, a cancellation), the timers it arms (the Run deadline,
//! each Approval Request's and connection request's deadline, the idle tick,
//! the shutdown grace), which parked Invocations to wake and when to read
//! their waits, and what a lost commit means. The hosting task in [`host`]
//! drains the mailbox in arrival order, calls `apply`, and runs the effects:
//! it holds the chunk commit, the Program execution, the terminal write, the
//! settle of a recorded termination, the timers, the change subscription and
//! the snapshot checks of the parked waits, one write at a time. A timer
//! fires back into the mailbox, so a passed deadline is applied in order with
//! the cancel or the decision that races it.
//!
//! A parked Invocation holds nothing while it waits: the owner keeps it and
//! wakes it when its wait ends, back to its held recheck. A decision or an
//! expiry the owner records itself wakes the Invocation it ends. A connection
//! request ends outside the owner, so while one is waited on the owner
//! subscribes to the content-free change signal and, per coalesced ring and
//! at most once per wake gap, reads just its pending connection requests in a
//! snapshot. A periodic snapshot precheck of every parked wait against the
//! Dispatch Fence's facts finds the waits that ended or became moot without
//! a wake-up. A wake-up and a precheck are hints; the held recheck and the
//! fence decide.
//!
//! Row work that must join a chunk (an Invocation's admission, its held
//! rechecks, the Dispatch Fence, its completion, a decision) implements
//! [`ChunkWork`]: it runs inside the commit transaction and learns the
//! chunk's fate once the transaction ends. A chunk commit is one short
//! transaction: the Run row read without a hold, the row work, the Events,
//! the head update that asserts the owner's claim and the head it tracks in
//! memory, carries the termination intent, and renews its lease.
//!
//! An outside request the record does not answer already (a cancellation
//! with no termination recorded, a decision on a pending Approval Request)
//! reaches the owner through the Run Inbox, whose reader hands it over as
//! [`Message::Cancel`] or [`Message::Decide`] with the row that carried it.

mod chunk;
mod handle;
mod host;
mod state;

pub(crate) use chunk::ChunkWork;
#[cfg(test)]
pub(crate) use chunk::{CHUNK_WORK_CAP, ChunkBuildView, FENCE_BATCH_CAP};
pub(crate) use handle::{
    Check, DecisionRequest, Message, OwnerHandle, Owners, ParkedWait, StartedExecution, Timer,
};
#[cfg(test)]
pub(crate) use handle::{CommitRejection, Reply};
pub(crate) use host::{OwnerStart, host};
#[cfg(test)]
pub(crate) use state::{Effect, OwnerLimits, OwnerState, Phase, apply};
