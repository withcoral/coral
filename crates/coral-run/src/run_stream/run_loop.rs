//! The loop of one Run stream (ADR 0027): one in-process actor per Run
//! stream this Coral Instance serves. It is one of the Run's three writers:
//! the Coral Instance that receives an outside request (a decision on an
//! Approval Request, a cancellation) commits it directly, and the sweeper
//! writes what no stream decided in time. Each writes under the Run's claim
//! and head, so the first commit wins.
//!
//! The loop is [`state::apply`], a pure function from the loop's memory and
//! one [`handle::Message`] to [`state::Effect`]s, with no I/O: it applies the
//! stream's requests in number order, decides the Run Output Budget against
//! the exact in-memory total, the Applied cap, how Events and row work fill
//! chunks, the timers it arms (the Run deadline, each Approval Request's and
//! connection request's deadline), which parked Invocations to wake and when
//! to read their waits, and what a lost commit means. The hosting task in
//! [`host`] drains the mailbox in arrival order, calls `apply`, and runs the
//! effects: it holds the chunk commit, the terminal write, the timers and
//! the snapshot checks of the parked waits, one write at a time. A timer fires back into the mailbox, so a
//! passed deadline is applied in order with the request that races it.
//!
//! A parked Invocation holds nothing while it waits: the loop keeps it and
//! wakes it when its wait ends, back to its held recheck. Another writer's
//! commit moves the Run's head, which the Coral Instance's wake-up reader
//! sees and hands the loop as [`handle::Message::Woken`]. A periodic
//! snapshot precheck of every parked wait against the Policy Enforcement
//! Point's facts finds the waits that ended or became moot without a
//! wake-up. A wake-up and a precheck are hints; the held recheck and the
//! enforcement point decide.
//!
//! Row work that must join a chunk (an Invocation's admission, its held
//! rechecks, the Policy Enforcement Point, its completion) implements
//! [`ChunkWork`]: it runs inside the commit transaction and learns the
//! chunk's fate once the transaction ends. A chunk commit is one short
//! transaction: the Run row read without a hold, the row work, the Events,
//! and the head update that asserts the loop's claim and the head it tracks
//! in memory and stamps the commit time.
//!
//! Two writes of a Run stream go outside the loop's chunks (`completion`):
//! the timeout of a Run whose deadline passed before its Program started,
//! which the stream writes before any loop exists, and the terminal write
//! once the Program has ended.

mod chunk;
mod completion;
mod handle;
mod host;
mod state;

pub(crate) use chunk::ChunkWork;
#[cfg(test)]
pub(crate) use chunk::{CHUNK_WORK_CAP, ChunkBuildView, ENFORCEMENT_BATCH_CAP};
pub(crate) use handle::{
    Check, DecisionRequest, DrainStep, LoopHandle, LoopRegistration, Loops, Message, ParkedWait,
    RequestTicket, StartedExecution,
};
#[cfg(test)]
pub(crate) use handle::{CommitRejection, Reply, Timer};
pub(crate) use host::host;
pub(crate) use state::NO_DEADLINE;
#[cfg(test)]
pub(crate) use state::{Effect, LoopLimits, LoopState, Phase, apply};

impl crate::RunModule {
    /// Registers a Run stream's loop, which detaches the one it replaces
    /// ([`Loops::register_stream`]), and starts the wake-up reader of this
    /// Coral Instance's loops.
    pub(crate) fn register_loop(&self, handle: &LoopHandle) -> LoopRegistration {
        let registration = self.loops.register_stream(handle.clone());
        self.wakes.loop_registered(
            &self.loops,
            #[cfg(test)]
            &self.execution_observer,
        );
        registration
    }
}
