//! The Run Owner's loop as a pure function of its state and one message.
//!
//! [`apply`] does no I/O; it returns [`Effect`]s for the hosting task to run.
//! It decides, in memory:
//!
//! - the Run Output Budget, against the owner's exact total;
//! - the Applied cap on unrecorded output;
//! - how Events and row work fill chunks, and when a chunk commits;
//! - which ready Invocations one chunk's Dispatch Fence decides together,
//!   and that a stop answers the ones not yet committed at once;
//! - the Fan-out Limit: a ready Invocation above the owner's count of
//!   dispatching ones waits in memory, in readiness order, for an earlier
//!   one to end;
//! - the outside requests, keyed by the entity they decide: the first rides
//!   the next chunk, a repeat waits for its answer or reads it from memory;
//! - the termination intent (a cancellation, the deadline): applied at
//!   once, recorded by the next chunk's head update;
//! - the timers: the Run deadline, each Approval Request's and connection
//!   request's deadline, the idle tick and the shutdown grace are armed as
//!   effects and fire back into the mailbox, so a passed deadline is applied
//!   in order with the cancel or the decision that races it, and the first
//!   applied decides. A passed Run deadline times the Run out; a passed
//!   Approval deadline expires the request and ends its Invocation; a
//!   passed connection deadline wakes the Invocations parked on it;
//! - the parked Invocations: which to wake (a decision or an expiry the
//!   owner recorded, a connection deadline, what a snapshot check found),
//!   when to hold the change subscription (while a connection wait is
//!   parked), when to read the connection requests after the signal rang
//!   (coalesced, at most once per wake gap), and when the periodic precheck
//!   reads every parked wait;
//! - the Run Ledger head, which the owner tracks from its start mark and
//!   every chunk commit asserts;
//! - what a lost commit means;
//! - the owner's liveness. After one lease without a successful commit the
//!   owner is suspect and answers Program requests only once recorded.
//!   After three it gives its Run up: the Program ends, and one terminal
//!   chunk is written as soon as the database answers, recording
//!   `Interrupted`, or `TimedOut` when the Run deadline has passed meanwhile;
//! - a recovered owner's one chunk: the expired-lease sweep recreates the
//!   owner of a Run whose process is gone with its mailbox queued (every
//!   Approval Request timer that passed, in deadline order, then the end),
//!   and the owner
//!   records the expiries, the timeout or the interruption, or the
//!   termination already recorded, in one chunk, then retires. No outside
//!   request reaches it: until the chunk lands a request for the Run is
//!   answered `OwnedElsewhere`, and a retry reads the record.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use coral_workspace::Principal;

use crate::approval::{ApprovalId, DecideWork};
use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::invocation::{FenceBatch, FenceRequest, ParkedInvocation, PrecheckBatch};
use crate::lifecycle::{RequestedTermination, RunTerminationKind};
use crate::run_repository::{OwnedEvent, OwnedEventKind};
use crate::termination::ShutdownAction;
use crate::{CoralFailure, Error};

use super::chunk::{ChunkBuild, ChunkItem, ChunkWork, PendingRequest};
use super::handle::{
    Check, CommitRejection, DecisionAnswer, DecisionRequest, Message, ParkedWait, Reply,
    RequestKey, RequestReply, StartedExecution, Timer,
};

/// What the hosting task does for the owner.
#[derive(Debug)]
pub(crate) enum Effect {
    /// Answer one Program request.
    Reply(Reply, Result<(), Error>),
    /// Answer one outside request: with what its chunk recorded for it, or
    /// with its loss.
    Answer(RequestReply, Result<DecisionAnswer, Error>),
    /// Commit the owner's first chunk: the execution-start mark.
    Start,
    /// Execute the accepted package.
    StartProgram(ProgramPackage),
    /// Commit one chunk, built on the head it carries; `output_total` is the
    /// Run Output Budget total it records.
    Commit {
        chunk: ChunkBuild,
        output_total: usize,
    },
    /// Terminate the Program and wake every stop-aware wait.
    TerminateProgram,
    /// Arm `timer` to fire as [`Message::Timer`] into the mailbox after
    /// `after_nanos`, replacing the same timer armed before.
    ArmTimer { timer: Timer, after_nanos: i64 },
    /// Send one parked Invocation back to its held recheck: its wait ended,
    /// or a snapshot says it did.
    Wake(Box<ParkedInvocation>),
    /// Subscribe to the change signal, which rings as [`Message::Changed`]:
    /// a connection wait is parked.
    Subscribe,
    /// Drop the change subscription: no connection wait is parked.
    Unsubscribe,
    /// Read the batch's parked waits in one snapshot, holding nothing; the
    /// findings come back as [`Message::Prechecked`].
    Precheck(PrecheckBatch),
    /// Settle row work the owner will never commit.
    Abandon(Box<dyn ChunkWork>, Error),
    /// Write the Run's terminal state for the Program's completion, with
    /// the termination intent the owner applied but no chunk recorded.
    CompleteExecution {
        completion: ExecutionCompletion,
        intent: Option<RequestedTermination>,
    },
    /// One attempt to settle the recorded termination: end every nonterminal
    /// Invocation and the Run once no dispatch is in flight, or once the
    /// shutdown grace is out. `again` means the previous attempt found a
    /// dispatch in flight: the host waits a moment before this one. The owner
    /// keeps recording dispatch results between attempts, never during one.
    Settle {
        shutdown: ShutdownAction,
        again: bool,
    },
    /// The owner became suspect: no commit succeeded for longer than the
    /// suspect bound. Program requests now answer only once recorded.
    Suspect { last_commit_age_nanos: i64 },
    /// The owner interrupts its Run: no commit succeeded for longer than the
    /// self-interrupt bound. The Program is terminated with its dispatches,
    /// and the Interrupted chunk is written as soon as the database answers.
    Interrupt { last_commit_age_nanos: i64 },
    /// A recovered owner's one chunk, written until the database answers:
    /// the `expired` Approval Requests expire, every nonterminal Invocation
    /// ends, and the Run ends with the termination its row records, else
    /// timed out past its deadline or interrupted. The Run is taken over with
    /// the owner's new claim.
    Recover { expired: Vec<ApprovalId> },
    /// The owner is done. A Run it leaves running renews no lease any more,
    /// and the expired-lease sweep ends it once the lease lapses.
    Retire,
}

/// The limits one owner decides against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OwnerLimits {
    pub(crate) run_output_bytes: usize,
    pub(crate) applied_cap_bytes: usize,
    /// The Fan-out Limit: Invocations of the Run `dispatching` at once at most.
    pub(crate) fan_out_invocations: usize,
    pub(crate) deadline_at_unix_nanos: i64,
    /// The shutdown grace a termination intent the owner applies gives the
    /// Run's dispatches in flight.
    pub(crate) shutdown_grace_nanos: i64,
    /// How often the owner checks whether its lease needs renewing: half a
    /// lease.
    pub(crate) idle_tick_nanos: i64,
    /// How long after its last successful commit the owner is suspect.
    pub(crate) suspect_after_nanos: i64,
    /// How long after its last successful commit the owner interrupts its Run.
    pub(crate) self_interrupt_after_nanos: i64,
    /// How often the owner prechecks its parked waits while it has any.
    pub(crate) precheck_interval_nanos: i64,
    /// The shortest gap between two reads of the parked connection requests
    /// after the change signal rang.
    pub(crate) wake_gap_nanos: i64,
}

/// The owner's change subscription and its snapshot checks of the parked
/// waits.
#[derive(Debug, Default)]
struct Wakeups {
    signal: Signal,
    /// The reads of the parked connection requests after the signal rang:
    /// one at a time, a wake gap apart.
    connections: ConnectionChecks,
    precheck: Precheck,
}

/// The change subscription, held while a connection wait is parked.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Signal {
    #[default]
    Unsubscribed,
    /// Nothing rang since the last connection check started.
    Quiet,
    /// The signal rang since the last connection check started.
    Rang,
}

#[derive(Debug, Default)]
struct ConnectionChecks {
    /// A connection check runs.
    running: bool,
    /// The wake gap since the last connection check started is not out.
    in_gap: bool,
}

/// The periodic precheck of every parked wait.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Precheck {
    /// Nothing is parked, or the timer fired with nothing to read.
    #[default]
    Unarmed,
    Armed,
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The first chunk is not committed yet.
    Starting,
    /// The Program is executing.
    Running,
    /// The Program ended; the pending chunks drain before the terminal write.
    Ending,
    /// The terminal write is running.
    Completing,
    /// A termination is recorded and being settled; results of dispatches
    /// still in flight ride the owner's chunks until the Run ends.
    Settling,
    /// A commit was lost or refused; nothing more is committed. The Program's
    /// end decides what follows.
    Stopped(CommitRejection),
    /// The owner gave up on its Run: the Program is terminated, nothing
    /// pending commits, and the Interrupted chunk is being written until the
    /// database answers or another writer has the Run.
    Interrupting,
    /// A recovered owner: no Program runs here. It applies the timers the
    /// sweep queued, then writes its one chunk.
    Recovering,
    Retired,
}

struct InFlight {
    bytes: usize,
    waiters: Vec<Reply>,
    /// The outside requests the chunk records, each with what its work
    /// reported before the chunk's `Committed`.
    requests: Vec<PendingRequest>,
    /// The chunk carries the termination intent.
    terminates: bool,
}

impl InFlight {
    fn records(&self, key: &RequestKey) -> bool {
        (self.terminates && *key == RequestKey::Termination)
            || self.requests.iter().any(|request| request.key == *key)
    }
}

/// One Run Owner's memory.
pub(crate) struct OwnerState {
    phase: Phase,
    limits: OwnerLimits,
    /// No more Program requests: a stop, a termination intent, or the end of
    /// the Program's execution.
    stop_requested: bool,
    /// A settle attempt of the recorded termination runs: no chunk commits
    /// meanwhile.
    settling: bool,
    /// The recorded termination the owner settles, from its terminal write.
    shutdown: Option<ShutdownAction>,
    /// The Program's completion, kept until its terminal write has answered.
    completion: Option<ExecutionCompletion>,
    /// The termination intent the owner applied: a cancellation, or the
    /// deadline. It rides a chunk's head update; the terminal write finds it
    /// recorded, or records what it finds itself when no chunk landed.
    termination: Option<RequestedTermination>,
    /// The Run Ledger head: set by the start mark, moved by every chunk the
    /// owner commits and by its own terminal write, asserted by every chunk.
    head: u64,
    /// Outside requests whose chunk is recorded: a repeat is answered from
    /// here, with no chunk.
    recorded_requests: HashSet<RequestKey>,
    /// Outside requests (`Decide`, `Cancel`) waiting for the chunk in flight
    /// or the terminal write, applied again once it has answered.
    parked: Vec<Message>,
    /// The exact Run Output Budget total: recorded plus applied.
    output_total: usize,
    /// The total the last committed chunk recorded.
    recorded_total: usize,
    /// When the owner's last commit succeeded: acceptance, the start mark,
    /// or the last chunk that landed.
    last_commit_at_unix_nanos: i64,
    /// No commit succeeded for longer than the suspect bound: Program
    /// requests answer only once recorded, until one does.
    suspect: bool,
    /// Slots held under the Fan-out Limit: one per fence request the owner
    /// has batched whose dispatch it has not seen end. Owner memory only; a
    /// recovered owner never dispatches, so nothing recomputes it from rows.
    slots_held: usize,
    /// Ready Invocations waiting for a slot, in readiness order (their
    /// mailbox arrival order). Each one stays `pending`, with no Event, and
    /// joins a fence batch when an earlier dispatch ends.
    waiting: VecDeque<Box<FenceRequest>>,
    /// Invocations parked on a wait, holding nothing, by the serial the owner
    /// gave each park. Owner memory only: nothing is registered again after
    /// a restart, since a recovered owner runs no Program.
    parked_waits: BTreeMap<u64, Box<ParkedInvocation>>,
    /// The serial of the last park.
    last_park: u64,
    wakeups: Wakeups,
    open: ChunkBuild,
    closed: VecDeque<ChunkBuild>,
    in_flight: Option<InFlight>,
    /// A recovered owner's Approval Requests whose passed timers it applied.
    recovered_expiries: Vec<ApprovalId>,
}

impl OwnerState {
    /// An owner for a Run accepted at `accepted_at_unix_nanos`: acceptance,
    /// which wrote the claim, counts as its first successful commit.
    pub(crate) fn new(limits: OwnerLimits, accepted_at_unix_nanos: i64) -> Self {
        Self {
            phase: Phase::Starting,
            limits,
            stop_requested: false,
            settling: false,
            shutdown: None,
            completion: None,
            termination: None,
            head: 0,
            recorded_requests: HashSet::new(),
            parked: Vec::new(),
            output_total: 0,
            recorded_total: 0,
            last_commit_at_unix_nanos: accepted_at_unix_nanos,
            suspect: false,
            slots_held: 0,
            waiting: VecDeque::new(),
            parked_waits: BTreeMap::new(),
            last_park: 0,
            wakeups: Wakeups::default(),
            open: ChunkBuild::default(),
            closed: VecDeque::new(),
            in_flight: None,
            recovered_expiries: Vec::new(),
        }
    }

    /// The owner the expired-lease sweep recreates for a Run accepted at
    /// `accepted_at_unix_nanos` whose process is gone: it starts no Program
    /// and applies the mailbox the sweep queued.
    pub(crate) fn recovered(limits: OwnerLimits, accepted_at_unix_nanos: i64) -> Self {
        Self {
            phase: Phase::Recovering,
            ..Self::new(limits, accepted_at_unix_nanos)
        }
    }

    #[cfg(test)]
    pub(crate) const fn phase(&self) -> Phase {
        self.phase
    }

    /// The exact Run Output Budget total, applied Events included.
    #[cfg(test)]
    pub(crate) const fn output_total(&self) -> usize {
        self.output_total
    }

    /// The Run Ledger head the owner tracks.
    #[cfg(test)]
    pub(crate) const fn head(&self) -> u64 {
        self.head
    }

    /// The termination intent the owner applied.
    #[cfg(test)]
    pub(crate) const fn termination(&self) -> Option<&RequestedTermination> {
        self.termination.as_ref()
    }

    /// Outside requests waiting for the chunk in flight or the terminal write.
    #[cfg(test)]
    pub(crate) fn parked_requests(&self) -> usize {
        self.parked.len()
    }

    /// Output bytes applied but not yet recorded, the chunk in flight included.
    pub(crate) const fn unrecorded_bytes(&self) -> usize {
        self.output_total.saturating_sub(self.recorded_total)
    }

    /// Chunks built and waiting for the one in flight.
    pub(crate) fn pending_chunks(&self) -> usize {
        self.closed.len() + usize::from(!self.open.is_empty())
    }

    #[cfg(test)]
    pub(crate) const fn has_commit_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    #[cfg(test)]
    pub(crate) const fn is_suspect(&self) -> bool {
        self.suspect
    }

    /// Slots held under the Fan-out Limit.
    #[cfg(test)]
    pub(crate) const fn slots_held(&self) -> usize {
        self.slots_held
    }

    /// Ready Invocations waiting for a slot under the Fan-out Limit.
    #[cfg(test)]
    pub(crate) fn waiting_for_a_slot(&self) -> usize {
        self.waiting.len()
    }

    /// Invocations parked on a wait.
    #[cfg(test)]
    pub(crate) fn parked_invocations(&self) -> usize {
        self.parked_waits.len()
    }

    /// The owner's own liveness, decided on every message: past the suspect
    /// bound since the last successful commit it answers Program requests at
    /// Recorded only; past the self-interrupt bound it gives its Run up. Only
    /// while the owner would still commit the Program's work and before the
    /// Run's deadline: a terminal write or settle in progress keeps trying on
    /// its own, and past the deadline the Run times out instead.
    fn check_liveness(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        if !matches!(self.phase, Phase::Starting | Phase::Running | Phase::Ending)
            || now_unix_nanos >= self.limits.deadline_at_unix_nanos
        {
            return Vec::new();
        }
        let age = now_unix_nanos.saturating_sub(self.last_commit_at_unix_nanos);
        if age > self.limits.self_interrupt_after_nanos {
            return self.interrupt(age);
        }
        if age > self.limits.suspect_after_nanos && !self.suspect {
            self.suspect = true;
            return vec![Effect::Suspect {
                last_commit_age_nanos: age,
            }];
        }
        Vec::new()
    }

    fn commit_succeeded(&mut self, now_unix_nanos: i64) {
        self.last_commit_at_unix_nanos = now_unix_nanos;
        self.suspect = false;
    }

    /// Gives the Run up: the request waiting on the chunk in flight and every
    /// pending one learn the Program host ended, pending work is abandoned,
    /// the Program is terminated with its dispatches, and the Interrupted
    /// chunk is written until the database answers. A chunk still in flight
    /// may land meanwhile; its outcome changes nothing here.
    fn interrupt(&mut self, last_commit_age_nanos: i64) -> Vec<Effect> {
        let mut effects = self.stop_pending(|| Error::ProgramHostTerminated);
        self.completion = None;
        self.phase = Phase::Interrupting;
        effects.extend(self.answer_parked(|| Err(Error::OwnerLost)));
        effects.push(Effect::Interrupt {
            last_commit_age_nanos,
        });
        effects
    }

    /// Stops the Program and settles everything it waits on with the error
    /// `lost` makes: the request on the chunk in flight, every pending
    /// request and work item. An outside requester learns its outcome is
    /// unknown. The caller decides the phase that follows.
    fn stop_pending(&mut self, lost: impl Fn() -> Error) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(flight) = self.in_flight.take() {
            effects.extend(
                flight
                    .waiters
                    .into_iter()
                    .map(|reply| Effect::Reply(reply, Err(lost()))),
            );
            effects.extend(
                flight
                    .requests
                    .into_iter()
                    .filter_map(|request| request.reply)
                    .map(|reply| Effect::Answer(reply, Err(Error::OwnerLost))),
            );
        }
        effects.extend(self.abandon_pending(&lost, || Err(Error::OwnerLost)));
        self.stop_requested = true;
        effects.push(Effect::TerminateProgram);
        effects
    }

    fn accepts_program_requests(&self, now_unix_nanos: i64) -> bool {
        self.phase == Phase::Running
            && !self.stop_requested
            && now_unix_nanos < self.limits.deadline_at_unix_nanos
    }

    /// Row work and dispatch results are accepted while the owner can still
    /// commit them: an Invocation that ends after the Program did, during the
    /// terminal write, or while a recorded termination is being settled, still
    /// records its outcome. Work that arrives during the terminal write commits
    /// once that write has answered: before the settle, or before the terminal
    /// write runs again when it refused a Run with a nonterminal Invocation.
    fn accepts_work(&self) -> bool {
        matches!(
            self.phase,
            Phase::Running | Phase::Ending | Phase::Completing | Phase::Settling
        )
    }

    fn output_fits(&self, bytes: usize) -> bool {
        self.output_total
            .checked_add(bytes)
            .is_some_and(|total| total <= self.limits.run_output_bytes)
    }

    /// Adds `item` to the open chunk, closing it before when the item does
    /// not fit and after when it fills the chunk, and returns the chunk now
    /// holding the item.
    fn push(&mut self, item: ChunkItem, events: usize, bytes: usize) -> &mut ChunkBuild {
        if !self.open.fits(events, bytes) {
            self.close_open();
        }
        self.open.push(item, events, bytes);
        if self.open.is_full() {
            self.close_open();
            self.closed.back_mut().expect("the chunk just closed")
        } else {
            &mut self.open
        }
    }

    fn close_open(&mut self) {
        if !self.open.is_empty() {
            self.closed.push_back(std::mem::take(&mut self.open));
        }
    }

    /// Answers at Applied, or at Recorded while the Applied cap is exceeded or
    /// the owner is suspect: the reply then rides the chunk holding the
    /// request's item.
    fn answer(&mut self, reply: Reply) -> Vec<Effect> {
        if !self.suspect && self.unrecorded_bytes() <= self.limits.applied_cap_bytes {
            return vec![Effect::Reply(reply, Ok(()))];
        }
        let chunk = if self.open.is_empty() {
            self.closed.back_mut()
        } else {
            Some(&mut self.open)
        };
        match chunk {
            Some(chunk) => chunk.waiters.push(reply),
            None => return vec![Effect::Reply(reply, Ok(()))],
        }
        Vec::new()
    }

    fn output(&mut self, kind: OwnedEventKind, reply: Reply, now_unix_nanos: i64) -> Vec<Effect> {
        if !self.accepts_program_requests(now_unix_nanos) {
            return vec![Effect::Reply(
                reply,
                Err(Error::RunEventPersistence(
                    "Program Run no longer accepts output".to_string(),
                )),
            )];
        }
        let event = OwnedEvent::new(kind, now_unix_nanos);
        let bytes = event.output_bytes();
        if !self.output_fits(bytes) {
            return vec![Effect::Reply(reply, Err(Error::ProgramValueTooLarge))];
        }
        self.output_total = self.output_total.saturating_add(bytes);
        self.push(ChunkItem::Event(event), 1, bytes);
        self.answer(reply)
    }

    fn admit(
        &mut self,
        work: Box<dyn ChunkWork>,
        reply: Reply,
        now_unix_nanos: i64,
    ) -> Vec<Effect> {
        if !self.accepts_program_requests(now_unix_nanos) {
            return vec![
                Effect::Abandon(work, Error::ProgramHostTerminated),
                Effect::Reply(
                    reply,
                    Err(Error::InvocationPersistence(
                        "Program Run no longer accepts Operations".to_string(),
                    )),
                ),
            ];
        }
        self.push(ChunkItem::Work(work), 1, 0);
        self.answer(reply)
    }

    /// An upstream result the owner applies ends the dispatch that produced
    /// it: its slot under the Fan-out Limit goes to the Invocation that has
    /// waited longest, whose fence joins the chunk after this row's end. A
    /// result the owner refuses (over budget, or no longer taking work)
    /// leaves the row `dispatching` and the slot with the Invocation task,
    /// which frees it once the row's end is recorded.
    fn invocation_result(
        &mut self,
        bytes: usize,
        work: Box<dyn ChunkWork>,
        reply: Reply,
        now_unix_nanos: i64,
    ) -> Vec<Effect> {
        if !self.accepts_work() {
            return vec![
                Effect::Abandon(work, Error::ProgramHostTerminated),
                Effect::Reply(
                    reply,
                    Err(Error::InvocationPersistence(
                        "Program Run lost Invocation completion".to_string(),
                    )),
                ),
            ];
        }
        if !self.output_fits(bytes) {
            return vec![
                Effect::Abandon(
                    work,
                    Error::InvocationRejected(CoralFailure::RunOutputTooLarge.into()),
                ),
                Effect::Reply(
                    reply,
                    Err(Error::InvocationRejected(
                        CoralFailure::RunOutputTooLarge.into(),
                    )),
                ),
            ];
        }
        self.output_total = self.output_total.saturating_add(bytes);
        self.push(ChunkItem::Work(work), 1, bytes);
        let mut effects = self.answer(reply);
        effects.extend(self.dispatch_ended(now_unix_nanos));
        effects
    }

    fn work(&mut self, work: Box<dyn ChunkWork>) -> Vec<Effect> {
        if !self.accepts_work() {
            return vec![Effect::Abandon(work, Error::ProgramHostTerminated)];
        }
        // Row work may append one Event of its own inside the transaction;
        // reserve it so a chunk never exceeds the Events cap.
        self.push(ChunkItem::Work(work), 1, 0);
        Vec::new()
    }

    /// A ready Invocation asks for the Dispatch Fence. Under the Fan-out
    /// Limit it takes a slot and joins the batch of the open chunk, which
    /// commits when free; at the limit it waits in memory, in readiness
    /// order, with no Event and no required action, until an earlier
    /// dispatch ends. A Run that no longer takes Program requests (stopped,
    /// past its deadline in owner memory) answers at once, without the
    /// database.
    fn fence(&mut self, request: Box<FenceRequest>, now_unix_nanos: i64) -> Vec<Effect> {
        if !self.accepts_program_requests(now_unix_nanos) {
            return vec![Effect::Abandon(
                Box::new(FenceBatch::new(*request)),
                Error::ProgramHostTerminated,
            )];
        }
        if self.slots_held >= self.limits.fan_out_invocations {
            self.waiting.push_back(request);
            return Vec::new();
        }
        self.slots_held += 1;
        self.open.push_fence(*request);
        if self.open.is_full() {
            self.close_open();
        }
        Vec::new()
    }

    /// The dispatch a fence decided is over, whatever came of it: its slot
    /// under the Fan-out Limit goes to the Invocation that has waited
    /// longest, which asks for the fence now. The Run deadline covers a slot
    /// wait: once the Run takes no more Program requests, every waiting
    /// Invocation is answered at once instead.
    fn dispatch_ended(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        // Saturating: the `apply` table sends results no fence preceded. In
        // production a result or a `DispatchEnded` comes only from a task
        // whose request the owner batched, so the count is exact.
        self.slots_held = self.slots_held.saturating_sub(1);
        if !self.accepts_program_requests(now_unix_nanos) {
            return self.release_waiting(|| Error::ProgramHostTerminated);
        }
        match self.waiting.pop_front() {
            Some(request) => self.fence(request, now_unix_nanos),
            None => Vec::new(),
        }
    }

    /// The Run takes no more Program requests: every Invocation waiting for
    /// a slot is answered, as one batch, with the error `lost` makes (no
    /// dispatch of theirs ever starts; the slots the abandoned batches held
    /// stay counted), and every parked one is let go with the change
    /// subscription: its dropped wake tells it the Run ends.
    fn release_waiting(&mut self, lost: impl Fn() -> Error) -> Vec<Effect> {
        self.parked_waits.clear();
        let mut effects: Vec<Effect> = self.unsubscribe_when_idle().into_iter().collect();
        let mut waiting = self.waiting.drain(..);
        let Some(first) = waiting.next() else {
            return effects;
        };
        let mut batch = FenceBatch::new(*first);
        for request in waiting {
            batch.push(*request);
        }
        effects.push(Effect::Abandon(Box::new(batch), lost()));
        effects
    }

    /// A chunk that landed parked an Invocation: the owner keeps it until
    /// its wait ends and arms the wait's deadline, the periodic precheck
    /// unless it is armed, and the change subscription for a connection
    /// wait, whose first ring reads the request. Under a subscription already
    /// held, the park counts as a ring: the request may have ended between
    /// the held recheck's read and this message, and a ring that came first
    /// read a batch without it. A Run that takes no more Program requests
    /// keeps nothing: the dropped wake tells the Invocation the Run ends. An
    /// Approval Request decided first sends the Invocation back to its held
    /// recheck at once.
    fn park(&mut self, parked: Box<ParkedInvocation>, now_unix_nanos: i64) -> Vec<Effect> {
        if !self.accepts_program_requests(now_unix_nanos) {
            return Vec::new();
        }
        if let ParkedWait::Approval { approval, .. } = parked.wait()
            && self
                .recorded_requests
                .contains(&RequestKey::Approval(approval.clone()))
        {
            return vec![Effect::Wake(parked)];
        }
        let (timer, at) = parked.wait().deadline();
        let mut effects = vec![arm_at(timer, at, now_unix_nanos)];
        let on_connection = parked.wait().on_connection();
        self.last_park += 1;
        self.parked_waits.insert(self.last_park, parked);
        if on_connection {
            if self.wakeups.signal == Signal::Unsubscribed {
                self.wakeups.signal = Signal::Quiet;
                effects.push(Effect::Subscribe);
            } else {
                self.wakeups.signal = Signal::Rang;
                effects.extend(self.check_connections());
            }
        }
        effects.extend(self.arm_precheck());
        effects
    }

    /// Arms the periodic precheck while waits are parked, unless it is
    /// armed or running.
    fn arm_precheck(&mut self) -> Option<Effect> {
        if self.parked_waits.is_empty() || self.wakeups.precheck != Precheck::Unarmed {
            return None;
        }
        self.wakeups.precheck = Precheck::Armed;
        Some(Effect::ArmTimer {
            timer: Timer::Precheck,
            after_nanos: self.limits.precheck_interval_nanos,
        })
    }

    /// Sends the parked Invocations `woken` selects, by serial and wait,
    /// back to their held rechecks, and drops the change subscription once
    /// no connection wait is left.
    fn wake(&mut self, woken: impl Fn(u64, &ParkedWait) -> bool) -> Vec<Effect> {
        let mut effects: Vec<Effect> = self
            .parked_waits
            .extract_if(.., |serial, parked| woken(*serial, parked.wait()))
            .map(|(_, parked)| Effect::Wake(parked))
            .collect();
        effects.extend(self.unsubscribe_when_idle());
        effects
    }

    fn unsubscribe_when_idle(&mut self) -> Option<Effect> {
        if self.wakeups.signal == Signal::Unsubscribed
            || self
                .parked_waits
                .values()
                .any(|parked| parked.wait().on_connection())
        {
            return None;
        }
        self.wakeups.signal = Signal::Unsubscribed;
        Some(Effect::Unsubscribe)
    }

    /// The change signal rang: a hint that a connection request may have
    /// ended. A ring from a subscription since dropped changes nothing.
    fn changed(&mut self) -> Vec<Effect> {
        if self.wakeups.signal == Signal::Unsubscribed {
            return Vec::new();
        }
        self.wakeups.signal = Signal::Rang;
        self.check_connections()
    }

    /// Reads the requests of the parked connection waits once the signal
    /// rang since the last check, no check runs, and the wake gap since the
    /// last one is out: every ring meanwhile is coalesced into this one read.
    fn check_connections(&mut self) -> Vec<Effect> {
        if self.wakeups.signal != Signal::Rang
            || self.wakeups.connections.running
            || self.wakeups.connections.in_gap
        {
            return Vec::new();
        }
        self.wakeups.signal = Signal::Quiet;
        let Some(batch) = PrecheckBatch::of(
            Check::Connections,
            self.parked_waits
                .iter()
                .filter(|(_, parked)| parked.wait().on_connection()),
        ) else {
            return Vec::new();
        };
        self.wakeups.connections.running = true;
        self.wakeups.connections.in_gap = true;
        vec![
            Effect::Precheck(batch),
            Effect::ArmTimer {
                timer: Timer::WakeGap,
                after_nanos: self.limits.wake_gap_nanos,
            },
        ]
    }

    /// The periodic precheck is due: every parked wait is read in one
    /// snapshot against the Dispatch Fence's facts. Past the Run deadline on
    /// the owner's clock, the deadline is applied instead, as its timer
    /// would have been first.
    fn precheck_due(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        self.wakeups.precheck = Precheck::Unarmed;
        if now_unix_nanos >= self.limits.deadline_at_unix_nanos {
            return self.deadline(now_unix_nanos);
        }
        if !self.accepts_program_requests(now_unix_nanos) {
            return Vec::new();
        }
        let Some(batch) = PrecheckBatch::of(Check::Periodic, self.parked_waits.iter()) else {
            return Vec::new();
        };
        self.wakeups.precheck = Precheck::Running;
        vec![Effect::Precheck(batch)]
    }

    /// A check read its snapshot: the parks it found ended or moot, while
    /// still parked under the serial it read, go back to their held
    /// rechecks. A check that failed found nothing: a precheck is a hint, and
    /// the next one reads again.
    fn prechecked(&mut self, check: Check, found: Result<Vec<u64>, Error>) -> Vec<Effect> {
        match check {
            Check::Connections => self.wakeups.connections.running = false,
            Check::Periodic => self.wakeups.precheck = Precheck::Unarmed,
        }
        let found: BTreeSet<u64> = found.unwrap_or_default().into_iter().collect();
        let mut effects = self.wake(|serial, _| found.contains(&serial));
        match check {
            Check::Connections => effects.extend(self.check_connections()),
            Check::Periodic => effects.extend(self.arm_precheck()),
        }
        effects
    }

    /// A stop reached the owner, from an outside writer's recorded
    /// termination or from the owner's own termination intent: every fence
    /// request not yet committed, and every one waiting for a slot, is
    /// answered at once as terminated, so no dispatch starts after it. A
    /// fence in flight returns on its own; the pre-send check ends what it
    /// permitted.
    fn stop(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        // The first stop arms the shutdown grace: the Program has that long
        // to report its end before the owner ends the Run without it.
        if !self.stop_requested {
            self.stop_requested = true;
            effects.push(Effect::ArmTimer {
                timer: Timer::Grace,
                after_nanos: self.limits.shutdown_grace_nanos,
            });
        }
        for chunk in self
            .closed
            .iter_mut()
            .chain(std::iter::once(&mut self.open))
        {
            if let Some(batch) = chunk.take_fence() {
                effects.push(Effect::Abandon(batch, Error::ProgramHostTerminated));
            }
        }
        self.closed.retain(|chunk| !chunk.is_empty());
        effects.extend(self.release_waiting(|| Error::ProgramHostTerminated));
        effects.push(Effect::TerminateProgram);
        effects
    }

    /// Whether an outside request's entity is recorded by a chunk not yet
    /// answered: in flight, or built and waiting.
    fn request_is_pending(&self, key: &RequestKey) -> bool {
        self.in_flight
            .as_ref()
            .is_some_and(|flight| flight.records(key))
            || self
                .closed
                .iter()
                .chain(std::iter::once(&self.open))
                .any(|chunk| chunk.records(key))
    }

    /// Whether the owner can no longer answer an outside request: nothing it
    /// applies commits any more.
    fn refuses_requests(&self) -> bool {
        matches!(
            self.phase,
            Phase::Stopped(_) | Phase::Interrupting | Phase::Retired
        )
    }

    /// Answers an outside request from memory when the owner cannot take it
    /// on: its entity is recorded already, or the owner no longer takes
    /// requests (its outcome stays unknown to the requester, who reads the
    /// record). Otherwise hands the reply back for the request to go on.
    fn answer_from_memory(
        &self,
        key: &RequestKey,
        reply: RequestReply,
    ) -> Result<Vec<Effect>, RequestReply> {
        if self.recorded_requests.contains(key) {
            return Ok(vec![Effect::Answer(reply, Ok(DecisionAnswer::FromRecord))]);
        }
        if self.refuses_requests() {
            return Ok(vec![Effect::Answer(reply, Err(Error::OwnerLost))]);
        }
        Err(reply)
    }

    /// Whether an outside request waits for an answer in progress: the chunk
    /// recording its entity, or the terminal write.
    fn must_park(&self, key: &RequestKey) -> bool {
        self.request_is_pending(key) || self.phase == Phase::Completing
    }

    /// A decision on one Approval Request: the first for the request rides
    /// the next chunk, recording the decision while the Run accepts actions
    /// and closing the request otherwise; a repeat waits for that chunk or
    /// reads the recorded answer from memory.
    fn decide(
        &mut self,
        request: DecisionRequest,
        reply: RequestReply,
        now_unix_nanos: i64,
    ) -> Vec<Effect> {
        let key = RequestKey::Approval(request.approval.clone());
        let reply = match self.answer_from_memory(&key, reply) {
            Ok(effects) => return effects,
            Err(reply) => reply,
        };
        if self.must_park(&key) {
            self.parked.push(Message::Decide { request, reply });
            return Vec::new();
        }
        if !self.accepts_work() {
            return vec![Effect::Answer(reply, Err(Error::OwnerLost))];
        }
        // The request's deadline passed on the owner's clock before its
        // timer's message arrived: the expiry is applied first, as the
        // timer would have been, and the decision waits for its record.
        let decide = self.accepts_program_requests(now_unix_nanos);
        if decide && now_unix_nanos >= request.deadline_at_unix_nanos {
            let effects = self.expire_approval(&request.approval, now_unix_nanos);
            self.parked.push(Message::Decide { request, reply });
            return effects;
        }
        let work = DecideWork::decision(request, decide);
        self.push(ChunkItem::Work(Box::new(work)), 1, 0)
            .requests
            .push(PendingRequest {
                key,
                reply: Some(reply),
                answer: None,
            });
        Vec::new()
    }

    /// An Approval Request's deadline passed: the request expires and its
    /// Invocation ends, in the next chunk. Nothing when a decision came
    /// first (recorded, or riding a chunk), or the Run takes no more
    /// Program requests: its termination ends the Invocation and closes the
    /// request, as it would a decision applied then.
    fn expire_approval(&mut self, approval: &ApprovalId, now_unix_nanos: i64) -> Vec<Effect> {
        let key = RequestKey::Approval(approval.clone());
        if self.recorded_requests.contains(&key)
            || self.request_is_pending(&key)
            || !self.accepts_program_requests(now_unix_nanos)
        {
            return Vec::new();
        }
        let work = DecideWork::expiry(approval.clone());
        self.push(ChunkItem::Work(Box::new(work)), 1, 0)
            .requests
            .push(PendingRequest {
                key,
                reply: None,
                answer: None,
            });
        Vec::new()
    }

    /// A timer fired: applied here, in order with every other message.
    fn timer(&mut self, timer: Timer, now_unix_nanos: i64) -> Vec<Effect> {
        match timer {
            Timer::RunDeadline => self.deadline(now_unix_nanos),
            Timer::Grace => self.grace_expired(),
            Timer::IdleTick => self.idle_tick(),
            Timer::Approval(approval) => self.expire_approval(&approval, now_unix_nanos),
            // The Invocations parked on the request go back to their held
            // recheck, which records its expiry inside the owner's chunk.
            Timer::Connection(expired) => {
                if !self.accepts_program_requests(now_unix_nanos) {
                    return Vec::new();
                }
                self.wake(|_, wait| {
                    matches!(wait, ParkedWait::Connection { request, .. } if *request == expired)
                })
            }
            Timer::Precheck => self.precheck_due(now_unix_nanos),
            Timer::WakeGap => {
                self.wakeups.connections.in_gap = false;
                self.check_connections()
            }
        }
    }

    /// A cancellation: applied at once when no termination is recorded or in
    /// flight (the Program is terminated, the intent rides the next chunk),
    /// answered from memory once one is; past the Run deadline the Run times
    /// out instead, as a cancel found on the record would.
    fn cancel(
        &mut self,
        requester: Principal,
        reply: RequestReply,
        now_unix_nanos: i64,
    ) -> Vec<Effect> {
        let key = RequestKey::Termination;
        let reply = match self.answer_from_memory(&key, reply) {
            Ok(effects) => return effects,
            Err(reply) => reply,
        };
        if self.must_park(&key) {
            self.parked.push(Message::Cancel { requester, reply });
            return Vec::new();
        }
        match self.phase {
            // The terminal write recorded a termination of its own; this
            // changes nothing.
            Phase::Settling => vec![Effect::Answer(reply, Ok(DecisionAnswer::FromRecord))],
            Phase::Starting | Phase::Running | Phase::Ending => {
                // Past the Run deadline on the owner's clock the Run times
                // out, as its timer would have decided first.
                let mut effects = Vec::new();
                if now_unix_nanos >= self.limits.deadline_at_unix_nanos {
                    effects = self.deadline(now_unix_nanos);
                }
                // A timeout applied first decides the kind: the request is
                // answered from the record once the chunk carrying it, or
                // the terminal write, has recorded it.
                if self.termination.is_some() {
                    self.parked.push(Message::Cancel { requester, reply });
                    return effects;
                }
                self.terminate(
                    RunTerminationKind::Canceled,
                    Some(requester),
                    Some(reply),
                    now_unix_nanos,
                )
            }
            Phase::Completing
            | Phase::Stopped(_)
            | Phase::Interrupting
            | Phase::Recovering
            | Phase::Retired => vec![Effect::Answer(reply, Err(Error::OwnerLost))],
        }
    }

    /// The Run deadline passed: the timeout is applied like a cancellation
    /// (the intent rides the next chunk's head update, with no Event, and
    /// the Program is terminated at once), dated when the owner applies it
    /// and never before the deadline, so the shutdown grace runs from then.
    /// Nothing more once a termination is applied (a cancel applied first
    /// decides the kind), or past the point where the terminal write decides
    /// on its own.
    fn deadline(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        if self.termination.is_some()
            || !matches!(self.phase, Phase::Starting | Phase::Running | Phase::Ending)
        {
            return Vec::new();
        }
        let at = now_unix_nanos.max(self.limits.deadline_at_unix_nanos);
        self.terminate(RunTerminationKind::TimedOut, None, None, at)
    }

    /// The shutdown grace since the Program was stopped is out and the
    /// Program has not reported its end (it ignores the stop, or waits on
    /// something that does not): the owner ends the Run as if it had. A
    /// Program that reports its end later changes nothing.
    fn grace_expired(&mut self) -> Vec<Effect> {
        if self.phase == Phase::Running && self.stop_requested {
            return self.program_ended(ExecutionCompletion::ProgramHostTerminated);
        }
        Vec::new()
    }

    /// Applies a termination intent: it rides the next chunk's head update,
    /// a cancellation's `cancellation.requested` Event among that chunk's
    /// items in arrival order, and the Program is terminated at once. The
    /// requester, if any, is answered once that chunk is recorded.
    fn terminate(
        &mut self,
        kind: RunTerminationKind,
        requester: Option<Principal>,
        reply: Option<RequestReply>,
        now_unix_nanos: i64,
    ) -> Vec<Effect> {
        let intent = match RequestedTermination::new(
            kind,
            requester,
            now_unix_nanos,
            self.limits.shutdown_grace_nanos,
        ) {
            Ok(intent) => intent,
            // An intent the row cannot hold: a requester learns it; the
            // owner's own timeout still stops the Program, and the terminal
            // write records the timeout it finds passed.
            Err(error) => {
                return match reply {
                    Some(reply) => vec![Effect::Answer(reply, Err(error))],
                    None => self.stop(),
                };
            }
        };
        self.termination = Some(intent.clone());
        let chunk = match &intent.requester {
            Some(requester) => self.push(
                ChunkItem::Event(OwnedEvent::new(
                    OwnedEventKind::CancellationRequested(requester.clone()),
                    now_unix_nanos,
                )),
                1,
                0,
            ),
            None => &mut self.open,
        };
        chunk.termination = Some(intent);
        chunk.requests.extend(reply.map(|reply| PendingRequest {
            key: RequestKey::Termination,
            reply: Some(reply),
            answer: None,
        }));
        self.stop()
    }

    /// Applies the parked outside requests again, now that the chunk or the
    /// terminal write they waited for has answered.
    fn unpark(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        std::mem::take(&mut self.parked)
            .into_iter()
            .flat_map(|message| apply_message(self, message, now_unix_nanos))
            .collect()
    }

    /// Answers every parked outside request with `answer`.
    fn answer_parked(&mut self, answer: impl Fn() -> Result<DecisionAnswer, Error>) -> Vec<Effect> {
        std::mem::take(&mut self.parked)
            .into_iter()
            .map(|message| match message {
                Message::Decide { reply, .. } | Message::Cancel { reply, .. } => {
                    Effect::Answer(reply, answer())
                }
                _ => unreachable!("only outside requests are parked"),
            })
            .collect()
    }

    /// What an outside request's work recorded in the chunk in flight: its
    /// requester receives it once the chunk is answered.
    fn request_recorded(&mut self, key: &RequestKey, answer: DecisionAnswer) {
        if let Some(request) = self
            .in_flight
            .iter_mut()
            .flat_map(|flight| &mut flight.requests)
            .find(|request| request.key == *key)
        {
            request.answer = Some(answer);
        }
    }

    /// Commit-when-free: the oldest pending chunk, once nothing is in flight.
    fn commit_when_free(&mut self) -> Vec<Effect> {
        if self.in_flight.is_some()
            || self.settling
            || !matches!(self.phase, Phase::Running | Phase::Ending | Phase::Settling)
        {
            return Vec::new();
        }
        let chunk = match self.closed.pop_front() {
            Some(chunk) => chunk,
            None if self.open.is_empty() => return Vec::new(),
            None => std::mem::take(&mut self.open),
        };
        self.commit(chunk)
    }

    /// Commits the pending chunks when free; with none left while a recorded
    /// termination is being settled, attempts the settle.
    fn commit_or_settle_when_free(&mut self) -> Vec<Effect> {
        if self.pending_chunks() > 0 {
            return self.commit_when_free();
        }
        self.settle_when_free(false)
    }

    /// One settle attempt, once nothing is in flight and no chunk is
    /// pending: the owner records every dispatch result it holds before it
    /// looks at what is still dispatching.
    fn settle_when_free(&mut self, again: bool) -> Vec<Effect> {
        if self.phase != Phase::Settling || self.settling || self.in_flight.is_some() {
            return Vec::new();
        }
        if self.pending_chunks() > 0 {
            return self.commit_when_free();
        }
        let Some(shutdown) = self.shutdown.clone() else {
            return Vec::new();
        };
        self.settling = true;
        vec![Effect::Settle { shutdown, again }]
    }

    /// Starts one chunk commit; the idle tick is armed again from here, so
    /// an owner that commits renews its lease by its chunks alone.
    fn commit(&mut self, mut chunk: ChunkBuild) -> Vec<Effect> {
        let output_total = self.recorded_total.saturating_add(chunk.bytes);
        chunk.head = self.head;
        self.in_flight = Some(InFlight {
            bytes: chunk.bytes,
            waiters: std::mem::take(&mut chunk.waiters),
            requests: std::mem::take(&mut chunk.requests),
            terminates: chunk.termination.is_some(),
        });
        vec![
            self.arm_idle_tick(),
            Effect::Commit {
                chunk,
                output_total,
            },
        ]
    }

    fn committed(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        self.commit_succeeded(now_unix_nanos);
        self.head += 1;
        let mut effects = Vec::new();
        if let Some(flight) = self.in_flight.take() {
            self.recorded_total = self.recorded_total.saturating_add(flight.bytes);
            effects.extend(
                flight
                    .waiters
                    .into_iter()
                    .map(|reply| Effect::Reply(reply, Ok(()))),
            );
            if flight.terminates {
                self.recorded_requests.insert(RequestKey::Termination);
            }
            let mut decided = Vec::new();
            for request in flight.requests {
                if let RequestKey::Approval(approval) = &request.key {
                    decided.push(approval.clone());
                }
                self.recorded_requests.insert(request.key);
                let answer = request.answer.unwrap_or(DecisionAnswer::FromRecord);
                if let Some(reply) = request.reply {
                    effects.push(Effect::Answer(reply, Ok(answer)));
                }
            }
            // The chunk recorded these requests' decisions, expiries or
            // closes: the Invocations parked on them learn it with no read.
            if !decided.is_empty() {
                effects.extend(self.wake(|_, wait| {
                    matches!(wait, ParkedWait::Approval { approval, .. } if decided.contains(approval))
                }));
            }
        }
        effects.extend(self.unpark(now_unix_nanos));
        match self.phase {
            Phase::Running | Phase::Settling => effects.extend(self.commit_or_settle_when_free()),
            Phase::Ending => effects.extend(self.advance_ending()),
            Phase::Starting
            | Phase::Completing
            | Phase::Stopped(_)
            | Phase::Interrupting
            | Phase::Recovering
            | Phase::Retired => {}
        }
        effects
    }

    /// Retires the owner: whatever is still pending will never commit. An
    /// outside requester still waiting is answered from the record when the
    /// owner's own write `ended` the Run, and learns its outcome is unknown
    /// otherwise.
    fn retire(&mut self, ended: bool) -> Vec<Effect> {
        self.phase = Phase::Retired;
        let answer = move || {
            if ended {
                Ok(DecisionAnswer::FromRecord)
            } else {
                Err(Error::OwnerLost)
            }
        };
        let mut effects = self.abandon_pending(|| Error::ProgramHostTerminated, answer);
        effects.extend(self.answer_parked(answer));
        effects.push(Effect::Retire);
        effects
    }

    /// Settles every pending request and work item with the error `lost`
    /// makes, every Invocation waiting for a slot alike, and every outside
    /// requester of a pending chunk with `requests`.
    fn abandon_pending(
        &mut self,
        lost: impl Fn() -> Error,
        requests: impl Fn() -> Result<DecisionAnswer, Error>,
    ) -> Vec<Effect> {
        let mut effects = self.release_waiting(&lost);
        let pending = self
            .closed
            .drain(..)
            .chain(std::iter::once(std::mem::take(&mut self.open)));
        for chunk in pending {
            for item in chunk.items {
                if let ChunkItem::Work(work) = item {
                    effects.push(Effect::Abandon(work, lost()));
                }
            }
            if let Some(batch) = chunk.fence {
                effects.push(Effect::Abandon(batch, lost()));
            }
            effects.extend(
                chunk
                    .waiters
                    .into_iter()
                    .map(|reply| Effect::Reply(reply, Err(lost()))),
            );
            effects.extend(
                chunk
                    .requests
                    .into_iter()
                    .filter_map(|request| request.reply)
                    .map(|reply| Effect::Answer(reply, requests())),
            );
        }
        effects
    }

    /// After the Program ended: drain the pending chunks, then write the end.
    fn advance_ending(&mut self) -> Vec<Effect> {
        if self.in_flight.is_some() {
            return Vec::new();
        }
        if self.pending_chunks() > 0 {
            return self.commit_when_free();
        }
        if let Some(completion) = &self.completion {
            self.phase = Phase::Completing;
            vec![Effect::CompleteExecution {
                completion: completion.clone(),
                intent: self.unrecorded_termination(),
            }]
        } else {
            self.retire(false)
        }
    }

    /// The termination intent the owner applied and no chunk recorded, for
    /// the terminal write.
    fn unrecorded_termination(&self) -> Option<RequestedTermination> {
        if self.recorded_requests.contains(&RequestKey::Termination) {
            return None;
        }
        self.termination.clone()
    }

    /// The owner's first chunk landed: the head it left is the owner's. A
    /// stop or a termination that reached the owner meanwhile means the
    /// Program never starts; the Run ends as the terminal write finds it.
    fn started(&mut self, started: StartedExecution, now_unix_nanos: i64) -> Vec<Effect> {
        self.head = started.head;
        self.commit_succeeded(now_unix_nanos);
        if self.stop_requested {
            self.phase = Phase::Ending;
            self.completion = Some(ExecutionCompletion::ProgramHostTerminated);
            return self.advance_ending();
        }
        self.phase = Phase::Running;
        vec![self.arm_idle_tick(), Effect::StartProgram(started.package)]
    }

    /// The idle tick, half a lease from now.
    fn arm_idle_tick(&self) -> Effect {
        Effect::ArmTimer {
            timer: Timer::IdleTick,
            after_nanos: self.limits.idle_tick_nanos,
        }
    }

    fn program_ended(&mut self, completion: ExecutionCompletion) -> Vec<Effect> {
        match self.phase {
            Phase::Running => {
                self.phase = Phase::Ending;
                self.completion = Some(completion);
                // Wake every detached Invocation wait, as execution's end does;
                // an Invocation still waiting for a slot never dispatches.
                let mut effects = vec![Effect::TerminateProgram];
                effects.extend(self.release_waiting(|| Error::ProgramHostTerminated));
                effects.extend(self.advance_ending());
                effects
            }
            // A refused or failed commit leaves the terminal write to decide
            // (the deadline, a released claim, an interruption); a lost or
            // ended Run has nothing left to write, and a corrupt one gets
            // nothing more written.
            Phase::Stopped(CommitRejection::Refused | CommitRejection::Failed) => {
                self.phase = Phase::Completing;
                vec![Effect::CompleteExecution {
                    completion,
                    intent: self.unrecorded_termination(),
                }]
            }
            Phase::Stopped(
                CommitRejection::ClaimMismatch
                | CommitRejection::RunEnded
                | CommitRejection::Corrupt,
            ) => self.retire(false),
            Phase::Starting
            | Phase::Ending
            | Phase::Completing
            | Phase::Settling
            | Phase::Interrupting
            | Phase::Recovering
            | Phase::Retired => Vec::new(),
        }
    }

    /// The terminal write found or recorded a termination: the owner settles
    /// it, recording the results of dispatches in flight between attempts.
    fn settle_recorded(&mut self, shutdown: ShutdownAction, now_unix_nanos: i64) -> Vec<Effect> {
        self.phase = Phase::Settling;
        self.head = shutdown.head();
        self.completion = None;
        self.shutdown = Some(shutdown);
        let mut effects = self.unpark(now_unix_nanos);
        effects.extend(self.commit_or_settle_when_free());
        effects
    }

    /// A lost or refused commit stops the owner at once: the Program is
    /// terminated, nothing pending commits, and every waiting request learns
    /// its outcome is unknown.
    fn rejected(&mut self, cause: CommitRejection, now_unix_nanos: i64) -> Vec<Effect> {
        let mut effects = self.stop_pending(|| cause.lost());
        match self.phase {
            Phase::Running => self.phase = Phase::Stopped(cause),
            Phase::Ending => {
                if let (CommitRejection::Refused | CommitRejection::Failed, Some(completion)) =
                    (cause, &self.completion)
                {
                    self.phase = Phase::Completing;
                    effects.push(Effect::CompleteExecution {
                        completion: completion.clone(),
                        intent: self.unrecorded_termination(),
                    });
                } else {
                    effects.extend(self.retire(false));
                }
            }
            // The recorded termination still has to be settled; the owner
            // records nothing else.
            Phase::Settling => effects.extend(self.settle_when_free(false)),
            Phase::Starting
            | Phase::Completing
            | Phase::Stopped(_)
            | Phase::Interrupting
            | Phase::Recovering
            | Phase::Retired => {}
        }
        effects.extend(self.unpark(now_unix_nanos));
        effects
    }

    /// The idle tick, half a lease after the last commit started or the
    /// last tick: an empty chunk renews the lease while nothing else is
    /// pending or in flight; otherwise the tick is armed again, which also
    /// keeps the owner's liveness judged while nothing else reaches the
    /// mailbox.
    fn idle_tick(&mut self) -> Vec<Effect> {
        if self.phase == Phase::Retired {
            return Vec::new();
        }
        if self.phase != Phase::Running
            || self.stop_requested
            || self.in_flight.is_some()
            || self.pending_chunks() > 0
        {
            return vec![self.arm_idle_tick()];
        }
        self.commit(ChunkBuild::default())
    }

    /// A recovered owner applies one message (ADR 0018, Recovery): the sweep
    /// queued every Approval Request timer that passed, in deadline order,
    /// then [`Message::Recover`], which writes the expiries and the Run's end
    /// in one chunk. The write's answer, or a stop (the Shutdown Drain is
    /// out), retires the owner. No Program runs here and no outside request
    /// finds the owner, so nothing else reaches it.
    fn recovering(&mut self, message: Message) -> Vec<Effect> {
        match message {
            Message::Timer(Timer::Approval(approval)) => {
                self.recovered_expiries.push(approval);
                Vec::new()
            }
            Message::Recover => vec![Effect::Recover {
                expired: std::mem::take(&mut self.recovered_expiries),
            }],
            Message::Interrupted(_) | Message::Stop => self.retire(false),
            _ => Vec::new(),
        }
    }
}

/// Arms `timer` for `at_unix_nanos`: the host sleeps the rest of the way
/// from `now_unix_nanos` on its own clock.
fn arm_at(timer: Timer, at_unix_nanos: i64, now_unix_nanos: i64) -> Effect {
    Effect::ArmTimer {
        timer,
        after_nanos: at_unix_nanos.saturating_sub(now_unix_nanos).max(0),
    }
}

/// The Run Owner's loop: applies `message` to `state` at `now_unix_nanos` and
/// returns what the hosting task runs. No I/O happens here.
pub(crate) fn apply(state: &mut OwnerState, message: Message, now_unix_nanos: i64) -> Vec<Effect> {
    // A commit that succeeded counts before the owner judges its liveness:
    // its message may have waited in the mailbox past a bound the commit
    // itself met.
    if matches!(message, Message::Committed | Message::Started(Ok(Some(_)))) {
        let mut effects = apply_message(state, message, now_unix_nanos);
        effects.extend(state.check_liveness(now_unix_nanos));
        return effects;
    }
    let mut effects = state.check_liveness(now_unix_nanos);
    effects.extend(apply_message(state, message, now_unix_nanos));
    effects
}

#[expect(
    clippy::too_many_lines,
    reason = "one arm per message, each a call into the owner's state"
)]
fn apply_message(state: &mut OwnerState, message: Message, now_unix_nanos: i64) -> Vec<Effect> {
    if state.phase == Phase::Recovering {
        return state.recovering(message);
    }
    match message {
        // The idle tick is armed from the launch, not the start mark: a
        // start mark the database keeps not answering is judged by the
        // owner's liveness like any other write, and the Run deadline alone
        // does not end an owner still starting.
        Message::Launch => {
            state.phase = Phase::Starting;
            vec![
                Effect::Start,
                arm_at(
                    Timer::RunDeadline,
                    state.limits.deadline_at_unix_nanos,
                    now_unix_nanos,
                ),
                state.arm_idle_tick(),
            ]
        }
        // While the owner interrupts its Run, the chunk and the start mark
        // still in flight may answer and the Program may end: none of it
        // changes what is written. The Interrupted record alone retires it.
        // The Interrupted record this owner wrote ended its Run; a record
        // another writer made first did not.
        Message::Interrupted(recorded) if state.phase == Phase::Interrupting => {
            state.retire(recorded.is_ok())
        }
        Message::Started(_)
        | Message::ProgramEnded(_)
        | Message::Committed
        | Message::RequestRecorded { .. }
        | Message::CommitRejected(_)
        | Message::Completed(_)
        | Message::SettleAttempted(_)
        | Message::Timer(_)
        | Message::Parked(_)
        | Message::Changed
        | Message::Prechecked { .. }
        | Message::Interrupted(_)
            if state.phase == Phase::Interrupting =>
        {
            Vec::new()
        }
        // Only a recovered owner ends its Run on a queued end.
        Message::Interrupted(_) | Message::Recover => Vec::new(),
        Message::Started(Ok(Some(started))) => state.started(started, now_unix_nanos),
        // The terminal write refused a Run with a nonterminal Invocation (or
        // failed). Work that arrived meanwhile may be that Invocation's end,
        // and a cancellation parked meanwhile ends it: record them and write
        // the end again; otherwise retire and leave the Run to recovery.
        Message::Completed(Err(_)) => {
            state.phase = Phase::Ending;
            let mut effects = state.unpark(now_unix_nanos);
            if state.pending_chunks() > 0 {
                effects.extend(state.advance_ending());
            } else {
                effects.extend(state.retire(false));
            }
            effects
        }
        Message::Started(Ok(None) | Err(_)) => state.retire(false),
        Message::Completed(Ok(None)) => state.retire(true),
        Message::Completed(Ok(Some(shutdown))) => state.settle_recorded(shutdown, now_unix_nanos),
        Message::SettleAttempted(Ok(true)) => {
            state.settling = false;
            state.retire(true)
        }
        Message::SettleAttempted(Ok(false)) => {
            state.settling = false;
            state.settle_when_free(true)
        }
        // The settle gave a definite answer other than the Run's end: the
        // recorded termination is the recovery source.
        Message::SettleAttempted(Err(_)) => {
            state.settling = false;
            state.retire(false)
        }
        Message::Emit {
            value,
            location,
            reply,
        } => state.output(
            OwnedEventKind::Emitted(value.into_encoded_json(), location),
            reply,
            now_unix_nanos,
        ),
        Message::Log {
            level,
            message,
            location,
            reply,
        } => state.output(
            OwnedEventKind::Logged(level, message, location),
            reply,
            now_unix_nanos,
        ),
        Message::Admit { work, reply } => state.admit(work, reply, now_unix_nanos),
        Message::InvocationResult { bytes, work, reply } => {
            state.invocation_result(bytes, work, reply, now_unix_nanos)
        }
        Message::Work(work) => state.work(work),
        Message::Fence(request) => state.fence(request, now_unix_nanos),
        Message::DispatchEnded => state.dispatch_ended(now_unix_nanos),
        Message::Decide { request, reply } => state.decide(request, reply, now_unix_nanos),
        Message::Cancel { requester, reply } => state.cancel(requester, reply, now_unix_nanos),
        Message::Timer(timer) => state.timer(timer, now_unix_nanos),
        Message::Parked(parked) => state.park(parked, now_unix_nanos),
        Message::Changed => state.changed(),
        Message::Prechecked { check, found } => state.prechecked(check, found),
        Message::ProgramEnded(completion) => state.program_ended(completion),
        Message::Committed => state.committed(now_unix_nanos),
        Message::RequestRecorded { key, answer } => {
            state.request_recorded(&key, answer);
            Vec::new()
        }
        Message::CommitRejected(rejection) => state.rejected(rejection, now_unix_nanos),
        Message::Stop => state.stop(),
        Message::Drained => state.commit_when_free(),
    }
}
