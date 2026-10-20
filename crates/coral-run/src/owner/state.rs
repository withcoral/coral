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
//! - what a lost commit means;
//! - the owner's liveness. After one lease without a successful commit the
//!   owner is suspect and answers Program requests only once recorded.
//!   After three it gives its Run up: the Program ends, and one terminal
//!   chunk is written as soon as the database answers, recording
//!   `Interrupted`, or `TimedOut` when the Run deadline has passed meanwhile.

use std::collections::VecDeque;

use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::invocation::{FenceBatch, FenceRequest};
use crate::run_repository::{OwnedEvent, OwnedEventKind};
use crate::termination::ShutdownAction;
use crate::{CoralFailure, Error};

use super::chunk::{ChunkBuild, ChunkItem, ChunkWork};
use super::handle::{CommitRejection, Message, Reply};

/// What the hosting task does for the owner.
#[derive(Debug)]
pub(crate) enum Effect {
    /// Answer one Program request.
    Reply(Reply, Result<(), Error>),
    /// Commit the owner's first chunk: the execution-start mark.
    Start,
    /// Execute the accepted package.
    StartProgram(ProgramPackage),
    /// Commit one chunk; `output_total` is the Run Output Budget total it records.
    Commit {
        chunk: ChunkBuild,
        output_total: usize,
    },
    /// Terminate the Program and wake every stop-aware wait.
    TerminateProgram,
    /// Settle row work the owner will never commit.
    Abandon(Box<dyn ChunkWork>, Error),
    /// Write the Run's terminal state for the Program's completion.
    CompleteExecution(ExecutionCompletion),
    /// Settle the recorded termination: end what is still nonterminal within
    /// the shutdown grace. The owner keeps recording dispatch results meanwhile.
    Settle(ShutdownAction),
    /// The owner became suspect: no commit succeeded for longer than the
    /// suspect bound. Program requests now answer only once recorded.
    Suspect { last_commit_age_nanos: i64 },
    /// The owner interrupts its Run: no commit succeeded for longer than the
    /// self-interrupt bound. The Program is terminated with its dispatches,
    /// and the Interrupted chunk is written as soon as the database answers.
    Interrupt { last_commit_age_nanos: i64 },
    /// The owner is done.
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
    /// How long after its last successful commit the owner is suspect.
    pub(crate) suspect_after_nanos: i64,
    /// How long after its last successful commit the owner interrupts its Run.
    pub(crate) self_interrupt_after_nanos: i64,
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
    Retired,
}

struct InFlight {
    bytes: usize,
    waiters: Vec<Reply>,
}

/// One Run Owner's memory.
pub(crate) struct OwnerState {
    phase: Phase,
    limits: OwnerLimits,
    /// No more Program requests: an outside writer asked execution to stop.
    stop_requested: bool,
    /// The recorded termination is settled: the Run is terminal, nothing more
    /// commits, and the owner retires once the commit in flight has answered.
    settled: bool,
    /// The Program's completion, kept until its terminal write has answered.
    completion: Option<ExecutionCompletion>,
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
    /// Invocations whose dispatch the owner started and has not seen end:
    /// the count the Fan-out Limit bounds. Owner memory only; a recovered
    /// owner never dispatches, so nothing recomputes it from rows.
    dispatching: usize,
    /// Ready Invocations above the Fan-out Limit, in readiness order (their
    /// mailbox arrival order). Each one stays `pending`, with no Event, and
    /// joins a fence batch when an earlier dispatch ends.
    ready: VecDeque<Box<FenceRequest>>,
    open: ChunkBuild,
    closed: VecDeque<ChunkBuild>,
    in_flight: Option<InFlight>,
}

impl OwnerState {
    /// An owner for a Run accepted at `accepted_at_unix_nanos`: acceptance,
    /// which wrote the claim, counts as its first successful commit.
    pub(crate) fn new(limits: OwnerLimits, accepted_at_unix_nanos: i64) -> Self {
        Self {
            phase: Phase::Starting,
            limits,
            stop_requested: false,
            settled: false,
            completion: None,
            output_total: 0,
            recorded_total: 0,
            last_commit_at_unix_nanos: accepted_at_unix_nanos,
            suspect: false,
            dispatching: 0,
            ready: VecDeque::new(),
            open: ChunkBuild::default(),
            closed: VecDeque::new(),
            in_flight: None,
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

    /// Invocations whose dispatch the owner started and has not seen end.
    #[cfg(test)]
    pub(crate) const fn dispatching(&self) -> usize {
        self.dispatching
    }

    /// Ready Invocations waiting for a slot under the Fan-out Limit.
    #[cfg(test)]
    pub(crate) fn waiting_for_a_slot(&self) -> usize {
        self.ready.len()
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
        effects.push(Effect::Interrupt {
            last_commit_age_nanos,
        });
        effects
    }

    /// Stops the Program and settles everything it waits on with the error
    /// `lost` makes: the request on the chunk in flight, every pending
    /// request and work item. The caller decides the phase that follows.
    fn stop_pending(&mut self, lost: impl Fn() -> Error) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(flight) = self.in_flight.take() {
            effects.extend(
                flight
                    .waiters
                    .into_iter()
                    .map(|reply| Effect::Reply(reply, Err(lost()))),
            );
        }
        effects.extend(self.abandon_pending(&lost));
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
        !self.settled
            && matches!(
                self.phase,
                Phase::Running | Phase::Ending | Phase::Completing | Phase::Settling
            )
    }

    fn output_fits(&self, bytes: usize) -> bool {
        self.output_total
            .checked_add(bytes)
            .is_some_and(|total| total <= self.limits.run_output_bytes)
    }

    fn push(&mut self, item: ChunkItem, events: usize, bytes: usize) {
        if !self.open.fits(events, bytes) {
            self.close_open();
        }
        self.open.push(item, events, bytes);
        if self.open.is_full() {
            self.close_open();
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

    /// An upstream result ends the dispatch that produced it: whatever the
    /// result's own fate, its slot under the Fan-out Limit goes to the
    /// Invocation that has waited longest, whose fence joins the chunk
    /// after this row's end.
    fn invocation_result(
        &mut self,
        bytes: usize,
        work: Box<dyn ChunkWork>,
        reply: Reply,
        now_unix_nanos: i64,
    ) -> Vec<Effect> {
        let mut effects = if !self.accepts_work() {
            vec![
                Effect::Abandon(work, Error::ProgramHostTerminated),
                Effect::Reply(
                    reply,
                    Err(Error::InvocationPersistence(
                        "Program Run lost Invocation completion".to_string(),
                    )),
                ),
            ]
        } else if !self.output_fits(bytes) {
            vec![
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
            ]
        } else {
            self.output_total = self.output_total.saturating_add(bytes);
            self.push(ChunkItem::Work(work), 1, bytes);
            self.answer(reply)
        };
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
        if self.dispatching >= self.limits.fan_out_invocations {
            self.ready.push_back(request);
            return Vec::new();
        }
        self.dispatching += 1;
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
        self.dispatching = self.dispatching.saturating_sub(1);
        if !self.accepts_program_requests(now_unix_nanos) {
            return self.abandon_ready(|| Error::ProgramHostTerminated);
        }
        match self.ready.pop_front() {
            Some(request) => self.fence(request, now_unix_nanos),
            None => Vec::new(),
        }
    }

    /// Answers every Invocation waiting for a slot, as one batch, with the
    /// error `lost` makes: no dispatch of theirs ever starts.
    fn abandon_ready(&mut self, lost: impl Fn() -> Error) -> Vec<Effect> {
        let mut waiting = self.ready.drain(..);
        let Some(first) = waiting.next() else {
            return Vec::new();
        };
        let mut batch = FenceBatch::new(*first);
        for request in waiting {
            batch.push(*request);
        }
        vec![Effect::Abandon(Box::new(batch), lost())]
    }

    /// A stop reached the owner: every fence request not yet committed, and
    /// every one waiting for a slot, is answered at once as terminated, so no
    /// dispatch starts after it. A fence in flight returns on its own; the
    /// pre-send check ends what it permitted.
    fn stop(&mut self) -> Vec<Effect> {
        self.stop_requested = true;
        let mut effects = Vec::new();
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
        effects.extend(self.abandon_ready(|| Error::ProgramHostTerminated));
        effects.push(Effect::TerminateProgram);
        effects
    }

    /// Commit-when-free: the oldest pending chunk, once nothing is in flight.
    fn commit_when_free(&mut self) -> Vec<Effect> {
        if self.in_flight.is_some()
            || self.settled
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

    fn commit(&mut self, mut chunk: ChunkBuild) -> Vec<Effect> {
        let output_total = self.recorded_total.saturating_add(chunk.bytes);
        self.in_flight = Some(InFlight {
            bytes: chunk.bytes,
            waiters: std::mem::take(&mut chunk.waiters),
        });
        vec![Effect::Commit {
            chunk,
            output_total,
        }]
    }

    fn committed(&mut self, now_unix_nanos: i64) -> Vec<Effect> {
        self.commit_succeeded(now_unix_nanos);
        let mut effects = Vec::new();
        if let Some(flight) = self.in_flight.take() {
            self.recorded_total = self.recorded_total.saturating_add(flight.bytes);
            effects.extend(
                flight
                    .waiters
                    .into_iter()
                    .map(|reply| Effect::Reply(reply, Ok(()))),
            );
        }
        match self.phase {
            Phase::Settling if self.settled => effects.extend(self.retire()),
            Phase::Running | Phase::Settling => effects.extend(self.commit_when_free()),
            Phase::Ending => effects.extend(self.advance_ending()),
            Phase::Starting
            | Phase::Completing
            | Phase::Stopped(_)
            | Phase::Interrupting
            | Phase::Retired => {}
        }
        effects
    }

    /// Retires the owner: whatever is still pending will never commit.
    fn retire(&mut self) -> Vec<Effect> {
        self.phase = Phase::Retired;
        let mut effects = self.abandon_pending(|| Error::ProgramHostTerminated);
        effects.push(Effect::Retire);
        effects
    }

    /// Settles every pending request and work item, and every Invocation
    /// waiting for a slot, with the error `lost` makes.
    fn abandon_pending(&mut self, lost: impl Fn() -> Error) -> Vec<Effect> {
        let mut effects = self.abandon_ready(&lost);
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
            vec![Effect::CompleteExecution(completion.clone())]
        } else {
            self.retire()
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
                effects.extend(self.abandon_ready(|| Error::ProgramHostTerminated));
                effects.extend(self.advance_ending());
                effects
            }
            // A refused or failed commit leaves the terminal write to decide
            // (the deadline, a released claim, an interruption); a lost or
            // ended Run has nothing left to write, and a corrupt one gets
            // nothing more written.
            Phase::Stopped(CommitRejection::Refused | CommitRejection::Failed) => {
                self.phase = Phase::Completing;
                vec![Effect::CompleteExecution(completion)]
            }
            Phase::Stopped(
                CommitRejection::ClaimMismatch
                | CommitRejection::RunEnded
                | CommitRejection::Corrupt,
            ) => self.retire(),
            Phase::Starting
            | Phase::Ending
            | Phase::Completing
            | Phase::Settling
            | Phase::Interrupting
            | Phase::Retired => Vec::new(),
        }
    }

    /// A lost or refused commit stops the owner at once: the Program is
    /// terminated, nothing pending commits, and every waiting request learns
    /// its outcome is unknown.
    fn rejected(&mut self, cause: CommitRejection) -> Vec<Effect> {
        let mut effects = self.stop_pending(|| cause.lost());
        match self.phase {
            Phase::Running => self.phase = Phase::Stopped(cause),
            Phase::Ending => {
                if let (CommitRejection::Refused | CommitRejection::Failed, Some(completion)) =
                    (cause, &self.completion)
                {
                    self.phase = Phase::Completing;
                    effects.push(Effect::CompleteExecution(completion.clone()));
                } else {
                    effects.extend(self.retire());
                }
            }
            Phase::Settling if self.settled => effects.extend(self.retire()),
            // The settle in progress ends the Run; the owner retires with it.
            Phase::Starting
            | Phase::Settling
            | Phase::Completing
            | Phase::Stopped(_)
            | Phase::Interrupting
            | Phase::Retired => {}
        }
        effects
    }

    fn idle_tick(&mut self) -> Vec<Effect> {
        if self.phase != Phase::Running
            || self.stop_requested
            || self.in_flight.is_some()
            || self.pending_chunks() > 0
        {
            return Vec::new();
        }
        self.commit(ChunkBuild::default())
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

fn apply_message(state: &mut OwnerState, message: Message, now_unix_nanos: i64) -> Vec<Effect> {
    match message {
        Message::Launch => {
            state.phase = Phase::Starting;
            vec![Effect::Start]
        }
        // While the owner interrupts its Run, the chunk and the start mark
        // still in flight may answer and the Program may end: none of it
        // changes what is written. The Interrupted record alone retires it.
        Message::Interrupted(_) if state.phase == Phase::Interrupting => state.retire(),
        Message::Started(_)
        | Message::ProgramEnded(_)
        | Message::Committed
        | Message::CommitRejected(_)
        | Message::Completed(_)
        | Message::Settled
        | Message::Interrupted(_)
            if state.phase == Phase::Interrupting =>
        {
            Vec::new()
        }
        Message::Interrupted(_) => Vec::new(),
        Message::Started(Ok(Some(package))) => {
            state.phase = Phase::Running;
            state.commit_succeeded(now_unix_nanos);
            vec![Effect::StartProgram(package)]
        }
        // The terminal write refused a Run with a nonterminal Invocation (or
        // failed). Work that arrived meanwhile may be that Invocation's end:
        // record it and write the end again; otherwise retire and leave the
        // Run to recovery.
        Message::Completed(Err(_)) if state.pending_chunks() > 0 => {
            state.phase = Phase::Ending;
            state.advance_ending()
        }
        Message::Started(Ok(None) | Err(_)) | Message::Completed(Ok(None) | Err(_)) => {
            state.retire()
        }
        Message::Settled => {
            state.settled = true;
            if state.in_flight.is_some() {
                // The chunk in flight answers its waiters first.
                Vec::new()
            } else {
                state.retire()
            }
        }
        Message::Emit { value, reply } => state.output(
            OwnedEventKind::Emitted(value.into_encoded_json()),
            reply,
            now_unix_nanos,
        ),
        Message::Log {
            level,
            message,
            reply,
        } => state.output(
            OwnedEventKind::Logged(level, message),
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
        Message::ProgramEnded(completion) => state.program_ended(completion),
        Message::Committed => state.committed(now_unix_nanos),
        Message::CommitRejected(rejection) => state.rejected(rejection),
        Message::Completed(Ok(Some(shutdown))) => {
            state.phase = Phase::Settling;
            state.completion = None;
            let mut effects = vec![Effect::Settle(shutdown)];
            effects.extend(state.commit_when_free());
            effects
        }
        Message::Stop => state.stop(),
        Message::IdleTick => state.idle_tick(),
        Message::Drained => state.commit_when_free(),
    }
}
