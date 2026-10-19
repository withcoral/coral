//! The Run Owner's loop as a pure function of its state and one message.
//!
//! [`apply`] decides everything in memory: the Run Output Budget against the
//! owner's exact total, the Applied cap, how Events and row work fill chunks,
//! when a chunk commits, and what a lost commit means. It does no I/O; it
//! returns [`Effect`]s for the hosting task to run.

use std::collections::VecDeque;

use crate::engine::{ExecutionCompletion, ProgramPackage};
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
    /// The owner is done.
    Retire,
}

/// The limits one owner decides against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OwnerLimits {
    pub(crate) run_output_bytes: usize,
    pub(crate) applied_cap_bytes: usize,
    pub(crate) deadline_at_unix_nanos: i64,
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
    open: ChunkBuild,
    closed: VecDeque<ChunkBuild>,
    in_flight: Option<InFlight>,
}

impl OwnerState {
    pub(crate) fn new(limits: OwnerLimits) -> Self {
        Self {
            phase: Phase::Starting,
            limits,
            stop_requested: false,
            settled: false,
            completion: None,
            output_total: 0,
            recorded_total: 0,
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

    /// Answers at Applied, or at Recorded while the Applied cap is exceeded:
    /// the reply then rides the chunk holding the request's item.
    fn answer(&mut self, reply: Reply) -> Vec<Effect> {
        if self.unrecorded_bytes() <= self.limits.applied_cap_bytes {
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

    fn invocation_result(
        &mut self,
        bytes: usize,
        work: Box<dyn ChunkWork>,
        reply: Reply,
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
                    Error::InvocationRejected(CoralFailure::RunOutputTooLarge),
                ),
                Effect::Reply(
                    reply,
                    Err(Error::InvocationRejected(CoralFailure::RunOutputTooLarge)),
                ),
            ];
        }
        self.output_total = self.output_total.saturating_add(bytes);
        self.push(ChunkItem::Work(work), 1, bytes);
        self.answer(reply)
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

    fn committed(&mut self) -> Vec<Effect> {
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
            Phase::Starting | Phase::Completing | Phase::Stopped(_) | Phase::Retired => {}
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

    /// Settles every pending request and work item with the error `lost` makes.
    fn abandon_pending(&mut self, lost: impl Fn() -> Error) -> Vec<Effect> {
        let pending = self
            .closed
            .drain(..)
            .chain(std::iter::once(std::mem::take(&mut self.open)));
        let mut effects = Vec::new();
        for chunk in pending {
            for item in chunk.items {
                if let ChunkItem::Work(work) = item {
                    effects.push(Effect::Abandon(work, lost()));
                }
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
                // Wake every detached Invocation wait, as execution's end does.
                let mut effects = vec![Effect::TerminateProgram];
                effects.extend(self.advance_ending());
                effects
            }
            // A refused or failed commit leaves the terminal write to decide
            // (the deadline, a released claim, an interruption); a lost or
            // ended Run has nothing left to write.
            Phase::Stopped(CommitRejection::Refused | CommitRejection::Failed) => {
                self.phase = Phase::Completing;
                vec![Effect::CompleteExecution(completion)]
            }
            Phase::Stopped(CommitRejection::ClaimMismatch | CommitRejection::RunEnded) => {
                self.retire()
            }
            Phase::Starting
            | Phase::Ending
            | Phase::Completing
            | Phase::Settling
            | Phase::Retired => Vec::new(),
        }
    }

    /// A lost or refused commit stops the owner at once: the Program is
    /// terminated, nothing pending commits, and every waiting request learns
    /// its outcome is unknown.
    fn rejected(&mut self, cause: CommitRejection) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(flight) = self.in_flight.take() {
            effects.extend(
                flight
                    .waiters
                    .into_iter()
                    .map(|reply| Effect::Reply(reply, Err(cause.lost()))),
            );
        }
        effects.extend(self.abandon_pending(|| cause.lost()));
        self.stop_requested = true;
        effects.push(Effect::TerminateProgram);
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
    match message {
        Message::Launch => {
            state.phase = Phase::Starting;
            vec![Effect::Start]
        }
        Message::Started(Ok(Some(package))) => {
            state.phase = Phase::Running;
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
            state.invocation_result(bytes, work, reply)
        }
        Message::Work(work) => state.work(work),
        Message::ProgramEnded(completion) => state.program_ended(completion),
        Message::Committed => state.committed(),
        Message::CommitRejected(rejection) => state.rejected(rejection),
        Message::Completed(Ok(Some(shutdown))) => {
            state.phase = Phase::Settling;
            state.completion = None;
            let mut effects = vec![Effect::Settle(shutdown)];
            effects.extend(state.commit_when_free());
            effects
        }
        Message::Stop => {
            state.stop_requested = true;
            vec![Effect::TerminateProgram]
        }
        Message::IdleTick => state.idle_tick(),
        Message::Drained => state.commit_when_free(),
    }
}
