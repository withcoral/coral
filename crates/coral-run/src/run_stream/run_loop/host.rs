//! The task that hosts one Run stream's loop: it drains the mailbox, calls
//! [`apply`], and runs the effects, holding the chunk commit, the terminal
//! write and the timers the loop arms,
//! each a sleep on this process's clock that fires its [`Message::Timer`]
//! into the mailbox. While a connection wait is parked it listens to its
//! Coral Instance's wake-up reader, and it runs the snapshot checks of the
//! parked waits, whose findings come back as [`Message::Prechecked`].
//!
//! Every write the loop starts ends on an answer, never on the clock, unless
//! the loop retires first: then nothing is left to tell, and the reread
//! stops. A chunk commit the database does not answer is resolved by rereading the Run
//! row under the loop's claim: landed (the row carries the commit time and
//! the watermark this very attempt wrote), fenced (another claim, or an
//! ended Run), moved (another writer moved the head: the chunk is rebuilt on
//! it), not landed (recommit the chunk exactly as built), or corrupt. The
//! terminal write retries the same way.
//!
//! No chunk commit holds the Run row: the commit's predicate on the claim and
//! the head the loop tracks in memory is the fence, and the loop runs its
//! writes one at a time (ADR 0027).

use std::collections::HashMap;

use coral_telemetry::safe;
use coral_workspace::AuthenticatedPrincipal;
use std::sync::Arc;

use crate::execution::{Backoff, until_answered};
use crate::invocation::{EnforcementBatch, PrecheckBatch, ToolInvoker};
use crate::lifecycle::{RunRecord, RunTerminationKind};
use crate::run_repository::{self, Chunk, PendingEvent};
use crate::run_stream::StreamRun;
use crate::run_stream::run_loop::ChunkWork as _;
use crate::run_stream::run_loop::NO_DEADLINE;
use crate::trace::{Phase, phase_span};
use crate::types::AcceptedRun;
use crate::{Error, RunModule, nanos_duration};

use super::chunk::{ChunkBuild, ChunkItem};
use super::handle::{CommitRejection, LoopHandle, LoopRegistration, Mailbox, Message, Timer};
use super::state::{Effect, LoopLimits, LoopState, apply};

/// Hosts the loop of `accepted`, whose Program runs on `start`, until it
/// retires. Its start landed before the loop exists and is queued in its
/// mailbox.
pub(crate) async fn host(
    module: RunModule,
    accepted: AcceptedRun,
    start: Arc<StreamRun>,
    handle: LoopHandle,
    mut mailbox: Mailbox,
    registration: LoopRegistration,
) {
    #[cfg(test)]
    let _attempt = ExecutionAttemptObservation(module.execution_observer.clone());
    let (mut state, mut effects) = begin(&module, &accepted, &start);
    let mut timers = Timers::default();
    let mut subscription = Subscription::default();
    loop {
        let mut retired = false;
        for effect in effects.drain(..) {
            match effect {
                Effect::Retire => retired = true,
                Effect::Commit {
                    chunk,
                    output_total,
                } => spawn_commit(&module, &accepted, &handle, &start, chunk, output_total),
                Effect::ArmTimer { timer, after_nanos } => {
                    #[cfg(test)]
                    module.execution_observer.timer_armed(&timer, after_nanos);
                    timers.arm(&handle, timer, after_nanos);
                }
                Effect::Subscribe => subscription.subscribe(&module, &handle),
                Effect::Unsubscribe => subscription.unsubscribe(),
                Effect::TerminateProgram => terminate_program(&handle, &start, &state),
                other => run_effect(&module, &accepted, &start, &handle, other),
            }
        }
        if retired {
            break;
        }
        // The handle outlives the loop, so the mailbox never closes first.
        let Some(message) = mailbox.recv().await else {
            break;
        };
        if matches!(message, Message::Timer(_)) {
            timers.fired();
        }
        #[cfg(test)]
        module.execution_observer.loop_message_dequeued();
        // A clock that fails is an operational failure like a failed commit:
        // the loop stops first, then still applies the message it dequeued,
        // so a Program that ends under a broken clock still reaches its
        // terminal write (which fails on the same clock and retires the loop).
        effects = Vec::new();
        let now = match module.now() {
            Ok(now) => now.max(accepted.accepted_at_unix_nanos),
            Err(_clock) => {
                effects = apply(
                    &mut state,
                    Message::CommitRejected(CommitRejection::Failed),
                    accepted.accepted_at_unix_nanos,
                );
                accepted.accepted_at_unix_nanos
            }
        };
        effects.extend(apply(&mut state, message, now));
        // Apply everything that arrived meanwhile, in arrival order; then
        // commit when free.
        while let Ok(next) = mailbox.try_recv() {
            effects.extend(apply(&mut state, next, now));
        }
        effects.extend(apply(&mut state, Message::Drained, now));
        // After the batch and before its effects run: a chunk the batch
        // started is published in flight before it can land.
        publish_head(&module, &accepted, &handle, &state);
    }
    drop(timers);
    drop(subscription);
    drop(registration);
}

/// Publishes what the wake-up reader compares with the Run row. A loop that
/// begins listening has the reader read at once: a writer may have moved or
/// ended its Run before it registered.
fn publish_head(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &LoopHandle,
    state: &LoopState,
) {
    if handle.publish_head(state.head(), state.chunk_in_flight(), state.listens()) {
        module.wakes.wake_for(&accepted.id);
    }
}

/// Terminates the Program and wakes every stop-aware wait; a Run Executor
/// learns why on the Run stream, once a termination written first has
/// landed ([`spawn_stream_end`]).
fn terminate_program(handle: &LoopHandle, start: &StreamRun, state: &LoopState) {
    if state.terminated_as() == Some(RunTerminationKind::Canceled) {
        handle.stop().request_canceled();
    } else {
        handle.stop().request();
    }
    if !state.ends_stream_first() {
        start.link.terminate(state.terminated_as());
    }
}

/// The loop's memory as it begins, rebuilt from what the stream's start
/// resumed, and its first effects.
fn begin(
    module: &RunModule,
    accepted: &AcceptedRun,
    start: &StreamRun,
) -> (LoopState, Vec<Effect>) {
    let limits = LoopLimits {
        run_output_bytes: accepted.value_limits.run_output_bytes(),
        applied_cap_bytes: module.applied_output_cap.bytes(),
        fan_out_invocations: module.fan_out_limit.invocations(),
        // The start, queued in the mailbox, records the deadline.
        deadline_at_unix_nanos: NO_DEADLINE,
        precheck_interval_nanos: module.execution_policy.precheck_interval_nanos(),
        wake_gap_nanos: module.execution_policy.wake_gap_nanos(),
    };
    let resumed = start
        .resume
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .map_or((0, 0), |resume| (resume.watermark, resume.output_total));
    let state = LoopState::for_run_stream(limits, resumed);
    (state, Vec::new())
}

/// What the loop listens to while a connection wait is parked: its Coral
/// Instance's wake-up reader, which relays each ring of the change signal.
/// The loop subscribes to nothing of its own.
#[derive(Default)]
struct Subscription {
    listening: Option<(crate::run_wake::RunWakes, LoopHandle)>,
}

impl Subscription {
    fn subscribe(&mut self, module: &RunModule, handle: &LoopHandle) {
        self.unsubscribe();
        module.wakes.listen(handle);
        self.listening = Some((module.wakes.clone(), handle.clone()));
    }

    fn unsubscribe(&mut self) {
        if let Some((wakes, handle)) = self.listening.take() {
            wakes.unlisten(&handle);
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

/// Runs one snapshot check of parked waits; its findings reach the loop as
/// [`Message::Prechecked`].
fn spawn_precheck(
    handle: &LoopHandle,
    batch: PrecheckBatch,
    #[cfg(test)] observer: std::sync::Arc<dyn crate::execution::ExecutionObserver>,
) {
    let handle = handle.clone();
    tokio::spawn(async move {
        let check = batch.check();
        let found = batch.run().await;
        if let Err(error) = &found {
            tracing::debug!(
                coral.run.id = safe::id(handle.held_id()),
                error.type = safe::kind(error.code()),
                "The loop's check of its parked waits found nothing: the next one reads again"
            );
        }
        #[cfg(test)]
        observer.precheck_finished(check, found.as_ref().map_or(0, Vec::len));
        handle.send(Message::Prechecked { check, found });
    });
}

/// The timers the loop armed, one sleep each on this process's clock.
/// Arming a timer
/// again replaces its sleep; a sleep that ended is forgotten at the next
/// arm or fire (its message may be dequeued a moment before the task that
/// sent it ends, and a message from a sleep since replaced must not forget
/// the sleep that replaced it); retiring the loop aborts the rest.
#[derive(Default)]
struct Timers(HashMap<Timer, tokio::task::JoinHandle<()>>);

impl Timers {
    fn arm(&mut self, handle: &LoopHandle, timer: Timer, after_nanos: i64) {
        self.prune();
        let fire = handle.clone();
        let fired = timer.clone();
        let sleep = tokio::spawn(async move {
            tokio::time::sleep(nanos_duration(after_nanos)).await;
            fire.send(Message::Timer(fired));
        });
        if let Some(replaced) = self.0.insert(timer, sleep) {
            replaced.abort();
        }
    }

    /// A `Message::Timer` was dequeued.
    fn fired(&mut self) {
        self.prune();
    }

    /// Forgets every sleep that ended: it fired, or was replaced.
    fn prune(&mut self) {
        self.0.retain(|_, sleep| !sleep.is_finished());
    }
}

impl Drop for Timers {
    fn drop(&mut self) {
        for sleep in self.0.values() {
            sleep.abort();
        }
    }
}

/// Runs every effect but the commit, the timers, the wake-up
/// listening and the retirement, which the loop owns.
/// A write retried past the loop's retirement answers a mailbox nobody
/// reads.
fn run_effect(
    module: &RunModule,
    accepted: &AcceptedRun,
    start: &Arc<StreamRun>,
    handle: &LoopHandle,
    effect: Effect,
) {
    match effect {
        Effect::Reply(reply, result) => {
            // The requester may have stopped waiting; the loop's decision stands.
            let _delivered = reply.send(result);
        }
        Effect::Precheck(batch) => spawn_precheck(
            handle,
            batch,
            #[cfg(test)]
            module.execution_observer.clone(),
        ),
        Effect::StartProgram {
            deadline_at_unix_nanos,
        } => start_program(module, accepted, start, handle, deadline_at_unix_nanos),
        Effect::Wake(parked) => parked.wake(),
        Effect::Abandon(work, error) => work.settle(Err(error)),
        Effect::CompleteExecution { completion } => {
            let module = module.clone();
            let accepted = accepted.clone();
            let handle = handle.clone();
            let span = phase_span(
                accepted.trace.as_ref(),
                &accepted.held_id(),
                Phase::Complete,
            );
            tokio::spawn(span.instrument(async move {
                let completed = until_answered(
                    &accepted.held_id(),
                    "terminal write",
                    || !handle.is_retired(),
                    || module.complete_execution(&accepted, &completion),
                )
                .await;
                handle.send(Message::Completed(completed));
            }));
        }
        Effect::EndStreamRun(termination) => {
            spawn_stream_end(module, accepted, start, handle, termination);
        }
        Effect::MoveStream => move_stream(start, handle),
        Effect::Commit { .. }
        | Effect::TerminateProgram
        | Effect::ArmTimer { .. }
        | Effect::Subscribe
        | Effect::Unsubscribe
        | Effect::Retire => {
            debug_assert!(
                false,
                "the host loop runs commits, timers, the subscription and retirement itself"
            );
        }
    }
}

/// Moves the Run's stream in the Shutdown Drain. The answers the detach
/// releases never reach the Program: the Coral Instance the stream reopens
/// on answers what it waits on.
fn move_stream(start: &StreamRun, handle: &LoopHandle) {
    start.link.start_moving();
    handle.detach();
}

/// Starts the Program on the Run stream.
fn start_program(
    module: &RunModule,
    accepted: &AcceptedRun,
    start: &Arc<StreamRun>,
    handle: &LoopHandle,
    deadline_at_unix_nanos: i64,
) {
    crate::run_stream::start_program(
        module,
        accepted,
        start,
        handle,
        invoker(
            module,
            accepted,
            &start.actor,
            handle,
            deadline_at_unix_nanos,
        ),
        deadline_at_unix_nanos,
    );
}

/// The Tool invoker of the Program `actor` runs for the loop of `accepted`.
fn invoker(
    module: &RunModule,
    accepted: &AcceptedRun,
    actor: &AuthenticatedPrincipal,
    handle: &LoopHandle,
    deadline_at_unix_nanos: i64,
) -> ToolInvoker {
    ToolInvoker::new(
        module.accounts.clone(),
        module.providers.clone(),
        module.policies.clone(),
        module.provider_renamer.clone(),
        module.search.clone(),
        module.private_tools.clone(),
        module.sub_agents.clone(),
        module.human_interface.clone(),
        module.database.clone(),
        module.workspaces.clone(),
        actor.clone(),
        accepted,
        deadline_at_unix_nanos,
        module.execution_policy.approval_wait_nanos(),
        handle.clone(),
        module.upstream.clone(),
        module.clock.clone(),
        #[cfg(test)]
        module.invocation_observer.clone(),
    )
}

/// A Run stream's termination, written in one chunk until the database
/// answers; the Program hears of it on the stream only then, and the loop
/// retires. A definite failure means another writer ended or took the Run
/// first, and the stream's `Ended` tells the Program.
fn spawn_stream_end(
    module: &RunModule,
    accepted: &AcceptedRun,
    start: &StreamRun,
    handle: &LoopHandle,
    termination: crate::lifecycle::RequestedTermination,
) {
    let module = module.clone();
    let accepted = accepted.clone();
    let handle = handle.clone();
    let link = start.link.clone();
    let span = phase_span(
        accepted.trace.as_ref(),
        &accepted.held_id(),
        Phase::Terminate,
    );
    tokio::spawn(span.instrument(async move {
        let ended = module
            .record_interruption(
                &accepted.workspace_id,
                &accepted.held_id(),
                &accepted.claim_id,
                Some(&termination),
                crate::termination::Interrupter::Loop,
            )
            .await;
        if let Err(error) = &ended {
            tracing::warn!(
                coral.run.id = safe::id(&accepted.held_id()),
                error.type = safe::kind(error.code()),
                "a Run stream's termination was not written: the Run is no longer its own"
            );
        }
        if ended.is_ok() {
            link.terminate(Some(termination.termination.kind));
        }
        handle.send(Message::Completed(Ok(())));
    }));
}

fn spawn_commit(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &LoopHandle,
    start: &StreamRun,
    chunk: ChunkBuild,
    output_total: usize,
) {
    let module = module.clone();
    let accepted = accepted.clone();
    let handle = handle.clone();
    let link = start.link.clone();
    tokio::spawn(async move {
        let outcome = commit_until_answered(&module, &accepted, &handle, chunk, output_total).await;
        let message = match outcome {
            Some(Ok(request_number)) => {
                link.committed(request_number);
                Message::Committed
            }
            // Another writer ended the Run: its Program hears why in its
            // `Terminate`.
            Some(Err(CommitRejection::RunEnded)) => {
                let read = run_repository::find_exact(
                    &module.database,
                    &accepted.workspace_id,
                    &accepted.id,
                )
                .await;
                if let Ok(Some(run)) = read
                    && run.state.is_terminal()
                {
                    let kind = RunTerminationKind::of_ended(run.state);
                    handle.send(Message::EndedOutside(kind));
                }
                Message::CommitRejected(CommitRejection::RunEnded)
            }
            Some(Err(rejection)) => Message::CommitRejected(rejection),
            None => return,
        };
        handle.send(message);
    });
}

/// What the commit attempts so far wrote: enough to recognize the chunk on
/// the Run row when an attempt gets no answer.
#[derive(Debug, Clone, Copy)]
struct Written {
    /// The head the chunk is built on: the one the loop tracks.
    head_before: u64,
    /// The commit time the last attempt that got to write stamped, distinct
    /// from the one it found on the row, so a coarse clock cannot make the
    /// previous chunk's mark pass for this one.
    committed_at_unix_nanos: Option<i64>,
    /// The highest Run stream request number that attempt wrote.
    request_number: Option<u64>,
}

impl Written {
    /// Whether `run` carries the chunk: the commit time an attempt stamped
    /// is on the row with the watermark it wrote (another writer may stamp
    /// the same instant), and the head has moved past the one the chunk is
    /// built on.
    fn landed_on(self, run: &RunRecord) -> bool {
        self.committed_at_unix_nanos.is_some()
            && run.last_commit_at_unix_nanos == self.committed_at_unix_nanos
            && self.request_number.is_none_or(|number| {
                run.committed_request_number
                    .is_some_and(|committed| committed >= number)
            })
            && run.chunk_sequence > self.head_before
    }

    /// Whether another writer moved the head of a Run a Run Executor holds
    /// under this chunk's claim: the chunk is rebuilt on the new head.
    fn moved_on(self, run: &RunRecord, claim_id: &str) -> bool {
        run.run_executor_kind.is_some()
            && run.state == crate::RunState::Running
            && run.execution_claim_id.as_deref() == Some(claim_id)
            && run.chunk_sequence > self.head_before
    }
}

/// What a chunk records beside its items: the Run Output Budget total its
/// head update writes.
struct Records {
    output_total: usize,
    /// The highest Run stream request number the chunk commits.
    request_number: Option<u64>,
}

/// How one commit attempt ended.
enum Attempt {
    Landed,
    Rejected(CommitRejection),
    /// The database gave no answer.
    Unanswered(Error),
    /// Another writer moved the head of the Run a Run Executor holds, as
    /// the row read shows.
    Moved(Box<RunRecord>),
}

/// What the reread of the Run row says about an unanswered attempt.
enum Resolution {
    Landed,
    Rejected(CommitRejection),
    Recommit,
    Moved(Box<RunRecord>),
}

/// Why one attempt's transaction did not reach its commit.
enum CommitFailure {
    /// The Run row as read no longer takes this loop's chunks.
    Rejected(CommitRejection),
    Moved(Box<RunRecord>),
    Error(Error),
}

impl From<Error> for CommitFailure {
    fn from(error: Error) -> Self {
        Self::Error(error)
    }
}

/// Commits `chunk` until the database answers: the attempt, a reread after
/// an attempt without an answer (bounded backoff between them), a recommit
/// of the chunk exactly as built when the reread shows it did not land. Row
/// work learns the chunk's fate once, when it is known. `None`
/// once the loop retired while the chunk was still unanswered: the work is
/// dropped unsettled, which its waiters read as a lost loop.
async fn commit_until_answered(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &LoopHandle,
    chunk: ChunkBuild,
    output_total: usize,
) -> Option<Result<Option<u64>, CommitRejection>> {
    let chunk_bytes = chunk.bytes;
    let mut bytes = chunk_bytes;
    let mut items = chunk.items;
    let mut numbers = request_numbers(items.len(), chunk.numbered);
    let mut enforcement = chunk.enforcement;
    let mut records = Records {
        output_total,
        request_number: chunk.request_number,
    };
    let mut written = Written {
        head_before: chunk.head,
        committed_at_unix_nanos: None,
        request_number: None,
    };
    let held = accepted.held_id();
    let mut backoff = Backoff::new(&held, "chunk commit");
    let outcome = loop {
        let moved = match commit_once(
            module,
            accepted,
            &mut items,
            &numbers,
            &mut enforcement,
            &records,
            &mut written,
        )
        .await
        {
            Attempt::Landed => break Ok(()),
            Attempt::Rejected(rejection) => break Err(rejection),
            Attempt::Moved(run) => run,
            Attempt::Unanswered(error) => {
                let resolution =
                    reread_until_resolved(module, accepted, handle, written, &mut backoff, error)
                        .await;
                match resolution {
                    None => return None,
                    Some(Resolution::Landed) => break Ok(()),
                    Some(Resolution::Rejected(rejection)) => break Err(rejection),
                    Some(Resolution::Recommit) => continue,
                    Some(Resolution::Moved(run)) => run,
                }
            }
        };
        // The Run's stream moved to another loop: what this one has not
        // committed, the Run Executor sends that loop again.
        if moved.holder_instance_id.as_ref() != Some(&module.instance) || handle.is_detached() {
            break Err(CommitRejection::ClaimMismatch);
        }
        let Some(recorded_total) = rebase(
            &mut items,
            &mut numbers,
            &mut bytes,
            chunk_bytes,
            &mut records,
            &mut written,
            &moved,
        ) else {
            break Err(CommitRejection::Corrupt);
        };
        handle.send(Message::Rebased {
            head: written.head_before,
            recorded_total,
        });
    };
    if let Err(CommitRejection::Corrupt) = outcome {
        tracing::error!(
            coral.run.id = safe::id(&accepted.held_id()),
            coral.run.claim.id = crate::trace::claim(&accepted.claim_id)
                .as_ref()
                .map(safe::id),
            "Run Ledger head contradicts its loop's history; the loop stops and writes nothing more"
        );
    }
    if outcome.is_ok() {
        for item in &mut items {
            if let ChunkItem::Work(work) = item
                && let Some(parked) = work.take_parked()
            {
                handle.send(Message::Parked(parked));
            }
        }
    }
    for item in items {
        if let ChunkItem::Work(work) = item {
            work.settle(outcome.map_err(CommitRejection::lost));
        }
    }
    if let Some(batch) = enforcement {
        batch.settle(outcome.map_err(CommitRejection::lost));
    }
    Some(outcome.map(|()| written.request_number))
}

/// Each of `count` items' Run stream request number, from the items that
/// `numbered` names.
fn request_numbers(count: usize, numbered: Vec<(usize, u64)>) -> Vec<Option<u64>> {
    let mut numbers = vec![None; count];
    for (index, number) in numbered {
        if let Some(slot) = numbers.get_mut(index) {
            *slot = Some(number);
        }
    }
    numbers
}

/// Rebuilds the chunk on the head another writer left on `run`: the items of
/// the requests that writer committed are dropped (their work is recorded),
/// and the output total is the row's plus what remains of this chunk's
/// `bytes`. Returns the total the loop's record holds before this chunk
/// lands, which then adds the chunk's `original_bytes`.
fn rebase(
    items: &mut Vec<ChunkItem>,
    numbers: &mut Vec<Option<u64>>,
    bytes: &mut usize,
    original_bytes: usize,
    records: &mut Records,
    written: &mut Written,
    run: &RunRecord,
) -> Option<usize> {
    let covered = run.committed_request_number.unwrap_or(0);
    // Another writer committed an Invocation this loop admitted under the
    // same request: two rows would answer one call, so nothing more is
    // written. A loop whose stream moved never gets here: its chunk is
    // refused first.
    let admitted_elsewhere = items.iter().zip(numbers.iter()).any(|(item, number)| {
        matches!(item, ChunkItem::Work(_)) && number.is_some_and(|number| number <= covered)
    });
    if admitted_elsewhere {
        return None;
    }
    let mut kept = Vec::with_capacity(items.len());
    let mut kept_numbers = Vec::with_capacity(items.len());
    for (item, number) in items.drain(..).zip(numbers.drain(..)) {
        if number.is_some_and(|number| number <= covered) {
            if let ChunkItem::Event(event) = item {
                *bytes = bytes.saturating_sub(event.output_bytes());
            }
        } else {
            kept.push(item);
            kept_numbers.push(number);
        }
    }
    *items = kept;
    *numbers = kept_numbers;
    records.output_total = run.accumulated_output_bytes.saturating_add(*bytes);
    written.head_before = run.chunk_sequence;
    written.committed_at_unix_nanos = None;
    Some(records.output_total.saturating_sub(original_bytes))
}

/// Rereads the Run row after an attempt the database did not answer, backing
/// off between rereads that fail (a reread that fails is no answer either,
/// whatever its error), until the row says what the attempt did. `None` once
/// the loop retired meanwhile.
async fn reread_until_resolved(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &LoopHandle,
    written: Written,
    backoff: &mut Backoff<'_>,
    mut error: Error,
) -> Option<Resolution> {
    loop {
        backoff.wait(&error).await;
        if handle.is_retired() {
            return None;
        }
        let reread =
            run_repository::find_exact(&module.database, &accepted.workspace_id, &accepted.id)
                .await;
        match reread {
            Err(failed) => error = failed,
            Ok(None) => return Some(Resolution::Rejected(CommitRejection::ClaimMismatch)),
            Ok(Some(run)) => return Some(resolve(&run, &accepted.claim_id, written)),
        }
    }
}

/// Resolves an unanswered attempt from the Run row as reread.
fn resolve(run: &RunRecord, claim_id: &str, written: Written) -> Resolution {
    if written.landed_on(run) {
        return Resolution::Landed;
    }
    if written.moved_on(run, claim_id) {
        return Resolution::Moved(Box::new(run.clone()));
    }
    match refusal(run, claim_id, written.head_before) {
        Some(rejection) => Resolution::Rejected(rejection),
        None => Resolution::Recommit,
    }
}

/// Why the Run row as read no longer takes a chunk built on `head`, in the
/// order the evidence is judged: a terminal Run, another execution claim, a
/// row that refuses the loop's chunks, and a head below the one the chunk is
/// built on, which is corruption. `None` when the row takes the chunk.
fn refusal(run: &RunRecord, claim_id: &str, head: u64) -> Option<CommitRejection> {
    if run.state.is_terminal() {
        return Some(CommitRejection::RunEnded);
    }
    if run.execution_claim_id.as_deref() != Some(claim_id) {
        return Some(CommitRejection::ClaimMismatch);
    }
    if run.chunk_sequence < head {
        return Some(CommitRejection::Corrupt);
    }
    if !run.holds_loop_claim(claim_id) {
        return Some(CommitRejection::Refused);
    }
    if run.chunk_sequence != head {
        return Some(CommitRejection::Corrupt);
    }
    None
}

/// The chunk `run` takes, marked so a reread recognizes it: by its commit
/// time, which never repeats the instant on the row, with the watermark it
/// commits.
fn marked_chunk<'a>(
    run: &'a RunRecord,
    claim_id: &'a str,
    now: i64,
    records: &Records,
    written: &mut Written,
) -> Chunk<'a> {
    let committed_at = run.commit_mark(now);
    written.committed_at_unix_nanos = Some(committed_at);
    // Never below the watermark another writer left.
    written.request_number = records
        .request_number
        .map(|number| number.max(run.committed_request_number.unwrap_or(0)));
    let chunk = Chunk::new(run, Some(claim_id), committed_at);
    match written.request_number {
        Some(number) => chunk.request_number(number),
        None => chunk,
    }
}

/// One attempt to commit the chunk, in one transaction that holds no Run
/// row: the row work, then the enforcement point, then the head update
/// predicated on the loop's claim and the head it tracks. A recommit first checks whether the attempt it repeats
/// landed; a head update that matches no row is classified from the row read
/// again.
async fn commit_once(
    module: &RunModule,
    accepted: &AcceptedRun,
    items: &mut [ChunkItem],
    numbers: &[Option<u64>],
    enforcement: &mut Option<Box<EnforcementBatch>>,
    records: &Records,
    written: &mut Written,
) -> Attempt {
    let claim_id = accepted.claim_id.as_str();
    #[cfg(test)]
    module.execution_observer.before_chunk_commit().await;
    #[cfg(test)]
    if module.execution_observer.chunk_commit_unanswered().await {
        return Attempt::Unanswered(Error::Database(coral_db::Error::WriterBusy));
    }
    let outcome: Result<(), CommitFailure> = async {
        let mut transaction = module.database.begin().await.map_err(Error::from)?;
        let run = run_repository::find_exact_in_transaction(
            &mut transaction,
            &accepted.workspace_id,
            &accepted.id,
        )
        .await?
        .ok_or(Error::ClaimMismatch)?;
        if written.landed_on(&run) {
            // The attempt this one repeats landed between the reread and
            // this read.
            return Ok(());
        }
        // A head another writer moved, or a request it committed that this
        // chunk still carries (queued before the loop learned of that
        // writer), is rebuilt on the row as read.
        let refused = refusal(&run, claim_id, written.head_before);
        let covered = run.committed_request_number.unwrap_or(0);
        let carries_covered =
            refused.is_none() && numbers.iter().flatten().any(|number| *number <= covered);
        if written.moved_on(&run, claim_id) || carries_covered {
            return Err(CommitFailure::Moved(Box::new(run)));
        }
        let now = module.now()?.max(run.accepted_at_unix_nanos);
        if let Some(rejection) = refused {
            return Err(CommitFailure::Rejected(rejection));
        }
        if !run.accepts_loop_chunk(claim_id) {
            return Err(CommitFailure::Rejected(CommitRejection::Refused));
        }
        // Row work runs in arrival order; its Events take the work's place
        // among the applied ones.
        let mut produced = Vec::new();
        for item in items.iter_mut() {
            if let ChunkItem::Work(work) = item {
                produced.push(work.run(&mut transaction, &run, now).await?);
            }
        }
        // The enforcement point decides every ready Invocation of the chunk
        // together, after the row work; it writes no Event.
        if let Some(batch) = enforcement.as_mut() {
            batch.run(&mut transaction, &run, now).await?;
        }
        let mut produced_by_work = produced.iter();
        let mut events: Vec<&PendingEvent> = Vec::new();
        for item in items.iter() {
            match item {
                ChunkItem::Event(event) => events.push(event),
                ChunkItem::Work(_) => events.extend(produced_by_work.next().into_iter().flatten()),
            }
        }
        let chunk = marked_chunk(&run, claim_id, now, records, written)
            .pending_events(events)
            .output_bytes(records.output_total);
        run_repository::commit_chunk(&mut transaction, chunk).await?;
        transaction.commit().await.map_err(Error::from)?;
        #[cfg(test)]
        if module.execution_observer.chunk_commit_answer_lost().await {
            return Err(Error::Database(coral_db::Error::WriterBusy).into());
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Attempt::Landed,
        Err(CommitFailure::Rejected(rejection)) => Attempt::Rejected(rejection),
        Err(CommitFailure::Moved(run)) => Attempt::Moved(run),
        Err(CommitFailure::Error(error)) if error.is_operational() => Attempt::Unanswered(error),
        // The head update matched no row under the claim: a writer moved
        // the row between the read and the update. The row read again says
        // which writer it could be; an answer it does not give is an
        // unanswered attempt, resolved by the reread.
        Err(CommitFailure::Error(Error::HeadMismatch)) => {
            match run_repository::find_exact(&module.database, &accepted.workspace_id, &accepted.id)
                .await
            {
                Ok(Some(run)) if written.moved_on(&run, claim_id) => Attempt::Moved(Box::new(run)),
                Ok(Some(run)) => Attempt::Rejected(
                    refusal(&run, claim_id, written.head_before)
                        .unwrap_or(CommitRejection::Corrupt),
                ),
                Ok(None) => Attempt::Rejected(CommitRejection::ClaimMismatch),
                Err(error) => Attempt::Unanswered(error),
            }
        }
        Err(CommitFailure::Error(error)) => Attempt::Rejected(CommitRejection::from(error)),
    }
}

#[cfg(test)]
struct ExecutionAttemptObservation(std::sync::Arc<dyn crate::execution::ExecutionObserver>);

#[cfg(test)]
impl Drop for ExecutionAttemptObservation {
    fn drop(&mut self) {
        self.0.execution_attempt_finished();
    }
}
