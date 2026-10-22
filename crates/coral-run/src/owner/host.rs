//! The task that hosts one Run Owner: it drains the mailbox, calls
//! [`apply`], and runs the effects, holding the chunk commit, the Program
//! execution, the terminal write, the settle of a recorded termination and
//! the timers the owner arms, each a sleep on this process's clock that
//! fires its [`Message::Timer`] into the mailbox (the idle tick, re-armed by
//! every commit, is one sleep reset in place and fires into the loop). It
//! also holds the change subscription the owner asks for while a connection
//! wait is parked, ringing [`Message::Changed`] into the mailbox, and runs the
//! snapshot checks of the parked waits, whose findings come back as
//! [`Message::Prechecked`].
//!
//! Every write the owner starts ends on an answer, never on the clock. A
//! chunk commit the database does not answer is resolved by rereading the Run
//! row under the owner's claim: landed (the row carries the lease this very
//! attempt renewed), fenced (another claim, or an ended Run), not landed
//! (recommit the chunk exactly as built), or corrupt (a head the owner did
//! not move). The start mark, the terminal write, the settle and the
//! Interrupted record retry the same way.
//!
//! The owner is the only writer of its nonterminal Run's ledger, so no
//! chunk commit holds the Run row: the commit's predicate on the claim and
//! the head the owner tracks in memory is the fence, and the owner runs its
//! writes one at a time.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;

use coral_workspace::AuthenticatedPrincipal;

use crate::execution::{Backoff, until_answered};
use crate::invocation::{FenceBatch, OperationInvoker, PrecheckBatch};
use crate::lifecycle::RunRecord;
use crate::owner::ChunkWork as _;
use crate::program_output::ProgramOutputWriter;
use crate::recovery::RecoveryWrite;
use crate::run_repository::{self, Chunk, OwnedEvent};
use crate::types::AcceptedRun;
use crate::{Error, RunModule, engine, nanos_duration};

use super::chunk::{ChunkBuild, ChunkItem};
use super::handle::{CommitRejection, Mailbox, Message, OwnerHandle, OwnerRegistration, Timer};
use super::state::{Effect, OwnerLimits, OwnerState, apply};

/// The wait between two settle attempts while a dispatch is still in flight
/// within the shutdown grace.
const SETTLE_RETRY: Duration = Duration::from_millis(10);

/// How many times a recovered owner reads the Run again after another writer
/// moved its head between the read and the write.
const RECOVERY_HEAD_RETRIES: u32 = 3;

/// How an owner begins.
pub(crate) enum OwnerStart {
    /// The owner of a Run this process accepted: it commits the start mark
    /// and runs the Program as `actor`.
    Program(AuthenticatedPrincipal),
    /// The owner the expired-lease sweep recreated, its mailbox queued: it
    /// takes the Run over from the claim its handle names, and ends it.
    Recovery,
}

/// Hosts the owner of `accepted` until it retires.
pub(crate) async fn host(
    module: RunModule,
    accepted: AcceptedRun,
    start: OwnerStart,
    handle: OwnerHandle,
    mut mailbox: Mailbox,
    registration: OwnerRegistration,
) {
    #[cfg(test)]
    let _attempt = ExecutionAttemptObservation(module.execution_observer.clone());
    let (mut state, mut effects) = begin(&module, &accepted, &start);
    let mut timers = Timers::default();
    let mut subscription = Subscription::default();
    let idle = tokio::time::sleep_until(unarmed());
    tokio::pin!(idle);
    // The shutdown grace of a recorded termination, on this process's clock:
    // set by the first settle attempt, read by every later one.
    let mut settle_deadline: Option<Instant> = None;
    loop {
        let mut retired = false;
        for effect in effects.drain(..) {
            match effect {
                Effect::Retire => retired = true,
                Effect::Commit {
                    chunk,
                    output_total,
                } => spawn_commit(&module, &accepted, &handle, chunk, output_total),
                Effect::ArmTimer { timer, after_nanos } => {
                    #[cfg(test)]
                    module.execution_observer.timer_armed(&timer, after_nanos);
                    if timer == Timer::IdleTick {
                        idle.as_mut()
                            .reset(Instant::now() + nanos_duration(after_nanos));
                    } else {
                        timers.arm(&handle, timer, after_nanos);
                    }
                }
                Effect::Subscribe => subscription.subscribe(&module, &handle),
                Effect::Unsubscribe => subscription.unsubscribe(),
                Effect::Interrupt {
                    last_commit_age_nanos,
                } => interrupt(&module, &accepted, &handle, last_commit_age_nanos),
                Effect::Settle { shutdown, again } => {
                    let deadline =
                        *settle_deadline.get_or_insert_with(|| grace_deadline(&module, &shutdown));
                    spawn_settle(&module, &accepted, &handle, shutdown, again, deadline);
                }
                Effect::Recover { expired, intent } => {
                    let Some(taken) = handle.takes_over() else {
                        debug_assert!(false, "only a recovered owner writes a recovery");
                        continue;
                    };
                    spawn_recovery(&module, &accepted, &handle, taken.clone(), expired, intent);
                }
                other => run_effect(&module, &accepted, &start, &handle, other),
            }
        }
        if retired {
            break;
        }
        // The handle outlives the loop, so the mailbox never closes first.
        let message = tokio::select! {
            Some(message) = mailbox.recv() => message,
            () = &mut idle => {
                idle.as_mut().reset(unarmed());
                Message::Timer(Timer::IdleTick)
            }
        };
        if matches!(message, Message::Timer(_)) {
            timers.fired();
        }
        #[cfg(test)]
        module.execution_observer.owner_message_dequeued();
        // A clock that fails is an operational failure like a failed commit:
        // the owner stops first, then still applies the message it dequeued,
        // so a Program that ends under a broken clock still reaches its
        // terminal write (which fails on the same clock and retires the owner).
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
    }
    drop(timers);
    drop(subscription);
    drop(registration);
}

/// The owner's memory as it begins, and its first effects: a Program owner
/// is launched; a recovered owner applies the mailbox the sweep queued.
fn begin(
    module: &RunModule,
    accepted: &AcceptedRun,
    start: &OwnerStart,
) -> (OwnerState, Vec<Effect>) {
    let limits = OwnerLimits {
        run_output_bytes: accepted.value_limits.run_output_bytes(),
        applied_cap_bytes: module.applied_output_cap.bytes(),
        fan_out_invocations: module.fan_out_limit.invocations(),
        deadline_at_unix_nanos: accepted.deadline_at_unix_nanos,
        shutdown_grace_nanos: accepted.shutdown_grace_nanos,
        idle_tick_nanos: module.execution_policy.idle_tick_nanos(),
        suspect_after_nanos: module.execution_policy.suspect_after_nanos(),
        self_interrupt_after_nanos: module.execution_policy.self_interrupt_after_nanos(),
        precheck_interval_nanos: module.execution_policy.precheck_interval_nanos(),
        wake_gap_nanos: module.execution_policy.wake_gap_nanos(),
    };
    match start {
        OwnerStart::Program(_) => {
            let mut state = OwnerState::new(limits, accepted.accepted_at_unix_nanos);
            let effects = apply(&mut state, Message::Launch, launched_at(module, accepted));
            (state, effects)
        }
        OwnerStart::Recovery => (
            OwnerState::recovered(limits, accepted.accepted_at_unix_nanos),
            Vec::new(),
        ),
    }
}

/// The change subscription the owner holds while a connection wait is
/// parked: a task that rings [`Message::Changed`] into the mailbox once
/// subscribed (so a request that ended before then is read too) and after
/// changes, at most once per wake gap: the owner reads no more often, and
/// the signal keeps a change that came meanwhile. During a notification
/// outage it rings nothing and subscribes again once the signal is back, a
/// wake gap apart; the periodic precheck covers the outage.
#[derive(Default)]
struct Subscription(Option<tokio::task::JoinHandle<()>>);

impl Subscription {
    fn subscribe(&mut self, module: &RunModule, handle: &OwnerHandle) {
        self.unsubscribe();
        let database = module.database.clone();
        let gap = module.execution_policy.wake_gap();
        let handle = handle.clone();
        self.0 = Some(tokio::spawn(async move {
            loop {
                if let Ok(mut changes) = database.subscribe_changes() {
                    loop {
                        handle.send(Message::Changed);
                        tokio::time::sleep(gap).await;
                        if changes.changed().await.is_err() {
                            break;
                        }
                    }
                }
                tokio::time::sleep(gap).await;
            }
        }));
    }

    fn unsubscribe(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

/// Runs one snapshot check of parked waits; its findings reach the owner as
/// [`Message::Prechecked`].
fn spawn_precheck(
    handle: &OwnerHandle,
    batch: PrecheckBatch,
    #[cfg(test)] observer: std::sync::Arc<dyn crate::execution::ExecutionObserver>,
) {
    let handle = handle.clone();
    tokio::spawn(async move {
        let check = batch.check();
        let found = batch.run().await;
        if let Err(error) = &found {
            tracing::debug!(
                run_id = %handle.run_id(),
                ?check,
                %error,
                "Run Owner's check of its parked waits found nothing: the next one reads again"
            );
        }
        #[cfg(test)]
        observer.precheck_finished(check, found.as_ref().map_or(0, Vec::len));
        handle.send(Message::Prechecked { check, found });
    });
}

/// The owner gives its Run up: the Program and its dispatches stop, and the
/// Interrupted chunk is written until the database answers.
fn interrupt(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
    last_commit_age_nanos: i64,
) {
    tracing::warn!(
        run_id = %accepted.id,
        last_commit_age_ms = last_commit_age_nanos / 1_000_000,
        "Run Owner interrupts its Run: no commit succeeded within three leases"
    );
    handle.stop().request();
    spawn_interruption(module, accepted, handle);
}

/// An instant the idle tick's sleep never reaches on its own: it is reset
/// to the next tick by the owner's `ArmTimer`.
fn unarmed() -> Instant {
    Instant::now() + Duration::from_hours(24 * 365 * 30)
}

/// The clock sample the launch is applied at, like every message, so the
/// Run deadline is armed for the rest of the Run from now, not from
/// acceptance; a clock that fails reads as acceptance.
fn launched_at(module: &RunModule, accepted: &AcceptedRun) -> i64 {
    module.now().map_or(accepted.accepted_at_unix_nanos, |now| {
        now.max(accepted.accepted_at_unix_nanos)
    })
}

/// The timers the owner armed, one sleep each on this process's clock,
/// except the idle tick, which the loop keeps in place. Arming a timer
/// again replaces its sleep; a sleep that ended is forgotten at the next
/// arm or fire (its message may be dequeued a moment before the task that
/// sent it ends, and a message from a sleep since replaced must not forget
/// the sleep that replaced it); retiring the owner aborts the rest.
#[derive(Default)]
struct Timers(HashMap<Timer, tokio::task::JoinHandle<()>>);

impl Timers {
    fn arm(&mut self, handle: &OwnerHandle, timer: Timer, after_nanos: i64) {
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

/// Runs every effect but the commit, the settle, the interruption, the
/// timers, the change subscription and the retirement, which the loop owns.
/// A write retried past the owner's retirement answers a mailbox nobody
/// reads.
fn run_effect(
    module: &RunModule,
    accepted: &AcceptedRun,
    start: &OwnerStart,
    handle: &OwnerHandle,
    effect: Effect,
) {
    match effect {
        Effect::Reply(reply, result) => {
            // The requester may have stopped waiting; the owner's decision stands.
            let _delivered = reply.send(result);
        }
        // The requester may have stopped waiting; the record stands.
        Effect::Answer(reply, recorded) => {
            let _delivered = reply.send(recorded);
        }
        Effect::Suspect {
            last_commit_age_nanos,
        } => tracing::warn!(
            run_id = %accepted.id,
            last_commit_age_ms = last_commit_age_nanos / 1_000_000,
            "Run Owner is suspect: no commit succeeded within a lease; \
             Program requests answer once recorded"
        ),
        Effect::Precheck(batch) => spawn_precheck(
            handle,
            batch,
            #[cfg(test)]
            module.execution_observer.clone(),
        ),
        // A recovered owner starts no Program: its `apply` never asks.
        Effect::Start | Effect::StartProgram(_) if matches!(start, OwnerStart::Recovery) => {
            debug_assert!(false, "a recovered owner starts no Program");
        }
        Effect::Start => {
            let OwnerStart::Program(actor) = start else {
                return;
            };
            let module = module.clone();
            let accepted = accepted.clone();
            let actor = actor.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                #[cfg(test)]
                module
                    .execution_observer
                    .owner_starting(&accepted.id, &accepted.claim_id)
                    .await;
                let started = until_answered(
                    &accepted.id,
                    "execution start",
                    || !handle.is_retired(),
                    || module.start_execution(&accepted, &actor),
                )
                .await;
                handle.send(Message::Started(started));
            });
        }
        Effect::StartProgram(package) => {
            if let OwnerStart::Program(actor) = start {
                start_program(module, accepted, actor, handle, package);
            }
        }
        Effect::TerminateProgram => handle.stop().request(),
        Effect::Wake(parked) => parked.wake(),
        Effect::Abandon(work, error) => work.settle(Err(error)),
        Effect::CompleteExecution { completion, intent } => {
            let module = module.clone();
            let accepted = accepted.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                let completed = until_answered(
                    &accepted.id,
                    "terminal write",
                    || !handle.is_retired(),
                    || module.complete_execution(&accepted, &completion, intent.as_ref()),
                )
                .await;
                handle.send(Message::Completed(completed));
            });
        }
        Effect::Commit { .. }
        | Effect::Settle { .. }
        | Effect::Interrupt { .. }
        | Effect::Recover { .. }
        | Effect::ArmTimer { .. }
        | Effect::Subscribe
        | Effect::Unsubscribe
        | Effect::Retire => {
            debug_assert!(
                false,
                "the host loop runs commits, the interruption, timers, the subscription and retirement itself"
            );
        }
    }
}

/// Executes the accepted package with the Run's invoker and output, and
/// reports its end into the mailbox.
fn start_program(
    module: &RunModule,
    accepted: &AcceptedRun,
    actor: &AuthenticatedPrincipal,
    handle: &OwnerHandle,
    package: engine::ProgramPackage,
) {
    let invoker = OperationInvoker::new(
        module.accounts.clone(),
        module.providers.clone(),
        module.provider_publisher.clone(),
        module.human_interface.clone(),
        module.database.clone(),
        module.workspaces.clone(),
        actor.clone(),
        accepted,
        module.execution_policy.approval_wait_nanos(),
        handle.clone(),
        module.upstream.clone(),
        module.clock.clone(),
        #[cfg(test)]
        module.invocation_observer.clone(),
    );
    let output = ProgramOutputWriter::new(handle.clone(), accepted.value_limits);
    let engine = module.engine.clone();
    let linked_snapshots = accepted.linked_snapshots.clone();
    let limits = accepted.value_limits;
    let stop = handle.stop();
    let handle = handle.clone();
    tokio::spawn(async move {
        let completion = engine::execute(
            engine,
            package,
            linked_snapshots,
            invoker,
            output,
            limits,
            stop,
        )
        .await;
        handle.send(Message::ProgramEnded(completion));
    });
}

/// The end of a recorded termination's shutdown grace on this process's
/// clock, from how much of it is left on the domain clock.
fn grace_deadline(module: &RunModule, shutdown: &crate::termination::ShutdownAction) -> Instant {
    // A clock that fails never extends the grace: the attempt that follows
    // reads the same clock, fails on it, and retires the owner; the Run is
    // recovery's then.
    let now = module
        .now()
        .unwrap_or(shutdown.deadline_at_unix_nanos())
        .max(shutdown.accepted_at_unix_nanos());
    Instant::now() + crate::duration_until(shutdown.deadline_at_unix_nanos(), now)
}

/// One settle attempt, after a moment when the previous one found a
/// dispatch in flight; `deadline` is the end of the shutdown grace on this
/// process's clock, after which the attempt ends the Run whatever is still
/// dispatching.
fn spawn_settle(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
    shutdown: crate::termination::ShutdownAction,
    again: bool,
    deadline: Instant,
) {
    let module = module.clone();
    let accepted = accepted.clone();
    let handle = handle.clone();
    tokio::spawn(async move {
        if again {
            tokio::time::sleep(SETTLE_RETRY).await;
        }
        let attempted = until_answered(
            &accepted.id,
            "settle",
            || !handle.is_retired(),
            || module.settle_termination(&shutdown, Instant::now() >= deadline),
        )
        .await;
        handle.send(Message::SettleAttempted(attempted));
    });
}

fn spawn_interruption(module: &RunModule, accepted: &AcceptedRun, handle: &OwnerHandle) {
    let module = module.clone();
    let accepted = accepted.clone();
    let handle = handle.clone();
    tokio::spawn(async move {
        let recorded = module
            .record_interruption(
                &accepted.workspace_id,
                &accepted.id,
                &accepted.claim_id,
                None,
            )
            .await
            .map_err(CommitRejection::from);
        match &recorded {
            Ok(()) => tracing::warn!(run_id = %accepted.id, "Run Owner recorded its interruption"),
            Err(rejection) => tracing::warn!(
                run_id = %accepted.id,
                ?rejection,
                "Run Owner's interruption was not written: the Run is no longer its own"
            ),
        }
        handle.send(Message::Interrupted(recorded));
    });
}

/// The recovered owner's one chunk, written until the database answers; a
/// head another writer moved between the read and the write reads the row
/// again. Its outcome reaches the owner as [`Message::Interrupted`]: a
/// definite failure leaves the Run to the next sweep.
fn spawn_recovery(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
    taken: super::handle::TakenClaim,
    expired: Vec<crate::approval::ApprovalId>,
    intent: Option<crate::lifecycle::RequestedTermination>,
) {
    let module = module.clone();
    let accepted = accepted.clone();
    let handle = handle.clone();
    tokio::spawn(async move {
        let write = RecoveryWrite {
            workspace_id: &accepted.workspace_id,
            run_id: &accepted.id,
            taken: taken.as_deref(),
            takeover_claim_id: &accepted.claim_id,
            expired: &expired,
            intent: intent.as_ref(),
        };
        // A head another writer moved between the read and the write is read
        // again after a backoff, a few times; after that the next sweep
        // tries afresh.
        // A mass recovery writes a few chunks at a time; the owners exist and
        // hold their requests meanwhile.
        let _permit = module.recovery_writes.acquire().await;
        let mut moved = Backoff::new(&accepted.id, "recovery after a moved head");
        let mut attempts = 0;
        let recorded = loop {
            match until_answered(
                &accepted.id,
                "recovery",
                || !handle.is_retired(),
                || module.record_recovery(&write),
            )
            .await
            {
                Err(error @ Error::HeadMismatch) if attempts < RECOVERY_HEAD_RETRIES => {
                    attempts += 1;
                    moved.wait(&error).await;
                }
                answered => break answered,
            }
        };
        match &recorded {
            Ok(()) => tracing::info!(run_id = %accepted.id, "recovered Run Owner ended its Run"),
            Err(Error::RunEnded) => tracing::info!(
                run_id = %accepted.id,
                "recovered Run Owner found its Run ended"
            ),
            Err(error) => tracing::warn!(
                run_id = %accepted.id,
                %error,
                "recovered Run Owner wrote nothing: the Run is not its own, or the next sweep tries again"
            ),
        }
        handle.send(Message::Interrupted(
            recorded.map_err(CommitRejection::from),
        ));
    });
}

fn spawn_commit(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
    chunk: ChunkBuild,
    output_total: usize,
) {
    let module = module.clone();
    let accepted = accepted.clone();
    let handle = handle.clone();
    tokio::spawn(async move {
        let outcome = commit_until_answered(&module, &accepted, &handle, chunk, output_total).await;
        let message = match outcome {
            Some(Ok(())) => Message::Committed,
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
    /// The head the chunk is built on: the one the owner tracks.
    head_before: u64,
    /// The lease the last attempt that got to write renewed. No other writer
    /// renews the lease, and the owner commits one chunk at a time, so a row
    /// that carries exactly this instant carries this chunk.
    lease_until_unix_nanos: Option<i64>,
}

impl Written {
    /// Whether `run` carries the chunk: the lease an attempt renewed is on
    /// the row, and the head has moved past the one the chunk is built on.
    /// The commit makes its lease instant distinct from the one it found on
    /// the row, so a coarse clock cannot make the previous chunk's lease pass
    /// for this one.
    fn landed_on(self, run: &RunRecord) -> bool {
        self.lease_until_unix_nanos.is_some()
            && run.worker_claim_expires_at_unix_nanos == self.lease_until_unix_nanos
            && run.chunk_sequence > self.head_before
    }
}

/// How one commit attempt ended.
enum Attempt {
    Landed,
    Rejected(CommitRejection),
    /// The database gave no answer.
    Unanswered(Error),
}

/// What the reread of the Run row says about an unanswered attempt.
enum Resolution {
    Landed,
    Rejected(CommitRejection),
    Recommit,
}

/// Why one attempt's transaction did not reach its commit.
enum CommitFailure {
    /// The Run row as read no longer takes this owner's chunks.
    Rejected(CommitRejection),
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
/// work learns the chunk's fate once, when it is known, and what it recorded
/// for an outside request is reported before the chunk's own answer. `None`
/// once the owner retired while the chunk was still unanswered: the work is
/// dropped unsettled, which its waiters read as a lost owner.
async fn commit_until_answered(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
    chunk: ChunkBuild,
    output_total: usize,
) -> Option<Result<(), CommitRejection>> {
    let mut items = chunk.items;
    let mut fence = chunk.fence;
    let termination = chunk.termination;
    let mut written = Written {
        head_before: chunk.head,
        lease_until_unix_nanos: None,
    };
    let mut backoff = Backoff::new(&accepted.id, "chunk commit");
    let outcome = loop {
        match commit_once(
            module,
            accepted,
            &mut items,
            &mut fence,
            termination.as_ref(),
            output_total,
            &mut written,
        )
        .await
        {
            Attempt::Landed => break Ok(()),
            Attempt::Rejected(rejection) => break Err(rejection),
            Attempt::Unanswered(error) => {
                let resolution =
                    reread_until_resolved(module, accepted, handle, written, &mut backoff, error)
                        .await;
                match resolution {
                    None => return None,
                    Some(Resolution::Landed) => break Ok(()),
                    Some(Resolution::Rejected(rejection)) => break Err(rejection),
                    Some(Resolution::Recommit) => {}
                }
            }
        }
    };
    if let Err(CommitRejection::Corrupt) = outcome {
        tracing::error!(
            run_id = %accepted.id,
            claim_id = %accepted.claim_id,
            ?written,
            "Run Ledger head contradicts its Run Owner's history; the owner stops and writes nothing more"
        );
    }
    if outcome.is_ok() {
        for item in &mut items {
            if let ChunkItem::Work(work) = item {
                if let Some(parked) = work.take_parked() {
                    handle.send(Message::Parked(parked));
                }
                if let Some((key, answer)) = work.take_recorded_request() {
                    handle.send(Message::RequestRecorded { key, answer });
                }
            }
        }
    }
    for item in items {
        if let ChunkItem::Work(work) = item {
            work.settle(outcome.map_err(CommitRejection::lost));
        }
    }
    if let Some(batch) = fence {
        batch.settle(outcome.map_err(CommitRejection::lost));
    }
    Some(outcome)
}

/// Rereads the Run row after an attempt the database did not answer, backing
/// off between rereads that fail (a reread that fails is no answer either,
/// whatever its error), until the row says what the attempt did. `None` once
/// the owner retired meanwhile.
async fn reread_until_resolved(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
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
    match refusal(run, claim_id, written.head_before) {
        Some(rejection) => Resolution::Rejected(rejection),
        None => Resolution::Recommit,
    }
}

/// Why the Run row as read no longer takes a chunk built on `head`, in the
/// order the evidence is judged: a terminal Run, another execution claim, a
/// row that refuses the owner's chunks (a released worker claim, the
/// deadline), and a head the owner did not move, which one serial owner
/// cannot cause and is corruption. `None` when the row takes the chunk.
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
    if run.worker_claim_id.as_deref() != Some(claim_id) {
        return Some(CommitRejection::Refused);
    }
    if run.chunk_sequence != head {
        return Some(CommitRejection::Corrupt);
    }
    None
}

/// One attempt to commit the chunk, in one transaction that holds no Run
/// row: the row work, then the fence, then the head update predicated on
/// the owner's claim and the head it tracks. A recommit first checks whether
/// the attempt it repeats landed; a head update that matches no row is
/// classified from the row read again.
async fn commit_once(
    module: &RunModule,
    accepted: &AcceptedRun,
    items: &mut [ChunkItem],
    fence: &mut Option<Box<FenceBatch>>,
    termination: Option<&crate::lifecycle::RequestedTermination>,
    output_total: usize,
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
        let now = module.now()?.max(run.accepted_at_unix_nanos);
        if let Some(rejection) = refusal(&run, claim_id, written.head_before) {
            return Err(CommitFailure::Rejected(rejection));
        }
        if !run.accepts_owner_chunk(claim_id) {
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
        // The fence decides every ready Invocation of the chunk together,
        // after the row work; it writes no Event.
        if let Some(batch) = fence.as_mut() {
            batch.run(&mut transaction, &run, now).await?;
        }
        let mut produced_by_work = produced.iter();
        let mut events: Vec<&OwnedEvent> = Vec::new();
        for item in items.iter() {
            match item {
                ChunkItem::Event(event) => events.push(event),
                ChunkItem::Work(_) => events.extend(produced_by_work.next().into_iter().flatten()),
            }
        }
        // The renewed lease marks this chunk on the row; it never repeats the
        // instant already there.
        let mut lease_until = module.execution_policy.lease_until(now)?;
        if Some(lease_until) == run.worker_claim_expires_at_unix_nanos {
            lease_until = lease_until.saturating_add(1);
        }
        written.lease_until_unix_nanos = Some(lease_until);
        debug_assert!(
            termination.is_none_or(|intent| !intent.releases_worker_claim()),
            "a Run Owner never releases its own worker claim in a chunk"
        );
        run_repository::commit_chunk(
            &mut transaction,
            Chunk::new(&run, Some(claim_id))
                .owned_events(events)
                .output_bytes(output_total)
                .renew_lease(lease_until)
                .termination(termination),
        )
        .await?;
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
        Err(CommitFailure::Error(error)) if error.is_operational() => Attempt::Unanswered(error),
        // The head update matched no row under the claim: a writer moved
        // the row between the read and the update. The row read again says
        // which writer it could be; an answer it does not give is an
        // unanswered attempt, resolved by the reread.
        Err(CommitFailure::Error(Error::HeadMismatch)) => {
            match run_repository::find_exact(&module.database, &accepted.workspace_id, &accepted.id)
                .await
            {
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
