//! The task that hosts one Run Owner: it drains the mailbox, calls
//! [`apply`], and runs the effects, holding the chunk commit, the Program
//! execution and the terminal write.
//!
//! Every write the owner starts ends on an answer, never on the clock. A
//! chunk commit the database does not answer is resolved by rereading the Run
//! row under the owner's claim: landed (the row carries the lease this very
//! attempt renewed), fenced (another claim, or an ended Run), not landed
//! (recommit the chunk exactly as built), or corrupt (a head below the one
//! the chunk was built on). The start mark, the terminal write and the
//! Interrupted record retry the same way.

use tokio::time::Instant;

use coral_workspace::AuthenticatedPrincipal;

use crate::execution::{Backoff, until_answered};
use crate::invocation::OperationInvoker;
use crate::lifecycle::RunRecord;
use crate::program_output::ProgramOutputWriter;
use crate::run_repository::{self, Chunk, OwnedEvent};
use crate::types::AcceptedRun;
use crate::{Error, RunModule, engine};

use super::chunk::{ChunkBuild, ChunkItem};
use super::handle::{CommitRejection, Mailbox, Message, OwnerHandle, OwnerRegistration};
use super::state::{Effect, OwnerLimits, OwnerState, apply};

/// Until slice 6 routes approval decisions, cancellation and the deadline
/// through the owner, those writers move the Run Ledger head outside it, so
/// a reread after an uncertain commit may find the head moved without the
/// chunk having landed; the chunk is then recommitted on the head read under
/// the hold. Slice 6 sets this to `false`: from then on a head the owner did
/// not move is corruption.
const HEAD_MOVES_OUTSIDE_THE_OWNER: bool = true;

/// Hosts the owner of `accepted` until it retires, then settles a recorded
/// termination when the terminal write left one to settle.
pub(crate) async fn host(
    module: RunModule,
    accepted: AcceptedRun,
    actor: AuthenticatedPrincipal,
    handle: OwnerHandle,
    mut mailbox: Mailbox,
    registration: OwnerRegistration,
) {
    #[cfg(test)]
    let _attempt = ExecutionAttemptObservation(module.execution_observer.clone());
    #[cfg(test)]
    module
        .execution_observer
        .owner_starting(&accepted.id, &accepted.claim_id)
        .await;
    let mut state = OwnerState::new(
        OwnerLimits {
            run_output_bytes: accepted.value_limits.run_output_bytes(),
            applied_cap_bytes: module.applied_output_cap.bytes(),
            deadline_at_unix_nanos: accepted.deadline_at_unix_nanos,
            suspect_after_nanos: module.execution_policy.suspect_after_nanos(),
            self_interrupt_after_nanos: module.execution_policy.self_interrupt_after_nanos(),
        },
        accepted.accepted_at_unix_nanos,
    );
    let idle_tick = module.execution_policy.idle_tick();
    let idle = tokio::time::sleep(idle_tick);
    tokio::pin!(idle);
    let mut effects = apply(&mut state, Message::Launch, accepted.accepted_at_unix_nanos);
    loop {
        let mut retired = false;
        for effect in effects.drain(..) {
            match effect {
                Effect::Retire => retired = true,
                Effect::Commit {
                    chunk,
                    output_total,
                } => {
                    idle.as_mut().reset(Instant::now() + idle_tick);
                    spawn_commit(&module, &accepted, &handle, chunk, output_total);
                }
                Effect::Suspect {
                    last_commit_age_nanos,
                } => tracing::warn!(
                    run_id = %accepted.id,
                    last_commit_age_ms = last_commit_age_nanos / 1_000_000,
                    "Run Owner is suspect: no commit succeeded within a lease; \
                     Program requests answer once recorded"
                ),
                Effect::Interrupt {
                    last_commit_age_nanos,
                } => {
                    tracing::warn!(
                        run_id = %accepted.id,
                        last_commit_age_ms = last_commit_age_nanos / 1_000_000,
                        "Run Owner interrupts its Run: no commit succeeded within three leases"
                    );
                    handle.stop().request();
                    spawn_interruption(&module, &accepted, &handle);
                }
                other => run_effect(&module, &accepted, &actor, &handle, other),
            }
        }
        if retired {
            break;
        }
        let message = tokio::select! {
            Some(message) = mailbox.recv() => message,
            () = &mut idle => {
                idle.as_mut().reset(Instant::now() + idle_tick);
                Message::IdleTick
            }
        };
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
    drop(registration);
}

/// Runs every effect but the commit, the interruption and the retirement,
/// which the loop owns. A write retried past the owner's retirement answers
/// a mailbox nobody reads.
fn run_effect(
    module: &RunModule,
    accepted: &AcceptedRun,
    actor: &AuthenticatedPrincipal,
    handle: &OwnerHandle,
    effect: Effect,
) {
    match effect {
        Effect::Reply(reply, result) => {
            // The requester may have stopped waiting; the owner's decision stands.
            let _delivered = reply.send(result);
        }
        Effect::Start => {
            let module = module.clone();
            let accepted = accepted.clone();
            let actor = actor.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
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
            let invoker = OperationInvoker::new(
                module.accounts.clone(),
                module.providers.clone(),
                module.provider_publisher.clone(),
                module.human_interface.clone(),
                module.database.clone(),
                module.workspaces.clone(),
                actor.clone(),
                accepted,
                handle.clone(),
                module.upstream.clone(),
                module.clock.clone(),
                #[cfg(test)]
                module.invocation_observer.clone(),
            );
            let output = ProgramOutputWriter::new(handle.clone(), accepted.value_limits);
            let engine = module.engine.clone();
            let limits = accepted.value_limits;
            let stop = handle.stop();
            let handle = handle.clone();
            tokio::spawn(async move {
                let completion =
                    engine::execute(engine, package, invoker, output, limits, stop).await;
                handle.send(Message::ProgramEnded(completion));
            });
        }
        Effect::TerminateProgram => handle.stop().request(),
        Effect::Abandon(work, error) => work.settle(Err(error)),
        Effect::CompleteExecution(completion) => {
            let module = module.clone();
            let accepted = accepted.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                let completed = until_answered(
                    &accepted.id,
                    "terminal write",
                    || !handle.is_retired(),
                    || module.complete_execution(&accepted, &completion),
                )
                .await;
                handle.send(Message::Completed(completed));
            });
        }
        Effect::Settle(shutdown) => {
            let module = module.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                // The recorded termination is the recovery source when the
                // settle itself fails.
                let _settled = module.settle_within_grace(shutdown).await;
                handle.send(Message::Settled);
            });
        }
        Effect::Commit { .. }
        | Effect::Suspect { .. }
        | Effect::Interrupt { .. }
        | Effect::Retire => {
            debug_assert!(
                false,
                "the host loop runs commits, liveness transitions and retirement itself"
            );
        }
    }
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

/// What the commit attempts so far wrote before the database stopped
/// answering: enough to recognize the chunk on the Run row.
#[derive(Debug, Default, Clone, Copy)]
struct Written {
    /// The head the last attempt was built on, read under its hold.
    head_before: Option<u64>,
    /// The lease the last attempt that got to write renewed. No other writer
    /// renews the lease, and the owner commits one chunk at a time, so a row
    /// that carries exactly this instant carries this chunk.
    lease_until_unix_nanos: Option<i64>,
}

impl Written {
    /// Whether `run` carries the chunk: the lease an attempt renewed is on
    /// the row, and the head has moved past the one the attempt was built
    /// on. The commit makes its lease instant distinct from the one it found
    /// on the row, so a coarse clock cannot make the previous chunk's lease
    /// pass for this one.
    fn landed_on(self, run: &RunRecord) -> bool {
        self.lease_until_unix_nanos.is_some()
            && run.worker_claim_expires_at_unix_nanos == self.lease_until_unix_nanos
            && self
                .head_before
                .is_none_or(|head| run.chunk_sequence > head)
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
    /// The Run row no longer takes this owner's chunks.
    Refused,
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
/// work learns the chunk's fate once, when it is known. `None` once the owner
/// retired while the chunk was still unanswered: the work is dropped
/// unsettled, which its waiters read as a lost owner.
async fn commit_until_answered(
    module: &RunModule,
    accepted: &AcceptedRun,
    handle: &OwnerHandle,
    chunk: ChunkBuild,
    output_total: usize,
) -> Option<Result<(), CommitRejection>> {
    let mut items = chunk.items;
    let mut written = Written::default();
    let mut backoff = Backoff::new(&accepted.id, "chunk commit");
    let outcome = loop {
        match commit_once(module, accepted, &mut items, output_total, &mut written).await {
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
    for item in items {
        if let ChunkItem::Work(work) = item {
            work.settle(outcome.map_err(CommitRejection::lost));
        }
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
    let head_before = written.head_before;
    if written.landed_on(run) {
        return Resolution::Landed;
    }
    if run.execution_claim_id.as_deref() != Some(claim_id) {
        return Resolution::Rejected(CommitRejection::ClaimMismatch);
    }
    if run.state.is_terminal() {
        return Resolution::Rejected(CommitRejection::RunEnded);
    }
    if head_before.is_some_and(|head| run.chunk_sequence < head) {
        return Resolution::Rejected(CommitRejection::Corrupt);
    }
    if run.worker_claim_id.as_deref() != Some(claim_id) {
        return Resolution::Rejected(CommitRejection::Refused);
    }
    if head_before.is_none_or(|head| run.chunk_sequence == head) {
        return Resolution::Recommit;
    }
    // The head moved and the chunk is not on the row: an outside writer's
    // chunk until slice 6, corruption from then on.
    if HEAD_MOVES_OUTSIDE_THE_OWNER {
        Resolution::Recommit
    } else {
        Resolution::Rejected(CommitRejection::Corrupt)
    }
}

/// One attempt to commit the chunk: holds the Run row, checks that it still
/// takes this owner's chunks, runs the row work in arrival order, and writes
/// the Events and the head under the owner's claim with its lease renewed.
/// Until slice 6 the head is the one read under the hold: cancel, approval
/// decisions, the deadline and recovery still write outside the owner and
/// move it. A recommit finds under the hold whether the attempt it repeats
/// landed after all, and then writes nothing.
async fn commit_once(
    module: &RunModule,
    accepted: &AcceptedRun,
    items: &mut [ChunkItem],
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
        let run = run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
            .await?
            .ok_or(Error::ClaimMismatch)?;
        if written.landed_on(&run) {
            // The attempt this one repeats landed between the reread and
            // this hold.
            return Ok(());
        }
        written.head_before = Some(run.chunk_sequence);
        let now = module.now()?.max(run.accepted_at_unix_nanos);
        if run.state.is_terminal() {
            return Err(Error::RunEnded.into());
        }
        if run.execution_claim_id.as_deref() != Some(claim_id) {
            return Err(Error::ClaimMismatch.into());
        }
        if !run.accepts_owner_chunk(claim_id, now) {
            return Err(CommitFailure::Refused);
        }
        // Row work runs in arrival order; its Events take the work's place
        // among the applied ones.
        let mut produced = Vec::new();
        for item in items.iter_mut() {
            if let ChunkItem::Work(work) = item {
                produced.push(work.run(&mut transaction, &run, now).await?);
            }
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
        run_repository::commit_chunk(
            &mut transaction,
            Chunk::new(&run, Some(claim_id))
                .owned_events(events)
                .output_bytes(output_total)
                .renew_lease(lease_until),
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
        Err(CommitFailure::Refused) => Attempt::Rejected(CommitRejection::Refused),
        Err(CommitFailure::Error(error)) if error.is_operational() => Attempt::Unanswered(error),
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
