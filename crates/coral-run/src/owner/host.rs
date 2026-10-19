//! The task that hosts one Run Owner: it drains the mailbox, calls
//! [`apply`], and runs the effects, holding the chunk commit, the Program
//! execution and the terminal write.

use tokio::time::Instant;

use coral_workspace::AuthenticatedPrincipal;

use crate::invocation::OperationInvoker;
use crate::program_output::ProgramOutputWriter;
use crate::run_repository::{self, Chunk};
use crate::types::AcceptedRun;
use crate::{Error, RunModule, engine};

use super::chunk::{ChunkBuild, ChunkItem, ChunkWork};
use super::handle::{CommitRejection, Mailbox, Message, OwnerHandle, OwnerRegistration};
use super::state::{Effect, OwnerLimits, OwnerState, apply};

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
    let mut state = OwnerState::new(OwnerLimits {
        run_output_bytes: accepted.value_limits.run_output_bytes(),
        applied_cap_bytes: module.applied_output_cap.bytes(),
        deadline_at_unix_nanos: accepted.deadline_at_unix_nanos,
    });
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
                other => run_effect(&module, &accepted, &actor, &handle, other),
            }
        }
        if retired {
            break;
        }
        let message = tokio::select! {
            biased;
            Some(message) = mailbox.internal.recv() => message,
            Some(message) = mailbox.requests.recv() => message,
            () = &mut idle => {
                idle.as_mut().reset(Instant::now() + idle_tick);
                Message::IdleTick
            }
        };
        // A clock that fails is an operational failure like a failed commit:
        // the owner stops, and the terminal write decides what the Run records.
        let (message, now) = match module.now() {
            Ok(now) => (message, now.max(accepted.accepted_at_unix_nanos)),
            Err(_clock) => (
                Message::CommitRejected(CommitRejection::Failed),
                accepted.accepted_at_unix_nanos,
            ),
        };
        effects = apply(&mut state, message, now);
        // Apply everything that arrived meanwhile in order, then commit when free.
        loop {
            let next = match mailbox.internal.try_recv() {
                Ok(message) => message,
                Err(_empty) => match mailbox.requests.try_recv() {
                    Ok(message) => message,
                    Err(_empty) => break,
                },
            };
            effects.extend(apply(&mut state, next, now));
        }
        effects.extend(apply(&mut state, Message::Drained, now));
    }
    drop(registration);
}

/// Runs every effect but the commit and the retirement, which the loop owns.
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
                let started = module.start_execution(&accepted, &actor).await;
                handle.send_internal(Message::Started(started));
            });
        }
        Effect::StartProgram(package) => {
            let invoker = OperationInvoker::new(
                module.accounts.clone(),
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
                handle.send_internal(Message::ProgramEnded(completion));
            });
        }
        Effect::TerminateProgram => handle.stop().request(),
        Effect::Abandon(work, error) => work.settle(Err(error)),
        Effect::CompleteExecution(completion) => {
            let module = module.clone();
            let accepted = accepted.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                let completed = module.complete_execution(&accepted, completion).await;
                handle.send_internal(Message::Completed(completed));
            });
        }
        Effect::Settle(shutdown) => {
            let module = module.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                // The recorded termination is the recovery source when the
                // settle itself fails.
                let _settled = module.settle_within_grace(shutdown).await;
                handle.send_internal(Message::Settled);
            });
        }
        Effect::Commit { .. } | Effect::Retire => {
            debug_assert!(false, "the host loop runs commits and retirement itself");
        }
    }
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
        let message = match commit(&module, &accepted, chunk, output_total).await {
            Ok(()) => Message::Committed,
            Err(rejection) => Message::CommitRejected(rejection),
        };
        handle.send_internal(message);
    });
}

/// Commits one chunk: holds the Run row, checks that it still takes this
/// owner's chunks, runs the row work in arrival order, and writes the Events
/// and the head under the owner's claim with its lease renewed. Until slice 6
/// the head is the one read under the hold: cancel, approval decisions, the
/// deadline and recovery still write outside the owner and move it.
async fn commit(
    module: &RunModule,
    accepted: &AcceptedRun,
    chunk: ChunkBuild,
    output_total: usize,
) -> Result<(), CommitRejection> {
    let claim_id = accepted.claim_id.as_str();
    let mut ran: Vec<Box<dyn ChunkWork>> = Vec::new();
    let mut items = chunk.items.into_iter();
    #[cfg(test)]
    module.execution_observer.before_chunk_commit().await;
    let outcome: Result<(), CommitRejection> = async {
        let mut transaction = module.database.begin().await.map_err(Error::from)?;
        let run = run_repository::hold_run(&mut transaction, &accepted.workspace_id, &accepted.id)
            .await?
            .ok_or(CommitRejection::ClaimMismatch)?;
        let now = module.now()?.max(run.accepted_at_unix_nanos);
        if run.state.is_terminal() {
            return Err(CommitRejection::RunEnded);
        }
        if run.execution_claim_id.as_deref() != Some(claim_id) {
            return Err(CommitRejection::ClaimMismatch);
        }
        if !run.accepts_owner_chunk(claim_id, now) {
            return Err(CommitRejection::Refused);
        }
        let mut events = Vec::new();
        for item in items.by_ref() {
            match item {
                ChunkItem::Event(event) => events.push(event),
                ChunkItem::Work(mut work) => {
                    let produced = work.run(&mut transaction, &run, now).await;
                    ran.push(work);
                    events.extend(produced?);
                }
            }
        }
        let lease_until = module.execution_policy.lease_until(now)?;
        run_repository::commit_chunk(
            &mut transaction,
            Chunk::new(&run, Some(claim_id))
                .owned_events(&events)
                .output_bytes(output_total)
                .renew_lease(lease_until),
        )
        .await?;
        transaction.commit().await.map_err(Error::from)?;
        Ok(())
    }
    .await;
    for work in ran {
        work.settle(outcome.map_err(CommitRejection::lost));
    }
    if let Err(rejection) = outcome {
        for item in items {
            if let ChunkItem::Work(work) = item {
                work.settle(Err(rejection.lost()));
            }
        }
    }
    outcome
}

#[cfg(test)]
struct ExecutionAttemptObservation(std::sync::Arc<dyn crate::execution::ExecutionObserver>);

#[cfg(test)]
impl Drop for ExecutionAttemptObservation {
    fn drop(&mut self) {
        self.0.execution_attempt_finished();
    }
}
