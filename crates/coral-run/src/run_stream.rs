//! Coral's side of a Run stream (ADR 0027, The Run stream): the transient
//! work of one open stream. `Open` is answered by the Run row; `Loaded`
//! rechecks the Principal's authority and commits the start chunk; then the
//! Run Owner's loop commits the Program's requests, in number order, with
//! the stream in place of the in-process isolate ([`program`]).
//!
//! Reopening a started Run is not served yet: its `Open` is refused as
//! unavailable, and the Run Executor opens it again later.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use coral_run_executor::wire::v1::{
    Committed, Ended, LinkedSnapshot, LinkedToolRoot, RunEnd as WireEnd, RunPackage, RunRequest,
    RunResponse, Terminate, TerminateReason, run_request, run_response,
};
use coral_run_executor::{Closing, PROTOCOL_LEVEL, RunStreamServer, ServedRunStream};
use coral_workspace::{AgentRegistrationId, AuthenticatedPrincipal, PrincipalWorkspaceEvidence};
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt as _;

use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::execution::until_answered;
use crate::lifecycle::{RunRecord, RunTerminationKind};
use crate::owner::{self, Message, OwnerHandle, OwnerStart, StartedExecution};
use crate::trace::RunEnd;
use crate::types::{AcceptedRun, HeldRunId, LinkedTool, RunStartFacts};

/// Each Private and Shared Tool a Run linked, by its complete Call Path.
type LinkedTools = std::collections::HashMap<String, LinkedTool>;
use crate::{
    Error, RunFailure, RunFailureCode, RunId, RunModule, RunState, RunTerminalOutcome,
    run_repository,
};

mod program;

pub(crate) use program::{Requests, start as start_program};

impl RunModule {
    /// What serves this Coral Instance's Run streams, for its link.
    #[must_use]
    pub fn run_stream_server(&self) -> Arc<dyn RunStreamServer> {
        Arc::new(RunStreams(self.clone()))
    }
}

struct RunStreams(RunModule);

#[async_trait::async_trait]
impl RunStreamServer for RunStreams {
    async fn serve(&self, stream: ServedRunStream) {
        let link = Arc::new(StreamLink::new(stream.responses));
        self.0.serve_run_stream(stream.requests, &link).await;
        // The Program's reader stops with the stream's server.
        link.retired.send_replace(true);
    }
}

/// Coral's half of one open Run stream, shared by its server, the Run's
/// owner and the reader of the Program's requests.
pub(crate) struct StreamLink {
    responses: mpsc::UnboundedSender<Result<RunResponse, Closing>>,
    /// The highest request number a chunk committed.
    watermark: AtomicU64,
    /// The Program's `Finish` arrived: no `Terminate` follows it.
    finished: AtomicBool,
    /// The Run Executor broke the protocol.
    violated: AtomicBool,
    /// The stream's server is done with it.
    retired: watch::Sender<bool>,
}

impl StreamLink {
    fn new(responses: mpsc::UnboundedSender<Result<RunResponse, Closing>>) -> Self {
        Self {
            responses,
            watermark: AtomicU64::new(0),
            finished: AtomicBool::new(false),
            violated: AtomicBool::new(false),
            retired: watch::Sender::new(false),
        }
    }

    pub(crate) fn send(&self, message: run_response::Message) {
        let _closed = self.responses.send(Ok(RunResponse {
            message: Some(message),
        }));
    }

    fn close(&self, closing: Closing) {
        let _closed = self.responses.send(Err(closing));
    }

    fn end(&self, end: WireEnd) {
        self.send(run_response::Message::Ended(Ended { end: end.into() }));
    }

    /// A chunk landed; `number` is the highest request it committed.
    pub(crate) fn committed(&self, number: Option<u64>) {
        if let Some(number) = number {
            self.watermark.fetch_max(number, Ordering::SeqCst);
        }
        self.send(run_response::Message::Committed(Committed {
            request_number: self.watermark.load(Ordering::SeqCst),
        }));
    }

    /// The owner terminates the Program for `kind`, unless it finished.
    pub(crate) fn terminate(&self, kind: Option<RunTerminationKind>) {
        if self.finished.load(Ordering::SeqCst) {
            return;
        }
        let reason = match kind {
            Some(RunTerminationKind::Canceled) => TerminateReason::Canceled,
            Some(RunTerminationKind::TimedOut) => TerminateReason::TimedOut,
            Some(RunTerminationKind::Interrupted) => TerminateReason::Interrupted,
            Some(
                RunTerminationKind::MemoryLimitExceeded | RunTerminationKind::CpuLimitExceeded,
            )
            | None => TerminateReason::Unspecified,
        };
        self.send(run_response::Message::Terminate(Terminate {
            reason: reason.into(),
        }));
    }
}

/// What the start of a stream's Run found.
enum Start {
    Started(Box<(AuthenticatedPrincipal, StartedExecution)>),
    Ended(WireEnd),
    /// Another stream of the same claim started the Run.
    StartedElsewhere,
}

/// How a taken Run that `claim_id` opened has ended, or that it is not
/// held; `None` while it is held and running. A Run that ended unstarted
/// released the claim with its end, so a terminal Run with no claim is
/// answered by its end too.
fn taken_end(run: &RunRecord, claim_id: &str) -> Option<WireEnd> {
    let claim = run.execution_claim_id.as_deref();
    if run.state.is_terminal() && claim.is_none_or(|claim| claim == claim_id) {
        return Some(wire_end(run.state));
    }
    (claim != Some(claim_id)).then_some(WireEnd::NotHeld)
}

/// A Run stream's Run once its owner runs it: how the owner reaches the
/// stream.
pub(crate) struct StreamRun {
    pub(crate) actor: AuthenticatedPrincipal,
    pub(crate) link: Arc<StreamLink>,
    /// The Program's requests, until the Program starts and reads them.
    pub(crate) requests: Mutex<Option<Requests>>,
}

impl RunModule {
    async fn serve_run_stream(&self, mut requests: Requests, link: &Arc<StreamLink>) {
        let Some(RunRequest {
            message: Some(run_request::Message::Open(open)),
        }) = requests.next().await
        else {
            return link.close(Closing::InvalidArgument("a Run stream opens with Open"));
        };
        if open.protocol_level.abs_diff(PROTOCOL_LEVEL) > 1 {
            return link.close(Closing::FailedPrecondition(
                "the protocol levels are more than one apart",
            ));
        }
        let Ok(run_id) = RunId::parse(&open.run_id) else {
            return link.end(WireEnd::NotHeld);
        };
        let held = HeldRunId::of_held_row(run_id.clone());
        let claim = open.claim_id;
        let read = until_answered(
            &held,
            "Run stream open",
            || !link.responses.is_closed(),
            || self.open_taken(&run_id, &claim),
        )
        .await;
        let (run, package, linked) = match read {
            Ok(Some((run, Some((package, linked))))) => (run, package, linked),
            Ok(Some((run, None))) if run.state.is_terminal() => {
                return link.end(wire_end(run.state));
            }
            Ok(Some(_started)) => {
                return link.close(Closing::Unavailable(
                    "a started Run's stream is not served yet",
                ));
            }
            Ok(None) => return link.end(WireEnd::NotHeld),
            Err(_unanswered) => return link.close(Closing::Unavailable("the Run is not read")),
        };
        send_package(&run, &package, &linked, link);
        if !self.loaded(&mut requests, &run, &claim, link).await {
            return;
        }
        let attempted = AtomicBool::new(false);
        let started_at = std::sync::atomic::AtomicI64::new(0);
        let started = until_answered(
            &held,
            "Run stream start",
            || !link.responses.is_closed(),
            // The first attempt starts on the row `Open` read; one after an
            // attempt the database did not answer reads it again.
            || {
                let reread = attempted.swap(true, Ordering::SeqCst);
                self.start_stream(&run, &claim, &package, reread, &started_at)
            },
        )
        .await;
        let (actor, started) = match started {
            Ok(Start::Started(started)) => *started,
            Ok(Start::Ended(end)) => return link.end(end),
            Ok(Start::StartedElsewhere) => {
                return link.close(Closing::Unavailable("the Run started on another stream"));
            }
            Err(_unanswered) => {
                return link.close(Closing::Unavailable("the start is not written"));
            }
        };
        let (workspace_id, id) = (run.workspace_id.clone(), run.id.clone());
        self.run_on_stream(run, claim, linked, actor, started, requests, link)
            .await;
        if link.violated.load(Ordering::SeqCst) {
            return link.close(Closing::InvalidArgument(
                "the Run stream broke the protocol",
            ));
        }
        if let Ok(Some(run)) = run_repository::find_exact(&self.database, &workspace_id, &id).await
            && run.state.is_terminal()
        {
            link.end(wire_end(run.state));
        }
    }

    /// Waits for `Loaded` after the package: `false` once the stream is
    /// done instead, as a load failure's `Finish`, a message that breaks the
    /// protocol, or the stream's end leaves it.
    async fn loaded(
        &self,
        requests: &mut Requests,
        run: &RunRecord,
        claim: &str,
        link: &StreamLink,
    ) -> bool {
        let violation = match requests.next().await.and_then(|request| request.message) {
            Some(run_request::Message::Loaded(_)) => None,
            None => return false,
            // A package the isolate could not load ends the Run unstarted.
            Some(run_request::Message::Request(coral_run_executor::wire::v1::Request {
                number: 1,
                kind: Some(coral_run_executor::wire::v1::request::Kind::Finish(finish)),
            })) => match program::completion(finish, run.value_limits) {
                Some(completion) => {
                    match self
                        .end_unstarted_answered(run, claim, &completion, link)
                        .await
                    {
                        Ok(end) => link.end(end),
                        Err(_unanswered) => {
                            link.close(Closing::Unavailable("the end is not written"));
                        }
                    }
                    return false;
                }
                None => Some("the Finish is invalid"),
            },
            Some(run_request::Message::Request(_)) => Some("Started was not sent"),
            Some(_) => Some("Loaded was expected"),
        };
        if let Some(reason) = violation {
            // A message that breaks the protocol interrupts the Run before
            // any Program code ran.
            let _interrupted = self
                .end_unstarted_answered(
                    run,
                    claim,
                    &ExecutionCompletion::ProgramHostTerminated,
                    link,
                )
                .await;
            link.close(Closing::InvalidArgument(reason));
            return false;
        }
        true
    }

    /// The Run an `Open` names, by its claim: with its package and the Tools
    /// it linked while it has not started, in two reads that hold nothing.
    async fn open_taken(
        &self,
        run_id: &RunId,
        claim_id: &str,
    ) -> Result<Option<(RunRecord, Option<(ProgramPackage, LinkedTools)>)>, Error> {
        // The package and the linked Tools are written once, at acceptance:
        // no snapshot is needed to read them beside the row.
        let Some((run, package)) =
            run_repository::find_taken(&self.database, run_id, claim_id).await?
        else {
            return Ok(None);
        };
        if run.state.is_terminal() || run.execution_started() {
            return Ok(Some((run, None)));
        }
        let linked =
            run_repository::linked_tools(&self.database, &run.workspace_id, &run.id).await?;
        Ok(Some((run, Some((package, linked)))))
    }

    /// The start of a taken Run: the authority recheck as the actor the row
    /// names, then one chunk with the start mark, the deadline and this
    /// Coral Instance as the holder, predicated on the take's claim. A lost
    /// authority fails the Run and no Program code runs. Run again after an
    /// attempt the database did not answer, it finds that attempt's start.
    async fn start_stream(
        &self,
        taken: &RunRecord,
        claim_id: &str,
        package: &ProgramPackage,
        reread: bool,
        started_at: &std::sync::atomic::AtomicI64,
    ) -> Result<Start, Error> {
        match self
            .start_on(taken, claim_id, package, reread, started_at)
            .await
        {
            // The row moved since `Open` read it: read it again.
            Err(Error::HeadMismatch | Error::RunEnded) if !reread => {
                self.start_on(taken, claim_id, package, true, started_at)
                    .await
            }
            Err(Error::ClaimMismatch) => Ok(Start::Ended(WireEnd::NotHeld)),
            started => started,
        }
    }

    /// [`Self::start_stream`] on `taken` as `Open` read it, or on the row
    /// read again when `reread`. `started_at` keeps the start mark an
    /// earlier attempt wrote: a started Run is this stream's only when its
    /// mark is that one.
    async fn start_on(
        &self,
        taken: &RunRecord,
        claim_id: &str,
        package: &ProgramPackage,
        reread: bool,
        started_at: &std::sync::atomic::AtomicI64,
    ) -> Result<Start, Error> {
        let mut transaction = self.database.begin().await?;
        let run = if reread {
            let Some(run) = run_repository::find_exact_in_transaction(
                &mut transaction,
                &taken.workspace_id,
                &taken.id,
            )
            .await?
            else {
                return Ok(Start::Ended(WireEnd::NotHeld));
            };
            run
        } else {
            taken.clone()
        };
        if let Some(end) = taken_end(&run, claim_id) {
            transaction.commit().await?;
            return Ok(Start::Ended(end));
        }
        let resumed = run.execution_started();
        if resumed && run.execution_started_at_unix_nanos != Some(started_at.load(Ordering::SeqCst))
        {
            transaction.commit().await?;
            return Ok(Start::StartedElsewhere);
        }
        let checked_at = self.now()?.max(run.accepted_at_unix_nanos);
        let checked = self
            .recheck_actor(&mut transaction, &run, checked_at)
            .await?;
        // The start mark is dated after the recheck's holds, and authority
        // must still hold at that instant.
        let now = self.now()?.max(checked_at);
        let authority = checked.and_then(|(actor, evidence)| {
            evidence.require_current_at(now).map_err(Error::from)?;
            Ok(actor)
        });
        let actor = match authority {
            Ok(actor) => actor,
            Err(error) if error.database_error().is_some() => return Err(error),
            Err(_lost) => {
                let outcome = RunTerminalOutcome::Failed(RunFailure::new(
                    RunFailureCode::RunAuthorityLost,
                    "Run authority was lost before Program execution started".to_string(),
                ));
                // A start mark that landed unanswered keeps the claim; no
                // Program code has run either way.
                if resumed {
                    run_repository::finish_run(&mut transaction, &run, claim_id, &outcome, now)
                        .await?;
                } else {
                    run_repository::finish_taken_run(
                        &mut transaction,
                        &run,
                        claim_id,
                        &outcome,
                        now,
                    )
                    .await?;
                }
                transaction.commit().await?;
                crate::trace::record_run_end(&run, &RunEnd::Unstarted(&outcome), now);
                return Ok(Start::Ended(WireEnd::Failed));
            }
        };
        let package = package.clone();
        // The attempt this one repeats landed: the Program starts once.
        if resumed {
            transaction.commit().await?;
            let deadline_at_unix_nanos = run
                .deadline_at_unix_nanos
                .ok_or_else(|| Error::CorruptData("started Run has no deadline".into()))?;
            return Ok(Start::Started(Box::new((
                actor,
                StartedExecution {
                    package,
                    head: run.chunk_sequence,
                    deadline_at_unix_nanos,
                },
            ))));
        }
        let deadline_at_unix_nanos = run
            .time_limit_nanos
            .and_then(|limit| now.checked_add(limit))
            .ok_or_else(|| Error::CorruptData("taken Run has no time limit".into()))?;
        started_at.store(now, Ordering::SeqCst);
        run_repository::start_taken(
            &mut transaction,
            &run,
            claim_id,
            now,
            deadline_at_unix_nanos,
            &self.instance,
        )
        .await?;
        transaction.commit().await?;
        Ok(Start::Started(Box::new((
            actor,
            StartedExecution {
                package,
                head: run.chunk_sequence + 1,
                deadline_at_unix_nanos,
            },
        ))))
    }

    /// The actor `run` names, rebuilt from current facts, with the evidence
    /// of its Run authority at `now`. The inner error is a lost authority,
    /// or a database that gave no answer.
    async fn recheck_actor(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        run: &RunRecord,
        now: i64,
    ) -> Result<Result<(AuthenticatedPrincipal, PrincipalWorkspaceEvidence), Error>, Error> {
        let registration = run
            .agent_registration_id
            .as_deref()
            .map(AgentRegistrationId::parse)
            .transpose()
            .map_err(|_invalid| {
                Error::CorruptData("Run has an invalid Agent Registration".into())
            })?;
        Ok(self
            .workspaces
            .check_run_start_in_transaction(
                transaction,
                &run.principal,
                registration.as_ref(),
                &run.workspace_id,
                now,
            )
            .await
            .map_err(Error::from))
    }

    /// [`Self::end_unstarted`], written until the database answers.
    /// It stops trying once the stream is gone.
    async fn end_unstarted_answered(
        &self,
        taken: &RunRecord,
        claim_id: &str,
        completion: &ExecutionCompletion,
        link: &StreamLink,
    ) -> Result<WireEnd, Error> {
        until_answered(
            &taken.held_id(),
            "unstarted end",
            || !link.responses.is_closed(),
            || self.end_unstarted(taken, claim_id, completion),
        )
        .await
    }

    /// Ends a taken Run whose Program never started, as its `Finish` says.
    async fn end_unstarted(
        &self,
        taken: &RunRecord,
        claim_id: &str,
        completion: &ExecutionCompletion,
    ) -> Result<WireEnd, Error> {
        let outcome = match completion {
            ExecutionCompletion::Failed(failure) => RunTerminalOutcome::Failed(failure.clone()),
            ExecutionCompletion::MemoryLimitExceeded => {
                RunTerminationKind::MemoryLimitExceeded.outcome()
            }
            ExecutionCompletion::CpuLimitExceeded => RunTerminationKind::CpuLimitExceeded.outcome(),
            ExecutionCompletion::Succeeded(_) | ExecutionCompletion::ProgramHostTerminated => {
                RunTerminationKind::Interrupted.outcome()
            }
        };
        let mut transaction = self.database.begin().await?;
        let Some(run) = run_repository::find_exact_in_transaction(
            &mut transaction,
            &taken.workspace_id,
            &taken.id,
        )
        .await?
        else {
            return Ok(WireEnd::NotHeld);
        };
        if let Some(end) = taken_end(&run, claim_id) {
            transaction.commit().await?;
            return Ok(end);
        }
        if run.execution_started() {
            transaction.commit().await?;
            return Ok(WireEnd::NotHeld);
        }
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        run_repository::finish_taken_run(&mut transaction, &run, claim_id, &outcome, now).await?;
        transaction.commit().await?;
        crate::trace::record_run_end(&run, &RunEnd::Unstarted(&outcome), now);
        Ok(wire_end(outcome.state()))
    }

    /// Runs the started Run's owner, with the stream as its Program, until
    /// it retires.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is one part of the started Run its owner takes"
    )]
    async fn run_on_stream(
        &self,
        run: RunRecord,
        claim_id: String,
        linked_snapshots: std::collections::HashMap<String, LinkedTool>,
        actor: AuthenticatedPrincipal,
        started: StartedExecution,
        requests: Requests,
        link: &Arc<StreamLink>,
    ) {
        let accepted = AcceptedRun {
            id: run.id,
            workspace_id: run.workspace_id,
            principal: run.principal,
            accepted_at_unix_nanos: run.accepted_at_unix_nanos,
            shutdown_grace_nanos: run.shutdown_grace_nanos,
            value_limits: run.value_limits,
            claim_id: claim_id.clone(),
            // A Run Executor's Run has no worker lease.
            worker_claim_expires_at_unix_nanos: run.accepted_at_unix_nanos,
            linked_snapshots,
            trace: run.trace,
            start: RunStartFacts {
                time_limit_nanos: run.time_limit_nanos,
                agent_registration_id: run.agent_registration_id,
            },
        };
        let (handle, mailbox): (OwnerHandle, _) = OwnerHandle::new(accepted.held_id(), claim_id);
        // Decisions and cancellations reach it through the Run Inbox until
        // the receiver commits them itself (#2907).
        let registration = self.register_owner(&handle);
        handle.send(Message::Started(Ok(Some(started))));
        let stream = Arc::new(StreamRun {
            actor,
            link: link.clone(),
            requests: Mutex::new(Some(requests)),
        });
        owner::host(
            self.clone(),
            accepted,
            OwnerStart::Stream(stream),
            handle,
            mailbox,
            registration,
        )
        .await;
    }
}

/// Sends `run`'s package, which links `linked`.
fn send_package(
    run: &RunRecord,
    package: &ProgramPackage,
    linked: &LinkedTools,
    link: &StreamLink,
) {
    let linked_snapshots = linked
        .iter()
        .filter_map(|(call_path, tool)| {
            Some(LinkedSnapshot {
                path: tool.kind.relative_path(call_path)?.to_owned(),
                tool_id: tool.binding.definition().to_owned(),
                root: match tool.kind {
                    coral_catalog::AuthoredToolKind::Private => LinkedToolRoot::Private,
                    coral_catalog::AuthoredToolKind::Shared => LinkedToolRoot::Shared,
                }
                .into(),
            })
        })
        .collect();
    link.send(run_response::Message::Package(RunPackage {
        sha256: coral_run_executor::package_sha256(&package.javascript, &package.source_map)
            .to_vec(),
        script: package.javascript.clone(),
        source_map: package.source_map.clone(),
        value_limits: Some(run.value_limits.into()),
        linked_snapshots,
        protocol_level: PROTOCOL_LEVEL,
        traceparent: run
            .trace
            .as_ref()
            .map_or_else(String::new, |trace| trace.as_str().to_owned()),
    }));
}

/// How a Run in `state` ended, on the stream.
const fn wire_end(state: RunState) -> WireEnd {
    match state {
        RunState::Running => WireEnd::Unspecified,
        RunState::Succeeded => WireEnd::Succeeded,
        RunState::Failed => WireEnd::Failed,
        RunState::Canceled => WireEnd::Canceled,
        RunState::TimedOut => WireEnd::TimedOut,
        RunState::Interrupted => WireEnd::Interrupted,
    }
}
