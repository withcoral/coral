//! The reopen of a started Run's stream. The Coral Instance that lands the
//! `Open` writes itself as the stream's holder in one chunk under the claim
//! and the head, which also settles the calls that were in flight on the loop
//! before, and answers `Committed`. The loop is
//! rebuilt from the ledger: the head, the output total, the request order
//! after the watermark, and each open Invocation; the resent requests apply
//! on top, and `Started` goes out again. Each accepted reopen counts as
//! `coral.run.reconnects`; a refused one counts nothing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use coral_provider::ResolvedInvocationTool;
use coral_run_executor::wire::v1::{Awaited, RunEnd as WireEnd};
use coral_run_executor::{Closing, SHUTTING_DOWN};
use coral_telemetry::metric;
use coral_workspace::AuthenticatedPrincipal;

use super::program::stream_violated;
use super::{LinkedTools, Requests, StreamLink, taken_end, wire_end};
use crate::execution::until_answered;
use crate::invocation_lifecycle::{self, InvocationState};
use crate::invocation_repository::{self, RecordedOutcome, StreamInvocation};
use crate::lifecycle::RunRecord;
use crate::run_stream::run_loop::StartedExecution;
use crate::{Error, InvocationId, RunModule, run_repository};

/// How many times a reopen reads the Run again after a writer moved its
/// head under it, before the Run Executor is told to open it later.
const REOPEN_ATTEMPTS: usize = 8;

/// What a reopened stream's loop starts from, beside the Run row.
pub(crate) struct Resume {
    /// The highest request number committed when the stream reopened.
    pub(crate) watermark: u64,
    /// The Run Output Budget total the Run row recorded then.
    pub(crate) output_total: usize,
    /// Each Invocation a request opened that is open, with the Tool it
    /// retained, or that the Run Executor waits on, in request order.
    pub(crate) invocations: Vec<(StreamInvocation, Option<ResolvedInvocationTool>)>,
    /// The request numbers of every Perform that ended, the reopen's own
    /// settles included: `invocations` holds only the open and awaited ones.
    pub(crate) ended_performs: Vec<u64>,
    /// The requests the Run Executor waits on an answer to that the
    /// watermark covers.
    pub(crate) awaited: Vec<Awaited>,
}

/// What the reopen found.
enum Reopen {
    Reopened(Box<(AuthenticatedPrincipal, RunRecord, Resume)>),
    Ended(WireEnd),
    /// No actor can be rebuilt for the Run: its Principal is gone.
    Unowned,
    /// An older build recorded a termination, and the loop that recorded it
    /// is gone.
    Terminating(Box<RunRecord>),
    Unavailable(&'static str),
}

/// The Tool each open Invocation retained, by its id, as a read snapshot
/// rebuilt it: `None` when it is gone.
type RetainedTools = HashMap<InvocationId, Option<ResolvedInvocationTool>>;

impl RunModule {
    /// Serves the reopened stream of `run`, a started Run that `claim_id`
    /// holds, as `Open` read it with its linked Tools.
    pub(super) async fn reopen_stream(
        &self,
        requests: Requests,
        link: &Arc<StreamLink>,
        run: RunRecord,
        claim_id: String,
        linked: LinkedTools,
        awaited: Vec<Awaited>,
    ) {
        let held = run.held_id();
        // Held until the loop is registered, so a Shutdown Drain counts it.
        let Ok(admitted) = self.admission.admit() else {
            return link.close(Closing::Unavailable(SHUTTING_DOWN));
        };
        // A loop of this Run still here commits nothing more on a moved head.
        self.loops.supersede(&run.id);
        if !awaited_well_formed(&awaited) {
            return self.refuse_broken_open(&run, &claim_id, link).await;
        }
        let reopened = until_answered(
            &held,
            "Run stream reopen",
            || !link.responses.is_closed(),
            || self.reopen(&run, &claim_id, &linked, &awaited),
        )
        .await;
        // `Resume` keeps the entries it answers: the list is not kept.
        drop(awaited);
        let (actor, run, resume) = match reopened {
            Ok(Reopen::Reopened(reopened)) => *reopened,
            Ok(Reopen::Ended(end)) => return link.end(end),
            Ok(Reopen::Unowned) => {
                return match self.interrupt_on_reopen(&run, &claim_id).await {
                    Ok(()) => link.end(WireEnd::Interrupted),
                    Err(_unanswered) => link.close(Closing::Unavailable("the end is not written")),
                };
            }
            Ok(Reopen::Terminating(run)) => {
                return self.end_terminating(&run, &claim_id, link).await;
            }
            Ok(Reopen::Unavailable(reason)) => return link.close(Closing::Unavailable(reason)),
            Err(_unanswered) => {
                return link.close(Closing::Unavailable("the reopen is not written"));
            }
        };
        let Some(deadline_at_unix_nanos) = run.deadline_at_unix_nanos else {
            return link.close(Closing::Unavailable("the started Run has no deadline"));
        };
        if let Some(kind) = run.run_executor_kind {
            metric::run_reconnected(kind.metric());
        }
        link.committed(Some(resume.watermark));
        let started = StartedExecution {
            head: run.chunk_sequence,
            deadline_at_unix_nanos,
        };
        let (workspace_id, id) = (run.workspace_id.clone(), run.id.clone());
        self.run_on_stream(
            run,
            claim_id,
            linked,
            actor,
            started,
            requests,
            link,
            admitted,
            Some(resume),
        )
        .await;
        self.close_served(link, &workspace_id, &id).await;
    }

    /// Interrupts a Run whose `Open` broke the protocol, as a break on its
    /// stream would, and closes the stream. Nothing is reopened.
    async fn refuse_broken_open(&self, run: &RunRecord, claim_id: &str, link: &StreamLink) {
        stream_violated(&run.held_id());
        link.violated.store(true, Ordering::SeqCst);
        // A Run that ended or that another writer took has nothing to
        // interrupt: the stream closes the same way.
        let _interrupted = self.interrupt_on_reopen(run, claim_id).await;
        self.close_served(link, &run.workspace_id, &run.id).await;
    }

    /// [`Self::record_interruption`] by a reopen, which does not hold the
    /// Run's stream yet.
    async fn interrupt_on_reopen(&self, run: &RunRecord, claim_id: &str) -> Result<(), Error> {
        self.record_interruption(
            &run.workspace_id,
            &run.held_id(),
            claim_id,
            None,
            crate::termination::Interrupter::Reopen,
        )
        .await
    }

    /// Ends a Run whose termination an older build's loop recorded and did
    /// not end the Run with: one chunk ends its open Invocations, whose
    /// dispatches were lost with that loop, and the Run with the recorded
    /// termination, under the claim and the head; no grace. The Run Executor
    /// then learns how it ended.
    async fn end_terminating(&self, run: &RunRecord, claim_id: &str, link: &StreamLink) {
        let ended = self.interrupt_on_reopen(run, claim_id).await;
        if !matches!(ended, Ok(()) | Err(Error::RunEnded)) {
            return link.close(Closing::Unavailable("the end is not written"));
        }
        self.close_served(link, &run.workspace_id, &run.id).await;
    }

    async fn reopen(
        &self,
        opened: &RunRecord,
        claim_id: &str,
        linked: &LinkedTools,
        awaited: &[Awaited],
    ) -> Result<Reopen, Error> {
        // The Tools are rebuilt before the write, which then holds no
        // schema graph load; the enforcement point checks them current.
        let retained = self.retained_tools(opened, linked).await?;
        for _attempt in 0..REOPEN_ATTEMPTS {
            match self
                .reopen_once(opened, claim_id, linked, awaited, &retained)
                .await
            {
                Err(Error::HeadMismatch) => {}
                reopened => return reopened,
            }
        }
        Ok(Reopen::Unavailable("the Run keeps moving under its reopen"))
    }

    /// The Tool each open Invocation of `run` retained, in a read snapshot.
    async fn retained_tools(
        &self,
        run: &RunRecord,
        linked: &LinkedTools,
    ) -> Result<RetainedTools, Error> {
        let mut snapshot = self.database.begin_read_snapshot().await?;
        let invocations = invocation_repository::for_run_stream(&mut snapshot, run, &[]).await?;
        let mut retained = HashMap::with_capacity(invocations.len());
        for invocation in &invocations {
            let tool = self
                .retained_tool(&mut snapshot, run, linked, invocation)
                .await?;
            retained.insert(invocation.id.clone(), tool);
        }
        snapshot.commit().await?;
        Ok(retained)
    }

    /// One attempt at the reopen's chunk, on the Run read again: the calls
    /// in flight on the loop before are settled, each open Invocation takes
    /// the Tool it retained (one whose Tool is gone ends as the enforcement
    /// point would end it), and this Coral Instance holds the stream.
    async fn reopen_once(
        &self,
        opened: &RunRecord,
        claim_id: &str,
        linked: &LinkedTools,
        awaited: &[Awaited],
        retained: &RetainedTools,
    ) -> Result<Reopen, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) = run_repository::find_exact_in_transaction(
            &mut transaction,
            &opened.workspace_id,
            &opened.id,
        )
        .await?
        else {
            return Ok(Reopen::Ended(WireEnd::NotHeld));
        };
        if let Some(end) = taken_end(&run, claim_id) {
            transaction.commit().await?;
            return Ok(Reopen::Ended(end));
        }
        if run.termination.is_some() {
            transaction.commit().await?;
            return Ok(Reopen::Terminating(Box::new(run)));
        }
        let actor = match self
            .workspaces
            .run_actor_in_transaction(&mut transaction, &run.principal, &run.workspace_id)
            .await
            .map_err(Error::from)
        {
            Ok(actor) => actor,
            Err(error) if error.database_error().is_some() => return Err(error),
            Err(_gone) => {
                transaction.rollback().await?;
                return Ok(Reopen::Unowned);
            }
        };
        let watermark = run.committed_request_number.unwrap_or(0);
        // The requests the Run Executor waits on that the watermark covers,
        // a prefix of the list in number order: the rest it sends again.
        let answered: Vec<Awaited> = awaited
            .iter()
            .take_while(|awaited| awaited.number <= watermark)
            .copied()
            .collect();
        let numbers: Vec<u64> = answered
            .iter()
            .flat_map(|awaited| [awaited.number, awaited.perform])
            .filter(|number| *number != 0)
            .collect();
        // A `Complete` the watermark covers was taken by the loop before:
        // its Perform's end, if it has none yet, was lost with that loop.
        let completes_lost: HashSet<u64> = answered
            .iter()
            .map(|awaited| awaited.perform)
            .filter(|perform| *perform != 0)
            .collect();
        let mut invocations =
            invocation_repository::for_run_stream(&mut transaction, &run, &numbers).await?;
        let now = run.commit_mark(self.now()?);
        let mut events = invocation_lifecycle::settle_lost_dispatches(
            &mut transaction,
            &run,
            &mut invocations,
            |invocation| linked.contains_key(&invocation.call_path),
            &completes_lost,
            now,
        )
        .await?;
        let mut resumed = Vec::with_capacity(invocations.len());
        for mut invocation in invocations {
            let target = match (invocation.state, retained.get(&invocation.id)) {
                (InvocationState::Ended, _) => None,
                (_, Some(target)) => target.clone(),
                // Admitted after the snapshot.
                (_, None) => {
                    self.retained_tool(&mut transaction, &run, linked, &invocation)
                        .await?
                }
            };
            if target.is_none() {
                self.end_without_tool(&mut transaction, &run, &mut invocation, &mut events, now)
                    .await?;
            }
            resumed.push((invocation, target));
        }
        // Read after the settles above, which this transaction wrote.
        let ended_performs =
            invocation_repository::ended_performs(&mut transaction, &run, linked.keys()).await?;
        run_repository::reopen_taken(
            &mut transaction,
            &run,
            claim_id,
            &events,
            now,
            &self.instance,
        )
        .await?;
        transaction.commit().await?;
        let mut run = run;
        run.chunk_sequence += 1;
        let resume = Resume {
            watermark,
            output_total: run.accumulated_output_bytes,
            invocations: resumed,
            ended_performs,
            awaited: answered,
        };
        Ok(Reopen::Reopened(Box::new((actor, run, resume))))
    }

    /// Ends an open `invocation` whose Tool is gone: a `pending` one as the
    /// enforcement point would end it, a `dispatching` Perform, which
    /// nothing can complete now, as `indeterminate`.
    async fn end_without_tool(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        run: &RunRecord,
        invocation: &mut StreamInvocation,
        events: &mut Vec<crate::run_repository::PendingEvent>,
        now: i64,
    ) -> Result<(), Error> {
        match invocation.state {
            InvocationState::Ended => {}
            InvocationState::Pending => {
                events.push(
                    invocation_lifecycle::end_unreachable(transaction, run, &invocation.id, now)
                        .await?,
                );
                invocation.state = InvocationState::Ended;
                invocation.outcome =
                    Some(RecordedOutcome::failed(crate::CoralFailure::AuthorityLost));
            }
            InvocationState::Dispatching => events.push(
                invocation_lifecycle::settle_lost(transaction, run, invocation, None, now).await?,
            ),
        }
        Ok(())
    }

    /// The Tool `invocation` retained: the linked definition for a Private
    /// or Shared Tool, the recorded Generation's Tool for any other. `None`
    /// when it is gone.
    async fn retained_tool(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        run: &RunRecord,
        linked: &LinkedTools,
        invocation: &StreamInvocation,
    ) -> Result<Option<ResolvedInvocationTool>, Error> {
        if let Some(tool) = linked.get(&invocation.call_path) {
            let Some(relative_path) = tool.kind.relative_path(&invocation.call_path) else {
                return Ok(None);
            };
            return Ok(self
                .providers
                .resolve_authored_tool_in_transaction(
                    transaction,
                    &run.workspace_id,
                    tool.kind,
                    relative_path,
                    tool.binding.clone(),
                    tool.declared_default,
                )
                .await?);
        }
        Ok(self
            .providers
            .retained_invocation_tool_in_transaction(
                transaction,
                &run.workspace_id,
                &invocation.provider_id,
                invocation.credential_route,
                invocation.provider_version,
                &invocation.generation,
                &invocation.tool,
            )
            .await?)
    }

    /// The end of a served stream: a broken protocol closes it, and a Run
    /// that ended says how.
    pub(super) async fn close_served(
        &self,
        link: &StreamLink,
        workspace_id: &coral_workspace::WorkspaceId,
        id: &crate::RunId,
    ) {
        if link.violated.load(Ordering::SeqCst) {
            return link.close(Closing::InvalidArgument(
                "the Run stream broke the protocol",
            ));
        }
        // The stream moves: the Coral Instance it reopens on answers how the
        // Run stands.
        if link.moving.load(Ordering::SeqCst) {
            return link.close(Closing::Moved);
        }
        if let Ok(Some(run)) = run_repository::find_exact(&self.database, workspace_id, id).await
            && run.state.is_terminal()
        {
            link.end(wire_end(run.state));
        }
    }
}

/// Whether `awaited` lists requests as a Run Executor waits on them: each
/// once, in ascending number order, every number above zero, and each
/// `Complete` after the Perform it completes. A reopen then answers again at
/// most one entry for each request the Run committed.
fn awaited_well_formed(awaited: &[Awaited]) -> bool {
    awaited
        .iter()
        .try_fold(0, |last, entry| {
            (last < entry.number && entry.perform < entry.number).then_some(entry.number)
        })
        .is_some()
}
