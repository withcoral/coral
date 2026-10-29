//! Non-consuming Run observation: one authorized snapshot per read, with
//! bounded waiting on committed changes. A deadline wakes nothing by itself:
//! the Run Owner's record of a timeout or an expiry is a committed change.

use std::time::Duration;

use coral_db::{Database, Transaction};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::lifecycle::RunRecord;
use crate::{
    Error, RequiredAction, RunCursor, RunEvent, RunId, RunModule, RunObservation, RunUpdate,
    required_actions, run_repository,
};

/// Minimum spacing between one waiter's rereads.
const OBSERVE_MIN_REREAD_GAP: Duration = Duration::from_millis(50);
/// Reread cadence for a waiter whose change notifications are unavailable.
const OBSERVE_FALLBACK_POLL: Duration = Duration::from_millis(500);

/// Reads `settled` again after every ring of the change signal, at most once
/// per reread gap, until it holds or `until` passes, as [`RunModule::observe`]
/// waits: without the signal it reads again at the fallback poll.
pub(crate) async fn read_until<Read>(
    database: &Database,
    until: tokio::time::Instant,
    mut settled: impl FnMut() -> Read,
) -> Result<bool, Error>
where
    Read: Future<Output = Result<bool, Error>>,
{
    let mut changes = database.subscribe_changes().ok();
    loop {
        let read_at = tokio::time::Instant::now();
        if settled().await? {
            return Ok(true);
        }
        if read_at >= until {
            return Ok(false);
        }
        match &mut changes {
            Some(subscription) => tokio::select! {
                result = subscription.changed() => if result.is_err() { changes = None },
                () = tokio::time::sleep_until(until) => {}
            },
            None => tokio::time::sleep_until(until.min(read_at + OBSERVE_FALLBACK_POLL)).await,
        }
        tokio::time::sleep_until(until.min(read_at + OBSERVE_MIN_REREAD_GAP)).await;
    }
}

impl RunModule {
    /// Returns a non-consuming observation of an authorized Run, optionally waiting.
    ///
    /// Independent callers can replay authored Emissions/Logs from the same cursor.
    /// A terminal outcome appears on the final output page, without draining internal Events. Required
    /// actions are a current snapshot; their content revision grants no authority.
    /// Every read rechecks visibility, including while waiting. Waits reread after
    /// committed changes, and poll slowly if notification fails.
    ///
    /// # Errors
    ///
    /// An absent Run, inaccessible Workspace, cross-Principal Run, or Agent
    /// history access returns [`Error::RunNotFound`]. Invalid cursors and
    /// persistence corruption are also rejected.
    pub async fn observe(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<RunUpdate, Error> {
        let link = !observation.is_by_acceptor();
        self.observe_linking(actor, workspace_id, run_id, observation, link)
            .await
    }

    /// [`Self::observe`]; `link` links the current request to the Run Trace
    /// at the first snapshot, unless the caller already has.
    pub(crate) async fn observe_linking(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
        link: bool,
    ) -> Result<RunUpdate, Error> {
        validate_cursor(run_id, observation)?;
        let deadline = tokio::time::Instant::now() + observation.wait();
        // Subscribe before the first snapshot so a commit during it still wakes this wait.
        // A wake-up is only a hint: the database stays authoritative, so an unavailable
        // notification path degrades to slow polling instead of failing the wait.
        let mut changes = if observation.wait().is_zero() {
            None
        } else {
            self.database.subscribe_changes().ok()
        };
        let mut seen_sequence = None;
        loop {
            let update = self
                .observe_snapshot(actor, workspace_id, run_id, observation)
                .await?;
            let read_at = tokio::time::Instant::now();
            if link && seen_sequence.is_none() {
                // The caller may see the Run: this request links to its trace once.
                crate::trace::link_request(update.run_trace.as_ref());
            }
            let seen_sequence =
                *seen_sequence.get_or_insert_with(|| observation.seen_sequence(&update));
            if observation.is_ready(&update, seen_sequence) || read_at >= deadline {
                return Ok(update);
            }
            match &mut changes {
                Some(subscription) => tokio::select! {
                    result = subscription.changed() => if result.is_err() { changes = None },
                    () = tokio::time::sleep_until(deadline) => {}
                },
                None => {
                    tokio::time::sleep_until(deadline.min(read_at + OBSERVE_FALLBACK_POLL)).await;
                }
            }
            // Busy Runs commit often. Coalesce their wake-ups into bounded rereads.
            tokio::time::sleep_until(deadline.min(read_at + OBSERVE_MIN_REREAD_GAP)).await;
        }
    }

    async fn observe_snapshot(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<RunUpdate, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let include_workspace_agents = self
            .visibility_scope_in_transaction(&mut transaction, actor, workspace_id)
            .await?;
        let run = run_repository::find_visible_in_transaction(
            &mut transaction,
            workspace_id,
            run_id,
            actor.principal(),
            include_workspace_agents,
        )
        .await?
        .ok_or(Error::RunNotFound)?;
        let actions =
            required_actions::snapshot(&mut transaction, &run, actor.principal(), self.now()?)
                .await?;
        let update = read_update(&mut transaction, run, observation, actions).await?;
        transaction.commit().await?;
        Ok(update)
    }
}

async fn read_update(
    transaction: &mut Transaction<'_>,
    run: RunRecord,
    observation: &RunObservation,
    actions: Vec<RequiredAction>,
) -> Result<RunUpdate, Error> {
    let after_sequence = observation.cursor().map_or(0, RunCursor::event_sequence);
    let last_sequence = run
        .last_event_sequence
        .ok_or_else(|| Error::CorruptData("observed Run was read without its Events".into()))?;
    if after_sequence > last_sequence {
        return Err(Error::InvalidInput("Run cursor is invalid".to_string()));
    }
    let (events, has_more) = run_repository::output_page_in_transaction(
        transaction,
        &run.workspace_id,
        &run.id,
        after_sequence,
        observation.event_limit(),
        run.value_limits,
    )
    .await?;
    // A complete output page acknowledges hidden records through this snapshot.
    // A partial page stops at its last delivered occurrence, never beyond omitted output.
    let next_sequence = if has_more {
        events.last().map_or(after_sequence, RunEvent::sequence)
    } else {
        last_sequence
    };
    let trace_id = run.diagnostic_trace_id();
    let run_trace = run.trace.clone();
    let mut update = RunUpdate::new(
        run.id.clone(),
        run.state,
        events,
        RunCursor::new(&run.id, next_sequence, required_actions::revision(&actions)),
        has_more,
        run.terminal_outcome,
        run.value_limits,
    )?;
    update.required_actions = actions;
    update.trace_id = trace_id;
    update.run_trace = run_trace;
    Ok(update)
}

pub(crate) fn validate_cursor(run_id: &RunId, observation: &RunObservation) -> Result<(), Error> {
    if observation
        .cursor()
        .is_some_and(|cursor| !cursor.belongs_to(run_id))
    {
        return Err(Error::InvalidInput("Run cursor is invalid".to_string()));
    }
    Ok(())
}
