//! Non-consuming Run observation: one authorized snapshot per read, with
//! bounded waiting on committed changes. A deadline wakes nothing by itself:
//! the loop's record of a timeout or an expiry is a committed change.

use std::time::Duration;

use coral_db::{ChangeSubscription, Database, Transaction};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::lifecycle::RunRecord;
use crate::{
    Error, RequiredAction, RunCursor, RunEvent, RunFailureCode, RunId, RunModule, RunObservation,
    RunTerminalOutcome, RunUpdate, invocation_repository, required_actions, run_repository,
    tool_result_shape,
};

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
        let link = observation.links_trace();
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
        // Start the wait before the first snapshot, so a commit during it still wakes this wait.
        let mut wait = self.database.wait_for_changes(deadline);
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
            if observation.is_ready(&update, seen_sequence) || !wait.pause(read_at).await {
                return Ok(update);
            }
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
    // Only the page that carries the failure reads what the Program received.
    let tool_result_shapes = match &run.terminal_outcome {
        Some(RunTerminalOutcome::Failed(failure))
            if !has_more && failure.code() == RunFailureCode::ProgramRejected =>
        {
            let (received, unread) = invocation_repository::received_results(
                transaction,
                &run.workspace_id,
                &run.id,
                tool_result_shape::RESULTS_READ,
                tool_result_shape::RESULTS_READ_MAX_BYTES,
            )
            .await?;
            tool_result_shape::sketch(received, unread)
        }
        _ => crate::ToolResultShapes::default(),
    };
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
    update.tool_result_shapes = tool_result_shapes;
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

/// Returns once the change signal rings, or an outage of it begins (the
/// next call then waits for its return); never without a subscription.
pub(crate) async fn rung(changes: &mut Option<ChangeSubscription>) {
    match changes {
        Some(changes) => {
            let _rang_or_lost = changes.changed().await;
        }
        None => std::future::pending().await,
    }
}

/// The shortest gap between two reads of a reader that follows the change
/// signal: the wake-up reader's.
const READ_GAP: Duration = Duration::from_millis(250);
/// The longest gap between two such reads: the backstop for a change signal
/// that does not ring.
const BACKSTOP: Duration = Duration::from_secs(15);

/// What started one read of a [`Cadence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Turn {
    /// The reader's first read.
    First,
    /// The change signal rang, or an outage of it began.
    Rang,
    /// A writer in this Coral Instance woke the reader.
    Woken,
    /// The backstop.
    Backstop,
}

/// When a reader reads: on subscribe; after a ring of the change signal
/// (the first after an outage included), at most once per gap; at once on
/// a wake; and at the backstop.
#[derive(Default)]
pub(crate) struct Cadence {
    changes: Option<ChangeSubscription>,
    last_read: Option<tokio::time::Instant>,
    /// The change signal rang (or an outage of it began) before the last
    /// read, whatever started it.
    rang: bool,
}

impl Cadence {
    /// Waits for the next read and says what started it. A ring that comes
    /// within the read gap is kept when a wake starts the read first.
    pub(crate) async fn turn(&mut self, database: &Database, woken: &tokio::sync::Notify) -> Turn {
        let mut turn = Turn::First;
        let mut rang = false;
        if let Some(last_read) = self.last_read {
            let changes = &mut self.changes;
            turn = loop {
                tokio::select! {
                    () = rung(changes), if !rang => rang = true,
                    () = tokio::time::sleep_until(last_read + READ_GAP), if rang => break Turn::Rang,
                    () = woken.notified() => break Turn::Woken,
                    () = tokio::time::sleep_until(last_read + BACKSTOP) => break Turn::Backstop,
                }
            };
        }
        if self.changes.is_none() {
            self.changes = database.subscribe_changes().ok();
        }
        self.rang = rang;
        self.last_read = Some(tokio::time::Instant::now());
        turn
    }

    /// Whether the change signal rang before the last read, whichever turn
    /// started it.
    pub(crate) const fn rang(&self) -> bool {
        self.rang
    }
}
