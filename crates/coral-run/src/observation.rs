//! Non-consuming Run observation: one authorized snapshot per read, with
//! bounded waiting on committed changes and required-action expiry.

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::lifecycle::RunRecord;
use crate::{
    Error, RequiredAction, RunCursor, RunEvent, RunId, RunModule, RunObservation, RunUpdate,
    duration_until, required_actions, run_repository,
};

impl RunModule {
    /// Returns a non-consuming observation of an authorized Run, optionally waiting.
    ///
    /// Independent callers can replay authored Emissions/Logs from the same cursor.
    /// A terminal outcome appears on the final output page, without draining internal Events. Required
    /// actions are a current snapshot; their content revision grants no authority.
    /// Every read rechecks visibility, including while waiting. Waits reread after
    /// committed changes and action expiries, and poll slowly if notification fails.
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
        validate_cursor(run_id, observation)?;
        let mut wait = self.database.wait_for_changes(observation.wait());
        let mut seen_sequence = None;
        loop {
            let update = self
                .observe_snapshot(actor, workspace_id, run_id, observation)
                .await?;
            let seen_sequence =
                *seen_sequence.get_or_insert_with(|| observation.seen_sequence(&update));
            if observation.is_ready(&update, seen_sequence) {
                return Ok(update);
            }
            // Expiry changes action visibility without a commit.
            let read_at = tokio::time::Instant::now();
            let now = self.now()?;
            let expiry = update
                .required_actions()
                .iter()
                .map(|action| read_at + duration_until(action.deadline_at_unix_nanos, now))
                .min();
            if !wait.pause(expiry).await {
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
    let mut update = RunUpdate::new(
        run.id.clone(),
        run.state,
        events,
        RunCursor::new(run.id, next_sequence, required_actions::revision(&actions)),
        has_more,
        run.terminal_outcome,
        run.value_limits,
    )?;
    update.required_actions = actions;
    Ok(update)
}

pub(crate) fn validate_cursor(run_id: &RunId, observation: &RunObservation) -> Result<(), Error> {
    if observation
        .cursor()
        .is_some_and(|cursor| cursor.run_id() != run_id)
    {
        return Err(Error::InvalidInput("Run cursor is invalid".to_string()));
    }
    Ok(())
}
