//! How a Turn ends when its Coral Instance stops: at once when the instance
//! shuts down, or at the next read of the Turn once its lease lapses. An
//! ended Turn leaves no tool call without a result in its thread.

use coral_db::Transaction;
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;

use super::{Error, RunningTurn, SubAgents, Turn, TurnState};
use crate::model::Message;
use crate::repository;

/// What the model reads as the result of a call that its Turn never got.
const NO_RESULT: &str = "No result: the Turn stopped before this call returned. A Run that the call started may still be running.";

impl SubAgents {
    /// Ends the Turn as interrupted once this Coral Instance begins to shut
    /// down.
    pub(super) async fn interrupt_at_shutdown(&self, turn: &RunningTurn) {
        let mut stopping = self.stopping.clone();
        // A dropped sender reads as a shutdown.
        drop(stopping.wait_for(|stopping| *stopping).await);
        let (workspace_id, interrupted) = (&turn.caller.workspace_id, &TurnState::Interrupted);
        let interrupted = self.end_turn(workspace_id, &turn.sub_agent_id, &turn.id, interrupted);
        if let Err(error) = interrupted.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "the interruption of a Sub-agent Turn was not recorded"
            );
        }
    }

    /// Ends a running Turn whose lease lapsed as interrupted, and closes the
    /// tool calls it left without results: its Coral Instance stopped without
    /// shutting down.
    pub(super) async fn interrupt_if_lapsed(
        &self,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
        turn: Turn,
    ) -> Result<Turn, Error> {
        let now = self.clock.now();
        if turn.state != TurnState::Running || now < turn.lease_expires_at {
            return Ok(turn);
        }
        let mut transaction = self.database.begin().await?;
        let interrupted =
            repository::interrupt_lapsed_turn(&mut transaction, workspace_id, &turn.id, now)
                .await?;
        if interrupted {
            close_calls(&mut transaction, workspace_id, sub_agent_id, &turn.id).await?;
        }
        transaction.commit().await?;
        if !interrupted {
            return Ok(turn);
        }
        Ok(Turn {
            state: TurnState::Interrupted,
            ..turn
        })
    }
}

/// Answers each tool call of the Turn's last message with [`NO_RESULT`].
/// Only the last message can hold calls without results: a Turn stores the
/// results of a reply's calls together, right after the reply.
pub(super) async fn close_calls(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    turn_id: &str,
) -> Result<(), Error> {
    let last = repository::last_message(&mut *transaction, workspace_id, sub_agent_id, turn_id);
    let last = last.await?;
    let Some((position, message)) = last else {
        return Ok(());
    };
    let no_results: Vec<_> = message
        .tool_calls()
        .map(|call| call.result(NO_RESULT.to_owned(), None))
        .collect();
    if no_results.is_empty() {
        return Ok(());
    }
    let results = Message::tool_results(no_results);
    let after = position.saturating_add(1);
    let closed = repository::insert_message_unless_taken(
        transaction,
        workspace_id,
        sub_agent_id,
        turn_id,
        after,
        &results,
    );
    Ok(closed.await?)
}
