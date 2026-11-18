//! How a Turn ends when its Coral Instance stops: at once when the instance
//! shuts down, or at the next read of the Turn once its lease lapses.

use coral_telemetry::safe;
use coral_workspace::WorkspaceId;

use super::{Error, SubAgents, Turn, TurnState};
use crate::repository;

impl SubAgents {
    /// Ends the Turn as interrupted once this Coral Instance begins to shut
    /// down.
    pub(super) async fn interrupt_at_shutdown(&self, workspace_id: &WorkspaceId, turn_id: &str) {
        let mut stopping = self.stopping.clone();
        // A dropped sender reads as a shutdown.
        drop(stopping.wait_for(|stopping| *stopping).await);
        let interrupted = self.end_turn(workspace_id, turn_id, &TurnState::Interrupted);
        if let Err(error) = interrupted.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "the interruption of a Sub-agent Turn was not recorded"
            );
        }
    }

    /// Ends a running Turn whose lease lapsed as interrupted: its Coral
    /// Instance stopped without shutting down.
    pub(super) async fn interrupt_if_lapsed(
        &self,
        workspace_id: &WorkspaceId,
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
