//! The lead's messages to a Sub-agent after its first. A running Turn reads
//! them before its next model call, and before it ends with an answer. Once
//! the Turn has ended, a new Turn answers them and continues the thread.

use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use tracing::Instrument as _;

use super::{Error, RunningTurn, SubAgents, TURN_LEASE, TurnFailure, TurnState};
use crate::model::Message;
use crate::repository;
use crate::run_tools::Caller;
use crate::telemetry::TurnTrace;

/// How often a message tries again after another call started a Turn first.
const MESSAGE_ATTEMPTS: usize = 3;

impl SubAgents {
    /// Gives the caller's own Sub-agent `message`: its running Turn reads it,
    /// or a new Turn answers it.
    pub(crate) async fn message(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
        message: &str,
    ) -> Result<(), Error> {
        let mut started = Ok(());
        for _attempt in 0..MESSAGE_ATTEMPTS {
            let turn = self.turn(actor, workspace_id, sub_agent_id).await?;
            if turn.state == TurnState::Running
                && self.add_unread(workspace_id, &turn.id, message).await?
            {
                return Ok(());
            }
            started = self
                .next_turn(actor, workspace_id, sub_agent_id, message)
                .await;
            match &started {
                // Another call started a Turn first, which can read this one.
                Err(Error::Database(error)) if error.is_unique_violation() => {}
                _ => return started,
            }
        }
        started
    }

    /// Adds the lead's unread messages to the thread, as one message.
    pub(super) async fn read_messages(
        &self,
        turn: &RunningTurn,
        thread: &mut Vec<Message>,
    ) -> Result<(), TurnFailure> {
        let position = i64::try_from(thread.len()).unwrap_or(i64::MAX);
        let read = async {
            let mut transaction = self.database.begin().await?;
            let workspace_id = &turn.caller.workspace_id;
            let unread = repository::take_unread_messages(&mut transaction, workspace_id, &turn.id);
            let message = unread.await?.map(Message::user);
            if let Some(message) = &message {
                repository::insert_message(&mut transaction, turn, position, message, None).await?;
            }
            transaction.commit().await?;
            Ok::<_, coral_db::Error>(message)
        };
        let message = read
            .await
            .map_err(|error| TurnFailure::internal(error.code()))?;
        thread.extend(message);
        Ok(())
    }

    /// Ends the Turn with `answer` unless the lead sent a message that it has
    /// not read. `false` means that a message waits.
    pub(super) async fn end_unless_messaged(
        &self,
        turn: &RunningTurn,
        answer: &str,
    ) -> Result<bool, TurnFailure> {
        let ended = async {
            let mut transaction = self.database.begin().await?;
            let workspace_id = &turn.caller.workspace_id;
            let ended = repository::complete_unless_unread(
                &mut transaction,
                workspace_id,
                &turn.id,
                answer,
            );
            let ended = ended.await?;
            transaction.commit().await?;
            Ok::<_, coral_db::Error>(ended)
        };
        ended
            .await
            .map_err(|error| TurnFailure::internal(error.code()))
    }

    async fn add_unread(
        &self,
        workspace_id: &WorkspaceId,
        turn_id: &str,
        message: &str,
    ) -> Result<bool, Error> {
        let mut transaction = self.database.begin().await?;
        let added =
            repository::add_unread_message(&mut transaction, workspace_id, turn_id, message);
        let added = added.await?;
        transaction.commit().await?;
        Ok(added)
    }

    /// Starts a Turn that answers `message` after the Sub-agent's thread, in
    /// the caller's open Task.
    async fn next_turn(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
        message: &str,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let task_id = repository::task_id(&mut transaction, workspace_id, sub_agent_id).await?;
        let task_id = task_id.ok_or(Error::SubAgentNotFound)?;
        let (task, authority) = self
            .tasks
            .hold_open_in_transaction(&mut transaction, actor, workspace_id, &task_id)
            .await?;
        let now = self.clock.now();
        authority
            .require_current_at(now)
            .map_err(coral_task::Error::from)?;
        let trace = TurnTrace::begin(sub_agent_id, actor, workspace_id, task.telemetry_id());
        let turn = RunningTurn {
            trace,
            id: uuid::Uuid::new_v4().to_string(),
            sub_agent_id: sub_agent_id.to_owned(),
            caller: Caller {
                principal: actor.clone(),
                workspace_id: workspace_id.clone(),
                task_id,
            },
        };
        let lease_end = now.saturating_add(TURN_LEASE);
        repository::insert_turn(
            &mut transaction,
            workspace_id,
            sub_agent_id,
            &turn.id,
            lease_end,
        )
        .await?;
        let position = repository::next_position(&mut transaction, workspace_id, sub_agent_id);
        let position = position.await?;
        let message = Message::user(message.to_owned());
        repository::insert_message(&mut transaction, &turn, position, &message, None).await?;
        transaction.commit().await?;
        let span = turn.trace.span();
        tokio::spawn(self.clone().run_turn(turn).instrument(span));
        Ok(())
    }
}
