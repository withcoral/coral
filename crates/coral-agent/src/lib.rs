//! Coral's own data agent: a model that answers one caller message at a time by
//! calling Coral's tools on the caller's behalf.
//!
//! # Interface
//!
//! - [`AgentModule::start_turn`] accepts one caller message as the next turn in a
//!   Task's thread and runs it in the background.
//! - [`AgentModule::observe`] returns a turn's state and progress Events after a
//!   cursor, waiting for new ones, and its final message and evidence once it ends.
//! - [`AgentModule::cancel`] ends a running turn.
//! - [`AgentModule::interrupt_abandoned`] ends turns a stopped host left open.
//!
//! # Invariants
//!
//! The Task is the thread: every turn belongs to one open Task the caller
//! started, and a Task runs one turn at a time. The agent is not a Principal.
//! It calls only the [`Toolbox`] the Adapter binds to the caller, so every
//! search, description and Run is the caller's own, attributed to the Task. Only
//! the caller who started a turn can observe or cancel it. The thread keeps the
//! model's messages verbatim and only appends to them. The Module reads no
//! process environment; bootstrap supplies the model.

mod error;
mod model;
mod repository;
mod toolbox;
mod turn;
mod types;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use coral_db::Database;
use coral_task::{TaskId, TaskModule};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId, WorkspaceModule};
use serde_json::json;
use tokio::task::AbortHandle;

pub use error::Error;
pub use model::{
    AnthropicModel, AnthropicSettings, Message, Model, ModelError, ModelRequest, ModelResponse,
    Role, StopReason,
};
pub use toolbox::{RunObserved, ToolOutcome, ToolSpec, Toolbox};
pub use types::{
    AGENT_MESSAGE_MAX_CHARS, AgentMessage, EVIDENCE_VALUE_MAX_BYTES, Evidence, RequiredAction,
    TURN_EVENT_PAGE_MAX, TURN_WAIT_MAX, TurnCursor, TurnEvent, TurnEventKind, TurnId,
    TurnObservation, TurnState, TurnUpdate,
};

const INTERRUPTED: &str = "Coral restarted while the agent worked. Send the message again.";

/// Agent Turns and their background loops.
#[derive(Clone)]
pub struct AgentModule {
    database: Arc<Database>,
    tasks: TaskModule,
    workspaces: WorkspaceModule,
    model: Arc<dyn Model>,
    running: Arc<Mutex<HashMap<TurnId, AbortHandle>>>,
}

impl AgentModule {
    /// Builds the Module from initialized persistence, Task and Workspace
    /// authority, and the model bootstrap selected.
    #[must_use]
    pub fn new(
        database: Arc<Database>,
        tasks: TaskModule,
        workspaces: WorkspaceModule,
        model: Arc<dyn Model>,
    ) -> Self {
        Self {
            database,
            tasks,
            workspaces,
            model,
            running: Arc::default(),
        }
    }

    /// Accepts `message` as the next turn in the Task's thread and starts it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TurnInProgress`] while another turn in the Task runs, and
    /// Task errors for an absent, closed, or other Principal's Task. A rejected
    /// turn changes nothing.
    pub async fn start_turn(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        task_id: &TaskId,
        message: &AgentMessage,
        toolbox: Arc<dyn Toolbox>,
    ) -> Result<TurnId, Error> {
        let turn_id = TurnId::generate();
        // Read before the write lock; only a new thread uses it.
        let briefing = toolbox.briefing().await;
        let mut transaction = self.database.begin().await?;
        self.tasks
            .hold_open_in_transaction(&mut transaction, actor, workspace_id, task_id)
            .await?;
        let thread = repository::thread(&mut transaction, workspace_id, task_id).await?;
        repository::insert_turn(
            &mut transaction,
            &repository::NewTurn {
                id: &turn_id,
                workspace_id,
                task_id,
                principal: actor.principal(),
                message,
                started_at_unix_nanos: now()?,
            },
        )
        .await?;
        // Close calls a canceled or failed turn left open, so the thread stays
        // a valid sequence of calls and results.
        let mut content = match thread.last() {
            Some(last) if last.role == Role::Assistant => turn::tool_uses(&last.content)
                .iter()
                .map(|(id, _name, _input)| turn::closing_result(id))
                .collect(),
            _ => Vec::new(),
        };
        if thread.is_empty() {
            content.push(json!({"type": "text", "text": format!("task_id: {task_id}")}));
            if let Some(briefing) = briefing {
                content.push(json!({"type": "text", "text": briefing}));
            }
        }
        content.push(json!({"type": "text", "text": message.as_str()}));
        repository::append_message(
            &mut transaction,
            workspace_id,
            task_id,
            &turn_id,
            &Message {
                role: Role::User,
                content,
            },
            None,
        )
        .await?;
        transaction.commit().await?;

        let turn = turn::TurnContext {
            database: Arc::clone(&self.database),
            model: Arc::clone(&self.model),
            toolbox,
            workspace_id: workspace_id.clone(),
            task_id: task_id.clone(),
            turn_id: turn_id.clone(),
        };
        let running = Arc::clone(&self.running);
        let finished = turn_id.clone();
        // Hold the lock across spawn so the loop cannot remove its entry first.
        let mut handles = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        let handle = tokio::spawn(async move {
            turn::run(turn).await;
            running
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&finished);
        });
        handles.insert(turn_id.clone(), handle.abort_handle());
        Ok(turn_id)
    }

    /// Returns a non-consuming observation of the caller's own turn, optionally waiting.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TurnNotFound`] for an absent or other Principal's turn,
    /// and rejects cursors from another turn.
    pub async fn observe(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        turn_id: &TurnId,
        observation: &TurnObservation,
    ) -> Result<TurnUpdate, Error> {
        if observation
            .cursor()
            .is_some_and(|cursor| cursor.turn_id() != turn_id)
        {
            return Err(Error::InvalidInput("cursor belongs to another turn".into()));
        }
        let mut wait = self.database.wait_for_changes(observation.wait());
        loop {
            let update = self
                .snapshot(actor, workspace_id, turn_id, observation)
                .await?;
            if observation.is_ready(&update) || !wait.pause(None).await {
                return Ok(update);
            }
        }
    }

    async fn snapshot(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        turn_id: &TurnId,
        observation: &TurnObservation,
    ) -> Result<TurnUpdate, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let now = now()?;
        self.workspaces
            .check_principal_inspection_in_snapshot(&mut transaction, actor, workspace_id, now)
            .await
            .map_err(concealed)?
            .require_current_at(now)?;
        let turn = repository::find_turn(&mut transaction, workspace_id, turn_id)
            .await?
            .filter(|turn| turn.belongs_to(actor.principal()))
            .ok_or(Error::TurnNotFound)?;
        let after = observation.cursor().map_or(0, TurnCursor::event_sequence);
        if after > turn.last_event_sequence {
            return Err(Error::InvalidInput("turn cursor is invalid".into()));
        }
        let (events, has_more) = repository::events_after(
            &mut transaction,
            workspace_id,
            turn_id,
            after,
            observation.event_limit(),
        )
        .await?;
        transaction.commit().await?;
        let next_sequence = if has_more {
            events.last().map_or(after, TurnEvent::sequence)
        } else {
            turn.last_event_sequence
        };
        // The result arrives on the last page, after every Event.
        let last_page = !has_more;
        Ok(TurnUpdate {
            turn_id: turn.id.clone(),
            task_id: turn.task_id,
            state: turn.state,
            events,
            next_cursor: TurnCursor::new(turn.id, next_sequence),
            has_more,
            final_message: turn.final_message.filter(|_message| last_page),
            evidence: turn.evidence.filter(|_evidence| last_page),
            failure: turn.failure.filter(|_failure| last_page),
            required_action: turn.required_action,
        })
    }

    /// Ends the caller's running turn and stops its loop. Ending an ended turn
    /// changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TurnNotFound`] for an absent or other Principal's turn.
    pub async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        turn_id: &TurnId,
        observation: &TurnObservation,
    ) -> Result<TurnUpdate, Error> {
        let mut transaction = self.database.begin().await?;
        let now = now()?;
        self.workspaces
            .check_principal_inspection_in_transaction(&mut transaction, actor, workspace_id, now)
            .await
            .map_err(concealed)?
            .require_current_at(now)?;
        repository::find_turn(&mut transaction, workspace_id, turn_id)
            .await?
            .filter(|turn| turn.belongs_to(actor.principal()))
            .ok_or(Error::TurnNotFound)?;
        repository::finish_turn(
            &mut transaction,
            workspace_id,
            turn_id,
            &repository::Ending {
                state: TurnState::Canceled,
                final_message: None,
                evidence: None,
                failure: None,
                ended_at_unix_nanos: now,
            },
        )
        .await?;
        transaction.commit().await?;
        if let Some(handle) = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(turn_id)
        {
            handle.abort();
        }
        self.snapshot(actor, workspace_id, turn_id, observation)
            .await
    }

    /// Ends every turn that a stopped host left running. Call it at startup,
    /// before serving, while this host owns no turn loops.
    ///
    /// # Errors
    ///
    /// Returns persistence failures.
    pub async fn interrupt_abandoned(&self) -> Result<u64, Error> {
        let mut transaction = self.database.begin().await?;
        let ended = repository::interrupt_open_turns(&mut transaction, INTERRUPTED, now()?).await?;
        transaction.commit().await?;
        Ok(ended)
    }

    /// Stops every turn loop on this host. Their turns stay open until the next
    /// startup interrupts them.
    pub fn stop(&self) {
        for (_turn, handle) in self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
        {
            handle.abort();
        }
    }
}

fn concealed(error: coral_workspace::Error) -> Error {
    match error {
        coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_) => {
            Error::TurnNotFound
        }
        other => other.into(),
    }
}

pub(crate) fn now() -> Result<i64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .ok_or(Error::Clock)
}
