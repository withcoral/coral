//! Sub-agents and their Turns. A Sub-agent belongs to its caller's Task. A
//! Turn is its work from one message to its response.

use std::sync::Arc;
use std::time::Duration;

use coral_db::{Clock, Database, Transaction};
use coral_task::{TaskId, TaskModule};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::repository;

/// How long a running Turn stays held after its Coral Instance last renewed
/// the lease.
const TURN_LEASE: Duration = Duration::from_secs(30);

/// Starts, reads and cancels Sub-agents, each for its own caller.
#[derive(Clone)]
pub(crate) struct SubAgents {
    database: Arc<Database>,
    tasks: TaskModule,
    clock: Arc<dyn Clock>,
}

impl SubAgents {
    pub(crate) const fn new(
        database: Arc<Database>,
        tasks: TaskModule,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            tasks,
            clock,
        }
    }

    /// Starts a Sub-agent in the caller's open Task, with its Turn. Returns
    /// the `sub_agent_id`.
    pub(crate) async fn start(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        task_id: &TaskId,
    ) -> Result<String, Error> {
        let mut transaction = self.database.begin().await?;
        let (_task, authority) = self
            .tasks
            .hold_open_in_transaction(&mut transaction, actor, workspace_id, task_id)
            .await?;
        let now = self.clock.now();
        authority
            .require_current_at(now)
            .map_err(coral_task::Error::from)?;
        let sub_agent_id = insert_sub_agent(&mut transaction, actor, workspace_id, task_id).await?;
        let turn_id = uuid::Uuid::new_v4().to_string();
        let lease_end = now.saturating_add(TURN_LEASE);
        repository::insert_turn(
            &mut transaction,
            workspace_id,
            &sub_agent_id,
            &turn_id,
            lease_end,
        )
        .await?;
        transaction.commit().await?;
        Ok(sub_agent_id)
    }

    /// Reads the Sub-agent's Turn once it has ended or `wait` has passed.
    pub(crate) async fn wait(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
        wait: Duration,
    ) -> Result<TurnState, Error> {
        let mut changes = self
            .database
            .wait_for_changes(tokio::time::Instant::now() + wait);
        loop {
            let turn = self.turn(actor, workspace_id, sub_agent_id).await?;
            let read_at = tokio::time::Instant::now();
            if turn.state != TurnState::Running || !changes.pause(read_at).await {
                return Ok(turn.state);
            }
        }
    }

    /// Cancels the Sub-agent's Turn if it is running, then reads it.
    pub(crate) async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
    ) -> Result<TurnState, Error> {
        let turn = self.turn(actor, workspace_id, sub_agent_id).await?;
        self.end_turn(workspace_id, &turn.id, &TurnState::Canceled)
            .await?;
        Ok(self.turn(actor, workspace_id, sub_agent_id).await?.state)
    }

    /// The Turn of the caller's own Sub-agent. Any other reads as not found.
    pub(crate) async fn turn(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
    ) -> Result<Turn, Error> {
        let principal = actor.principal();
        repository::find_turn(&*self.database, workspace_id, sub_agent_id, principal)
            .await?
            .ok_or(Error::SubAgentNotFound)
    }

    /// Ends the Turn unless it has ended already: the first ending stands.
    pub(crate) async fn end_turn(
        &self,
        workspace_id: &WorkspaceId,
        turn_id: &str,
        ending: &TurnState,
    ) -> Result<bool, coral_db::Error> {
        let mut transaction = self.database.begin().await?;
        let ended = repository::end_turn(&mut transaction, workspace_id, turn_id, ending).await?;
        transaction.commit().await?;
        Ok(ended)
    }
}

/// Where a Turn stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TurnState {
    /// The model has not replied yet.
    Running,
    /// The model replied, and its reply is the answer.
    Completed { answer: String },
    /// The model gave no answer.
    Failed { failure: String },
    /// The caller canceled the Turn.
    Canceled,
}

/// A Turn as its row holds it.
#[derive(Debug)]
pub(crate) struct Turn {
    pub(crate) id: String,
    pub(crate) state: TurnState,
}

/// Why a Sub-agent call did nothing.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// No Sub-agent of this caller has this id in this Workspace.
    #[error("Sub-agent was not found")]
    SubAgentNotFound,
    /// The Task module refused the caller or the Task.
    #[error(transparent)]
    Task(#[from] coral_task::Error),
    #[error("Sub-agent identifier could not be issued")]
    IdentifierIssuance,
    #[error("database contains corrupt Sub-agent data")]
    CorruptData,
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

/// Inserts the Sub-agent under a new identifier. Only an insert proves that
/// an identifier is unused, so a conflict draws another.
async fn insert_sub_agent(
    transaction: &mut Transaction<'_>,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
) -> Result<String, Error> {
    let principal = actor.principal();
    for _attempt in 0..coral_identifier::ISSUE_ATTEMPTS {
        let id =
            coral_identifier::generate().map_err(|_no_random_bytes| Error::IdentifierIssuance)?;
        if repository::insert_sub_agent(transaction, &id, workspace_id, task_id, principal).await? {
            return Ok(id);
        }
    }
    Err(Error::IdentifierIssuance)
}
