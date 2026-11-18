//! Sub-agents and their Turns. A Sub-agent belongs to its caller's Task. A
//! Turn is its work from one message to its answer: it calls the model, runs
//! the tool calls of each reply, and ends on a reply without one.

use std::sync::Arc;
use std::time::Duration;

use coral_db::{Clock, Database, Transaction, UnixNanos};
use coral_run::RunModule;
use coral_task::{TaskCommands, TaskId, TaskModule};
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use futures_util::future::join_all;

use crate::model::{Message, ModelCall, ModelClient, ModelError, ToolCall};
use crate::run_tools::{Caller, RunTools};
use crate::{prompt, repository};

/// How long a running Turn stays held after its Coral Instance last renewed
/// the lease.
const TURN_LEASE: Duration = Duration::from_secs(30);

/// The most model calls one Turn makes.
const MODEL_CALLS_PER_TURN: usize = 60;

/// Starts, reads and cancels Sub-agents, each for its own caller.
#[derive(Clone)]
pub(crate) struct SubAgents {
    database: Arc<Database>,
    tasks: TaskModule,
    runs: RunModule,
    run_tools: Arc<RunTools>,
    model: Arc<ModelClient>,
    clock: Arc<dyn Clock>,
}

impl SubAgents {
    pub(crate) fn new(
        database: Arc<Database>,
        tasks: TaskModule,
        commands: TaskCommands,
        runs: RunModule,
        model: ModelClient,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            tasks,
            run_tools: Arc::new(RunTools::new(commands, runs.clone())),
            runs,
            model: Arc::new(model),
            clock,
        }
    }

    /// Starts a Sub-agent in the caller's open Task, and its Turn, which
    /// answers `message` in the background. Returns the `sub_agent_id`.
    pub(crate) async fn start(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        task_id: &TaskId,
        message: String,
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
        let turn = RunningTurn {
            id: uuid::Uuid::new_v4().to_string(),
            sub_agent_id: sub_agent_id.clone(),
            caller: Caller {
                principal: actor.clone(),
                workspace_id: workspace_id.clone(),
                task_id: task_id.clone(),
            },
        };
        let lease_end = now.saturating_add(TURN_LEASE);
        repository::insert_turn(
            &mut transaction,
            workspace_id,
            &sub_agent_id,
            &turn.id,
            lease_end,
        )
        .await?;
        let brief = Message::user(brief(task_id, now, &message));
        repository::insert_message(&mut transaction, &turn, 0, &brief, None).await?;
        transaction.commit().await?;
        tokio::spawn(self.clone().run_turn(turn));
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

    /// Runs the Turn to its end, under its lease.
    async fn run_turn(self, turn: RunningTurn) {
        let workspace_id = &turn.caller.workspace_id;
        let answered = tokio::select! {
            answered = self.answer(&turn) => answered,
            () = self.hold_lease(workspace_id, &turn.id) => return,
        };
        let ending = match answered {
            Ok(answer) => TurnState::Completed { answer },
            Err(failure) => TurnState::Failed {
                failure: failure.to_string(),
            },
        };
        if let Err(error) = self.end_turn(workspace_id, &turn.id, &ending).await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "the end of a Sub-agent Turn was not recorded"
            );
        }
    }

    /// Calls the model until it replies without a tool call, and returns that
    /// reply's text. The tool calls of one reply run at the same time.
    async fn answer(&self, turn: &RunningTurn) -> Result<String, TurnFailure> {
        let caller = &turn.caller;
        let providers = self
            .runs
            .listed_providers(&caller.principal, &caller.workspace_id)
            .await
            .map_err(|error| TurnFailure::internal(error.code()))?;
        let system = prompt::system_prompt(&providers);
        let mut thread =
            repository::thread(&*self.database, &caller.workspace_id, &turn.sub_agent_id)
                .await
                .map_err(|error| TurnFailure::internal(error.code()))?;
        for _model_call in 0..MODEL_CALLS_PER_TURN {
            let reply = self.model.reply(&system, &thread, &prompt::TOOLS).await?;
            let calls: Vec<ToolCall> = reply.message.tool_calls().cloned().collect();
            let answer = reply.message.text();
            self.add(turn, &mut thread, reply.message, Some(&reply.call))
                .await?;
            if calls.is_empty() {
                if answer.trim().is_empty() {
                    return Err(TurnFailure::NoAnswer);
                }
                return Ok(answer);
            }
            let results = join_all(calls.iter().map(|call| async {
                let result = self.run_tools.call(caller, &call.name, &call.arguments);
                call.result(result.await)
            }));
            let results = Message::tool_results(results.await);
            self.add(turn, &mut thread, results, None).await?;
        }
        Err(TurnFailure::TooManyModelCalls)
    }

    /// Stores `message` at the end of the Turn's thread, then keeps it there
    /// in memory: a Turn is the only writer of its thread.
    async fn add(
        &self,
        turn: &RunningTurn,
        thread: &mut Vec<Message>,
        message: Message,
        call: Option<&ModelCall>,
    ) -> Result<(), TurnFailure> {
        let position = i64::try_from(thread.len()).unwrap_or(i64::MAX);
        let stored = async {
            let mut transaction = self.database.begin().await?;
            repository::insert_message(&mut transaction, turn, position, &message, call).await?;
            transaction.commit().await
        };
        stored
            .await
            .map_err(|error| TurnFailure::internal(error.code()))?;
        thread.push(message);
        Ok(())
    }

    /// Renews the Turn's lease until a renewal finds that the Turn has ended,
    /// as a cancellation ends it.
    async fn hold_lease(&self, workspace_id: &WorkspaceId, turn_id: &str) {
        loop {
            tokio::time::sleep(TURN_LEASE / 3).await;
            let lease_end = self.clock.now().saturating_add(TURN_LEASE);
            let renewed =
                repository::renew_lease(&self.database, workspace_id, turn_id, lease_end).await;
            match renewed {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "the lease of a Sub-agent Turn was not renewed"
                ),
            }
        }
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
    /// The model has not answered yet.
    Running,
    /// The model replied without a tool call, and its reply is the answer.
    Completed { answer: String },
    /// The Turn ended without an answer.
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

/// A Turn that this Coral Instance runs, and the caller its tools act as.
pub(crate) struct RunningTurn {
    pub(crate) id: String,
    pub(crate) sub_agent_id: String,
    pub(crate) caller: Caller,
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

impl Error {
    /// The error's stable kind, for an event's `error.type`.
    const fn code(&self) -> &'static str {
        match self {
            Self::SubAgentNotFound => "sub_agent_not_found",
            Self::Task(_) => "task",
            Self::IdentifierIssuance => "identifier_issuance",
            Self::CorruptData => "corrupt_data",
            Self::Database(error) => error.code(),
        }
    }
}

/// Why a Turn ended without an answer. The lead reads the text.
#[derive(Debug, thiserror::Error)]
enum TurnFailure {
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(
        "the Sub-agent made {MODEL_CALLS_PER_TURN} model calls without an answer: send it a narrower message"
    )]
    TooManyModelCalls,
    #[error("the model gave no answer: send the message again")]
    NoAnswer,
    #[error("the Sub-agent stopped on an internal error")]
    Internal,
}

impl TurnFailure {
    /// A failure that only the operator can act on; `code` names it in the log.
    fn internal(code: &'static str) -> Self {
        tracing::warn!(
            error.type = safe::kind(code),
            "a Sub-agent Turn stopped on an internal error"
        );
        Self::Internal
    }
}

/// The first message of a Sub-agent's thread: the `task_id` its `exec` calls
/// pass and the date, which the system prompt says the brief holds, then the
/// lead's message.
fn brief(task_id: &TaskId, now: UnixNanos, message: &str) -> String {
    let date = chrono::DateTime::from_timestamp_nanos(now.get()).date_naive();
    format!("task_id: {}\ndate: {date}\n\n{message}", task_id.as_str())
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
