//! Sub-agents and their Turns. A Sub-agent belongs to its caller's Task. A
//! Turn is its work from one message to its answer: it calls the model, runs
//! the tool calls of each reply, and ends on a reply without one.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use coral_db::{Clock, Database, Transaction, UnixNanos};
use coral_run::{RunId, RunModule, RunObservation};
use coral_search::UniversalSearchModule;
use coral_task::{TaskCommands, TaskId, TaskModule};
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use futures_util::future::join_all;
use tracing::Instrument as _;

use crate::model::{Message, ModelCall, ModelClient, ModelError, ToolCall, Usage};
use crate::run_tools::{Caller, RunTools};
use crate::telemetry::{TurnEnd, TurnTrace};
use crate::{prompt, repository, search};

mod compaction;
mod interruption;
use interruption::close_calls;

mod steering;
mod steps;

pub(crate) use steps::{TurnPage, WaitingAction};

/// How long a running Turn stays held after its Coral Instance last renewed
/// the lease.
pub(crate) const TURN_LEASE: Duration = Duration::from_secs(30);

/// The most model calls one Turn makes.
const MODEL_CALLS_PER_TURN: usize = 60;

/// Starts, reads and cancels Sub-agents, each for its own caller.
#[derive(Clone)]
pub(crate) struct SubAgents {
    database: Arc<Database>,
    tasks: TaskModule,
    runs: RunModule,
    run_tools: Arc<RunTools>,
    search: UniversalSearchModule,
    model: Arc<ModelClient>,
    clock: Arc<dyn Clock>,
    stopping: tokio::sync::watch::Receiver<bool>,
}

impl SubAgents {
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a module or a dependency that bootstrap hands over"
    )]
    pub(crate) fn new(
        database: Arc<Database>,
        tasks: TaskModule,
        commands: TaskCommands,
        runs: RunModule,
        search: UniversalSearchModule,
        model: ModelClient,
        clock: Arc<dyn Clock>,
        stopping: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            database,
            tasks,
            run_tools: Arc::new(RunTools::new(commands, runs.clone())),
            runs,
            search,
            model: Arc::new(model),
            clock,
            stopping,
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
        let (task, authority) = self
            .tasks
            .hold_open_in_transaction(&mut transaction, actor, workspace_id, task_id)
            .await?;
        let now = self.clock.now();
        authority
            .require_current_at(now)
            .map_err(coral_task::Error::from)?;
        let sub_agent_id = insert_sub_agent(&mut transaction, actor, workspace_id, task_id).await?;
        let turn = RunningTurn {
            trace: TurnTrace::begin(&sub_agent_id, actor, workspace_id, task.telemetry_id()),
            id: uuid::Uuid::new_v4().to_string(),
            sub_agent_id: sub_agent_id.clone(),
            caller: Caller {
                principal: actor.clone(),
                workspace_id: workspace_id.clone(),
                task_id: task_id.clone(),
                started_runs: Mutex::default(),
            },
        };
        let lease_end = now.saturating_add(TURN_LEASE);
        repository::insert_turn(
            &mut transaction,
            workspace_id,
            &sub_agent_id,
            &turn.id,
            now,
            lease_end,
        )
        .await?;
        let brief = Message::user(brief(task_id, now, &message));
        repository::insert_message(&mut transaction, &turn, 0, &brief, None).await?;
        transaction.commit().await?;
        let span = turn.trace.span();
        tokio::spawn(self.clone().run_turn(turn).instrument(span));
        Ok(sub_agent_id)
    }

    /// Reads the Sub-agent's Turn once it has ended, a Run of it waits on a
    /// request that `cursor` has not seen, or `wait` has passed.
    pub(crate) async fn wait(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
        cursor: Option<&str>,
        wait: Duration,
    ) -> Result<TurnPage, Error> {
        let mut changes = self
            .database
            .wait_for_changes(tokio::time::Instant::now() + wait);
        let mut seen = None;
        loop {
            let page = self.page(actor, workspace_id, sub_agent_id).await?;
            let read_at = tokio::time::Instant::now();
            let seen = match &seen {
                Some(seen) => seen,
                None => seen.insert(page.seen(cursor)?),
            };
            if page.state != TurnState::Running || page.newly_needs_a_person(seen) {
                return Ok(page);
            }
            // No write marks a lapsed lease: read again once it lapses.
            let lapse = page
                .lease_expires_at
                .saturating_duration_since(self.clock.now());
            if tokio::time::timeout(lapse, changes.pause(read_at)).await == Ok(false) {
                return Ok(page);
            }
        }
    }

    /// Cancels the Sub-agent's Turn if it is running, then reads it.
    pub(crate) async fn cancel(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
    ) -> Result<TurnPage, Error> {
        let turn = self.turn(actor, workspace_id, sub_agent_id).await?;
        let canceled = &TurnState::Canceled;
        self.end_turn(workspace_id, sub_agent_id, &turn.id, canceled)
            .await?;
        // A running Turn cancels the Runs it holds in memory when it stops.
        // The Runs that the stored thread names are asked to stop here, so that
        // a Turn that has ended does not leave them running. A cancel that fails
        // is logged, and that Run keeps running.
        let thread = repository::thread(&*self.database, workspace_id, sub_agent_id).await?;
        let started = steps::steps(&thread).into_iter();
        let started = started.filter_map(|step| step.run_id).collect();
        self.cancel_runs(actor, workspace_id, started).await;
        self.page(actor, workspace_id, sub_agent_id).await
    }

    /// The Turn of the caller's own Sub-agent, interrupted if its lease
    /// lapsed. Any other reads as not found.
    pub(crate) async fn turn(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
    ) -> Result<Turn, Error> {
        let principal = actor.principal();
        let turn = repository::find_turn(&*self.database, workspace_id, sub_agent_id, principal)
            .await?
            .ok_or(Error::SubAgentNotFound)?;
        self.interrupt_if_lapsed(workspace_id, sub_agent_id, turn)
            .await
    }

    /// Runs the Turn to its end, under its lease.
    async fn run_turn(self, turn: RunningTurn) {
        let workspace_id = &turn.caller.workspace_id;
        let answered = tokio::select! {
            answered = self.answer(&turn) => answered,
            () = self.hold_lease(&turn) => Err(TurnFailure::Canceled),
            () = self.interrupt_at_shutdown(&turn) => {
                turn.trace.ended(TurnEnd::Interrupted);
                return;
            }
        };
        let end = TurnEnd::of(&answered);
        let ending = match answered {
            Ok(answer) => TurnState::Completed { answer },
            Err(TurnFailure::Canceled) => TurnState::Canceled,
            Err(failure) => TurnState::Failed {
                failure: failure.to_string(),
            },
        };
        let ended = self.end_turn(workspace_id, &turn.sub_agent_id, &turn.id, &ending);
        match ended.await {
            // Its own ending stands, so the lead did not cancel it: its Runs keep running.
            Ok(true) => {
                turn.trace.ended(end);
                return;
            }
            Ok(false) => turn.trace.ended(TurnEnd::Canceled),
            Err(error) => tracing::warn!(
                error.type = safe::kind(error.code()),
                "the end of a Sub-agent Turn was not recorded"
            ),
        }
        self.cancel_runs_if_canceled(&turn).await;
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
            repository::thread_entries(&*self.database, &caller.workspace_id, &turn.sub_agent_id)
                .await
                .map_err(|error| TurnFailure::internal(error.code()))?;
        for _model_call in 0..MODEL_CALLS_PER_TURN {
            if !self.task_usable(caller).await {
                return Err(TurnFailure::Canceled);
            }
            self.read_messages(turn, &mut thread).await?;
            turn.trace.count_model_call();
            let reply = self.reply_within_window(turn, &system, &prompt::TOOLS, &mut thread);
            let reply = reply.await?;
            let calls: Vec<ToolCall> = reply.message.tool_calls().cloned().collect();
            let answer = reply.message.text();
            self.add(turn, &mut thread, reply.message, Some(&reply.call))
                .await?;
            if calls.is_empty() {
                if answer.trim().is_empty() {
                    return Err(TurnFailure::NoAnswer);
                }
                if self.end_unless_messaged(turn, &answer).await? {
                    return Ok(answer);
                }
                continue;
            }
            let results = join_all(calls.iter().map(|call| async {
                let result = search::call(
                    &self.search,
                    &self.run_tools,
                    caller,
                    &call.name,
                    &call.arguments,
                );
                let output = turn.trace.tool_call(&call.name, result).await;
                call.result(output.content, output.is_error, output.run_id)
            }));
            let results = Message::tool_results(results.await);
            self.add(turn, &mut thread, results, None).await?;
        }
        Err(TurnFailure::TooManyModelCalls)
    }

    /// Stores `message` at the end of the Turn's thread, then keeps it there
    /// in memory: a Turn is the only writer of its thread. A Turn that has
    /// ended stops here.
    async fn add(
        &self,
        turn: &RunningTurn,
        thread: &mut Vec<ThreadEntry>,
        message: Message,
        call: Option<&ModelCall>,
    ) -> Result<(), TurnFailure> {
        let position = i64::try_from(thread.len()).unwrap_or(i64::MAX);
        let stored = async {
            let mut transaction = self.database.begin().await?;
            let stored =
                repository::insert_message(&mut transaction, turn, position, &message, call);
            let stored = stored.await?;
            transaction.commit().await?;
            Ok::<_, coral_db::Error>(stored)
        };
        let stored = stored.await;
        if !stored.map_err(|error| TurnFailure::internal(error.code()))? {
            return Err(TurnFailure::Canceled);
        }
        thread.push(ThreadEntry {
            message,
            usage: call.and_then(|call| call.usage),
        });
        Ok(())
    }

    /// Renews the Turn's lease until a renewal finds that the Turn has ended,
    /// as a cancellation ends it, or that its caller can no longer use its
    /// Task.
    async fn hold_lease(&self, turn: &RunningTurn) {
        let workspace_id = &turn.caller.workspace_id;
        loop {
            tokio::time::sleep(TURN_LEASE / 3).await;
            if !self.task_usable(&turn.caller).await {
                return;
            }
            let lease_end = self.clock.now().saturating_add(TURN_LEASE);
            let renewed =
                repository::renew_lease(&self.database, workspace_id, &turn.id, lease_end).await;
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

    /// Ends the Turn unless it has ended already, and closes the tool calls
    /// that it left without results: the first ending stands.
    pub(crate) async fn end_turn(
        &self,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
        turn_id: &str,
        ending: &TurnState,
    ) -> Result<bool, Error> {
        let mut transaction = self.database.begin().await?;
        let now = self.clock.now();
        let ended =
            repository::end_turn(&mut transaction, workspace_id, turn_id, ending, now).await?;
        if ended {
            close_calls(&mut transaction, workspace_id, sub_agent_id, turn_id).await?;
        }
        transaction.commit().await?;
        Ok(ended)
    }

    /// Whether the caller can still use its Task: the Task is open and the
    /// caller has not lost access. A database error reads as usable: the
    /// Turn's next write meets it.
    async fn task_usable(&self, caller: &Caller) -> bool {
        let (principal, workspace_id) = (&caller.principal, &caller.workspace_id);
        let checked = self
            .tasks
            .check_open(principal, workspace_id, &caller.task_id);
        match checked.await {
            Ok(()) => true,
            Err(refused) => refused.database_error().is_some(),
        }
    }

    /// Cancels the Runs that a Turn started if the lead canceled it. A Run
    /// that has ended stays as it is.
    async fn cancel_runs_if_canceled(&self, turn: &RunningTurn) {
        let caller = &turn.caller;
        let (principal, workspace_id) = (&caller.principal, &caller.workspace_id);
        match repository::turn_canceled(&*self.database, workspace_id, &turn.id).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "the Runs of a canceled Sub-agent Turn were not canceled"
                );
                return;
            }
        }
        let started = caller.started_runs.lock();
        let started = started.unwrap_or_else(PoisonError::into_inner).clone();
        self.cancel_runs(principal, workspace_id, started).await;
    }

    /// Cancels `runs` as `principal`. A Run that has ended stays as it is.
    async fn cancel_runs(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        runs: Vec<RunId>,
    ) {
        let canceled = async {
            let observation = &RunObservation::immediate(None, 1)?;
            let cancels = runs.iter().map(|run_id| {
                self.runs
                    .cancel(principal, workspace_id, run_id, observation)
            });
            join_all(cancels)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, Error>(())
        };
        if let Err(error) = canceled.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "the Runs of a canceled Sub-agent were not all canceled"
            );
        }
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
    /// The Coral Instance that ran the Turn stopped before the Turn ended.
    Interrupted,
}

/// A Turn as its row holds it.
#[derive(Debug)]
pub(crate) struct Turn {
    pub(crate) id: String,
    pub(crate) state: TurnState,
    pub(crate) lease_expires_at: UnixNanos,
}

/// One message in a Sub-agent's thread, with the token usage the model
/// reported when it wrote that message, if any.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThreadEntry {
    pub(crate) message: Message,
    pub(crate) usage: Option<Usage>,
}

impl ThreadEntry {
    pub(crate) const fn plain(message: Message) -> Self {
        Self {
            message,
            usage: None,
        }
    }
}

/// A Turn that this Coral Instance runs, and the caller its tools act as.
pub(crate) struct RunningTurn {
    pub(crate) trace: TurnTrace,
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
    /// The Run module could not read a Run of the Turn.
    #[error(transparent)]
    Run(#[from] coral_run::Error),
    #[error("cursor belongs to another Turn")]
    CursorOfAnotherTurn,
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
            Self::Run(_) => "run",
            Self::CursorOfAnotherTurn => "cursor_of_another_turn",
            Self::IdentifierIssuance => "identifier_issuance",
            Self::CorruptData => "corrupt_data",
            Self::Database(error) => error.code(),
        }
    }
}

/// Why a Turn ended without an answer. The lead reads the text.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TurnFailure {
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
    /// The lead canceled the Turn or ended its Task, or the caller can no
    /// longer use the Task: the Turn ends as canceled.
    #[error("the Sub-agent was canceled")]
    Canceled,
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
