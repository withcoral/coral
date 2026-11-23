//! The rows of Sub-agents, their Turns and their threads. Every statement
//! matches the Workspace, and a read that a caller asks for matches the
//! caller too.

use coral_db::{Database, QueryExecutor, Transaction, UnixNanos};
use coral_run::RunId;
use coral_task::TaskId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Order, Query};
use sqlx::FromRow;

use crate::model::{Message, ModelCall, Role, Usage};
use crate::sub_agents::{Error, RunningTurn, ThreadEntry, Turn, TurnState};

mod lead_messages;

pub(crate) use lead_messages::{
    add_unread_message, complete_unless_unread, next_position, take_unread_messages, task_id,
    turn_messages,
};

#[derive(Iden)]
enum SubAgentRecords {
    Table,
    Id,
    WorkspaceId,
    TaskId,
    PrincipalId,
    PrincipalKind,
}

#[derive(Iden)]
enum SubAgentTurns {
    Table,
    Id,
    WorkspaceId,
    SubAgentId,
    State,
    Answer,
    Failure,
    LeaseExpiresAtUnixNanos,
    UnreadMessages,
    StartedAtUnixNanos,
    EndedAtUnixNanos,
}

#[derive(Iden)]
enum SubAgentThreadMessages {
    Table,
    WorkspaceId,
    SubAgentId,
    Position,
    TurnId,
    Role,
    ContentJson,
    Model,
    LatencyMs,
    InputTokens,
    CacheReadInputTokens,
    OutputTokens,
}

#[derive(Iden)]
enum SubAgentRuns {
    Table,
    RunId,
    WorkspaceId,
    TaskId,
}

const RUNNING: &str = "running";
const INTERRUPTED: &str = "interrupted";

/// Inserts the Sub-agent of `principal` in its Task. `false` means that `id`
/// is in use and nothing was written.
pub(crate) async fn insert_sub_agent(
    transaction: &mut Transaction<'_>,
    id: &str,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    principal: &Principal,
) -> Result<bool, coral_db::Error> {
    let statement = Query::insert()
        .into_table(SubAgentRecords::Table)
        .columns([
            SubAgentRecords::Id,
            SubAgentRecords::WorkspaceId,
            SubAgentRecords::TaskId,
            SubAgentRecords::PrincipalId,
            SubAgentRecords::PrincipalKind,
        ])
        .values_panic([
            Expr::val(id),
            Expr::val(workspace_id.to_string()),
            Expr::val(task_id.as_str()),
            Expr::val(principal.id().as_str()),
            Expr::val(principal.kind().as_storage_str()),
        ])
        .on_conflict(
            OnConflict::column(SubAgentRecords::Id)
                .do_nothing()
                .to_owned(),
        )
        .to_owned();
    Ok(transaction.execute_change(statement).await? == 1)
}

/// Records that a Sub-agent's `exec` started the Run, in the Task it works in.
pub(crate) async fn insert_sub_agent_run(
    database: &Database,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    run_id: &RunId,
) -> Result<(), coral_db::Error> {
    let statement = Query::insert()
        .into_table(SubAgentRuns::Table)
        .columns([
            SubAgentRuns::RunId,
            SubAgentRuns::WorkspaceId,
            SubAgentRuns::TaskId,
        ])
        .values_panic([
            Expr::val(run_id.as_str()),
            Expr::val(workspace_id.to_string()),
            Expr::val(task_id.as_str()),
        ])
        .to_owned();
    database.execute(statement).await.map(drop)
}

/// Whether a Sub-agent's `exec` started the Run.
pub(crate) async fn started_by_sub_agent(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<bool, coral_db::Error> {
    let statement = Query::select()
        .column(SubAgentRuns::RunId)
        .from(SubAgentRuns::Table)
        .and_where(Expr::col(SubAgentRuns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentRuns::RunId).eq(run_id.as_str()))
        .to_owned();
    let row: Option<(String,)> = executor.fetch_optional(statement).await?;
    Ok(row.is_some())
}

/// Inserts the Sub-agent's running Turn, started at `now` and held until
/// `lease_expires_at`.
pub(crate) async fn insert_turn(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    turn_id: &str,
    now: UnixNanos,
    lease_expires_at: UnixNanos,
) -> Result<(), coral_db::Error> {
    let statement = Query::insert()
        .into_table(SubAgentTurns::Table)
        .columns([
            SubAgentTurns::Id,
            SubAgentTurns::WorkspaceId,
            SubAgentTurns::SubAgentId,
            SubAgentTurns::State,
            SubAgentTurns::StartedAtUnixNanos,
            SubAgentTurns::LeaseExpiresAtUnixNanos,
        ])
        .values_panic([
            Expr::val(turn_id),
            Expr::val(workspace_id.to_string()),
            Expr::val(sub_agent_id),
            Expr::val(RUNNING),
            Expr::val(now),
            Expr::val(lease_expires_at),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

/// The latest Turn of the Sub-agent that `principal` started in the
/// Workspace: the Turn of the last message in its thread.
pub(crate) async fn find_turn(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    principal: &Principal,
) -> Result<Option<Turn>, Error> {
    let of_sub_agent = Expr::col((SubAgentTurns::Table, SubAgentTurns::SubAgentId))
        .equals((SubAgentRecords::Table, SubAgentRecords::Id))
        .and(
            Expr::col((SubAgentTurns::Table, SubAgentTurns::WorkspaceId))
                .equals((SubAgentRecords::Table, SubAgentRecords::WorkspaceId)),
        );
    let latest = Query::select()
        .column(SubAgentThreadMessages::TurnId)
        .from(SubAgentThreadMessages::Table)
        .and_where(Expr::col(SubAgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentThreadMessages::SubAgentId).eq(sub_agent_id))
        .order_by(SubAgentThreadMessages::Position, Order::Desc)
        .limit(1)
        .to_owned();
    let statement = Query::select()
        .column((SubAgentTurns::Table, SubAgentTurns::Id))
        .column((SubAgentTurns::Table, SubAgentTurns::State))
        .column((SubAgentTurns::Table, SubAgentTurns::Answer))
        .column((SubAgentTurns::Table, SubAgentTurns::Failure))
        .column((SubAgentTurns::Table, SubAgentTurns::LeaseExpiresAtUnixNanos))
        .from(SubAgentTurns::Table)
        .inner_join(SubAgentRecords::Table, of_sub_agent)
        .and_where(
            Expr::col((SubAgentRecords::Table, SubAgentRecords::WorkspaceId))
                .eq(workspace_id.to_string()),
        )
        .and_where(Expr::col((SubAgentRecords::Table, SubAgentRecords::Id)).eq(sub_agent_id))
        .and_where(
            Expr::col((SubAgentRecords::Table, SubAgentRecords::PrincipalId))
                .eq(principal.id().as_str()),
        )
        .and_where(
            Expr::col((SubAgentRecords::Table, SubAgentRecords::PrincipalKind))
                .eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col((SubAgentTurns::Table, SubAgentTurns::Id)).in_subquery(latest))
        .to_owned();
    let row: Option<TurnRow> = executor.fetch_optional(statement).await?;
    row.map(TurnRow::decode).transpose()
}

/// Ends the Turn at `now` as `ending` says. `false` means that it had ended
/// otherwise and nothing was written: a Turn that recorded this answer as it
/// read the lead's last message has ended so, and keeps its end time.
pub(crate) async fn end_turn(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    ending: &TurnState,
    now: UnixNanos,
) -> Result<bool, coral_db::Error> {
    let (state, answer, failure) = stored(ending);
    let ended_at = Expr::col(SubAgentTurns::EndedAtUnixNanos).if_null(now);
    let statement = Query::update()
        .table(SubAgentTurns::Table)
        .values([
            (SubAgentTurns::State, Expr::val(state)),
            (SubAgentTurns::Answer, Expr::val(answer)),
            (SubAgentTurns::Failure, Expr::val(failure)),
            (SubAgentTurns::EndedAtUnixNanos, ended_at),
        ])
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .and_where(
            Expr::col(SubAgentTurns::State)
                .eq(RUNNING)
                .or(Expr::col(SubAgentTurns::State)
                    .eq(state)
                    .and(Expr::col(SubAgentTurns::Answer).eq(answer))),
        )
        .to_owned();
    Ok(transaction.execute_change(statement).await? == 1)
}

/// Ends the running Turn as interrupted if its lease lapsed by `now`.
/// `false` means that it was renewed or had ended, and nothing was written.
pub(crate) async fn interrupt_lapsed_turn(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    now: UnixNanos,
) -> Result<bool, coral_db::Error> {
    let statement = Query::update()
        .table(SubAgentTurns::Table)
        .value(SubAgentTurns::State, INTERRUPTED)
        .value(SubAgentTurns::EndedAtUnixNanos, now)
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .and_where(Expr::col(SubAgentTurns::State).eq(RUNNING))
        .and_where(Expr::col(SubAgentTurns::LeaseExpiresAtUnixNanos).lte(now))
        .to_owned();
    Ok(transaction.execute_change(statement).await? == 1)
}

/// Holds the running Turn until `lease_expires_at`. `false` means that the
/// Turn has ended. A renewal wakes no reader.
pub(crate) async fn renew_lease(
    database: &Database,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    lease_expires_at: UnixNanos,
) -> Result<bool, coral_db::Error> {
    let statement = Query::update()
        .table(SubAgentTurns::Table)
        .value(SubAgentTurns::LeaseExpiresAtUnixNanos, lease_expires_at)
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .and_where(Expr::col(SubAgentTurns::State).eq(RUNNING))
        .to_owned();
    Ok(database.execute(statement).await? == 1)
}

/// Whether the Turn ended as canceled.
pub(crate) async fn turn_canceled(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    turn_id: &str,
) -> Result<bool, coral_db::Error> {
    let (canceled, _, _) = stored(&TurnState::Canceled);
    let statement = Query::select()
        .column(SubAgentTurns::Id)
        .from(SubAgentTurns::Table)
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .and_where(Expr::col(SubAgentTurns::State).eq(canceled))
        .to_owned();
    let row: Option<(String,)> = executor.fetch_optional(statement).await?;
    Ok(row.is_some())
}

/// Adds `message` at `position` in the Sub-agent's thread, marked with its
/// Turn and the record of the model call that produced it, if one did.
/// `false` means that the Turn has ended and nothing was written.
pub(crate) async fn insert_message(
    transaction: &mut Transaction<'_>,
    turn: &RunningTurn,
    position: i64,
    message: &Message,
    call: Option<&ModelCall>,
) -> Result<bool, coral_db::Error> {
    let running = lead_messages::hold_unread(transaction, &turn.caller.workspace_id, &turn.id);
    if running.await?.is_none() {
        return Ok(false);
    }
    let usage = call.and_then(|call| call.usage);
    let latency_ms = call.map(|call| i64::try_from(call.latency.as_millis()).unwrap_or(i64::MAX));
    let statement = Query::insert()
        .into_table(SubAgentThreadMessages::Table)
        .columns([
            SubAgentThreadMessages::WorkspaceId,
            SubAgentThreadMessages::SubAgentId,
            SubAgentThreadMessages::Position,
            SubAgentThreadMessages::TurnId,
            SubAgentThreadMessages::Role,
            SubAgentThreadMessages::ContentJson,
            SubAgentThreadMessages::Model,
            SubAgentThreadMessages::LatencyMs,
            SubAgentThreadMessages::InputTokens,
            SubAgentThreadMessages::CacheReadInputTokens,
            SubAgentThreadMessages::OutputTokens,
        ])
        .values_panic([
            Expr::val(turn.caller.workspace_id.to_string()),
            Expr::val(turn.sub_agent_id.as_str()),
            Expr::val(position),
            Expr::val(turn.id.as_str()),
            Expr::val(stored_role(message.role)),
            Expr::val(serde_json::json!(message.content).to_string()),
            Expr::val(call.map(|call| call.model.as_str())),
            Expr::val(latency_ms),
            Expr::val(usage.map(|usage| usage.input_tokens)),
            Expr::val(usage.map(|usage| usage.cache_read_input_tokens)),
            Expr::val(usage.map(|usage| usage.output_tokens)),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(true)
}

/// The Sub-agent's thread, in order.
pub(crate) async fn thread(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
) -> Result<Vec<Message>, Error> {
    let entries = thread_entries(executor, workspace_id, sub_agent_id).await?;
    Ok(entries.into_iter().map(|entry| entry.message).collect())
}

/// The Sub-agent's thread, in order, with each row's reported token usage.
pub(crate) async fn thread_entries(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
) -> Result<Vec<ThreadEntry>, Error> {
    let statement = Query::select()
        .columns([
            SubAgentThreadMessages::Role,
            SubAgentThreadMessages::ContentJson,
            SubAgentThreadMessages::InputTokens,
            SubAgentThreadMessages::CacheReadInputTokens,
            SubAgentThreadMessages::OutputTokens,
        ])
        .from(SubAgentThreadMessages::Table)
        .and_where(Expr::col(SubAgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentThreadMessages::SubAgentId).eq(sub_agent_id))
        .order_by(SubAgentThreadMessages::Position, Order::Asc)
        .to_owned();
    let rows: Vec<ThreadEntryRow> = executor.fetch_all(statement).await?;
    rows.into_iter().map(ThreadEntryRow::decode).collect()
}

/// The last message of the Turn in the Sub-agent's thread, and its position.
pub(crate) async fn last_message(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    turn_id: &str,
) -> Result<Option<(i64, Message)>, Error> {
    let statement = Query::select()
        .columns([
            SubAgentThreadMessages::Position,
            SubAgentThreadMessages::Role,
            SubAgentThreadMessages::ContentJson,
        ])
        .from(SubAgentThreadMessages::Table)
        .and_where(Expr::col(SubAgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentThreadMessages::SubAgentId).eq(sub_agent_id))
        .and_where(Expr::col(SubAgentThreadMessages::TurnId).eq(turn_id))
        .order_by(SubAgentThreadMessages::Position, Order::Desc)
        .limit(1)
        .to_owned();
    let row: Option<PositionedMessageRow> = executor.fetch_optional(statement).await?;
    row.map(|row| Ok((row.position, row.message.decode()?)))
        .transpose()
}

/// Adds `message` at `position` in the Sub-agent's thread, marked with the
/// Turn `turn_id`, unless a message holds that position already.
pub(crate) async fn insert_message_unless_taken(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    turn_id: &str,
    position: i64,
    message: &Message,
) -> Result<(), coral_db::Error> {
    let statement = Query::insert()
        .into_table(SubAgentThreadMessages::Table)
        .columns([
            SubAgentThreadMessages::WorkspaceId,
            SubAgentThreadMessages::SubAgentId,
            SubAgentThreadMessages::Position,
            SubAgentThreadMessages::TurnId,
            SubAgentThreadMessages::Role,
            SubAgentThreadMessages::ContentJson,
        ])
        .values_panic([
            Expr::val(workspace_id.to_string()),
            Expr::val(sub_agent_id),
            Expr::val(position),
            Expr::val(turn_id),
            Expr::val(stored_role(message.role)),
            Expr::val(serde_json::json!(message.content).to_string()),
        ])
        .on_conflict(
            OnConflict::columns([
                SubAgentThreadMessages::SubAgentId,
                SubAgentThreadMessages::Position,
            ])
            .do_nothing()
            .to_owned(),
        )
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

#[derive(FromRow)]
struct MessageRow {
    role: String,
    content_json: String,
}

#[derive(FromRow)]
struct ThreadEntryRow {
    #[sqlx(flatten)]
    message: MessageRow,
    input_tokens: Option<i64>,
    cache_read_input_tokens: Option<i64>,
    output_tokens: Option<i64>,
}

impl ThreadEntryRow {
    fn decode(self) -> Result<ThreadEntry, Error> {
        let message = self.message.decode()?;
        let usage = match (
            self.input_tokens,
            self.cache_read_input_tokens,
            self.output_tokens,
        ) {
            (Some(input_tokens), Some(cache_read_input_tokens), Some(output_tokens)) => {
                Some(Usage {
                    input_tokens,
                    cache_read_input_tokens,
                    output_tokens,
                })
            }
            _ => None,
        };
        Ok(ThreadEntry { message, usage })
    }
}

#[derive(FromRow)]
struct PositionedMessageRow {
    position: i64,
    #[sqlx(flatten)]
    message: MessageRow,
}

impl MessageRow {
    fn decode(self) -> Result<Message, Error> {
        let role = match self.role.as_str() {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            _ => return Err(Error::CorruptData),
        };
        let content =
            serde_json::from_str(&self.content_json).map_err(|_error| Error::CorruptData)?;
        Ok(Message { role, content })
    }
}

const fn stored_role(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

#[derive(FromRow)]
struct TurnRow {
    id: String,
    state: String,
    answer: Option<String>,
    failure: Option<String>,
    lease_expires_at_unix_nanos: UnixNanos,
}

impl TurnRow {
    fn decode(self) -> Result<Turn, Error> {
        let state = match (self.state.as_str(), self.answer, self.failure) {
            (RUNNING, None, None) => TurnState::Running,
            ("completed", Some(answer), None) => TurnState::Completed { answer },
            ("failed", None, Some(failure)) => TurnState::Failed { failure },
            ("canceled", None, None) => TurnState::Canceled,
            (INTERRUPTED, None, None) => TurnState::Interrupted,
            _ => return Err(Error::CorruptData),
        };
        Ok(Turn {
            id: self.id,
            state,
            lease_expires_at: self.lease_expires_at_unix_nanos,
        })
    }
}

/// The state, the answer and the failure that the row of a Turn holds.
fn stored(state: &TurnState) -> (&'static str, Option<&str>, Option<&str>) {
    match state {
        TurnState::Running => (RUNNING, None, None),
        TurnState::Completed { answer } => ("completed", Some(answer), None),
        TurnState::Failed { failure } => ("failed", None, Some(failure)),
        TurnState::Canceled => ("canceled", None, None),
        TurnState::Interrupted => (INTERRUPTED, None, None),
    }
}
