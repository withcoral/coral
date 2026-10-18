//! Agent Turn, Event and thread transcript persistence.

use coral_db::{QueryExecutor, Transaction};
use coral_task::TaskId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Func, Iden, Order, Query};
use serde_json::Value;
use sqlx::FromRow;

use crate::model::{Message, Role};
use crate::{
    AgentMessage, Error, Evidence, RequiredAction, TurnEvent, TurnEventKind, TurnId, TurnState,
};

#[derive(Iden)]
enum AgentTurns {
    Table,
    Id,
    WorkspaceId,
    TaskId,
    PrincipalId,
    PrincipalKind,
    Sequence,
    State,
    Message,
    FinalMessage,
    EvidenceJson,
    Failure,
    RequiredActionJson,
    NextEventSequence,
    StartedAtUnixNanos,
    EndedAtUnixNanos,
}

#[derive(Iden)]
enum AgentTurnEvents {
    Table,
    WorkspaceId,
    TurnId,
    Sequence,
    Kind,
    PayloadJson,
    OccurredAtUnixNanos,
}

#[derive(Iden)]
enum AgentThreadMessages {
    Table,
    WorkspaceId,
    TaskId,
    Sequence,
    TurnId,
    Role,
    ContentJson,
}

/// The largest sequence so far, absent when there is none.
#[derive(FromRow)]
struct Last {
    last: Option<i64>,
}

const OPEN_STATES: [&str; 2] = ["running", "needs_action"];

/// The end time, never before the start even when the clock stepped back.
fn ended_at(now_unix_nanos: i64) -> sea_query::SimpleExpr {
    Expr::case(
        Expr::col(AgentTurns::StartedAtUnixNanos).gt(now_unix_nanos),
        Expr::col(AgentTurns::StartedAtUnixNanos),
    )
    .finally(Expr::val(now_unix_nanos))
    .into()
}

/// One stored turn, decoded.
#[derive(Debug, Clone)]
pub(crate) struct TurnRecord {
    pub(crate) id: TurnId,
    pub(crate) task_id: TaskId,
    pub(crate) principal_id: String,
    pub(crate) principal_kind: String,
    pub(crate) state: TurnState,
    pub(crate) final_message: Option<String>,
    pub(crate) evidence: Option<Evidence>,
    pub(crate) failure: Option<String>,
    pub(crate) required_action: Option<RequiredAction>,
    pub(crate) last_event_sequence: u64,
}

impl TurnRecord {
    pub(crate) fn belongs_to(&self, principal: &Principal) -> bool {
        self.principal_id == principal.id().as_str()
            && self.principal_kind == principal.kind().as_storage_str()
    }
}

#[derive(FromRow)]
struct TurnRow {
    id: String,
    task_id: String,
    principal_id: String,
    principal_kind: String,
    state: String,
    final_message: Option<String>,
    evidence_json: Option<String>,
    failure: Option<String>,
    required_action_json: Option<String>,
    next_event_sequence: i64,
}

impl TurnRow {
    fn decode(self) -> Result<TurnRecord, Error> {
        let corrupt = |what: &str| Error::CorruptData(format!("Agent Turn has invalid {what}"));
        Ok(TurnRecord {
            id: TurnId::parse(&self.id).map_err(|_error| corrupt("identity"))?,
            task_id: TaskId::parse(&self.task_id).map_err(|_error| corrupt("Task identity"))?,
            principal_id: self.principal_id,
            principal_kind: self.principal_kind,
            state: TurnState::from_storage_str(&self.state)?,
            final_message: self.final_message,
            evidence: self
                .evidence_json
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(|_error| corrupt("evidence"))?,
            failure: self.failure,
            required_action: self
                .required_action_json
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(|_error| corrupt("required action"))?,
            last_event_sequence: u64::try_from(self.next_event_sequence - 1)
                .map_err(|_error| corrupt("Event sequence"))?,
        })
    }
}

/// The turn to accept.
pub(crate) struct NewTurn<'a> {
    pub(crate) id: &'a TurnId,
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) task_id: &'a TaskId,
    pub(crate) principal: &'a Principal,
    pub(crate) message: &'a AgentMessage,
    pub(crate) started_at_unix_nanos: i64,
}

/// Inserts a running turn as the thread's next. The open-turn index rejects a
/// second running turn in the same Task.
pub(crate) async fn insert_turn(
    transaction: &mut Transaction<'_>,
    turn: &NewTurn<'_>,
) -> Result<(), Error> {
    let sequence = next_turn_sequence(transaction, turn.workspace_id, turn.task_id).await?;
    let statement = Query::insert()
        .into_table(AgentTurns::Table)
        .columns([
            AgentTurns::Id,
            AgentTurns::WorkspaceId,
            AgentTurns::TaskId,
            AgentTurns::PrincipalId,
            AgentTurns::PrincipalKind,
            AgentTurns::Sequence,
            AgentTurns::State,
            AgentTurns::Message,
            AgentTurns::NextEventSequence,
            AgentTurns::StartedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(turn.id.to_string()),
            Expr::val(turn.workspace_id.to_string()),
            Expr::val(turn.task_id.to_string()),
            Expr::val(turn.principal.id().as_str()),
            Expr::val(turn.principal.kind().as_storage_str()),
            Expr::val(sequence),
            Expr::val(TurnState::Running.as_storage_str()),
            Expr::val(turn.message.as_str()),
            Expr::val(1_i64),
            Expr::val(turn.started_at_unix_nanos),
        ])
        .to_owned();
    match transaction.execute_change(statement).await {
        Ok(_rows) => Ok(()),
        Err(error) if error.is_unique_violation() => Err(Error::TurnInProgress),
        Err(error) => Err(error.into()),
    }
}

async fn next_turn_sequence(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
) -> Result<i64, Error> {
    let last: Option<Last> = transaction
        .fetch_optional(
            Query::select()
                .expr_as(Func::max(Expr::col(AgentTurns::Sequence)), "last")
                .from(AgentTurns::Table)
                .and_where(Expr::col(AgentTurns::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentTurns::TaskId).eq(task_id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(last.and_then(|row| row.last).unwrap_or(0) + 1)
}

/// Reads one turn in its Workspace.
pub(crate) async fn find_turn(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
) -> Result<Option<TurnRecord>, Error> {
    let row: Option<TurnRow> = executor
        .fetch_optional(
            Query::select()
                .columns([
                    AgentTurns::Id,
                    AgentTurns::TaskId,
                    AgentTurns::PrincipalId,
                    AgentTurns::PrincipalKind,
                    AgentTurns::State,
                    AgentTurns::FinalMessage,
                    AgentTurns::EvidenceJson,
                    AgentTurns::Failure,
                    AgentTurns::RequiredActionJson,
                    AgentTurns::NextEventSequence,
                ])
                .from(AgentTurns::Table)
                .and_where(Expr::col(AgentTurns::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentTurns::Id).eq(turn_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(TurnRow::decode).transpose()
}

/// Appends one Event to an open turn. Returns false once the turn has ended.
pub(crate) async fn append_event(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
    kind: &TurnEventKind,
    occurred_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let Some(turn) = find_turn(&mut *transaction, workspace_id, turn_id).await? else {
        return Err(Error::TurnNotFound);
    };
    if turn.state.is_terminal() {
        return Ok(false);
    }
    let sequence = i64::try_from(turn.last_event_sequence + 1)
        .map_err(|_error| Error::CorruptData("Agent Turn Event sequence overflowed".into()))?;
    let advanced = transaction
        .execute_change(
            Query::update()
                .table(AgentTurns::Table)
                .value(AgentTurns::NextEventSequence, sequence + 1)
                .and_where(Expr::col(AgentTurns::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentTurns::Id).eq(turn_id.to_string()))
                .and_where(Expr::col(AgentTurns::NextEventSequence).eq(sequence))
                .and_where(Expr::col(AgentTurns::State).is_in(OPEN_STATES))
                .to_owned(),
        )
        .await?;
    if advanced == 0 {
        return Ok(false);
    }
    let payload = serde_json::to_string(kind)
        .map_err(|_error| Error::CorruptData("Agent Turn Event did not encode".into()))?;
    transaction
        .execute_change(
            Query::insert()
                .into_table(AgentTurnEvents::Table)
                .columns([
                    AgentTurnEvents::WorkspaceId,
                    AgentTurnEvents::TurnId,
                    AgentTurnEvents::Sequence,
                    AgentTurnEvents::Kind,
                    AgentTurnEvents::PayloadJson,
                    AgentTurnEvents::OccurredAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(workspace_id.to_string()),
                    Expr::val(turn_id.to_string()),
                    Expr::val(sequence),
                    Expr::val(kind.as_storage_str()),
                    Expr::val(payload),
                    Expr::val(occurred_at_unix_nanos),
                ])
                .to_owned(),
        )
        .await?;
    Ok(true)
}

/// Moves an open turn between running and needing action. Returns false once it has ended.
pub(crate) async fn set_open_state(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
    required_action: Option<&RequiredAction>,
) -> Result<bool, Error> {
    let (state, json) =
        match required_action {
            Some(action) => (
                TurnState::NeedsAction,
                Some(serde_json::to_string(action).map_err(|_error| {
                    Error::CorruptData("required action did not encode".into())
                })?),
            ),
            None => (TurnState::Running, None),
        };
    let rows = transaction
        .execute_change(
            Query::update()
                .table(AgentTurns::Table)
                .value(AgentTurns::State, state.as_storage_str())
                .value(AgentTurns::RequiredActionJson, json)
                .and_where(Expr::col(AgentTurns::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentTurns::Id).eq(turn_id.to_string()))
                .and_where(Expr::col(AgentTurns::State).is_in(OPEN_STATES))
                .to_owned(),
        )
        .await?;
    Ok(rows == 1)
}

/// How a turn ended.
pub(crate) struct Ending<'a> {
    pub(crate) state: TurnState,
    pub(crate) final_message: Option<&'a str>,
    pub(crate) evidence: Option<&'a Evidence>,
    pub(crate) failure: Option<&'a str>,
    pub(crate) ended_at_unix_nanos: i64,
}

/// Ends an open turn. The first ending wins; returns false if it had already ended.
pub(crate) async fn finish_turn(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
    ending: &Ending<'_>,
) -> Result<bool, Error> {
    let evidence = ending
        .evidence
        .map(serde_json::to_string)
        .transpose()
        .map_err(|_error| Error::CorruptData("evidence did not encode".into()))?;
    let rows = transaction
        .execute_change(
            Query::update()
                .table(AgentTurns::Table)
                .value(AgentTurns::State, ending.state.as_storage_str())
                .value(AgentTurns::FinalMessage, ending.final_message)
                .value(AgentTurns::EvidenceJson, evidence)
                .value(AgentTurns::Failure, ending.failure)
                .value(AgentTurns::RequiredActionJson, Option::<String>::None)
                .value(
                    AgentTurns::EndedAtUnixNanos,
                    ended_at(ending.ended_at_unix_nanos),
                )
                .and_where(Expr::col(AgentTurns::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentTurns::Id).eq(turn_id.to_string()))
                .and_where(Expr::col(AgentTurns::State).is_in(OPEN_STATES))
                .to_owned(),
        )
        .await?;
    Ok(rows == 1)
}

/// Ends every open turn as interrupted. A restarted host owns no turn loops.
pub(crate) async fn interrupt_open_turns(
    transaction: &mut Transaction<'_>,
    failure: &str,
    ended_at_unix_nanos: i64,
) -> Result<u64, Error> {
    Ok(transaction
        .execute_change(
            Query::update()
                .table(AgentTurns::Table)
                .value(AgentTurns::State, TurnState::Interrupted.as_storage_str())
                .value(AgentTurns::Failure, failure)
                .value(AgentTurns::RequiredActionJson, Option::<String>::None)
                .value(AgentTurns::EndedAtUnixNanos, ended_at(ended_at_unix_nanos))
                .and_where(Expr::col(AgentTurns::State).is_in(OPEN_STATES))
                .to_owned(),
        )
        .await?)
}

#[derive(FromRow)]
struct EventRow {
    sequence: i64,
    payload_json: String,
    occurred_at_unix_nanos: i64,
}

/// Reads up to `limit` Events after `after`, and whether more follow.
pub(crate) async fn events_after(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
    after: u64,
    limit: u16,
) -> Result<(Vec<TurnEvent>, bool), Error> {
    let after = i64::try_from(after)
        .map_err(|_error| Error::InvalidInput("turn cursor is invalid".into()))?;
    let rows: Vec<EventRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    AgentTurnEvents::Sequence,
                    AgentTurnEvents::PayloadJson,
                    AgentTurnEvents::OccurredAtUnixNanos,
                ])
                .from(AgentTurnEvents::Table)
                .and_where(Expr::col(AgentTurnEvents::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentTurnEvents::TurnId).eq(turn_id.to_string()))
                .and_where(Expr::col(AgentTurnEvents::Sequence).gt(after))
                .order_by(AgentTurnEvents::Sequence, Order::Asc)
                .limit(u64::from(limit) + 1)
                .to_owned(),
        )
        .await?;
    let has_more = rows.len() > usize::from(limit);
    let events = rows
        .into_iter()
        .take(usize::from(limit))
        .map(|row| {
            Ok(TurnEvent {
                sequence: u64::try_from(row.sequence).map_err(|_error| {
                    Error::CorruptData("Agent Turn Event has invalid sequence".into())
                })?,
                occurred_at_unix_nanos: row.occurred_at_unix_nanos,
                kind: serde_json::from_str(&row.payload_json).map_err(|_error| {
                    Error::CorruptData("Agent Turn Event has invalid payload".into())
                })?,
            })
        })
        .collect::<Result<_, Error>>()?;
    Ok((events, has_more))
}

#[derive(FromRow)]
struct MessageRow {
    role: String,
    content_json: String,
}

/// Reads one Task's thread, oldest first.
pub(crate) async fn thread(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
) -> Result<Vec<Message>, Error> {
    let rows: Vec<MessageRow> = transaction
        .fetch_all(
            Query::select()
                .columns([AgentThreadMessages::Role, AgentThreadMessages::ContentJson])
                .from(AgentThreadMessages::Table)
                .and_where(Expr::col(AgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentThreadMessages::TaskId).eq(task_id.to_string()))
                .order_by(AgentThreadMessages::Sequence, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let corrupt = || Error::CorruptData("Agent thread message is invalid".into());
            Ok(Message {
                role: Role::parse(&row.role).ok_or_else(corrupt)?,
                content: serde_json::from_str::<Vec<Value>>(&row.content_json)
                    .map_err(|_error| corrupt())?,
            })
        })
        .collect()
}

/// Appends one message to the thread on behalf of an open turn. Returns false
/// once the turn has ended, so a canceled loop leaves the thread as it was.
pub(crate) async fn append_message(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    turn_id: &TurnId,
    message: &Message,
) -> Result<bool, Error> {
    let Some(turn) = find_turn(&mut *transaction, workspace_id, turn_id).await? else {
        return Err(Error::TurnNotFound);
    };
    if turn.state.is_terminal() {
        return Ok(false);
    }
    let last: Option<Last> = transaction
        .fetch_optional(
            Query::select()
                .expr_as(Func::max(Expr::col(AgentThreadMessages::Sequence)), "last")
                .from(AgentThreadMessages::Table)
                .and_where(Expr::col(AgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentThreadMessages::TaskId).eq(task_id.to_string()))
                .to_owned(),
        )
        .await?;
    let sequence = last.and_then(|row| row.last).unwrap_or(0) + 1;
    let content = serde_json::to_string(&message.content)
        .map_err(|_error| Error::CorruptData("Agent thread message did not encode".into()))?;
    transaction
        .execute(
            Query::insert()
                .into_table(AgentThreadMessages::Table)
                .columns([
                    AgentThreadMessages::WorkspaceId,
                    AgentThreadMessages::TaskId,
                    AgentThreadMessages::Sequence,
                    AgentThreadMessages::TurnId,
                    AgentThreadMessages::Role,
                    AgentThreadMessages::ContentJson,
                ])
                .values_panic([
                    Expr::val(workspace_id.to_string()),
                    Expr::val(task_id.to_string()),
                    Expr::val(sequence),
                    Expr::val(turn_id.to_string()),
                    Expr::val(message.role.as_str()),
                    Expr::val(content),
                ])
                .to_owned(),
        )
        .await?;
    Ok(true)
}
