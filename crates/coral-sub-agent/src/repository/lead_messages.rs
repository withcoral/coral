//! The lead's messages after its first, which a running Turn holds unread,
//! and the messages of one Turn.

use coral_db::{QueryExecutor, RowLock, Transaction};
use coral_task::TaskId;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Order, Query};

use super::{
    MessageRow, RUNNING, SubAgentRecords, SubAgentThreadMessages, SubAgentTurns, end_turn,
};
use crate::model::Message;
use crate::sub_agents::{Error, TurnState};

/// Adds `message` to what the running Turn has not read. `false` means that
/// the Turn has ended and nothing was written.
pub(crate) async fn add_unread_message(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    message: &str,
) -> Result<bool, coral_db::Error> {
    let held = hold_unread(transaction, workspace_id, turn_id).await?;
    let Some((unread,)) = held else {
        return Ok(false);
    };
    let unread = match unread {
        Some(unread) => format!("{unread}\n\n{message}"),
        None => message.to_owned(),
    };
    set_unread(transaction, workspace_id, turn_id, Some(unread)).await?;
    Ok(true)
}

/// Takes what the running Turn has not read, as one text.
pub(crate) async fn take_unread_messages(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
) -> Result<Option<String>, coral_db::Error> {
    let unread = hold_unread(transaction, workspace_id, turn_id).await?;
    let unread = unread.and_then(|(unread,)| unread);
    if unread.is_some() {
        set_unread(transaction, workspace_id, turn_id, None).await?;
    }
    Ok(unread)
}

/// Completes the Turn with `answer` unless it holds an unread message.
/// `false` means that a message waits and nothing was written; `true` that
/// the Turn has ended, now or before.
pub(crate) async fn complete_unless_unread(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    answer: &str,
) -> Result<bool, coral_db::Error> {
    match hold_unread(transaction, workspace_id, turn_id).await? {
        Some((Some(_unread),)) => Ok(false),
        Some((None,)) => {
            let completed = TurnState::Completed {
                answer: answer.to_owned(),
            };
            end_turn(transaction, workspace_id, turn_id, &completed).await?;
            Ok(true)
        }
        None => Ok(true),
    }
}

/// The Task of the Sub-agent.
pub(crate) async fn task_id(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
) -> Result<Option<TaskId>, Error> {
    let statement = Query::select()
        .column(SubAgentRecords::TaskId)
        .from(SubAgentRecords::Table)
        .and_where(Expr::col(SubAgentRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentRecords::Id).eq(sub_agent_id))
        .to_owned();
    let row: Option<(String,)> = executor.fetch_optional(statement).await?;
    row.map(|(task_id,)| TaskId::parse(&task_id).map_err(|_corrupt| Error::CorruptData))
        .transpose()
}

/// The position after the last message of the Sub-agent's thread.
pub(crate) async fn next_position(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
) -> Result<i64, coral_db::Error> {
    let statement = Query::select()
        .expr(Expr::col(SubAgentThreadMessages::Position).max())
        .from(SubAgentThreadMessages::Table)
        .and_where(Expr::col(SubAgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentThreadMessages::SubAgentId).eq(sub_agent_id))
        .to_owned();
    let row: Option<(Option<i64>,)> = executor.fetch_optional(statement).await?;
    let last = row.and_then(|(last,)| last);
    Ok(last.map_or(0, |last| last.saturating_add(1)))
}

/// The messages of one Turn of the Sub-agent, in order.
pub(crate) async fn turn_messages(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    turn_id: &str,
) -> Result<Vec<Message>, Error> {
    let statement = Query::select()
        .columns([
            SubAgentThreadMessages::Role,
            SubAgentThreadMessages::ContentJson,
        ])
        .from(SubAgentThreadMessages::Table)
        .and_where(Expr::col(SubAgentThreadMessages::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentThreadMessages::SubAgentId).eq(sub_agent_id))
        .and_where(Expr::col(SubAgentThreadMessages::TurnId).eq(turn_id))
        .order_by(SubAgentThreadMessages::Position, Order::Asc)
        .to_owned();
    let rows: Vec<MessageRow> = executor.fetch_all(statement).await?;
    rows.into_iter().map(MessageRow::decode).collect()
}

/// Holds the row of the running Turn, and reads what it has not read.
async fn hold_unread(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
) -> Result<Option<(Option<String>,)>, coral_db::Error> {
    let statement = Query::select()
        .column(SubAgentTurns::UnreadMessages)
        .from(SubAgentTurns::Table)
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .and_where(Expr::col(SubAgentTurns::State).eq(RUNNING))
        .to_owned();
    transaction
        .fetch_optional_locked(statement, RowLock::Exclusive)
        .await
}

async fn set_unread(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    unread: Option<String>,
) -> Result<(), coral_db::Error> {
    let statement = Query::update()
        .table(SubAgentTurns::Table)
        .value(SubAgentTurns::UnreadMessages, unread)
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}
