//! The rows of Sub-agents and their Turns. Every statement matches the
//! Workspace, and a read by `sub_agent_id` matches the caller too.

use coral_db::{Database, QueryExecutor, Transaction, UnixNanos};
use coral_task::TaskId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};
use sqlx::FromRow;

use crate::sub_agents::{Error, Turn, TurnState};

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
}

const RUNNING: &str = "running";

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

/// Inserts the Sub-agent's running Turn, held until `lease_expires_at`.
pub(crate) async fn insert_turn(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    sub_agent_id: &str,
    turn_id: &str,
    lease_expires_at: UnixNanos,
) -> Result<(), coral_db::Error> {
    let statement = Query::insert()
        .into_table(SubAgentTurns::Table)
        .columns([
            SubAgentTurns::Id,
            SubAgentTurns::WorkspaceId,
            SubAgentTurns::SubAgentId,
            SubAgentTurns::State,
            SubAgentTurns::LeaseExpiresAtUnixNanos,
        ])
        .values_panic([
            Expr::val(turn_id),
            Expr::val(workspace_id.to_string()),
            Expr::val(sub_agent_id),
            Expr::val(RUNNING),
            Expr::val(lease_expires_at),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

/// The Turn of the Sub-agent that `principal` started in the Workspace.
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
    let statement = Query::select()
        .column((SubAgentTurns::Table, SubAgentTurns::Id))
        .column((SubAgentTurns::Table, SubAgentTurns::State))
        .column((SubAgentTurns::Table, SubAgentTurns::Answer))
        .column((SubAgentTurns::Table, SubAgentTurns::Failure))
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
        .to_owned();
    let row: Option<TurnRow> = executor.fetch_optional(statement).await?;
    row.map(TurnRow::decode).transpose()
}

/// Ends the Turn as `ending` says. `false` means that it had ended already
/// and nothing was written.
pub(crate) async fn end_turn(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    turn_id: &str,
    ending: &TurnState,
) -> Result<bool, coral_db::Error> {
    let (state, answer, failure) = stored(ending);
    let statement = Query::update()
        .table(SubAgentTurns::Table)
        .values([
            (SubAgentTurns::State, Expr::val(state)),
            (SubAgentTurns::Answer, Expr::val(answer)),
            (SubAgentTurns::Failure, Expr::val(failure)),
        ])
        .and_where(Expr::col(SubAgentTurns::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SubAgentTurns::Id).eq(turn_id))
        .and_where(Expr::col(SubAgentTurns::State).eq(RUNNING))
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

#[derive(FromRow)]
struct TurnRow {
    id: String,
    state: String,
    answer: Option<String>,
    failure: Option<String>,
}

impl TurnRow {
    fn decode(self) -> Result<Turn, Error> {
        let state = match (self.state.as_str(), self.answer, self.failure) {
            (RUNNING, None, None) => TurnState::Running,
            ("completed", Some(answer), None) => TurnState::Completed { answer },
            ("failed", None, Some(failure)) => TurnState::Failed { failure },
            ("canceled", None, None) => TurnState::Canceled,
            _ => return Err(Error::CorruptData),
        };
        Ok(Turn { id: self.id, state })
    }
}

/// The state, the answer and the failure that the row of a Turn holds.
fn stored(state: &TurnState) -> (&'static str, Option<&str>, Option<&str>) {
    match state {
        TurnState::Running => (RUNNING, None, None),
        TurnState::Completed { answer } => ("completed", Some(answer), None),
        TurnState::Failed { failure } => ("failed", None, Some(failure)),
        TurnState::Canceled => ("canceled", None, None),
    }
}
