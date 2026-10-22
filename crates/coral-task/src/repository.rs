pub(crate) mod activity;
use sea_query::{Condition, Expr, ExprTrait as _, Iden, OnConflict, Order, Query};
use sqlx::FromRow;

use coral_db::{Database, QueryExecutor};
use coral_workspace::{Principal, PrincipalId, PrincipalKind, WorkspaceId};

use crate::{Error, Task, TaskId, TaskIntent, TaskOutcome};

#[derive(Iden)]
enum TaskRecords {
    Table,
    Id,
    WorkspaceId,
    PrincipalId,
    PrincipalKind,
    Intent,
    Outcome,
    Feedback,
    StartedAtUnixNanos,
    EndedAtUnixNanos,
    LastActivityAtUnixNanos,
}

#[derive(Debug, FromRow)]
struct TaskRow {
    id: String,
    workspace_id: String,
    principal_id: String,
    principal_kind: String,
    intent: String,
    outcome: Option<String>,
    feedback: Option<String>,
    started_at_unix_nanos: i64,
    ended_at_unix_nanos: Option<i64>,
}

impl TaskRow {
    fn decode(self) -> Result<Task, Error> {
        let id = TaskId::parse(&self.id)
            .map_err(|_parse_error| Error::CorruptData("Task has invalid identity".to_string()))?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_parse_error| {
            Error::CorruptData("Task has invalid Workspace identity".to_string())
        })?;
        let principal_id = PrincipalId::parse(&self.principal_id).map_err(|_parse_error| {
            Error::CorruptData("Task has invalid Principal identity".to_string())
        })?;
        let principal_kind = PrincipalKind::from_storage_str(&self.principal_kind)?;
        let intent = TaskIntent::parse(&self.intent)
            .map_err(|_parse_error| Error::CorruptData("Task has invalid intent".to_string()))?;
        let outcome = self
            .outcome
            .as_deref()
            .map(TaskOutcome::from_storage_str)
            .transpose()?;
        if let Some(value) = &self.feedback {
            crate::types::validate_feedback(value)
                .map_err(|_invalid| Error::CorruptData("Task has invalid feedback".into()))?;
        }
        if (outcome.is_none() && self.feedback.is_some())
            || outcome.is_some() != self.ended_at_unix_nanos.is_some()
            || self
                .ended_at_unix_nanos
                .is_some_and(|ended| ended < self.started_at_unix_nanos)
        {
            return Err(Error::CorruptData(
                "Task has inconsistent terminal evidence".to_string(),
            ));
        }
        Ok(Task::new(
            id,
            workspace_id,
            Principal::from_parts(principal_id, principal_kind),
            intent,
            outcome,
            self.feedback,
            self.started_at_unix_nanos,
            self.ended_at_unix_nanos,
        ))
    }
}

/// Inserts the Task row. `false` means a Task with this identifier already
/// exists and nothing was written: the caller draws a new identifier.
pub(super) async fn insert(
    database: &mut coral_db::Transaction<'_>,
    task: &Task,
) -> Result<bool, Error> {
    let statement = Query::insert()
        .into_table(TaskRecords::Table)
        .columns([
            TaskRecords::Id,
            TaskRecords::WorkspaceId,
            TaskRecords::PrincipalId,
            TaskRecords::PrincipalKind,
            TaskRecords::Intent,
            TaskRecords::Outcome,
            TaskRecords::StartedAtUnixNanos,
            TaskRecords::EndedAtUnixNanos,
            TaskRecords::LastActivityAtUnixNanos,
        ])
        .values_panic([
            Expr::val(task.id().to_string()),
            Expr::val(task.workspace_id().to_string()),
            Expr::val(task.principal().id().as_str()),
            Expr::val(task.principal().kind().as_storage_str()),
            Expr::val(task.intent().as_str()),
            Expr::val(Option::<String>::None),
            Expr::val(task.started_at_unix_nanos()),
            Expr::val(Option::<i64>::None),
            Expr::val(task.started_at_unix_nanos()),
        ])
        .on_conflict(OnConflict::column(TaskRecords::Id).do_nothing().to_owned())
        .to_owned();
    let rows = database.execute_change(statement).await?;
    Ok(rows == 1)
}

pub(super) async fn find_visible(
    database: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    principal: &Principal,
    include_workspace_agents: bool,
) -> Result<Option<Task>, Error> {
    let statement = base_select()
        .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(TaskRecords::Id).eq(task_id.to_string()))
        .cond_where(visibility_condition(principal, include_workspace_agents))
        .to_owned();
    let row: Option<TaskRow> = database.fetch_optional(statement).await?;
    row.map(TaskRow::decode).transpose()
}

pub(super) async fn find_attributed(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    principal: &Principal,
) -> Result<Option<Task>, Error> {
    let row: Option<TaskRow> = executor
        .fetch_optional(find_statement(workspace_id, task_id, principal))
        .await?;
    row.map(TaskRow::decode).transpose()
}

pub(super) async fn complete_for_principal(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    principal: &Principal,
    outcome: TaskOutcome,
    feedback: Option<&str>,
    ended_at_unix_nanos: i64,
) -> Result<Option<Task>, Error> {
    let statement = Query::update()
        .table(TaskRecords::Table)
        .values([
            (TaskRecords::Outcome, Expr::val(outcome.as_storage_str())),
            (TaskRecords::Feedback, Expr::val(feedback)),
            (
                TaskRecords::EndedAtUnixNanos,
                Expr::val(ended_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(TaskRecords::Id).eq(task_id.to_string()))
        .and_where(Expr::col(TaskRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(Expr::col(TaskRecords::PrincipalKind).eq(principal.kind().as_storage_str()))
        .and_where(Expr::col(TaskRecords::Outcome).is_null())
        .and_where(Expr::col(TaskRecords::StartedAtUnixNanos).lte(ended_at_unix_nanos))
        .to_owned();
    let changed = transaction.execute_change(statement).await? == 1;
    let task = find_attributed(&mut *transaction, workspace_id, task_id, principal).await?;
    if !changed && task.as_ref().is_some_and(|task| task.outcome().is_none()) {
        return Err(Error::InvalidInput(
            "Task end time cannot precede its start time".to_string(),
        ));
    }
    if changed {
        touch(transaction, workspace_id, task_id, ended_at_unix_nanos).await?;
    }
    Ok(task)
}

fn find_statement(
    workspace_id: &WorkspaceId,
    task_id: &TaskId,
    principal: &Principal,
) -> sea_query::SelectStatement {
    base_select()
        .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(TaskRecords::Id).eq(task_id.to_string()))
        .and_where(Expr::col(TaskRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(Expr::col(TaskRecords::PrincipalKind).eq(principal.kind().as_storage_str()))
        .to_owned()
}

pub(super) async fn list_visible(
    database: &Database,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    include_workspace_agents: bool,
) -> Result<Vec<Task>, Error> {
    let statement = base_select()
        .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace_id.to_string()))
        .cond_where(visibility_condition(principal, include_workspace_agents))
        .order_by(TaskRecords::StartedAtUnixNanos, Order::Asc)
        .order_by(TaskRecords::Id, Order::Asc)
        .to_owned();
    let rows: Vec<TaskRow> = database.fetch_all(statement).await?;
    rows.into_iter().map(TaskRow::decode).collect()
}

fn visibility_condition(principal: &Principal, include_workspace_agents: bool) -> Condition {
    let own_tasks = Condition::all()
        .add(Expr::col(TaskRecords::PrincipalId).eq(principal.id().as_str()))
        .add(Expr::col(TaskRecords::PrincipalKind).eq(principal.kind().as_storage_str()));
    let condition = Condition::any().add(own_tasks);
    if include_workspace_agents {
        condition
            .add(Expr::col(TaskRecords::PrincipalKind).eq(PrincipalKind::Agent.as_storage_str()))
    } else {
        condition
    }
}

pub(super) async fn retention_group(
    transaction: &mut coral_db::Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
) -> Result<Option<(Task, i64)>, Error> {
    let row: Option<TaskRow> = transaction
        .fetch_optional(
            base_select()
                .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskRecords::Id).eq(task.to_string()))
                .to_owned(),
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let activity: Option<(i64,)> = transaction
        .fetch_optional(
            Query::select()
                .column(TaskRecords::LastActivityAtUnixNanos)
                .from(TaskRecords::Table)
                .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskRecords::Id).eq(task.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(Some((
        row.decode()?,
        activity.ok_or(Error::TaskNotFound)?.0,
    )))
}

pub(super) async fn delete_group(
    transaction: &mut coral_db::Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::delete()
                .from_table(TaskRecords::Table)
                .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskRecords::Id).eq(task.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// A portable row lock, shared by begin, completion, ending, and cleanup.
pub(super) async fn hold(
    transaction: &mut coral_db::Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
) -> Result<bool, Error> {
    Ok(transaction
        .execute(
            Query::update()
                .table(TaskRecords::Table)
                .value(
                    TaskRecords::LastActivityAtUnixNanos,
                    Expr::col(TaskRecords::LastActivityAtUnixNanos),
                )
                .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskRecords::Id).eq(task.to_string()))
                .to_owned(),
        )
        .await?
        == 1)
}

pub(super) async fn touch(
    transaction: &mut coral_db::Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    now: i64,
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::update()
                .table(TaskRecords::Table)
                .value(TaskRecords::LastActivityAtUnixNanos, now)
                .and_where(Expr::col(TaskRecords::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskRecords::Id).eq(task.to_string()))
                .and_where(Expr::col(TaskRecords::LastActivityAtUnixNanos).lt(now))
                .to_owned(),
        )
        .await?;
    Ok(())
}

fn base_select() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            TaskRecords::Id,
            TaskRecords::WorkspaceId,
            TaskRecords::PrincipalId,
            TaskRecords::PrincipalKind,
            TaskRecords::Intent,
            TaskRecords::Outcome,
            TaskRecords::StartedAtUnixNanos,
            TaskRecords::EndedAtUnixNanos,
            TaskRecords::Feedback,
        ])
        .from(TaskRecords::Table)
        .to_owned()
}
