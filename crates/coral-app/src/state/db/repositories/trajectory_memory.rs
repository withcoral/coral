#[cfg(test)]
use sea_query::ExprTrait;
use sea_query::{Expr, Query};

use crate::state::db::schema::TrajectoryRawSteps;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct RawTrajectoryStepRecord {
    pub(crate) id: String,
    pub(crate) task_id: String,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) completed_at_unix_nanos: i64,
    pub(crate) operation: String,
    pub(crate) input: String,
    pub(crate) status: String,
    pub(crate) row_count: Option<i64>,
    pub(crate) output_summary_json: Option<String>,
    pub(crate) error_kind: Option<String>,
    pub(crate) error_type: Option<String>,
    pub(crate) error_message: Option<String>,
}

pub(crate) struct TrajectoryMemoryRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> TrajectoryMemoryRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn insert_raw_step(
        &mut self,
        workspace_id: &str,
        step: &RawTrajectoryStepRecord,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(TrajectoryRawSteps::Table)
            .columns([
                TrajectoryRawSteps::WorkspaceId,
                TrajectoryRawSteps::Id,
                TrajectoryRawSteps::TaskId,
                TrajectoryRawSteps::StartedAtUnixNanos,
                TrajectoryRawSteps::CompletedAtUnixNanos,
                TrajectoryRawSteps::Operation,
                TrajectoryRawSteps::Input,
                TrajectoryRawSteps::Status,
                TrajectoryRawSteps::RowCount,
                TrajectoryRawSteps::OutputSummaryJson,
                TrajectoryRawSteps::ErrorKind,
                TrajectoryRawSteps::ErrorType,
                TrajectoryRawSteps::ErrorMessage,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(step.id.clone()),
                Expr::val(step.task_id.clone()),
                Expr::val(step.started_at_unix_nanos),
                Expr::val(step.completed_at_unix_nanos),
                Expr::val(step.operation.clone()),
                Expr::val(step.input.clone()),
                Expr::val(step.status.clone()),
                Expr::val(step.row_count),
                Expr::val(step.output_summary_json.clone()),
                Expr::val(step.error_kind.clone()),
                Expr::val(step.error_type.clone()),
                Expr::val(step.error_message.clone()),
            ])
            .to_owned();
        self.session.execute(statement).await
    }

    #[cfg(test)]
    pub(crate) async fn list_raw_steps_for_task(
        &mut self,
        workspace_id: &str,
        task_id: &str,
    ) -> Result<Vec<RawTrajectoryStepRecord>, DbError> {
        let statement = Query::select()
            .columns(raw_step_columns())
            .from(TrajectoryRawSteps::Table)
            .and_where(Expr::col(TrajectoryRawSteps::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TrajectoryRawSteps::TaskId).eq(task_id))
            .order_by(
                TrajectoryRawSteps::StartedAtUnixNanos,
                sea_query::Order::Asc,
            )
            .order_by(
                TrajectoryRawSteps::CompletedAtUnixNanos,
                sea_query::Order::Asc,
            )
            .order_by(TrajectoryRawSteps::Id, sea_query::Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }
}

#[cfg(test)]
fn raw_step_columns() -> [TrajectoryRawSteps; 12] {
    [
        TrajectoryRawSteps::Id,
        TrajectoryRawSteps::TaskId,
        TrajectoryRawSteps::StartedAtUnixNanos,
        TrajectoryRawSteps::CompletedAtUnixNanos,
        TrajectoryRawSteps::Operation,
        TrajectoryRawSteps::Input,
        TrajectoryRawSteps::Status,
        TrajectoryRawSteps::RowCount,
        TrajectoryRawSteps::OutputSummaryJson,
        TrajectoryRawSteps::ErrorKind,
        TrajectoryRawSteps::ErrorType,
        TrajectoryRawSteps::ErrorMessage,
    ]
}
