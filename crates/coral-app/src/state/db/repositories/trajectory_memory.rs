use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::state::db::schema::{
    TrajectoryDistillations, TrajectoryDistilledSteps, TrajectoryRawSteps,
};
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

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct DistillationRecord {
    pub(crate) id: String,
    pub(crate) task_id: String,
    pub(crate) strategy: String,
    pub(crate) normalized_intent: String,
    pub(crate) path_key: String,
    pub(crate) input_step_count: i64,
    pub(crate) output_step_count: i64,
    pub(crate) created_at_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct DistilledStepRecord {
    pub(crate) id: String,
    pub(crate) distillation_id: String,
    pub(crate) source_raw_step_id: String,
    pub(crate) ordinal: i64,
    pub(crate) sql_template: String,
    pub(crate) relations_json: String,
    pub(crate) result_row_count: Option<i64>,
    pub(crate) result_column_count: Option<i64>,
    pub(crate) exact_key: String,
    pub(crate) created_at_unix_nanos: i64,
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

    pub(crate) async fn upsert_distillation(
        &mut self,
        workspace_id: &str,
        distillation: &DistillationRecord,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(TrajectoryDistillations::Table)
            .columns([
                TrajectoryDistillations::WorkspaceId,
                TrajectoryDistillations::Id,
                TrajectoryDistillations::TaskId,
                TrajectoryDistillations::Strategy,
                TrajectoryDistillations::NormalizedIntent,
                TrajectoryDistillations::PathKey,
                TrajectoryDistillations::InputStepCount,
                TrajectoryDistillations::OutputStepCount,
                TrajectoryDistillations::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(distillation.id.clone()),
                Expr::val(distillation.task_id.clone()),
                Expr::val(distillation.strategy.clone()),
                Expr::val(distillation.normalized_intent.clone()),
                Expr::val(distillation.path_key.clone()),
                Expr::val(distillation.input_step_count),
                Expr::val(distillation.output_step_count),
                Expr::val(distillation.created_at_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([
                    TrajectoryDistillations::WorkspaceId,
                    TrajectoryDistillations::Id,
                ])
                .update_columns([
                    TrajectoryDistillations::TaskId,
                    TrajectoryDistillations::Strategy,
                    TrajectoryDistillations::NormalizedIntent,
                    TrajectoryDistillations::PathKey,
                    TrajectoryDistillations::InputStepCount,
                    TrajectoryDistillations::OutputStepCount,
                    TrajectoryDistillations::CreatedAtUnixNanos,
                ])
                .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    #[cfg(test)]
    pub(crate) async fn get_distillation(
        &mut self,
        workspace_id: &str,
        distillation_id: &str,
    ) -> Result<Option<DistillationRecord>, DbError> {
        let statement = Query::select()
            .columns(distillation_columns())
            .from(TrajectoryDistillations::Table)
            .and_where(Expr::col(TrajectoryDistillations::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TrajectoryDistillations::Id).eq(distillation_id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }

    pub(crate) async fn delete_distilled_steps(
        &mut self,
        workspace_id: &str,
        distillation_id: &str,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(TrajectoryDistilledSteps::Table)
            .and_where(Expr::col(TrajectoryDistilledSteps::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TrajectoryDistilledSteps::DistillationId).eq(distillation_id))
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn insert_distilled_step(
        &mut self,
        workspace_id: &str,
        step: &DistilledStepRecord,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(TrajectoryDistilledSteps::Table)
            .columns([
                TrajectoryDistilledSteps::WorkspaceId,
                TrajectoryDistilledSteps::Id,
                TrajectoryDistilledSteps::DistillationId,
                TrajectoryDistilledSteps::SourceRawStepId,
                TrajectoryDistilledSteps::Ordinal,
                TrajectoryDistilledSteps::SqlTemplate,
                TrajectoryDistilledSteps::RelationsJson,
                TrajectoryDistilledSteps::ResultRowCount,
                TrajectoryDistilledSteps::ResultColumnCount,
                TrajectoryDistilledSteps::ExactKey,
                TrajectoryDistilledSteps::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(step.id.clone()),
                Expr::val(step.distillation_id.clone()),
                Expr::val(step.source_raw_step_id.clone()),
                Expr::val(step.ordinal),
                Expr::val(step.sql_template.clone()),
                Expr::val(step.relations_json.clone()),
                Expr::val(step.result_row_count),
                Expr::val(step.result_column_count),
                Expr::val(step.exact_key.clone()),
                Expr::val(step.created_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await
    }

    #[cfg(test)]
    pub(crate) async fn list_distilled_steps(
        &mut self,
        workspace_id: &str,
        distillation_id: &str,
    ) -> Result<Vec<DistilledStepRecord>, DbError> {
        let statement = Query::select()
            .columns(distilled_step_columns())
            .from(TrajectoryDistilledSteps::Table)
            .and_where(Expr::col(TrajectoryDistilledSteps::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TrajectoryDistilledSteps::DistillationId).eq(distillation_id))
            .order_by(TrajectoryDistilledSteps::Ordinal, sea_query::Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }
}

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

#[cfg(test)]
fn distillation_columns() -> [TrajectoryDistillations; 8] {
    [
        TrajectoryDistillations::Id,
        TrajectoryDistillations::TaskId,
        TrajectoryDistillations::Strategy,
        TrajectoryDistillations::NormalizedIntent,
        TrajectoryDistillations::PathKey,
        TrajectoryDistillations::InputStepCount,
        TrajectoryDistillations::OutputStepCount,
        TrajectoryDistillations::CreatedAtUnixNanos,
    ]
}

#[cfg(test)]
fn distilled_step_columns() -> [TrajectoryDistilledSteps; 10] {
    [
        TrajectoryDistilledSteps::Id,
        TrajectoryDistilledSteps::DistillationId,
        TrajectoryDistilledSteps::SourceRawStepId,
        TrajectoryDistilledSteps::Ordinal,
        TrajectoryDistilledSteps::SqlTemplate,
        TrajectoryDistilledSteps::RelationsJson,
        TrajectoryDistilledSteps::ResultRowCount,
        TrajectoryDistilledSteps::ResultColumnCount,
        TrajectoryDistilledSteps::ExactKey,
        TrajectoryDistilledSteps::CreatedAtUnixNanos,
    ]
}
