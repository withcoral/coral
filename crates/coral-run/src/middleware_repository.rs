//! Run-owned Middleware evidence: the pinned chain, the frozen Operation
//! policy, and Middleware executions.

use coral_catalog::OperationKey;
use coral_db::Transaction;
use coral_middleware::MiddlewareSelection;
use coral_policy::{ExactOperationRules, OperationAccessState};
use coral_provider::ProviderId;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use crate::middleware::{
    MiddlewareExecutionStatus, MiddlewareExecutionSummary, MiddlewarePhase, MiddlewareScope,
    PinnedMiddleware,
};
use crate::types::AcceptedRun;
use crate::{Error, RunId};

#[derive(Iden)]
enum ProgramRunOperationAccessRules {
    Table,
    RunId,
    WorkspaceId,
    ProviderId,
    OperationKey,
    AccessState,
}

#[derive(Iden)]
enum ProgramRunMiddlewareSnapshots {
    Table,
    RunId,
    WorkspaceId,
    Position,
    MiddlewareId,
    Name,
    Source,
    RunScope,
    FunctionScope,
    OperationScope,
    Revision,
}

#[derive(Iden)]
enum MiddlewareExecutions {
    Table,
    Id,
    WorkspaceId,
    RunId,
    Position,
    MiddlewareId,
    MiddlewareName,
    Scope,
    Target,
    Phase,
    Status,
    StartedAtUnixNanos,
    EndedAtUnixNanos,
    PreviewJson,
    FailureSummary,
}

pub(crate) async fn insert_admission_snapshots(
    transaction: &mut Transaction<'_>,
    run: &AcceptedRun,
    chain: &[MiddlewareSelection],
    rules: &ExactOperationRules,
) -> Result<(), Error> {
    for (provider_id, operation_key, state) in rules.rules() {
        transaction
            .execute(
                Query::insert()
                    .into_table(ProgramRunOperationAccessRules::Table)
                    .columns([
                        ProgramRunOperationAccessRules::RunId,
                        ProgramRunOperationAccessRules::WorkspaceId,
                        ProgramRunOperationAccessRules::ProviderId,
                        ProgramRunOperationAccessRules::OperationKey,
                        ProgramRunOperationAccessRules::AccessState,
                    ])
                    .values_panic([
                        Expr::val(run.id.to_string()),
                        Expr::val(run.workspace_id.to_string()),
                        Expr::val(provider_id.to_string()),
                        Expr::val(operation_key.as_str()),
                        Expr::val(state.as_storage_str()),
                    ])
                    .to_owned(),
            )
            .await?;
    }
    for (position, middleware) in chain.iter().enumerate() {
        let position = i64::try_from(position)
            .map_err(|_overflow| Error::CorruptData("Middleware chain is too long".into()))?;
        transaction
            .execute(
                Query::insert()
                    .into_table(ProgramRunMiddlewareSnapshots::Table)
                    .columns([
                        ProgramRunMiddlewareSnapshots::RunId,
                        ProgramRunMiddlewareSnapshots::WorkspaceId,
                        ProgramRunMiddlewareSnapshots::Position,
                        ProgramRunMiddlewareSnapshots::MiddlewareId,
                        ProgramRunMiddlewareSnapshots::Name,
                        ProgramRunMiddlewareSnapshots::Source,
                        ProgramRunMiddlewareSnapshots::RunScope,
                        ProgramRunMiddlewareSnapshots::FunctionScope,
                        ProgramRunMiddlewareSnapshots::OperationScope,
                        ProgramRunMiddlewareSnapshots::Revision,
                    ])
                    .values_panic([
                        Expr::val(run.id.to_string()),
                        Expr::val(run.workspace_id.to_string()),
                        Expr::val(position),
                        Expr::val(middleware.id().to_string()),
                        Expr::val(middleware.name().as_str()),
                        Expr::val(middleware.source().as_str()),
                        Expr::val(middleware.scopes().run),
                        Expr::val(middleware.scopes().function),
                        Expr::val(middleware.scopes().operation),
                        Expr::val(middleware.revision()),
                    ])
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn pinned_chain(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Vec<PinnedMiddleware>, Error> {
    let rows: Vec<(i64, String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    ProgramRunMiddlewareSnapshots::Position,
                    ProgramRunMiddlewareSnapshots::MiddlewareId,
                    ProgramRunMiddlewareSnapshots::Name,
                ])
                .from(ProgramRunMiddlewareSnapshots::Table)
                .and_where(
                    Expr::col(ProgramRunMiddlewareSnapshots::WorkspaceId)
                        .eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(ProgramRunMiddlewareSnapshots::RunId).eq(run_id.to_string()))
                .order_by(ProgramRunMiddlewareSnapshots::Position, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(position, id, name)| {
            Ok(PinnedMiddleware {
                position: usize::try_from(position).map_err(|_error| {
                    Error::CorruptData("pinned Middleware has an invalid position".into())
                })?,
                id,
                name,
            })
        })
        .collect()
}

/// Returns the frozen exact rule for one Operation, if the Run pinned one.
pub(crate) async fn frozen_rule(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    provider_id: &ProviderId,
    operation_key: &OperationKey,
) -> Result<Option<OperationAccessState>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProgramRunOperationAccessRules::AccessState)
                .from(ProgramRunOperationAccessRules::Table)
                .and_where(
                    Expr::col(ProgramRunOperationAccessRules::WorkspaceId)
                        .eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(ProgramRunOperationAccessRules::RunId).eq(run_id.to_string()))
                .and_where(
                    Expr::col(ProgramRunOperationAccessRules::ProviderId)
                        .eq(provider_id.to_string()),
                )
                .and_where(
                    Expr::col(ProgramRunOperationAccessRules::OperationKey)
                        .eq(operation_key.as_str()),
                )
                .to_owned(),
        )
        .await?;
    row.map(|(state,)| OperationAccessState::from_storage_str(&state))
        .transpose()
        .map_err(Into::into)
}

pub(crate) struct NewExecution<'a> {
    pub(crate) id: &'a str,
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) run_id: &'a RunId,
    pub(crate) middleware: &'a PinnedMiddleware,
    pub(crate) scope: MiddlewareScope,
    pub(crate) target: Option<&'a str>,
    pub(crate) started_at_unix_nanos: i64,
}

pub(crate) async fn insert_execution(
    transaction: &mut Transaction<'_>,
    execution: NewExecution<'_>,
) -> Result<(), Error> {
    let position = i64::try_from(execution.middleware.position)
        .map_err(|_overflow| Error::CorruptData("Middleware position overflowed".into()))?;
    transaction
        .execute_change(
            Query::insert()
                .into_table(MiddlewareExecutions::Table)
                .columns([
                    MiddlewareExecutions::Id,
                    MiddlewareExecutions::WorkspaceId,
                    MiddlewareExecutions::RunId,
                    MiddlewareExecutions::Position,
                    MiddlewareExecutions::MiddlewareId,
                    MiddlewareExecutions::MiddlewareName,
                    MiddlewareExecutions::Scope,
                    MiddlewareExecutions::Target,
                    MiddlewareExecutions::Phase,
                    MiddlewareExecutions::Status,
                    MiddlewareExecutions::StartedAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(execution.id),
                    Expr::val(execution.workspace_id.to_string()),
                    Expr::val(execution.run_id.to_string()),
                    Expr::val(position),
                    Expr::val(execution.middleware.id.as_str()),
                    Expr::val(execution.middleware.name.as_str()),
                    Expr::val(execution.scope.as_storage_str()),
                    Expr::val(execution.target),
                    Expr::val(MiddlewarePhase::Enter.as_storage_str()),
                    Expr::val(MiddlewareExecutionStatus::Running.as_storage_str()),
                    Expr::val(execution.started_at_unix_nanos),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

fn running(workspace_id: &WorkspaceId, run_id: &RunId, id: &str) -> sea_query::UpdateStatement {
    Query::update()
        .table(MiddlewareExecutions::Table)
        .and_where(Expr::col(MiddlewareExecutions::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(MiddlewareExecutions::RunId).eq(run_id.to_string()))
        .and_where(Expr::col(MiddlewareExecutions::Id).eq(id))
        .and_where(
            Expr::col(MiddlewareExecutions::Status)
                .eq(MiddlewareExecutionStatus::Running.as_storage_str()),
        )
        .to_owned()
}

/// Whether one Middleware execution of this Run is still running.
pub(crate) async fn is_running(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    id: &str,
) -> Result<bool, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(MiddlewareExecutions::Id)
                .from(MiddlewareExecutions::Table)
                .and_where(
                    Expr::col(MiddlewareExecutions::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(MiddlewareExecutions::RunId).eq(run_id.to_string()))
                .and_where(Expr::col(MiddlewareExecutions::Id).eq(id))
                .and_where(
                    Expr::col(MiddlewareExecutions::Status)
                        .eq(MiddlewareExecutionStatus::Running.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}

pub(crate) async fn mark_exit(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    id: &str,
) -> Result<bool, Error> {
    Ok(transaction
        .execute_change(
            running(workspace_id, run_id, id)
                .value(
                    MiddlewareExecutions::Phase,
                    MiddlewarePhase::Exit.as_storage_str(),
                )
                .to_owned(),
        )
        .await?
        == 1)
}

pub(crate) async fn finish_execution(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    id: &str,
    outcome: &coral_v8::MiddlewareExecutionOutcome,
    ended_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let (status, preview, failure) = match outcome {
        coral_v8::MiddlewareExecutionOutcome::Succeeded { preview_json } => (
            MiddlewareExecutionStatus::Succeeded,
            preview_json.clone(),
            None,
        ),
        coral_v8::MiddlewareExecutionOutcome::Failed { summary } => (
            MiddlewareExecutionStatus::Failed,
            None,
            Some(summary.clone()),
        ),
    };
    Ok(transaction
        .execute_change(
            running(workspace_id, run_id, id)
                .values([
                    (
                        MiddlewareExecutions::Status,
                        Expr::val(status.as_storage_str()),
                    ),
                    (
                        MiddlewareExecutions::EndedAtUnixNanos,
                        Expr::val(ended_at_unix_nanos),
                    ),
                    (MiddlewareExecutions::PreviewJson, Expr::val(preview)),
                    (MiddlewareExecutions::FailureSummary, Expr::val(failure)),
                ])
                .to_owned(),
        )
        .await?
        == 1)
}

/// Settles every execution still running when its Run reaches a terminal state.
pub(crate) async fn settle_running(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    ended_at_unix_nanos: i64,
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::update()
                .table(MiddlewareExecutions::Table)
                .values([
                    (
                        MiddlewareExecutions::Status,
                        Expr::val(MiddlewareExecutionStatus::RunTerminated.as_storage_str()),
                    ),
                    (
                        MiddlewareExecutions::EndedAtUnixNanos,
                        Expr::val(ended_at_unix_nanos),
                    ),
                ])
                .and_where(
                    Expr::col(MiddlewareExecutions::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(MiddlewareExecutions::RunId).eq(run_id.to_string()))
                .and_where(
                    Expr::col(MiddlewareExecutions::Status)
                        .eq(MiddlewareExecutionStatus::Running.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

#[derive(FromRow)]
struct ExecutionRow {
    id: String,
    run_id: String,
    position: i64,
    middleware_id: String,
    middleware_name: String,
    scope: String,
    target: Option<String>,
    phase: String,
    status: String,
    started_at_unix_nanos: i64,
    ended_at_unix_nanos: Option<i64>,
    preview_json: Option<String>,
    failure_summary: Option<String>,
}

pub(crate) async fn execution_summary(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    id: &str,
    now: i64,
) -> Result<MiddlewareExecutionSummary, Error> {
    let row: ExecutionRow = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    MiddlewareExecutions::Id,
                    MiddlewareExecutions::RunId,
                    MiddlewareExecutions::Position,
                    MiddlewareExecutions::MiddlewareId,
                    MiddlewareExecutions::MiddlewareName,
                    MiddlewareExecutions::Scope,
                    MiddlewareExecutions::Target,
                    MiddlewareExecutions::Phase,
                    MiddlewareExecutions::Status,
                    MiddlewareExecutions::StartedAtUnixNanos,
                    MiddlewareExecutions::EndedAtUnixNanos,
                    MiddlewareExecutions::PreviewJson,
                    MiddlewareExecutions::FailureSummary,
                ])
                .from(MiddlewareExecutions::Table)
                .and_where(
                    Expr::col(MiddlewareExecutions::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(MiddlewareExecutions::RunId).eq(run_id.to_string()))
                .and_where(Expr::col(MiddlewareExecutions::Id).eq(id))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("Middleware Event has no execution".into()))?;
    let started_at = row.started_at_unix_nanos;
    Ok(MiddlewareExecutionSummary {
        id: row.id,
        run: RunId::parse(&row.run_id)?,
        position: usize::try_from(row.position)
            .map_err(|_error| Error::CorruptData("Middleware position is invalid".into()))?,
        middleware_id: row.middleware_id,
        name: row.middleware_name,
        scope: MiddlewareScope::from_storage_str(&row.scope)?,
        target: row.target,
        phase: MiddlewarePhase::from_storage_str(&row.phase)?,
        status: MiddlewareExecutionStatus::from_storage_str(&row.status)?,
        started_at,
        ended_at: row.ended_at_unix_nanos,
        duration: std::cmp::Ord::max(
            row.ended_at_unix_nanos
                .unwrap_or(now)
                .saturating_sub(started_at),
            0,
        ),
        preview_json: row.preview_json,
        failure_summary: row.failure_summary,
    })
}
