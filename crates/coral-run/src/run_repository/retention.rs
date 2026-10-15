//! Retention activity and deletion across Run-owned execution history.

use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Query};

use super::ProgramRunRecords;
use crate::{Error, RunId};

pub(crate) async fn retention_activity(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<i64, Error> {
    use sea_query::Alias;
    let latest = |table: &str, column: &str| {
        Query::select()
            .expr_as(Expr::col(Alias::new(column)).max(), Alias::new("at"))
            .from(Alias::new(table))
            .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
            .and_where(Expr::col(Alias::new("run_id")).eq(run.to_string()))
            .to_owned()
    };
    let mut sources = latest("program_run_events", "occurred_at_unix_nanos");
    for (table, column) in [
        ("invocation_approvals", "created_at_unix_nanos"),
        ("invocation_approvals", "decided_at_unix_nanos"),
        ("operation_invocations", "dispatch_started_at_unix_nanos"),
        (
            "operation_invocations",
            "account_wait_started_at_unix_nanos",
        ),
    ] {
        sources.union(sea_query::UnionType::All, latest(table, column));
    }
    sources.union(
        sea_query::UnionType::All,
        crate::invocation_repository::connection_activity(workspace, run),
    );
    let row: Option<(Option<i64>,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::col(Alias::new("at")).max())
                .from_subquery(sources, Alias::new("activity"))
                .to_owned(),
        )
        .await?;
    Ok(row.and_then(|row| row.0).unwrap_or(i64::MIN))
}

pub(crate) async fn delete_retained_history(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<(), Error> {
    // These tables are all execution-dependent and Run-owned. Account connection requests
    // and administrative events deliberately have no place in this deletion set.
    for table in [
        "program_run_events",
        "invocation_approvals",
        "operation_invocations",
        "middleware_executions",
        "program_run_middleware_snapshots",
        "program_run_operation_access_rules",
        "program_run_saved_function_snapshots",
        "program_run_packages",
    ] {
        transaction
            .execute(
                Query::delete()
                    .from_table(sea_query::Alias::new(table))
                    .and_where(
                        Expr::col(sea_query::Alias::new("workspace_id")).eq(workspace.to_string()),
                    )
                    .and_where(Expr::col(sea_query::Alias::new("run_id")).eq(run.to_string()))
                    .to_owned(),
            )
            .await?;
    }
    transaction
        .execute_change(
            Query::delete()
                .from_table(ProgramRunRecords::Table)
                .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProgramRunRecords::Id).eq(run.to_string()))
                .and_where(Expr::col(ProgramRunRecords::State).ne("running"))
                .to_owned(),
        )
        .await?;
    Ok(())
}
