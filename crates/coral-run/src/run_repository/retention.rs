//! Retention activity and deletion across Run-owned execution history.

use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Query};

use super::run_row::RunRow;
use super::{ProgramRunRecords, RUN_COLUMNS};
use crate::lifecycle::RunRecord;
use crate::{Error, RunId, RunState};

/// Holds a terminal Run's row for the rest of `transaction` (a no-op update
/// of its state, which an ended Run never changes again) and returns the Run
/// as held, in one round trip; `None` when no terminal Run has that identity.
/// A nonterminal Run's row is its Run Owner's alone: this matches nothing for
/// it and writes nothing.
///
/// The held Run carries no `last_event_sequence`: the statement reads no
/// Event rows.
pub(crate) async fn hold_terminal_run(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Option<RunRecord>, Error> {
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .value(
            ProgramRunRecords::State,
            Expr::col(ProgramRunRecords::State),
        )
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).ne(RunState::Running.as_storage_str()))
        .returning(Query::returning().columns(RUN_COLUMNS))
        .to_owned();
    let row: Option<RunRow> = transaction.fetch_optional_returning(statement).await?;
    row.map(RunRow::decode).transpose()
}

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
        ("tool_invocations", "dispatch_started_at_unix_nanos"),
        ("tool_invocations", "account_wait_started_at_unix_nanos"),
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
        "tool_invocations",
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
