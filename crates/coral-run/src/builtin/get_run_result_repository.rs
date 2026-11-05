//! The Task a Run belongs to, for `getRunResult`'s scoping. coral-task
//! records it beside the Run's acceptance, in the same commit, so a Run's
//! Program always finds its own Task here.

use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use crate::{Error, RunId};

#[derive(Iden)]
enum TaskInteractions {
    Table,
    WorkspaceId,
    TaskId,
    RunId,
}

/// The id of the Task whose `submit` accepted `run`; `None` for a Run no
/// Task accepted.
pub(super) async fn task_of_run(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Option<String>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(TaskInteractions::TaskId)
                .from(TaskInteractions::Table)
                .and_where(Expr::col(TaskInteractions::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskInteractions::RunId).eq(run.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(row.map(|(task,)| task))
}
