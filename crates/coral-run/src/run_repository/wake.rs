//! Wake rows (ADR 0027): one per Run at most, keyed by the Run, with no
//! payload. A writer other than the Run's own loop raises it in the
//! transaction of its commit on the running Run; the loop deletes it in a
//! chunk commit, and a reopen of the Run's stream deletes it.

use coral_db::{Database, Transaction, UnixNanos};
use coral_workspace::WorkspaceId;
use sea_query::{Condition, Expr, ExprTrait as _, Iden, OnConflict, Query};

use super::ProgramRunRecords;
use super::run_row::UndecodableRun;
use crate::types::HeldRunId;
use crate::{CoralInstanceId, Error, RunId, RunState};

#[derive(Iden)]
enum ProgramRunWakes {
    Table,
    WorkspaceId,
    RunId,
    RaisedAtUnixNanos,
}

/// Raises the Run's wake row at `raised_at_unix_nanos`, unless one is up
/// already: the first raise stands. It rings the change signal at commit.
pub(crate) async fn raise_wake(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    raised_at_unix_nanos: UnixNanos,
) -> Result<(), Error> {
    let insert = Query::insert()
        .into_table(ProgramRunWakes::Table)
        .columns([
            ProgramRunWakes::WorkspaceId,
            ProgramRunWakes::RunId,
            ProgramRunWakes::RaisedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(workspace_id.to_string()),
            Expr::val(run_id.as_str()),
            Expr::val(raised_at_unix_nanos),
        ])
        .on_conflict(
            OnConflict::columns([ProgramRunWakes::WorkspaceId, ProgramRunWakes::RunId])
                .do_nothing()
                .to_owned(),
        )
        .to_owned();
    transaction.execute_change(insert).await?;
    Ok(())
}

/// Deletes the Run's wake row in `transaction`: a hint's removal wakes no
/// observer.
pub(crate) async fn clear_wake(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::delete()
                .from_table(ProgramRunWakes::Table)
                .and_where(Expr::col(ProgramRunWakes::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProgramRunWakes::RunId).eq(run_id.as_str()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// One wake row of a Run whose stream an instance holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldWake {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) run_id: RunId,
    pub(crate) raised_at_unix_nanos: UnixNanos,
    /// The Run is terminal.
    pub(crate) ended: bool,
}

/// The wake rows of the Runs whose row names `instance` as their stream's
/// holder, each decoded on its own: one that does not decode names its Run
/// when its stored id does. The read starts from the small wake table and
/// looks each Run up by its key.
pub(crate) async fn wakes_held_by(
    database: &Database,
    instance: &CoralInstanceId,
) -> Result<Vec<Result<HeldWake, UndecodableRun>>, Error> {
    let rows: Vec<(String, String, UnixNanos, String)> = database
        .fetch_all(
            Query::select()
                .column((ProgramRunWakes::Table, ProgramRunWakes::WorkspaceId))
                .column((ProgramRunWakes::Table, ProgramRunWakes::RunId))
                .column((ProgramRunWakes::Table, ProgramRunWakes::RaisedAtUnixNanos))
                .column((ProgramRunRecords::Table, ProgramRunRecords::State))
                .from(ProgramRunWakes::Table)
                .inner_join(
                    ProgramRunRecords::Table,
                    Condition::all()
                        .add(
                            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::WorkspaceId))
                                .equals((ProgramRunWakes::Table, ProgramRunWakes::WorkspaceId)),
                        )
                        .add(
                            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::Id))
                                .equals((ProgramRunWakes::Table, ProgramRunWakes::RunId)),
                        ),
                )
                .and_where(
                    Expr::col((
                        ProgramRunRecords::Table,
                        ProgramRunRecords::HolderInstanceId,
                    ))
                    .eq(instance.as_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(workspace_id, run_id, raised_at_unix_nanos, state)| {
            let parsed = RunId::parse(&run_id).ok();
            let undecodable = |error: Error| UndecodableRun {
                id: parsed.clone().map(HeldRunId::of_held_row),
                error: Box::new(error),
            };
            let invalid =
                || undecodable(Error::CorruptData("wake row has invalid identity".into()));
            Ok(HeldWake {
                workspace_id: WorkspaceId::parse(&workspace_id).map_err(|_error| invalid())?,
                run_id: parsed.clone().ok_or_else(invalid)?,
                raised_at_unix_nanos,
                ended: RunState::from_storage_str(&state)
                    .map_err(undecodable)?
                    .is_terminal(),
            })
        })
        .collect())
}

/// Deletes the wake rows of `wakes` whose Run is terminal: no write follows
/// a Run's end, so its hint is spent once handed.
pub(crate) async fn clear_ended_wakes(
    database: &Database,
    wakes: &[HeldWake],
) -> Result<(), Error> {
    if wakes.is_empty() {
        return Ok(());
    }
    let runs = wakes.iter().fold(Condition::any(), |runs, wake| {
        runs.add(
            Condition::all()
                .add(Expr::col(ProgramRunWakes::WorkspaceId).eq(wake.workspace_id.to_string()))
                .add(Expr::col(ProgramRunWakes::RunId).eq(wake.run_id.as_str())),
        )
    });
    let ended = Query::select()
        .expr(Expr::val(1))
        .from(ProgramRunRecords::Table)
        .and_where(
            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::WorkspaceId))
                .equals((ProgramRunWakes::Table, ProgramRunWakes::WorkspaceId)),
        )
        .and_where(
            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::Id))
                .equals((ProgramRunWakes::Table, ProgramRunWakes::RunId)),
        )
        .and_where(
            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::State))
                .ne(RunState::Running.as_storage_str()),
        )
        .to_owned();
    database
        .execute(
            Query::delete()
                .from_table(ProgramRunWakes::Table)
                .cond_where(runs)
                .and_where(Expr::exists(ended))
                .to_owned(),
        )
        .await?;
    Ok(())
}
