//! Run Inbox rows (ADR 0025): the outside Run Requests that wait for their
//! Run's Run Owner, one row per decided thing.

use coral_db::{Database, Transaction};
use coral_workspace::{Principal, PrincipalId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Order, Query};

use super::{ProgramRunRecords, principal_kind_from_storage};
use crate::run_inbox::{Delivery, InboxRequest, InboxRow};
use crate::{CoralInstanceId, Error, RunId, RunState};

#[derive(Iden)]
enum ProgramRunInbox {
    Table,
    Id,
    WorkspaceId,
    RunId,
    ApprovalId,
    RequesterId,
    RequesterKind,
    InsertedAtUnixNanos,
}

/// Inserts `request` as a cancellation of its Run, unless the Run is not
/// running or its cancellation waits already: one row per decided thing,
/// and the first wins. Reads the Run in the statement itself, holding
/// nothing.
pub(crate) async fn insert_cancellation(
    database: &Database,
    request: &InboxRequest,
    inserted_at_unix_nanos: i64,
) -> Result<(), Error> {
    let running = Query::select()
        .expr(Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()))
        .columns([ProgramRunRecords::WorkspaceId, ProgramRunRecords::Id])
        .expr(Expr::val(request.requester.id().as_str()))
        .expr(Expr::val(request.requester.kind().as_storage_str()))
        .expr(Expr::val(inserted_at_unix_nanos))
        .from(ProgramRunRecords::Table)
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(request.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(request.run_id.as_str()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .to_owned();
    let insert = Query::insert()
        .into_table(ProgramRunInbox::Table)
        .columns([
            ProgramRunInbox::Id,
            ProgramRunInbox::WorkspaceId,
            ProgramRunInbox::RunId,
            ProgramRunInbox::RequesterId,
            ProgramRunInbox::RequesterKind,
            ProgramRunInbox::InsertedAtUnixNanos,
        ])
        .select_from(running)
        .map_err(|error| Error::CorruptData(error.to_string()))?
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .to_owned();
    let mut transaction = database.begin().await?;
    transaction.execute_change(insert).await?;
    transaction.commit().await?;
    Ok(())
}

/// Whether the cancellation of `run_id` waits in its Run Inbox: its row is
/// there and the Run is still running.
pub(crate) async fn cancellation_waits(
    database: &Database,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<bool, Error> {
    let waiting: Option<(String,)> = database
        .fetch_optional(
            waiting_rows(None)
                .and_where(Expr::col(ProgramRunInbox::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProgramRunInbox::RunId).eq(run_id.as_str()))
                .to_owned(),
        )
        .await?;
    Ok(waiting.is_some())
}

/// The cancellations waiting for the Runs `instance` holds, in insert order.
pub(crate) async fn held_by(
    database: &Database,
    instance: &CoralInstanceId,
) -> Result<Vec<Delivery>, Error> {
    let rows: Vec<(String, String, String, String, String)> = database
        .fetch_all(
            waiting_rows(Some(instance))
                .columns([
                    ProgramRunInbox::WorkspaceId,
                    ProgramRunInbox::RunId,
                    ProgramRunInbox::RequesterId,
                    ProgramRunInbox::RequesterKind,
                ])
                .order_by(ProgramRunInbox::InsertedAtUnixNanos, Order::Asc)
                .order_by(ProgramRunInbox::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, workspace_id, run_id, requester_id, requester_kind)| {
            let corrupt =
                |what: &str| Error::CorruptData(format!("Run Inbox row has invalid {what}"));
            Ok(Delivery {
                row: InboxRow(id),
                request: InboxRequest {
                    workspace_id: WorkspaceId::parse(&workspace_id)
                        .map_err(|_error| corrupt("Workspace"))?,
                    run_id: RunId::parse(&run_id).map_err(|_error| corrupt("Run"))?,
                    requester: Principal::from_parts(
                        PrincipalId::parse(&requester_id).map_err(|_error| corrupt("requester"))?,
                        principal_kind_from_storage(&requester_kind)?,
                    ),
                },
            })
        })
        .collect()
}

/// Deletes `rows` of the Run in the chunk commit that records them.
pub(crate) async fn delete(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    rows: &[InboxRow],
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::delete()
                .from_table(ProgramRunInbox::Table)
                .and_where(Expr::col(ProgramRunInbox::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProgramRunInbox::RunId).eq(run_id.as_str()))
                .and_where(
                    Expr::col(ProgramRunInbox::Id).is_in(rows.iter().map(|row| row.0.as_str())),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// The waiting cancellation rows: their Run is running, and held by
/// `holder` when one is named. A row of an ended Run waits for nothing and is
/// never read. The read starts from the small Run Inbox and looks each Run up
/// by its key.
fn waiting_rows(holder: Option<&CoralInstanceId>) -> sea_query::SelectStatement {
    let mut run = Query::select()
        .expr(Expr::val(1))
        .from(ProgramRunRecords::Table)
        .and_where(
            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::WorkspaceId))
                .equals((ProgramRunInbox::Table, ProgramRunInbox::WorkspaceId)),
        )
        .and_where(
            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::Id))
                .equals((ProgramRunInbox::Table, ProgramRunInbox::RunId)),
        )
        .and_where(
            Expr::col((ProgramRunRecords::Table, ProgramRunRecords::State))
                .eq(RunState::Running.as_storage_str()),
        )
        .to_owned();
    if let Some(holder) = holder {
        run.and_where(
            Expr::col((
                ProgramRunRecords::Table,
                ProgramRunRecords::HolderInstanceId,
            ))
            .eq(holder.as_str()),
        );
    }
    Query::select()
        .column(ProgramRunInbox::Id)
        .from(ProgramRunInbox::Table)
        .and_where(Expr::col(ProgramRunInbox::ApprovalId).is_null())
        .and_where(Expr::exists(run))
        .to_owned()
}
