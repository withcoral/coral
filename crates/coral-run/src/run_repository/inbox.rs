//! Run Inbox rows (ADR 0025): the outside Run Requests that wait for their
//! Run's Run Owner, one row per decided thing.

use coral_db::{Database, Transaction};
use coral_workspace::{Principal, PrincipalId, TraceId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Order, Query};
use sqlx::FromRow;

use super::{ProgramRunRecords, principal_kind_from_storage};
use crate::approval::{ApprovalDecision, ApprovalId, ApprovalState};
use crate::owner::DecisionRequest;
use crate::run_inbox::{Delivery, InboxRequest, InboxRow, OutsideRequest};
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
    Decision,
    Comment,
    TraceId,
    DeadlineAtUnixNanos,
    InsertedAtUnixNanos,
}

/// Inserts `request` into its Run's Run Inbox, unless the Run is not
/// running, a cancellation's Run has its termination recorded (the record
/// answers), or a request for the same decided thing waits already: one row
/// per decided thing, and the first wins. Reads the Run in the statement
/// itself, holding nothing.
pub(crate) async fn insert(
    transaction: &mut Transaction<'_>,
    request: &InboxRequest,
    inserted_at_unix_nanos: i64,
) -> Result<(), Error> {
    let (requester, decision) = match &request.request {
        OutsideRequest::Cancel(requester) => (requester, None),
        OutsideRequest::Decide(decision) => (&decision.actor, Some(decision)),
    };
    let mut running = Query::select()
        .expr(Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()))
        .columns([ProgramRunRecords::WorkspaceId, ProgramRunRecords::Id])
        .expr(Expr::val(
            decision.map(|decision| decision.approval.to_string()),
        ))
        .expr(Expr::val(requester.id().as_str()))
        .expr(Expr::val(requester.kind().as_storage_str()))
        .expr(Expr::val(decision.map(|decision| {
            decision.decision.state().storage().to_owned()
        })))
        .expr(Expr::val(
            decision.and_then(|decision| decision.comment.clone()),
        ))
        .expr(Expr::val(
            decision.map(|decision| decision.trace.as_str().to_owned()),
        ))
        .expr(Expr::val(
            decision.map(|decision| decision.deadline_at_unix_nanos),
        ))
        .expr(Expr::val(inserted_at_unix_nanos))
        .from(ProgramRunRecords::Table)
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(request.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(request.run_id.as_str()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .to_owned();
    if decision.is_none() {
        running.and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null());
    }
    let insert = Query::insert()
        .into_table(ProgramRunInbox::Table)
        .columns([
            ProgramRunInbox::Id,
            ProgramRunInbox::WorkspaceId,
            ProgramRunInbox::RunId,
            ProgramRunInbox::ApprovalId,
            ProgramRunInbox::RequesterId,
            ProgramRunInbox::RequesterKind,
            ProgramRunInbox::Decision,
            ProgramRunInbox::Comment,
            ProgramRunInbox::TraceId,
            ProgramRunInbox::DeadlineAtUnixNanos,
            ProgramRunInbox::InsertedAtUnixNanos,
        ])
        .select_from(running)
        .map_err(|error| Error::CorruptData(error.to_string()))?
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .to_owned();
    transaction.execute_change(insert).await?;
    Ok(())
}

/// Whether `request` waits in its Run's Run Inbox: the row for what it
/// decides is there and the Run is still running.
pub(crate) async fn waits(database: &Database, request: &InboxRequest) -> Result<bool, Error> {
    let decided = match &request.request {
        OutsideRequest::Cancel(_) => Expr::col(ProgramRunInbox::ApprovalId).is_null(),
        OutsideRequest::Decide(decision) => {
            Expr::col(ProgramRunInbox::ApprovalId).eq(decision.approval.to_string())
        }
    };
    let waiting: Option<(String,)> = database
        .fetch_optional(
            waiting_rows(None)
                .and_where(
                    Expr::col(ProgramRunInbox::WorkspaceId).eq(request.workspace_id.to_string()),
                )
                .and_where(Expr::col(ProgramRunInbox::RunId).eq(request.run_id.as_str()))
                .and_where(decided)
                .to_owned(),
        )
        .await?;
    Ok(waiting.is_some())
}

/// One Run Inbox row as `held_by` reads it.
#[derive(FromRow)]
struct StoredRequest {
    id: String,
    workspace_id: String,
    run_id: String,
    approval_id: Option<String>,
    requester_id: String,
    requester_kind: String,
    decision: Option<String>,
    comment: Option<String>,
    trace_id: Option<String>,
    deadline_at_unix_nanos: Option<i64>,
}

/// The requests waiting for the Runs `instance` holds, in insert order.
pub(crate) async fn held_by(
    database: &Database,
    instance: &CoralInstanceId,
) -> Result<Vec<Delivery>, Error> {
    let rows: Vec<StoredRequest> = database
        .fetch_all(
            waiting_rows(Some(instance))
                .columns([
                    ProgramRunInbox::WorkspaceId,
                    ProgramRunInbox::RunId,
                    ProgramRunInbox::ApprovalId,
                    ProgramRunInbox::RequesterId,
                    ProgramRunInbox::RequesterKind,
                    ProgramRunInbox::Decision,
                    ProgramRunInbox::Comment,
                    ProgramRunInbox::TraceId,
                    ProgramRunInbox::DeadlineAtUnixNanos,
                ])
                .order_by(ProgramRunInbox::InsertedAtUnixNanos, Order::Asc)
                .order_by(ProgramRunInbox::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(delivery).collect()
}

fn delivery(
    StoredRequest {
        id,
        workspace_id,
        run_id,
        approval_id,
        requester_id,
        requester_kind,
        decision,
        comment,
        trace_id,
        deadline_at_unix_nanos: deadline,
    }: StoredRequest,
) -> Result<Delivery, Error> {
    let corrupt = |what: &str| Error::CorruptData(format!("Run Inbox row has invalid {what}"));
    let requester = Principal::from_parts(
        PrincipalId::parse(&requester_id).map_err(|_error| corrupt("requester"))?,
        principal_kind_from_storage(&requester_kind)?,
    );
    let request = match approval_id {
        None => OutsideRequest::Cancel(requester),
        Some(approval) => OutsideRequest::Decide(DecisionRequest {
            approval: ApprovalId::parse(&approval).map_err(|_error| corrupt("Approval Request"))?,
            deadline_at_unix_nanos: deadline.ok_or_else(|| corrupt("deadline"))?,
            decision: match decision.map(ApprovalState::try_from).transpose()? {
                Some(ApprovalState::Approved) => ApprovalDecision::Approve,
                Some(ApprovalState::Declined) => ApprovalDecision::Decline,
                _ => return Err(corrupt("decision")),
            },
            actor: requester,
            comment,
            trace: trace_id
                .as_deref()
                .and_then(|trace| TraceId::parse(trace).ok())
                .ok_or_else(|| corrupt("trace"))?,
        }),
    };
    Ok(Delivery {
        row: InboxRow(id),
        request: InboxRequest {
            workspace_id: WorkspaceId::parse(&workspace_id)
                .map_err(|_error| corrupt("Workspace"))?,
            run_id: RunId::parse(&run_id).map_err(|_error| corrupt("Run"))?,
            request,
        },
    })
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

/// The waiting rows: their Run is running, and held by
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
        .and_where(Expr::exists(run))
        .to_owned()
}
