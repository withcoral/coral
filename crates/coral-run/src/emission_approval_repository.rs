//! Private persistence for Emission Approval Requests. Callers supply the
//! transaction and own Run holds, authorization, transition selection and
//! commit. Rows are decoded here before reaching the domain.

use coral_db::{Transaction, col};
use coral_workspace::{Principal, PrincipalId, PrincipalKind, WorkspaceId};
use sea_query::{Alias, Expr, ExprTrait as _, Iden, IntoIden as _, Order, Query, SimpleExpr};
use sqlx::FromRow;

use crate::approval::{ApprovalId, ApprovalState};
use crate::source_location_repository::{self, StoredSourceLocation};
use crate::{Error, RunId, SourceLocation};

#[derive(Iden)]
enum EmissionApprovals {
    Table,
    Id,
    WorkspaceId,
    RunId,
    PayloadJson,
    DeadlineAtUnixNanos,
    CreatedAtUnixNanos,
    State,
    DecidedAtUnixNanos,
    ApproverId,
    Comment,
}

/// The Call Path every Emission Approval Request shows.
pub(crate) const EMIT_CALL_PATH: &str = "coral.emit";

pub(crate) struct StoredEmissionApproval {
    pub id: ApprovalId,
    pub run_id: RunId,
    /// The exact final payload bytes the request binds.
    pub payload_json: String,
    pub source_location: Option<SourceLocation>,
    pub deadline_at_unix_nanos: i64,
    pub state: ApprovalState,
    pub decided_at_unix_nanos: Option<i64>,
    pub approver: Option<Principal>,
    pub comment: Option<String>,
}

#[derive(FromRow)]
struct Row {
    id: String,
    run_id: String,
    payload_json: String,
    deadline_at_unix_nanos: i64,
    #[sqlx(try_from = "String")]
    state: ApprovalState,
    decided_at_unix_nanos: Option<i64>,
    approver_id: Option<String>,
    comment: Option<String>,
    #[sqlx(flatten)]
    source_location: StoredSourceLocation,
}

impl TryFrom<Row> for StoredEmissionApproval {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Error> {
        Ok(Self {
            id: ApprovalId::parse(&row.id)?,
            run_id: RunId::parse(&row.run_id)?,
            payload_json: row.payload_json,
            source_location: row.source_location.decode()?,
            deadline_at_unix_nanos: row.deadline_at_unix_nanos,
            state: row.state,
            decided_at_unix_nanos: row.decided_at_unix_nanos,
            approver: row
                .approver_id
                .as_deref()
                .map(|id| {
                    Ok::<_, Error>(Principal::from_parts(
                        PrincipalId::parse(id)?,
                        PrincipalKind::User,
                    ))
                })
                .transpose()?,
            comment: row.comment,
        })
    }
}

fn select(workspace: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .columns([
            EmissionApprovals::Id,
            EmissionApprovals::RunId,
            EmissionApprovals::PayloadJson,
            EmissionApprovals::DeadlineAtUnixNanos,
            EmissionApprovals::State,
            EmissionApprovals::DecidedAtUnixNanos,
            EmissionApprovals::ApproverId,
            EmissionApprovals::Comment,
        ])
        .columns(source_location_repository::COLUMNS.map(Alias::new))
        .from(EmissionApprovals::Table)
        .and_where(Expr::col(EmissionApprovals::WorkspaceId).eq(workspace.to_string()))
        .to_owned()
}

/// The request with this identity in `workspace`, if it is an Emission request.
pub(crate) async fn find(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &ApprovalId,
) -> Result<Option<StoredEmissionApproval>, Error> {
    let row: Option<Row> = tx
        .fetch_optional(
            select(workspace)
                .and_where(Expr::col(EmissionApprovals::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(StoredEmissionApproval::try_from).transpose()
}

/// The request with this identity in exactly this Run, for the waiting host.
pub(crate) async fn find_for_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    id: &ApprovalId,
) -> Result<Option<StoredEmissionApproval>, Error> {
    let row: Option<Row> = tx
        .fetch_optional(
            select(workspace)
                .and_where(Expr::col(EmissionApprovals::RunId).eq(run.to_string()))
                .and_where(Expr::col(EmissionApprovals::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(StoredEmissionApproval::try_from).transpose()
}

pub(crate) async fn list_for_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Vec<StoredEmissionApproval>, Error> {
    let rows: Vec<Row> = tx
        .fetch_all(
            select(workspace)
                .and_where(Expr::col(EmissionApprovals::RunId).eq(run.to_string()))
                .order_by(EmissionApprovals::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(StoredEmissionApproval::try_from)
        .collect()
}

pub(crate) struct NewEmissionApproval<'a> {
    pub id: &'a ApprovalId,
    pub run: &'a RunId,
    pub payload_json: &'a str,
    pub source_location: Option<&'a SourceLocation>,
    pub deadline_at_unix_nanos: i64,
}

pub(crate) async fn insert_pending(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    request: &NewEmissionApproval<'_>,
    now: i64,
) -> Result<(), Error> {
    let (columns, values): (Vec<_>, Vec<_>) = [
        (EmissionApprovals::Id, Expr::val(request.id.to_string())),
        (
            EmissionApprovals::WorkspaceId,
            Expr::val(workspace.to_string()),
        ),
        (EmissionApprovals::RunId, Expr::val(request.run.to_string())),
        (
            EmissionApprovals::PayloadJson,
            Expr::val(request.payload_json),
        ),
        (
            EmissionApprovals::DeadlineAtUnixNanos,
            Expr::val(request.deadline_at_unix_nanos),
        ),
        (EmissionApprovals::CreatedAtUnixNanos, Expr::val(now)),
        (
            EmissionApprovals::State,
            Expr::val(ApprovalState::Pending.storage()),
        ),
    ]
    .into_iter()
    .map(|(column, value)| (column.into_iden(), value))
    .chain(
        source_location_repository::values(request.source_location)
            .map(|(column, value)| (column.into_iden(), value)),
    )
    .unzip();
    tx.execute_change(
        Query::insert()
            .into_table(EmissionApprovals::Table)
            .columns(columns)
            .values_panic(values)
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Moves a pending request to `state`. False when it was no longer pending.
pub(crate) async fn update_pending(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &ApprovalId,
    state: ApprovalState,
    now: i64,
    human: Option<(&Principal, Option<&str>)>,
) -> Result<bool, Error> {
    let changed = tx
        .execute_change(
            Query::update()
                .table(EmissionApprovals::Table)
                .values([
                    (EmissionApprovals::State, Expr::val(state.storage())),
                    (EmissionApprovals::DecidedAtUnixNanos, Expr::val(now)),
                    (
                        EmissionApprovals::ApproverId,
                        Expr::val(human.map(|(actor, _)| actor.id().as_str())),
                    ),
                    (
                        EmissionApprovals::Comment,
                        Expr::val(human.and_then(|(_, comment)| comment)),
                    ),
                ])
                .and_where(Expr::col(EmissionApprovals::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(EmissionApprovals::Id).eq(id.to_string()))
                .and_where(Expr::col(EmissionApprovals::State).eq(ApprovalState::Pending.storage()))
                .to_owned(),
        )
        .await?;
    Ok(changed == 1)
}

/// Identities of this Run's pending requests, whatever their deadline.
pub(crate) async fn pending_ids_for_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Vec<ApprovalId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(EmissionApprovals::Id)
                .from(EmissionApprovals::Table)
                .and_where(Expr::col(EmissionApprovals::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(EmissionApprovals::RunId).eq(run.to_string()))
                .and_where(Expr::col(EmissionApprovals::State).eq(ApprovalState::Pending.storage()))
                .order_by(EmissionApprovals::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| ApprovalId::parse(&id))
        .collect()
}

/// A current pending request for the Run aliased `r`, for required-action and
/// history projections that join this table as `e`.
pub(crate) fn pending_for_run_alias(now: i64) -> SimpleExpr {
    col("e", "workspace_id")
        .eq(col("r", "workspace_id"))
        .and(col("e", "run_id").eq(col("r", "id")))
        .and(col("e", "state").eq(ApprovalState::Pending.storage()))
        .and(col("e", "deadline_at_unix_nanos").gt(now))
}

/// Whether the Run aliased `r` has a current pending Emission request.
pub(crate) fn run_has_pending(now: i64) -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(EmissionApprovals::Table, Alias::new("e"))
            .and_where(pending_for_run_alias(now))
            .to_owned(),
    )
}

/// This Run's current pending requests with their deadlines, summary only.
pub(crate) async fn pending_for_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    now: i64,
) -> Result<Vec<(ApprovalId, i64)>, Error> {
    let rows: Vec<(String, i64)> = tx
        .fetch_all(
            Query::select()
                .columns([
                    EmissionApprovals::Id,
                    EmissionApprovals::DeadlineAtUnixNanos,
                ])
                .from(EmissionApprovals::Table)
                .and_where(Expr::col(EmissionApprovals::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(EmissionApprovals::RunId).eq(run.to_string()))
                .and_where(Expr::col(EmissionApprovals::State).eq(ApprovalState::Pending.storage()))
                .and_where(Expr::col(EmissionApprovals::DeadlineAtUnixNanos).gt(now))
                .order_by(EmissionApprovals::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, deadline)| Ok((ApprovalId::parse(&id)?, deadline)))
        .collect()
}
