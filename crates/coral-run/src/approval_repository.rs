//! Private approval persistence. Callers supply the transaction and own Run holds,
//! authorization, transition selection, lifecycle orchestration, and commit.
//! SQL rows are decoded here before reaching Approval.

use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, PrincipalKind, TraceId, WorkspaceId};
use sea_query::{
    Alias, Expr, ExprTrait as _, Iden, IntoColumnRef as _, JoinType, Order, Query, SimpleExpr,
};
use sqlx::FromRow;
use uuid::Uuid;

use crate::approval::{ApprovalId, ApprovalState};
use crate::{Error, InvocationId, RunId};

#[cfg(test)]
pub(super) async fn set_approval_deadline(
    database: &coral_db::Database,
    workspace: &WorkspaceId,
    approval: &ApprovalId,
    deadline_at_unix_nanos: i64,
) -> Result<(), Error> {
    database
        .execute(
            Query::update()
                .table(InvocationApprovals::Table)
                .value(
                    InvocationApprovals::DeadlineAtUnixNanos,
                    deadline_at_unix_nanos,
                )
                .and_where(Expr::col(InvocationApprovals::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(InvocationApprovals::Id).eq(approval.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

#[derive(Iden)]
enum InvocationApprovals {
    Table,
    Id,
    WorkspaceId,
    InvocationId,
    RunId,
    DeadlineAtUnixNanos,
    CreatedAtUnixNanos,
    State,
    DecidedAtUnixNanos,
    ApproverId,
    Comment,
}

#[derive(Iden)]
enum ApprovalLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    RequestId,
    ActorUserId,
    State,
    OccurredAtUnixNanos,
    TraceId,
}

pub(super) struct StoredApproval {
    pub id: ApprovalId,
    pub invocation_id: InvocationId,
    pub run_id: RunId,
    pub deadline_at_unix_nanos: i64,
    pub state: ApprovalState,
    pub decided_at_unix_nanos: Option<i64>,
    pub approver: Option<Principal>,
    pub comment: Option<String>,
}

pub(super) struct StoredLifecycleEvent {
    pub id: String,
    pub actor: Option<Principal>,
    pub state: ApprovalState,
    pub occurred_at_unix_nanos: i64,
    pub trace_id: TraceId,
}

#[derive(FromRow)]
struct ApprovalRow {
    id: String,
    invocation_id: String,
    run_id: String,
    deadline_at_unix_nanos: i64,
    #[sqlx(try_from = "String")]
    state: ApprovalState,
    decided_at_unix_nanos: Option<i64>,
    approver_id: Option<String>,
    comment: Option<String>,
}

impl TryFrom<ApprovalRow> for StoredApproval {
    type Error = Error;

    fn try_from(row: ApprovalRow) -> Result<Self, Error> {
        Ok(Self {
            id: ApprovalId::parse(&row.id)?,
            invocation_id: InvocationId::parse(&row.invocation_id)?,
            run_id: RunId::parse(&row.run_id)?,
            deadline_at_unix_nanos: row.deadline_at_unix_nanos,
            state: row.state,
            decided_at_unix_nanos: row.decided_at_unix_nanos,
            approver: row.approver_id.as_deref().map(user_principal).transpose()?,
            comment: row.comment,
        })
    }
}

#[derive(FromRow)]
struct EventRow {
    id: String,
    actor_user_id: Option<String>,
    #[sqlx(try_from = "String")]
    state: ApprovalState,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl TryFrom<EventRow> for StoredLifecycleEvent {
    type Error = Error;

    fn try_from(row: EventRow) -> Result<Self, Error> {
        Ok(Self {
            id: row.id,
            actor: row
                .actor_user_id
                .as_deref()
                .map(user_principal)
                .transpose()?,
            state: row.state,
            occurred_at_unix_nanos: row.occurred_at_unix_nanos,
            trace_id: TraceId::parse(&row.trace_id)
                .map_err(|_error| Error::CorruptData("invalid approval trace".into()))?,
        })
    }
}

fn user_principal(id: &str) -> Result<Principal, Error> {
    Ok(Principal::from_parts(
        PrincipalId::parse(id)?,
        PrincipalKind::User,
    ))
}

fn select(workspace: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .columns([
            InvocationApprovals::Id,
            InvocationApprovals::InvocationId,
            InvocationApprovals::RunId,
            InvocationApprovals::DeadlineAtUnixNanos,
            InvocationApprovals::State,
            InvocationApprovals::DecidedAtUnixNanos,
            InvocationApprovals::ApproverId,
            InvocationApprovals::Comment,
        ])
        .from(InvocationApprovals::Table)
        .and_where(Expr::col(InvocationApprovals::WorkspaceId).eq(workspace.to_string()))
        .to_owned()
}

pub(super) async fn list_for_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Vec<StoredApproval>, Error> {
    let rows: Vec<ApprovalRow> = tx
        .fetch_all(
            select(workspace)
                .and_where(Expr::col(InvocationApprovals::RunId).eq(run.to_string()))
                .order_by(InvocationApprovals::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(StoredApproval::try_from).collect()
}

pub(super) async fn load(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &ApprovalId,
) -> Result<StoredApproval, Error> {
    let row: ApprovalRow = tx
        .fetch_optional(
            select(workspace)
                .and_where(Expr::col(InvocationApprovals::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::ApprovalNotFound)?;
    row.try_into()
}

pub(super) async fn find_for_invocation(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    invocation: &InvocationId,
) -> Result<Option<StoredApproval>, Error> {
    let row: Option<ApprovalRow> = tx
        .fetch_optional(
            select(workspace)
                .and_where(Expr::col(InvocationApprovals::InvocationId).eq(invocation.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(StoredApproval::try_from).transpose()
}

pub(super) async fn find_for_run_invocation(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    invocation: &InvocationId,
) -> Result<Option<StoredApproval>, Error> {
    let row: Option<ApprovalRow> = tx
        .fetch_optional(
            select(workspace)
                .and_where(Expr::col(InvocationApprovals::InvocationId).eq(invocation.to_string()))
                .and_where(Expr::col(InvocationApprovals::RunId).eq(run.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(StoredApproval::try_from).transpose()
}

/// Complete pending-approval match for repository queries using `a` (approval)
/// and `i` (Invocation). Queue existence, card pages and Run Updates share it.
/// A request stays pending until its Run Owner records its expiry: readers
/// show the recorded state and its deadline, never a state derived from the
/// clock.
pub(super) fn pending_invocation() -> SimpleExpr {
    use coral_db::col;
    let column = |field: &str| (Alias::new("i"), Alias::new(field)).into_column_ref();
    col("a", "workspace_id")
        .eq(col("i", "workspace_id"))
        .and(col("a", "invocation_id").eq(col("i", "id")))
        .and(col("a", "run_id").eq(col("i", "run_id")))
        .and(col("i", "state").eq("pending"))
        .and(col("a", "state").eq(ApprovalState::Pending.storage()))
        .and(coral_policy::history_repository::approval_required(
            column("workspace_id"),
            column("provider_id"),
            column("provider_generation_id"),
            column("tool_key"),
        ))
}

// Agents' summaries must not load arguments, Account metadata, or comments.
// This read joins the same snapshot as Run visibility and the other human actions.
pub(super) async fn pending_for_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Vec<(ApprovalId, i64)>, Error> {
    let rows: Vec<(String, i64)> = tx
        .fetch_all(
            Query::select()
                .columns([
                    (Alias::new("a"), Alias::new("id")),
                    (Alias::new("a"), Alias::new("deadline_at_unix_nanos")),
                ])
                .from_as(InvocationApprovals::Table, Alias::new("a"))
                .join_as(
                    JoinType::InnerJoin,
                    Alias::new("tool_invocations"),
                    Alias::new("i"),
                    pending_invocation(),
                )
                .and_where(
                    Expr::col((Alias::new("a"), Alias::new("workspace_id")))
                        .eq(workspace.to_string()),
                )
                .and_where(Expr::col((Alias::new("a"), Alias::new("run_id"))).eq(run.to_string()))
                .order_by((Alias::new("a"), Alias::new("id")), Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, deadline)| Ok((ApprovalId::parse(&id)?, deadline)))
        .collect()
}

/// The Run's undecided Approval Requests whose deadline is at or before
/// `by_unix_nanos`, with their deadlines, in deadline order.
pub(super) async fn undecided_by(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    by_unix_nanos: i64,
) -> Result<Vec<(ApprovalId, i64)>, Error> {
    let rows: Vec<(String, i64)> = tx
        .fetch_all(
            Query::select()
                .columns([
                    InvocationApprovals::Id,
                    InvocationApprovals::DeadlineAtUnixNanos,
                ])
                .from(InvocationApprovals::Table)
                .and_where(Expr::col(InvocationApprovals::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(InvocationApprovals::RunId).eq(run.to_string()))
                .and_where(
                    Expr::col(InvocationApprovals::State).eq(ApprovalState::Pending.storage()),
                )
                .and_where(Expr::col(InvocationApprovals::DeadlineAtUnixNanos).lte(by_unix_nanos))
                .order_by(InvocationApprovals::DeadlineAtUnixNanos, Order::Asc)
                .order_by(InvocationApprovals::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, deadline)| Ok((ApprovalId::parse(&id)?, deadline)))
        .collect()
}

pub(super) async fn insert_pending(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &ApprovalId,
    invocation: &InvocationId,
    run: &RunId,
    deadline: i64,
    now: i64,
) -> Result<(), Error> {
    tx.execute_change(
        Query::insert()
            .into_table(InvocationApprovals::Table)
            .columns([
                InvocationApprovals::Id,
                InvocationApprovals::WorkspaceId,
                InvocationApprovals::InvocationId,
                InvocationApprovals::RunId,
                InvocationApprovals::DeadlineAtUnixNanos,
                InvocationApprovals::CreatedAtUnixNanos,
                InvocationApprovals::State,
            ])
            .values_panic([
                Expr::val(id.to_string()),
                Expr::val(workspace.to_string()),
                Expr::val(invocation.to_string()),
                Expr::val(run.to_string()),
                Expr::val(deadline),
                Expr::val(now),
                Expr::val(ApprovalState::Pending.storage()),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn update_pending(
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
                .table(InvocationApprovals::Table)
                .values([
                    (InvocationApprovals::State, Expr::val(state.storage())),
                    (InvocationApprovals::DecidedAtUnixNanos, Expr::val(now)),
                    (
                        InvocationApprovals::ApproverId,
                        Expr::val(human.map(|(actor, _)| actor.id().as_str())),
                    ),
                    (
                        InvocationApprovals::Comment,
                        Expr::val(human.and_then(|(_, comment)| comment)),
                    ),
                ])
                .and_where(Expr::col(InvocationApprovals::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(InvocationApprovals::Id).eq(id.to_string()))
                .and_where(
                    Expr::col(InvocationApprovals::State).eq(ApprovalState::Pending.storage()),
                )
                .to_owned(),
        )
        .await?;
    Ok(changed == 1)
}

pub(super) async fn insert_lifecycle_event(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &ApprovalId,
    state: ApprovalState,
    now: i64,
    actor: Option<&Principal>,
    trace: &TraceId,
) -> Result<(), Error> {
    tx.execute(
        Query::insert()
            .into_table(ApprovalLifecycleEvents::Table)
            .columns([
                ApprovalLifecycleEvents::Id,
                ApprovalLifecycleEvents::WorkspaceId,
                ApprovalLifecycleEvents::RequestId,
                ApprovalLifecycleEvents::ActorUserId,
                ApprovalLifecycleEvents::State,
                ApprovalLifecycleEvents::OccurredAtUnixNanos,
                ApprovalLifecycleEvents::TraceId,
            ])
            .values_panic([
                Expr::val(Uuid::new_v4().to_string()),
                Expr::val(workspace.to_string()),
                Expr::val(id.to_string()),
                Expr::val(actor.map(|actor| actor.id().as_str())),
                Expr::val(state.storage()),
                Expr::val(now),
                Expr::val(trace.as_str()),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn list_lifecycle_events(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &ApprovalId,
) -> Result<Vec<StoredLifecycleEvent>, Error> {
    let rows: Vec<EventRow> = tx
        .fetch_all(
            Query::select()
                .columns([
                    ApprovalLifecycleEvents::Id,
                    ApprovalLifecycleEvents::ActorUserId,
                    ApprovalLifecycleEvents::State,
                    ApprovalLifecycleEvents::OccurredAtUnixNanos,
                    ApprovalLifecycleEvents::TraceId,
                ])
                .from(ApprovalLifecycleEvents::Table)
                .and_where(
                    Expr::col(ApprovalLifecycleEvents::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(ApprovalLifecycleEvents::RequestId).eq(id.to_string()))
                .order_by(ApprovalLifecycleEvents::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(StoredLifecycleEvent::try_from)
        .collect()
}
