//! Run-owned source-free projections. Only repositories compose the position facet.
use crate::{Error, InvocationStatus, InvocationSummary, RunSummary};
use coral_db::{Transaction, col};
use coral_workspace::{HistoryAuthority, WorkspaceId};
use sea_query::{
    Alias, Expr, ExprTrait as _, Func, IntoColumnRef as _, JoinType, Query, SelectStatement,
    SimpleExpr,
};

/// Authorized source-free Run positions for upper-owner grouping in the same snapshot.
#[must_use]
pub fn run_positions(authority: &HistoryAuthority) -> SelectStatement {
    let mut select = Query::select();
    select
        .from_as(Alias::new("program_run_records"), Alias::new("r"))
        .expr_as(col("r", "workspace_id"), Alias::new("workspace_id"))
        .expr_as(col("r", "id"), Alias::new("id"))
        .expr_as(col("r", "accepted_at_unix_nanos"), Alias::new("sort_key"));
    restrict(&mut select, authority);
    select
}
/// One Run's context measurement, for each row of a Task-owned aggregate query.
pub struct ContextMeasurement {
    /// The Run's measured bytes. Null while it runs, when it is unmeasurable, or
    /// when the authority cannot see it.
    pub bytes: SimpleExpr,
    /// Whether the Run is running. Null when the authority cannot see it.
    pub running: SimpleExpr,
}

/// Measures the Run that `workspace` and `run` name in the enclosing row. Each
/// value finds the Run by its primary key alone, then matches its Workspace and
/// `authority` in the selected expression, so a Run in another Workspace reads as
/// absent. A Workspace or principal filter would let a planner without fresh
/// statistics pick a principal index and scan the Workspace's Runs for each row.
/// With none, a query measures only the Runs its rows name.
#[must_use]
pub fn context_measurement(
    authority: &HistoryAuthority,
    workspace: SimpleExpr,
    run: SimpleExpr,
) -> ContextMeasurement {
    let column = |field: &str| (Alias::new("r"), Alias::new(field)).into_column_ref();
    let authorized = col("r", "workspace_id").eq(workspace).and(
        coral_workspace::history_repository::authorizes(
            authority,
            column("workspace_id"),
            column("principal_id"),
            column("principal_kind"),
        ),
    );
    let key = col("r", "id").eq(run);
    let value = |expr: SimpleExpr| {
        let select = Query::select()
            .expr(Expr::case(authorized.clone(), expr))
            .from_as(Alias::new("program_run_records"), Alias::new("r"))
            .and_where(key.clone())
            .to_owned();
        Expr::SubQuery(
            None,
            Box::new(sea_query::SubQueryStatement::SelectStatement(select)),
        )
    };
    ContextMeasurement {
        bytes: value(measured_context()),
        running: value(running()),
    }
}

/// Restricts Runs aliased `r` to `authority`, resolved in the same snapshot.
pub(crate) fn restrict(select: &mut SelectStatement, authority: &HistoryAuthority) {
    coral_workspace::history_repository::restrict(
        select,
        authority,
        (Alias::new("r"), Alias::new("workspace_id")).into_column_ref(),
        (Alias::new("r"), Alias::new("principal_id")).into_column_ref(),
        (Alias::new("r"), Alias::new("principal_kind")).into_column_ref(),
    );
}
fn current_approval() -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(Alias::new("invocation_approvals"), Alias::new("a"))
            .and_where(crate::approval_repository::pending_invocation())
            .to_owned(),
    )
}
/// The Invocation `i` is pending, as the literal that migration 0059's index
/// states. Given a bound state, `SQLite` would prepare the statement again on
/// every execution to compare the value with that index's predicate.
pub(crate) fn invocation_pending() -> SimpleExpr {
    col("i", "state").eq(Expr::cust("'pending'"))
}
fn invocation_action() -> SimpleExpr {
    let connection = coral_provider::account::history_repository::has_pending_connection(
        (Alias::new("i"), Alias::new("workspace_id")).into_column_ref(),
        (Alias::new("r"), Alias::new("principal_id")).into_column_ref(),
        (Alias::new("r"), Alias::new("principal_kind")).into_column_ref(),
        (Alias::new("i"), Alias::new("account_connection_request_id")).into_column_ref(),
    );
    invocation_pending().and(current_approval().or(connection))
}
fn invoking_authority(now: i64) -> SimpleExpr {
    let column = |field: &str| (Alias::new("r"), Alias::new(field)).into_column_ref();
    coral_workspace::history_repository::invoker_active(
        column("workspace_id"),
        column("principal_id"),
        column("principal_kind"),
        now,
    )
}
pub(super) async fn invoking_authority_current(
    tx: &mut Transaction<'_>,
    run: &crate::lifecycle::RunRecord,
    now: i64,
) -> Result<bool, Error> {
    let row: Option<(bool,)> = tx
        .fetch_optional(
            Query::select()
                .expr(invoking_authority(now))
                .from_as(Alias::new("program_run_records"), Alias::new("r"))
                .and_where(col("r", "workspace_id").eq(run.workspace_id.to_string()))
                .and_where(col("r", "id").eq(run.id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(row.is_some_and(|row| row.0))
}
pub(crate) fn running() -> SimpleExpr {
    col("r", "state").eq(crate::RunState::Running.as_storage_str())
}
/// A Run that still takes human actions, as recorded: running with no
/// termination intent (its deadline ends it only once the owner records the
/// timeout), under an invoking authority current at `now`.
pub(super) fn active_run(now: i64) -> SimpleExpr {
    running()
        .and(col("r", "termination_kind").is_null())
        .and(invoking_authority(now))
}
fn duration(table: &str, start: &str, end: &str, now: i64) -> SimpleExpr {
    Expr::expr(Func::coalesce([col(table, end), Expr::val(now)])).sub(col(table, start))
}
pub(crate) fn measured_context() -> SimpleExpr {
    Expr::case(
        running().or(incomplete_context(col("r", "workspace_id"), col("r", "id"))),
        Expr::val(Option::<i64>::None),
    )
    .finally(context_impact(&col("r", "workspace_id"), &col("r", "id")))
    .into()
}

fn context_impact(workspace: &SimpleExpr, run: &SimpleExpr) -> SimpleExpr {
    retained_bytes("program_run_events", "output_bytes", workspace, run, None).sub(retained_bytes(
        "tool_invocations",
        "result_bytes",
        workspace,
        run,
        None,
    ))
}

/// The sum of one Run's retained payload bytes in `table`, zero when it has none. `only`
/// narrows the rows summed.
pub(crate) fn retained_bytes(
    table: &str,
    field: &str,
    workspace: &SimpleExpr,
    run: &SimpleExpr,
    only: Option<SimpleExpr>,
) -> SimpleExpr {
    let mut query = Query::select();
    query
        .expr(Func::cast_as(
            Func::coalesce([Expr::col(Alias::new(field)).sum(), Expr::val(0_i64)]),
            Alias::new("bigint"),
        ))
        .from(Alias::new(table))
        .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.clone()))
        .and_where(Expr::col(Alias::new("run_id")).eq(run.clone()));
    if let Some(only) = only {
        query.and_where(only);
    }
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(query)),
    )
}

/// Whether retained Invocation evidence is incomplete. No payloads leave the repository.
fn incomplete_context(workspace: SimpleExpr, run: SimpleExpr) -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("tool_invocations"),
                Alias::new("context_invocation"),
            )
            .and_where(col("context_invocation", "workspace_id").eq(workspace))
            .and_where(col("context_invocation", "run_id").eq(run))
            .and_where(
                col("context_invocation", "state").ne("ended").or(col(
                    "context_invocation",
                    "result_kind",
                )
                .eq("indeterminate")),
            )
            .to_owned(),
    )
}

fn run_action() -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(Alias::new("tool_invocations"), Alias::new("i"))
            .and_where(col("i", "workspace_id").eq(col("r", "workspace_id")))
            .and_where(col("i", "run_id").eq(col("r", "id")))
            .and_where(invocation_action())
            .to_owned(),
    )
}
fn run_select(now: i64) -> SelectStatement {
    Query::select()
        .from_as(Alias::new("program_run_records"), Alias::new("r"))
        .columns(
            [
                "id",
                "workspace_id",
                "principal_id",
                "principal_kind",
                "state",
            ]
            .map(|name| (Alias::new("r"), Alias::new(name))),
        )
        .expr_as(
            coral_workspace::history_repository::workspace_label(
                (Alias::new("r"), Alias::new("workspace_id")).into_column_ref(),
            ),
            Alias::new("workspace_label"),
        )
        .expr_as(
            coral_workspace::history_repository::agent_label(
                (Alias::new("r"), Alias::new("workspace_id")).into_column_ref(),
                (Alias::new("r"), Alias::new("principal_id")).into_column_ref(),
            ),
            Alias::new("agent_label"),
        )
        .expr_as(measured_context(), Alias::new("context_impact_bytes"))
        .expr_as(col("r", "accepted_at_unix_nanos"), Alias::new("started_at"))
        .expr_as(col("r", "terminal_at_unix_nanos"), Alias::new("ended_at"))
        .expr_as(
            duration("r", "accepted_at_unix_nanos", "terminal_at_unix_nanos", now),
            Alias::new("duration"),
        )
        .expr_as(
            active_run(now).and(run_action()),
            Alias::new("requires_action"),
        )
        .to_owned()
}

#[derive(sqlx::FromRow)]
struct RunRow {
    workspace_label: String,
    agent_label: Option<String>,
    context_impact_bytes: Option<i64>,
    principal_id: String,
    principal_kind: String,
    id: String,
    workspace_id: String,
    state: String,
    started_at: i64,
    ended_at: Option<i64>,
    duration: i64,
    requires_action: bool,
}
impl RunRow {
    fn decode(self) -> Result<RunSummary, Error> {
        Ok(RunSummary {
            workspace_label: self.workspace_label,
            agent_label: self.agent_label,
            action_page: None,
            context_impact_bytes: self.context_impact_bytes,
            principal: coral_workspace::Principal::from_parts(
                coral_workspace::PrincipalId::parse(&self.principal_id)?,
                coral_workspace::PrincipalKind::from_storage_str(&self.principal_kind)?,
            ),
            workspace: WorkspaceId::parse(&self.workspace_id)?,
            id: crate::RunId::parse(&self.id)?,
            state: crate::RunState::from_storage_str(&self.state)?,
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration: self.duration,
            requires_action: self.requires_action,
        })
    }
}
pub(crate) async fn run_summary(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    workspace: &WorkspaceId,
    id: &crate::RunId,
    now: i64,
) -> Result<RunSummary, Error> {
    let mut select = run_select(now);
    select
        .and_where(col("r", "workspace_id").eq(workspace.to_string()))
        .and_where(col("r", "id").eq(id.to_string()));
    restrict(&mut select, authority);
    let row: RunRow = tx.fetch_optional(select).await?.ok_or(Error::RunNotFound)?;
    row.decode()
}
pub(crate) async fn visible_run_summary(
    tx: &mut Transaction<'_>,
    actor: &coral_workspace::Principal,
    include_agents: bool,
    workspace: &WorkspaceId,
    id: &crate::RunId,
    now: i64,
) -> Result<RunSummary, Error> {
    let mut select = run_select(now);
    select
        .and_where(col("r", "workspace_id").eq(workspace.to_string()))
        .and_where(col("r", "id").eq(id.to_string()));
    let own = col("r", "principal_id")
        .eq(actor.id().as_str())
        .and(col("r", "principal_kind").eq(actor.kind().as_storage_str()));
    select.and_where(if include_agents {
        own.or(col("r", "principal_kind").eq("agent"))
    } else {
        own
    });
    let row: RunRow = tx.fetch_optional(select).await?.ok_or(Error::RunNotFound)?;
    row.decode()
}
#[derive(sqlx::FromRow)]
struct InvocationRow {
    result_bytes: i64,
    principal_id: String,
    principal_kind: String,
    id: String,
    workspace_id: String,
    run_id: String,
    provider_id: String,
    tool_key: String,
    call_path: String,
    state: String,
    result_kind: Option<String>,
    started_at: i64,
    ended_at: Option<i64>,
    dispatched_at: Option<i64>,
    duration: i64,
    requires_action: bool,
    parent_id: Option<String>,
    parent_call_path: Option<String>,
    #[sqlx(flatten)]
    source_location: crate::source_location_repository::StoredSourceLocation,
}
impl InvocationRow {
    fn decode(self) -> Result<InvocationSummary, Error> {
        Ok(InvocationSummary {
            context_impact_bytes: (self.state == "ended"
                && self.result_kind.as_deref() != Some("indeterminate"))
            .then_some(-self.result_bytes),
            principal: coral_workspace::Principal::from_parts(
                coral_workspace::PrincipalId::parse(&self.principal_id)?,
                coral_workspace::PrincipalKind::from_storage_str(&self.principal_kind)?,
            ),
            workspace: WorkspaceId::parse(&self.workspace_id)?,
            id: crate::InvocationId::parse(&self.id)?,
            run: crate::RunId::parse(&self.run_id)?,
            provider: coral_provider::ProviderId::parse(&self.provider_id)?,
            tool: self.tool_key,
            tool_kind: crate::ToolKind::of_call_path(&self.call_path),
            call_path: self.call_path,
            parent: match (self.parent_id, self.parent_call_path) {
                (Some(id), Some(call_path)) => Some(crate::InvocationParent {
                    id: crate::InvocationId::parse(&id)?,
                    tool_kind: crate::ToolKind::of_call_path(&call_path),
                    call_path,
                }),
                (None, None) => None,
                _ => return Err(Error::CorruptData("incomplete parent Invocation".into())),
            },
            state: match self.state.as_str() {
                "pending" => InvocationStatus::Pending,
                "dispatching" => InvocationStatus::Dispatching,
                "ended" => InvocationStatus::Ended,
                _ => return Err(Error::CorruptData("invalid Invocation state".into())),
            },
            outcome: self
                .result_kind
                .as_deref()
                .map(crate::InvocationResultKind::from_storage_str)
                .transpose()?,
            started_at: self.started_at,
            ended_at: self.ended_at,
            dispatched_at: self.dispatched_at,
            duration: self.duration,
            requires_action: self.requires_action,
            source_location: self.source_location.decode()?,
        })
    }
}
pub(crate) async fn exact_invocation(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &crate::RunId,
    id: &crate::InvocationId,
    now: i64,
) -> Result<InvocationSummary, Error> {
    let query = Query::select()
        .from_as(Alias::new("tool_invocations"), Alias::new("i"))
        .join_as(
            JoinType::InnerJoin,
            Alias::new("program_run_records"),
            Alias::new("r"),
            col("i", "workspace_id")
                .eq(col("r", "workspace_id"))
                .and(col("i", "run_id").eq(col("r", "id"))),
        )
        .join_as(
            JoinType::LeftJoin,
            Alias::new("tool_invocations"),
            Alias::new("p"),
            col("p", "workspace_id")
                .eq(col("i", "workspace_id"))
                .and(col("p", "run_id").eq(col("i", "run_id")))
                .and(col("p", "id").eq(col("i", "parent_invocation_id"))),
        )
        .columns(["principal_id", "principal_kind"].map(|name| (Alias::new("r"), Alias::new(name))))
        .columns(
            [
                "id",
                "workspace_id",
                "run_id",
                "provider_id",
                "tool_key",
                "call_path",
                "state",
                "result_kind",
                "result_bytes",
            ]
            .into_iter()
            .chain(crate::source_location_repository::COLUMNS)
            .map(|name| (Alias::new("i"), Alias::new(name))),
        )
        .expr_as(col("p", "id"), Alias::new("parent_id"))
        .expr_as(col("p", "call_path"), Alias::new("parent_call_path"))
        .expr_as(col("i", "admitted_at_unix_nanos"), Alias::new("started_at"))
        .expr_as(col("i", "ended_at_unix_nanos"), Alias::new("ended_at"))
        .expr_as(
            col("i", "dispatch_started_at_unix_nanos"),
            Alias::new("dispatched_at"),
        )
        .expr_as(
            duration("i", "admitted_at_unix_nanos", "ended_at_unix_nanos", now),
            Alias::new("duration"),
        )
        .expr_as(
            active_run(now).and(invocation_action()),
            Alias::new("requires_action"),
        )
        .and_where(col("i", "workspace_id").eq(workspace.to_string()))
        .and_where(col("i", "run_id").eq(run.to_string()))
        .and_where(col("i", "id").eq(id.to_string()))
        .to_owned();
    let row: InvocationRow = tx.fetch_optional(query).await?.ok_or(Error::RunNotFound)?;
    row.decode()
}
