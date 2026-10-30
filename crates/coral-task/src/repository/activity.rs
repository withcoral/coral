use crate::{ActivityIdentity, ActivityWindow, Error, IntentQuery, TaskId};
use coral_db::{DatabaseSystem, Transaction};
use coral_workspace::{HistoryAuthority, WorkspaceId};
mod window;
use sea_query::{Alias, Expr, ExprTrait as _, IntoColumnRef as _, Order, Query, UnionType};
use window::{PositionIdentity, PositionWindow, select_window};

/// Exact scoped Task selection: no count, ordering, or neighboring history query.
pub(crate) async fn task_in_scope(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    identity: &crate::TaskIdentity,
) -> Result<crate::Task, Error> {
    use super::TaskRecords as T;
    let mut query = super::base_select();
    coral_workspace::history_repository::restrict(
        &mut query,
        authority,
        T::WorkspaceId.into_column_ref(),
        T::PrincipalId.into_column_ref(),
        T::PrincipalKind.into_column_ref(),
    );
    query
        .and_where(Expr::col(T::WorkspaceId).eq(identity.workspace.to_string()))
        .and_where(Expr::col(T::Id).eq(identity.task.to_string()));
    let row: super::TaskRow = tx.fetch_optional(query).await?.ok_or(Error::TaskNotFound)?;
    row.decode()
}

/// Scalar byte evidence for the domain's participating attempts across the whole Task.
pub(crate) struct ContextTotals {
    /// Sum of the measured children.
    pub bytes: i64,
    /// Children without bytes because their Run is still running.
    pub running: i64,
}

/// Sums the Task's measured participating children, across the whole Task.
/// A child without bytes adds nothing, including a Run hidden from `authority`.
pub(crate) async fn context_totals(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    workspace: &WorkspaceId,
    task: &TaskId,
) -> Result<ContextTotals, Error> {
    use coral_db::col;
    use sea_query::{Func, JoinType};
    let bytes = Expr::case(
        col("interaction", "run_id").is_not_null(),
        col("measurement", "context_impact_bytes"),
    )
    .finally(col("interaction", "response_bytes"));
    let running_child = Expr::case(col("measurement", "running"), Expr::val(1_i64));
    let children = Query::select()
        .expr_as(bytes, Alias::new("bytes"))
        .expr_as(running_child, Alias::new("running_child"))
        .from_as(Alias::new("task_interactions"), Alias::new("interaction"))
        .join_subquery(
            JoinType::LeftJoin,
            coral_run::history_repository::context_measurements(authority),
            Alias::new("measurement"),
            col("interaction", "workspace_id")
                .eq(col("measurement", "workspace_id"))
                .and(col("interaction", "run_id").eq(col("measurement", "id"))),
        )
        .and_where(col("interaction", "workspace_id").eq(workspace.to_string()))
        .and_where(col("interaction", "task_id").eq(task.to_string()))
        .and_where(
            col("interaction", "kind").is_in(
                crate::activity::CONTEXT_IMPACT_ATTEMPTS
                    .into_iter()
                    .map(crate::interaction_repository::encode_kind),
            ),
        )
        .to_owned();
    let query = Query::select()
        .expr(Func::cast_as(
            Func::coalesce([Expr::col(Alias::new("bytes")).sum(), Expr::val(0_i64)]),
            Alias::new("bigint"),
        ))
        .expr(Expr::col(Alias::new("running_child")).count())
        .from_subquery(children, Alias::new("children"))
        .to_owned();
    let (bytes, running) = tx.fetch_optional(query).await?.ok_or(Error::TaskNotFound)?;
    Ok(ContextTotals { bytes, running })
}

/// Net response bytes of context-impact attempts without a Run that completed at
/// or after `since`, per invoking Principal. A Run's bytes belong to the Run.
pub(crate) async fn discovery_context(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    since: i64,
) -> Result<Vec<coral_run::ActorContext>, Error> {
    use coral_db::{col, column};
    use sea_query::Func;
    let fields = ["workspace_id", "principal_id", "principal_kind"];
    let mut select = Query::select();
    select
        .from_as(Alias::new("task_interactions"), Alias::new("i"))
        .columns(fields.map(|name| (Alias::new("i"), Alias::new(name))))
        .expr(coral_workspace::history_repository::agent_label(
            column("i", "workspace_id"),
            column("i", "principal_id"),
        ))
        .expr(Func::cast_as(
            Expr::col((Alias::new("i"), Alias::new("response_bytes"))).sum(),
            Alias::new("bigint"),
        ))
        .and_where(
            col("i", "kind").is_in(
                crate::activity::CONTEXT_IMPACT_ATTEMPTS
                    .into_iter()
                    .map(crate::interaction_repository::encode_kind),
            ),
        )
        .and_where(col("i", "run_id").is_null())
        .and_where(col("i", "response_bytes").is_not_null())
        .and_where(col("i", "completed_at_unix_nanos").gte(since))
        .group_by_columns(fields.map(|name| (Alias::new("i"), Alias::new(name))));
    coral_workspace::history_repository::restrict(
        &mut select,
        authority,
        column("i", "workspace_id"),
        column("i", "principal_id"),
        column("i", "principal_kind"),
    );
    let rows: Vec<(String, String, String, Option<String>, i64)> = tx.fetch_all(select).await?;
    rows.into_iter()
        .map(|(_, id, kind, agent_label, bytes)| {
            Ok(coral_run::ActorContext {
                principal: coral_workspace::Principal::from_parts(
                    coral_workspace::PrincipalId::parse(&id)?,
                    coral_workspace::PrincipalKind::from_storage_str(&kind)?,
                ),
                agent_label,
                bytes,
            })
        })
        .collect()
}

/// `"{table}"."intent"` contains the query, ignoring ASCII case. Postgres
/// would fold other letters under its database locale; the "C" collation keeps
/// it to ASCII, as SQLite's `LOWER` is.
pub(crate) fn intent_contains(system: DatabaseSystem, table: &str, query: &IntentQuery) -> Expr {
    let column = format!("\"{table}\".\"intent\"");
    let folded = match system {
        DatabaseSystem::Sqlite => Expr::cust(format!("LOWER({column})")),
        DatabaseSystem::Postgres => Expr::cust(format!("LOWER({column} COLLATE \"C\")")),
    };
    let mut pattern = String::from("%");
    for character in query.folded().chars() {
        if matches!(character, '\\' | '%' | '_') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('%');
    folded.like(sea_query::LikeExpr::new(pattern).escape('\\'))
}

pub(crate) struct RootPage {
    pub identities: Vec<ActivityIdentity>,
    pub has_before: bool,
    pub has_after: bool,
    pub anchor_unavailable: bool,
}
fn identity(value: &ActivityIdentity) -> PositionIdentity {
    match value {
        ActivityIdentity::Task(w, id) => PositionIdentity {
            workspace: w.to_string(),
            id: format!("task:{id}"),
        },
        ActivityIdentity::Run(w, id) => PositionIdentity {
            workspace: w.to_string(),
            id: format!("run:{id}"),
        },
    }
}
/// The Task's intent, or the intent of one of its Run submissions, contains the query.
fn task_matches(system: DatabaseSystem, query: &IntentQuery) -> sea_query::Condition {
    use coral_db::col;
    let submission = Query::select()
        .expr(Expr::val(1_i64))
        .from_as(Alias::new("task_interactions"), Alias::new("submission"))
        .and_where(col("submission", "workspace_id").eq(col("task_records", "workspace_id")))
        .and_where(col("submission", "task_id").eq(col("task_records", "id")))
        .and_where(col("submission", "run_id").is_not_null())
        .and_where(intent_contains(system, "submission", query))
        .to_owned();
    sea_query::Condition::any()
        .add(intent_contains(system, "task_records", query))
        .add(Expr::exists(submission))
}
/// Root positions of the Runs started outside any Task.
fn standalone_runs(authority: &HistoryAuthority) -> sea_query::SelectStatement {
    let mut runs = Query::select();
    runs.from_subquery(
        coral_run::history_repository::run_positions(authority),
        Alias::new("roots"),
    )
    .column((Alias::new("roots"), Alias::new("workspace_id")))
    .expr_as(
        Expr::val("run:").binary(
            sea_query::BinOper::Custom("||"),
            Expr::col((Alias::new("roots"), Alias::new("id"))),
        ),
        Alias::new("id"),
    )
    .column((Alias::new("roots"), Alias::new("sort_key")))
    .and_where(
        Expr::exists(
            Query::select()
                .expr(Expr::val(1_i64))
                .from(Alias::new("task_interactions"))
                .and_where(
                    Expr::col(Alias::new("workspace_id"))
                        .eq(Expr::col((Alias::new("roots"), Alias::new("workspace_id")))),
                )
                .and_where(
                    Expr::col(Alias::new("run_id"))
                        .eq(Expr::col((Alias::new("roots"), Alias::new("id")))),
                )
                .to_owned(),
        )
        .not(),
    );
    runs
}
pub(crate) async fn roots(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    window: &ActivityWindow,
    limit: u16,
    query: Option<&IntentQuery>,
) -> Result<RootPage, Error> {
    use super::TaskRecords as T;
    let mut tasks = Query::select();
    tasks
        .from(T::Table)
        .column(T::WorkspaceId)
        .expr_as(
            Expr::val("task:").binary(sea_query::BinOper::Custom("||"), Expr::col(T::Id)),
            Alias::new("id"),
        )
        .expr_as(Expr::col(T::StartedAtUnixNanos), Alias::new("sort_key"));
    coral_workspace::history_repository::restrict(
        &mut tasks,
        authority,
        T::WorkspaceId.into_column_ref(),
        T::PrincipalId.into_column_ref(),
        T::PrincipalKind.into_column_ref(),
    );
    if let Some(query) = query {
        tasks.cond_where(task_matches(tx.system(), query));
    }
    // Standalone Runs have no intent, so a query never keeps them.
    if query.is_none() {
        tasks.union(UnionType::All, standalone_runs(authority));
    }
    let positions = Query::select()
        .column(sea_query::Asterisk)
        .from_subquery(tasks, Alias::new("activity_roots"))
        .to_owned();
    let window = match window {
        ActivityWindow::Edge => PositionWindow::Edge,
        ActivityWindow::Before(id) => PositionWindow::Before(identity(id)),
        ActivityWindow::After(id) => PositionWindow::After(identity(id)),
        ActivityWindow::At(id) => PositionWindow::At(identity(id)),
        ActivityWindow::Records(ids) => PositionWindow::Records(ids.iter().map(identity).collect()),
    };
    let page = select_window(tx, positions.clone(), &window, limit).await?;
    let mut selected = positions;
    page.restrict(
        &mut selected,
        &Alias::new("workspace_id").into_column_ref(),
        &Alias::new("id").into_column_ref(),
    );
    selected
        .order_by(Alias::new("sort_key"), Order::Desc)
        .order_by(Alias::new("workspace_id"), Order::Desc)
        .order_by(Alias::new("id"), Order::Desc);
    let rows: Vec<(String, String, i64)> = tx.fetch_all(selected).await?;
    let identities = rows
        .into_iter()
        .map(|(workspace, id, _)| {
            let workspace = WorkspaceId::parse(&workspace)?;
            if let Some(id) = id.strip_prefix("task:") {
                Ok(ActivityIdentity::Task(workspace, TaskId::parse(id)?))
            } else if let Some(id) = id.strip_prefix("run:") {
                Ok(ActivityIdentity::Run(
                    workspace,
                    coral_run::RunId::parse(id)?,
                ))
            } else {
                Err(Error::CorruptData("invalid activity root".into()))
            }
        })
        .collect::<Result<_, Error>>()?;
    Ok(RootPage {
        identities,
        has_before: page.has_before,
        has_after: page.has_after,
        anchor_unavailable: page.anchor_unavailable,
    })
}
