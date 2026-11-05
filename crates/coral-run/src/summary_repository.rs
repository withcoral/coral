//! Grouped, payload-free history totals. Each query restricts Runs to `authority`,
//! resolved in the same snapshot, before it groups.
use crate::summary::{
    ActorContext, ApprovalDecisions, DailyEvidence, OutcomeEvidence, ToolAddress,
};
use crate::{CoralFailure, Error, InvocationResultKind};
use coral_db::{Transaction, col, column};
use coral_workspace::HistoryAuthority;
use sea_query::{
    Alias, Asterisk, Expr, ExprTrait as _, Func, JoinType, Order, Query, SelectStatement,
};

/// Authorized Runs as `r`, each joined to its rows of `table` as `alias`.
fn authorized_runs_with(authority: &HistoryAuthority, table: &str, alias: &str) -> SelectStatement {
    let mut select = Query::select()
        .from_as(Alias::new("program_run_records"), Alias::new("r"))
        .join_as(
            JoinType::InnerJoin,
            Alias::new(table),
            Alias::new(alias),
            col(alias, "workspace_id")
                .eq(col("r", "workspace_id"))
                .and(col(alias, "run_id").eq(col("r", "id"))),
        )
        .to_owned();
    crate::history_repository::restrict(&mut select, authority);
    select
}

fn big_sum(expr: sea_query::SimpleExpr) -> sea_query::SimpleExpr {
    // Postgres sums BIGINT as NUMERIC; both backends then decode one i64.
    Func::cast_as(Func::sum(expr), Alias::new("bigint")).into()
}

#[derive(sqlx::FromRow)]
struct OutcomeRow {
    principal_id: String,
    principal_kind: String,
    agent_label: Option<String>,
    provider_id: String,
    tool_key: String,
    call_path: String,
    result_kind: String,
    failure_code: Option<String>,
    calls: i64,
    last_ended_at_unix_nanos: i64,
}

fn tool_address(provider_id: &str, tool_key: &str) -> Result<ToolAddress, Error> {
    Ok(ToolAddress {
        provider_id: coral_provider::ProviderId::parse(provider_id)?,
        tool_key: coral_catalog::ToolKey::parse(tool_key).map_err(coral_provider::Error::from)?,
    })
}

fn principal(id: &str, kind: &str) -> Result<coral_workspace::Principal, Error> {
    Ok(coral_workspace::Principal::from_parts(
        coral_workspace::PrincipalId::parse(id)?,
        coral_workspace::PrincipalKind::from_storage_str(kind)?,
    ))
}

/// Ended Invocations grouped by invoking Principal, Tool, Call Path and native outcome.
/// Only an ended Invocation holds an ended time, by the lifecycle constraint. So
/// the read names neither `state` nor `id`. Migration 0057 indexes ended times;
/// Tool identity still requires row reads. The query never selects payloads.
pub(crate) async fn call_outcomes(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    since: i64,
) -> Result<Vec<OutcomeEvidence>, Error> {
    let runs = ["workspace_id", "principal_id", "principal_kind"];
    let invocations = [
        "provider_id",
        "tool_key",
        "call_path",
        "result_kind",
        "failure_code",
    ];
    let mut grouped = authorized_runs_with(authority, "tool_invocations", "i");
    grouped
        .columns(runs.map(|name| (Alias::new("r"), Alias::new(name))))
        .columns(invocations.map(|name| (Alias::new("i"), Alias::new(name))))
        .expr_as(Expr::col(Asterisk).count(), Alias::new("calls"))
        .expr_as(
            Expr::col((Alias::new("i"), Alias::new("ended_at_unix_nanos"))).max(),
            Alias::new("last_ended_at_unix_nanos"),
        )
        .and_where(col("i", "ended_at_unix_nanos").gte(since))
        .group_by_columns(runs.map(|name| (Alias::new("r"), Alias::new(name))))
        .group_by_columns(invocations.map(|name| (Alias::new("i"), Alias::new(name))));
    // The label reads one Agent row per group, not per Invocation.
    let select = Query::select()
        .from_subquery(grouped, Alias::new("g"))
        .columns(
            [
                "principal_id",
                "principal_kind",
                "provider_id",
                "tool_key",
                "call_path",
                "result_kind",
                "failure_code",
                "calls",
                "last_ended_at_unix_nanos",
            ]
            .map(|name| (Alias::new("g"), Alias::new(name))),
        )
        .expr_as(
            coral_workspace::history_repository::agent_label(
                column("g", "workspace_id"),
                column("g", "principal_id"),
            ),
            Alias::new("agent_label"),
        )
        .to_owned();
    let rows: Vec<OutcomeRow> = tx.fetch_all(select).await?;
    rows.into_iter()
        .map(|row| {
            Ok(OutcomeEvidence {
                principal: principal(&row.principal_id, &row.principal_kind)?,
                agent_label: row.agent_label,
                tool: tool_address(&row.provider_id, &row.tool_key)?,
                call_path: row.call_path,
                last_ended_at_unix_nanos: row.last_ended_at_unix_nanos,
                kind: InvocationResultKind::from_storage_str(&row.result_kind)?,
                // A code this build no longer knows is still an ended failure.
                failure: row
                    .failure_code
                    .as_deref()
                    .and_then(CoralFailure::from_code),
                calls: row.calls,
            })
        })
        .collect()
}

#[derive(sqlx::FromRow)]
struct DailyRow {
    provider_id: String,
    tool_key: String,
    day: i64,
    calls: i64,
}

/// Ended Invocations grouped by Tool and by the day of the window they ended in,
/// counted in whole days of `day_nanos` from `since`.
pub(crate) async fn daily_calls(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    since: i64,
    day_nanos: i64,
) -> Result<Vec<DailyEvidence>, Error> {
    let day = col("i", "ended_at_unix_nanos").sub(since).div(day_nanos);
    let mut grouped = authorized_runs_with(authority, "tool_invocations", "i");
    grouped
        .columns(["provider_id", "tool_key"].map(|name| (Alias::new("i"), Alias::new(name))))
        .expr_as(day, Alias::new("day"))
        .expr_as(
            Expr::col((Alias::new("i"), Alias::new("id"))).count(),
            Alias::new("calls"),
        )
        .and_where(ended_since(since))
        .group_by_columns(
            ["provider_id", "tool_key"].map(|name| (Alias::new("i"), Alias::new(name))),
        )
        // By the output name: Postgres matches no two copies of an expression
        // whose bound values are separate parameters.
        .group_by_col(Alias::new("day"));
    let rows: Vec<DailyRow> = tx.fetch_all(grouped).await?;
    rows.into_iter()
        .map(|row| {
            Ok(DailyEvidence {
                tool: tool_address(&row.provider_id, &row.tool_key)?,
                // Only calls that ended in the window are selected, so a day is
                // never negative.
                day: u32::try_from(row.day).unwrap_or_default(),
                calls: row.calls,
            })
        })
        .collect()
}

/// Approvals decided in the window, joined to their authorized Runs.
fn decisions(authority: &HistoryAuthority, since: i64) -> SelectStatement {
    let mut select = authorized_runs_with(authority, "invocation_approvals", "a");
    select
        .and_where(col("a", "state").is_in(["approved", "declined"]))
        .and_where(col("a", "decided_at_unix_nanos").gte(since));
    select
}

/// Approved and declined requests, with the median request-to-decision wait.
pub(crate) async fn approval_decisions(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    since: i64,
) -> Result<ApprovalDecisions, Error> {
    let mut counts = decisions(authority, since);
    counts
        .column((Alias::new("a"), Alias::new("state")))
        .expr(Expr::col((Alias::new("a"), Alias::new("id"))).count())
        .group_by_col((Alias::new("a"), Alias::new("state")));
    let rows: Vec<(String, i64)> = tx.fetch_all(counts).await?;
    let mut decided = ApprovalDecisions::default();
    for (state, count) in rows {
        match state.as_str() {
            "approved" => decided.approved = count,
            _ => decided.declined = count,
        }
    }
    let total = decided.approved + decided.declined;
    if total == 0 {
        return Ok(decided);
    }
    // The middle one or two waits, so both backends need no percentile function.
    let wait = col("a", "decided_at_unix_nanos").sub(col("a", "created_at_unix_nanos"));
    let mut middle = decisions(authority, since);
    middle
        .expr(wait.clone())
        .order_by_expr(wait, Order::Asc)
        .offset(u64::try_from((total - 1) / 2).unwrap_or_default())
        .limit(if total % 2 == 0 { 2 } else { 1 });
    let waits: Vec<(i64,)> = tx.fetch_all(middle).await?;
    decided.median_wait_nanos = match waits.as_slice() {
        [(only,)] => Some(*only),
        [(low,), (high,)] => Some(low + (high - low) / 2),
        _ => None,
    };
    Ok(decided)
}

#[derive(sqlx::FromRow)]
struct ActorRow {
    principal_id: String,
    principal_kind: String,
    agent_label: Option<String>,
    bytes: Option<i64>,
}

/// Net bytes of measured Runs that ended in the window, per invoking Principal.
/// Running and indeterminate Runs have no total and add nothing. `SUM` skips them,
/// so a Principal with only such Runs sums to NULL and has no row in the result. A
/// SQL filter on `m.bytes` would run the measurement's subqueries a second time.
pub(crate) async fn actor_context(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    since: i64,
) -> Result<Vec<ActorContext>, Error> {
    let mut measured = Query::select()
        .from_as(Alias::new("program_run_records"), Alias::new("r"))
        .columns(
            ["workspace_id", "principal_id", "principal_kind"]
                .map(|name| (Alias::new("r"), Alias::new(name))),
        )
        .expr_as(
            crate::history_repository::measured_context(),
            Alias::new("bytes"),
        )
        .and_where(col("r", "terminal_at_unix_nanos").gte(since))
        .to_owned();
    crate::history_repository::restrict(&mut measured, authority);
    let fields = ["workspace_id", "principal_id", "principal_kind"];
    let select = Query::select()
        .from_subquery(measured, Alias::new("m"))
        .columns(fields.map(|name| (Alias::new("m"), Alias::new(name))))
        .expr_as(
            coral_workspace::history_repository::agent_label(
                column("m", "workspace_id"),
                column("m", "principal_id"),
            ),
            Alias::new("agent_label"),
        )
        .expr_as(big_sum(col("m", "bytes")), Alias::new("bytes"))
        .group_by_columns(fields.map(|name| (Alias::new("m"), Alias::new(name))))
        .to_owned();
    let rows: Vec<ActorRow> = tx.fetch_all(select).await?;
    rows.into_iter()
        .filter_map(|row| {
            let bytes = row.bytes?;
            let principal = principal(&row.principal_id, &row.principal_kind);
            Some(principal.map(|principal| ActorContext {
                principal,
                agent_label: row.agent_label,
                bytes,
            }))
        })
        .collect()
}

/// Whether any authorized Run is still running, or ended in the window without a
/// measurement, so the measured totals leave work out.
pub(crate) async fn unmeasured(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    since: i64,
) -> Result<bool, Error> {
    let mut select = Query::select()
        .expr(Expr::val(1_i64))
        .from_as(Alias::new("program_run_records"), Alias::new("r"))
        .and_where(
            crate::history_repository::running().or(col("r", "terminal_at_unix_nanos")
                .gte(since)
                .and(crate::history_repository::measured_context().is_null())),
        )
        .limit(1)
        .to_owned();
    crate::history_repository::restrict(&mut select, authority);
    let row: Option<(i64,)> = tx.fetch_optional(select).await?;
    Ok(row.is_some())
}
