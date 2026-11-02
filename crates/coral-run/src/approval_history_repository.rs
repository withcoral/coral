//! Settled Approval Request evidence, newest decision first. Rows carry no
//! arguments, comments or decision source. Call only after authorizing in the
//! same snapshot; `restrict` keeps the caller's Runs and owned Agent Runs.
use crate::approval::{ApprovalId, ApprovalState};
use crate::{Error, RunId};
use coral_db::{Transaction, col, column};
use coral_workspace::{HistoryAuthority, Principal, PrincipalId, PrincipalKind, WorkspaceId};
use sea_query::{Alias, ExprTrait as _, JoinType, Order, Query};

/// States a person or the deadline settled. `closed` stays out.
const SETTLED: [ApprovalState; 3] = [
    ApprovalState::Approved,
    ApprovalState::Declined,
    ApprovalState::Expired,
];

/// One settled request, with its Run's identity.
pub(crate) struct SettledEvidence {
    pub workspace: WorkspaceId,
    pub run: RunId,
    pub approval: ApprovalId,
    pub state: ApprovalState,
    pub settled_at_unix_nanos: i64,
    pub approver: Option<Principal>,
    pub call_path: String,
    pub provider_label: Option<String>,
}

#[derive(sqlx::FromRow)]
struct SettledRow {
    workspace_id: String,
    run_id: String,
    id: String,
    #[sqlx(try_from = "String")]
    state: ApprovalState,
    decided_at_unix_nanos: i64,
    approver_id: Option<String>,
    call_path: String,
    provider_label: Option<String>,
}

/// A page of settled requests after `after`, newest first, and whether more
/// follow. The keyset seeks past the position, so a
/// later decision cannot move a page already read.
pub(crate) async fn page(
    tx: &mut Transaction<'_>,
    authority: &HistoryAuthority,
    after: Option<&crate::SettledCursor>,
    limit: u16,
) -> Result<(Vec<SettledEvidence>, bool), Error> {
    let mut select = Query::select();
    select
        .from_as(Alias::new("invocation_approvals"), Alias::new("a"))
        .join_as(
            JoinType::InnerJoin,
            Alias::new("program_run_records"),
            Alias::new("r"),
            col("r", "workspace_id")
                .eq(col("a", "workspace_id"))
                .and(col("r", "id").eq(col("a", "run_id"))),
        )
        .join_as(
            JoinType::InnerJoin,
            Alias::new("tool_invocations"),
            Alias::new("i"),
            col("i", "workspace_id")
                .eq(col("a", "workspace_id"))
                .and(col("i", "id").eq(col("a", "invocation_id"))),
        )
        .columns(
            [
                "workspace_id",
                "run_id",
                "id",
                "state",
                "decided_at_unix_nanos",
                "approver_id",
            ]
            .map(|name| (Alias::new("a"), Alias::new(name))),
        )
        .expr_as(col("i", "call_path"), Alias::new("call_path"))
        .expr_as(
            coral_provider::history_repository::display_label(
                column("i", "workspace_id"),
                column("i", "provider_id"),
            ),
            Alias::new("provider_label"),
        )
        .and_where(col("a", "state").is_in(SETTLED.map(ApprovalState::storage)));
    // The Workspace match names the request's own column, so the settled index
    // serves it; the join makes it the Run's Workspace too.
    coral_workspace::history_repository::restrict(
        &mut select,
        authority,
        column("a", "workspace_id"),
        column("r", "principal_id"),
        column("r", "principal_kind"),
    );
    if let Some(after) = after {
        let at = after.settled_at_unix_nanos;
        select.and_where(
            col("a", "decided_at_unix_nanos")
                .lt(at)
                .or(col("a", "decided_at_unix_nanos")
                    .eq(at)
                    .and(col("a", "id").lt(after.approval.to_string()))),
        );
    }
    select
        .order_by(
            (Alias::new("a"), Alias::new("decided_at_unix_nanos")),
            Order::Desc,
        )
        .order_by((Alias::new("a"), Alias::new("id")), Order::Desc)
        .limit(u64::from(limit) + 1);
    let rows: Vec<SettledRow> = tx.fetch_all(select).await?;
    let has_more = rows.len() > usize::from(limit);
    let evidence = rows
        .into_iter()
        .take(usize::from(limit))
        .map(|row| {
            Ok(SettledEvidence {
                workspace: WorkspaceId::parse(&row.workspace_id)?,
                run: RunId::parse(&row.run_id)?,
                approval: ApprovalId::parse(&row.id)?,
                state: row.state,
                settled_at_unix_nanos: row.decided_at_unix_nanos,
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
                call_path: row.call_path,
                provider_label: row.provider_label,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((evidence, has_more))
}
