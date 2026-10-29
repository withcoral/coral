//! Bounded summary-only request evidence. Call only after authorizing the exact Run
//! in the same snapshot. Shared connection waiters collapse before the page limit.
use crate::{Error, RequiredActionRequest, RunId};
use coral_db::Transaction;
use coral_db::{col, column};
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, JoinType, Query, SelectStatement, UnionType};

/// One queued request with the waiting Run it is shown with.
pub(crate) struct QueuedEvidence {
    pub workspace: WorkspaceId,
    pub run: RunId,
    pub action: ActionEvidence,
}

pub(crate) struct ActionEvidence {
    pub request: RequiredActionRequest,
    pub first_call_path: String,
    pub last_call_path: String,
    pub provider_label: Option<String>,
    pub request_deadline: i64,
    pub run_deadline: i64,
}

impl ActionEvidence {
    /// The waiting Tool path, empty when a shared connection spans several.
    pub(crate) fn shared_call_path(&self) -> &str {
        if self.first_call_path == self.last_call_path {
            &self.first_call_path
        } else {
            ""
        }
    }
}

fn candidates(now: i64) -> SelectStatement {
    Query::select()
        .from_as(Alias::new("program_run_records"), Alias::new("r"))
        .join_as(
            JoinType::InnerJoin,
            Alias::new("tool_invocations"),
            Alias::new("i"),
            col("i", "workspace_id")
                .eq(col("r", "workspace_id"))
                .and(col("i", "run_id").eq(col("r", "id"))),
        )
        .join_as(
            JoinType::LeftJoin,
            Alias::new("invocation_approvals"),
            Alias::new("a"),
            crate::approval_repository::pending_invocation(),
        )
        .expr_as(col("r", "workspace_id"), Alias::new("workspace"))
        .expr_as(col("r", "id"), Alias::new("run"))
        .expr_as(
            coral_workspace::history_repository::invoker_deadline(
                column("r", "workspace_id"),
                column("r", "principal_id"),
                column("r", "principal_kind"),
            ),
            Alias::new("authority_deadline"),
        )
        .expr_as(col("i", "call_path"), Alias::new("call_path"))
        .expr_as(
            coral_provider::history_repository::display_label(
                column("i", "workspace_id"),
                column("i", "provider_id"),
            ),
            Alias::new("provider_label"),
        )
        .expr_as(
            col("r", "deadline_at_unix_nanos"),
            Alias::new("run_deadline"),
        )
        .expr_as(col("a", "id"), Alias::new(RequiredActionRequest::APPROVAL))
        .expr_as(
            col("a", "deadline_at_unix_nanos"),
            Alias::new("approval_deadline"),
        )
        .expr_as(
            col("i", "account_connection_request_id"),
            Alias::new(RequiredActionRequest::CONNECTION),
        )
        .expr_as(
            Expr::case(
                col("r", "principal_kind").eq("user"),
                coral_provider::account::history_repository::pending_connection_deadline(
                    column("i", "workspace_id"),
                    column("r", "principal_id"),
                    column("i", "account_connection_request_id"),
                ),
            )
            .finally(Expr::val(Option::<i64>::None)),
            Alias::new("connection_deadline"),
        )
        .and_where(crate::history_repository::active_run(now))
        .and_where(col("i", "state").eq("pending"))
        .to_owned()
}
fn requests(candidates: &SelectStatement) -> SelectStatement {
    let branch = |kind: &str, deadline: &str| {
        Query::select()
            .from_subquery(candidates.clone(), Alias::new("candidate"))
            .expr_as(col("candidate", "workspace"), Alias::new("workspace"))
            .expr_as(col("candidate", "run"), Alias::new("run"))
            .expr_as(Expr::val(kind), Alias::new("kind"))
            .expr_as(col("candidate", kind), Alias::new("request"))
            .expr_as(col("candidate", "call_path"), Alias::new("call_path"))
            .expr_as(
                col("candidate", "provider_label"),
                Alias::new("provider_label"),
            )
            .expr_as(col("candidate", "run_deadline"), Alias::new("run_deadline"))
            .expr_as(
                col("candidate", "authority_deadline"),
                Alias::new("authority_deadline"),
            )
            .expr_as(col("candidate", deadline), Alias::new("request_deadline"))
            .and_where(col("candidate", deadline).is_not_null())
            .to_owned()
    };
    let mut requests = branch(RequiredActionRequest::APPROVAL, "approval_deadline");
    requests.union(
        UnionType::All,
        branch(RequiredActionRequest::CONNECTION, "connection_deadline"),
    );
    requests
}

/// Current candidates restricted to `authority`, resolved in the same snapshot.
fn authorized_candidates(
    authority: &coral_workspace::HistoryAuthority,
    now: i64,
) -> SelectStatement {
    let mut candidates = candidates(now);
    crate::history_repository::restrict(&mut candidates, authority);
    candidates
}

/// Earliest expiry of the invoking authority behind a current action,
/// including requests outside loaded pages: the one time-dependent
/// visibility of the queue, since a Run's or a request's deadline changes
/// nothing a reader shows until the Run Owner records it. Scheduling
/// evidence only: reads do not settle requests or renew retention.
pub(crate) async fn next_authority_deadline(
    tx: &mut Transaction<'_>,
    authority: &coral_workspace::HistoryAuthority,
    now: i64,
) -> Result<Option<i64>, Error> {
    let candidates = authorized_candidates(authority, now);
    let (actor,): (Option<i64>,) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(Alias::new("authority_deadline")).min())
                .from_subquery(requests(&candidates), Alias::new("requests"))
                .to_owned(),
        )
        .await?
        .ok_or(Error::RunNotFound)?;
    Ok(actor)
}

/// Counts identities through the same current-request relation as exact Run summaries.
/// No exact payloads or per-Run enumeration leave this repository.
pub(crate) async fn count(
    tx: &mut Transaction<'_>,
    authority: &coral_workspace::HistoryAuthority,
    now: i64,
) -> Result<i64, Error> {
    let candidates = authorized_candidates(authority, now);
    let identities = Query::select()
        .from_subquery(requests(&candidates), Alias::new("requests"))
        .columns(["workspace", "kind", "request"].map(Alias::new))
        .distinct()
        .to_owned();
    let (count,): (i64,) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(Alias::new("request")).count())
                .from_subquery(identities, Alias::new("identities"))
                .to_owned(),
        )
        .await?
        .ok_or(Error::RunNotFound)?;
    Ok(count)
}

fn grouped_requests(workspace: &WorkspaceId, run: &RunId, now: i64) -> SelectStatement {
    let mut candidates = candidates(now);
    candidates
        .and_where(col("r", "workspace_id").eq(workspace.to_string()))
        .and_where(col("r", "id").eq(run.to_string()));
    let fields = [
        "kind",
        "request",
        "provider_label",
        "run_deadline",
        "request_deadline",
    ];
    Query::select()
        .from_subquery(requests(&candidates), Alias::new("requests"))
        .columns(fields.map(Alias::new))
        .expr_as(
            Expr::col(Alias::new("call_path")).min(),
            Alias::new("first_call_path"),
        )
        .expr_as(
            Expr::col(Alias::new("call_path")).max(),
            Alias::new("last_call_path"),
        )
        .group_by_columns(fields.map(Alias::new))
        .to_owned()
}

pub(crate) async fn page(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    after: Option<&crate::ActionCursor>,
    now: i64,
    limit: u16,
) -> Result<(Vec<ActionEvidence>, i64, bool), Error> {
    let query = grouped_requests(workspace, run, now);
    let (total,): (i64,) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(Alias::new("request")).count())
                .from_subquery(query.clone(), Alias::new("action_count"))
                .to_owned(),
        )
        .await?
        .ok_or(Error::RunNotFound)?;
    // Seek in the grouped relation, not the live anchor row or a changing ordinal.
    // Resolution/deletion of the cursor request cannot move or invalidate this position.
    let mut window = Query::select();
    window
        .from_subquery(query, Alias::new("actions"))
        .column(sea_query::Asterisk);
    if let Some(after) = after {
        let (kind, id) = (after.0.kind(), after.0.id());
        window.and_where(
            col("actions", "kind").gt(kind).or(col("actions", "kind")
                .eq(kind)
                .and(col("actions", "request").gt(id))),
        );
    }
    window
        .order_by(Alias::new("kind"), sea_query::Order::Asc)
        .order_by(Alias::new("request"), sea_query::Order::Asc)
        .limit(u64::from(limit) + 1);
    let rows: Vec<ActionRow> = tx.fetch_all(window).await?;
    let has_more = rows.len() > usize::from(limit);
    let evidence = rows
        .into_iter()
        .take(usize::from(limit))
        .map(|row| {
            Ok(ActionEvidence {
                request: RequiredActionRequest::from_parts(&row.kind, &row.request)
                    .ok_or_else(|| Error::CorruptData("invalid action request".into()))?,
                first_call_path: row.first_call_path,
                last_call_path: row.last_call_path,
                provider_label: row.provider_label,
                run_deadline: row.run_deadline,
                request_deadline: row.request_deadline,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((evidence, total, has_more))
}
#[derive(sqlx::FromRow)]
struct ActionRow {
    kind: String,
    request: String,
    first_call_path: String,
    last_call_path: String,
    provider_label: Option<String>,
    run_deadline: i64,
    request_deadline: i64,
}

/// Each current request once, with the waiting Run whose deadline comes first.
/// A shared connection keeps one row however many Runs wait on it.
fn queued_requests(authority: &coral_workspace::HistoryAuthority, now: i64) -> SelectStatement {
    let fields = [
        "workspace",
        "kind",
        "request",
        "run",
        "provider_label",
        "run_deadline",
        "request_deadline",
    ];
    let per_run = Query::select()
        .from_subquery(
            requests(&authorized_candidates(authority, now)),
            Alias::new("requests"),
        )
        .columns(fields.map(Alias::new))
        .expr_as(
            Expr::col(Alias::new("call_path")).min(),
            Alias::new("first_call_path"),
        )
        .expr_as(
            Expr::col(Alias::new("call_path")).max(),
            Alias::new("last_call_path"),
        )
        .group_by_columns(fields.map(Alias::new))
        .to_owned();
    let earlier = Query::select()
        .expr(Expr::val(1))
        .from_subquery(per_run.clone(), Alias::new("other"))
        .and_where(col("other", "kind").eq(col("chosen", "kind")))
        .and_where(col("other", "request").eq(col("chosen", "request")))
        .and_where(
            col("other", "run_deadline")
                .lt(col("chosen", "run_deadline"))
                .or(col("other", "run_deadline")
                    .eq(col("chosen", "run_deadline"))
                    .and(col("other", "run").lt(col("chosen", "run")))),
        )
        .to_owned();
    let chosen = Query::select()
        .from_subquery(per_run, Alias::new("chosen"))
        .column(sea_query::Asterisk)
        .expr_as(
            Expr::case(
                col("chosen", "request_deadline").lt(col("chosen", "run_deadline")),
                col("chosen", "request_deadline"),
            )
            .finally(col("chosen", "run_deadline")),
            Alias::new("deadline"),
        )
        .and_where(Expr::exists(earlier).not())
        .to_owned();
    Query::select()
        .from_subquery(chosen, Alias::new("queue"))
        .column(sea_query::Asterisk)
        .to_owned()
}

/// A page of the queue, most urgent first, after `after` = (deadline, request).
pub(crate) async fn queue(
    tx: &mut Transaction<'_>,
    authority: &coral_workspace::HistoryAuthority,
    after: Option<(i64, &RequiredActionRequest)>,
    now: i64,
    limit: u16,
) -> Result<(Vec<QueuedEvidence>, bool), Error> {
    #[derive(sqlx::FromRow)]
    struct QueueRow {
        workspace: String,
        run: String,
        kind: String,
        request: String,
        first_call_path: String,
        last_call_path: String,
        provider_label: Option<String>,
        run_deadline: i64,
        request_deadline: i64,
    }
    let mut window = queued_requests(authority, now);
    // Seek past the position, not the live row: resolving it cannot move the page.
    if let Some((deadline, request)) = after {
        let (kind, id) = (request.kind(), request.id());
        window.and_where(
            col("queue", "deadline")
                .gt(deadline)
                .or(col("queue", "deadline").eq(deadline).and(
                    col("queue", "kind").gt(kind).or(col("queue", "kind")
                        .eq(kind)
                        .and(col("queue", "request").gt(id))),
                )),
        );
    }
    window
        .order_by(Alias::new("deadline"), sea_query::Order::Asc)
        .order_by(Alias::new("kind"), sea_query::Order::Asc)
        .order_by(Alias::new("request"), sea_query::Order::Asc)
        .limit(u64::from(limit) + 1);
    let rows: Vec<QueueRow> = tx.fetch_all(window).await?;
    let has_more = rows.len() > usize::from(limit);
    let queued = rows
        .into_iter()
        .take(usize::from(limit))
        .map(|row| {
            Ok(QueuedEvidence {
                workspace: WorkspaceId::parse(&row.workspace)?,
                run: RunId::parse(&row.run)?,
                action: ActionEvidence {
                    request: RequiredActionRequest::from_parts(&row.kind, &row.request)
                        .ok_or_else(|| Error::CorruptData("invalid action request".into()))?,
                    first_call_path: row.first_call_path,
                    last_call_path: row.last_call_path,
                    provider_label: row.provider_label,
                    run_deadline: row.run_deadline,
                    request_deadline: row.request_deadline,
                },
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((queued, has_more))
}
