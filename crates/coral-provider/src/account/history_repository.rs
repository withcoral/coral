//! Account-owned repository facet for execution summary selection.
use coral_workspace::PrincipalKind;
use sea_query::{ColumnRef, Expr, ExprTrait as _, SimpleExpr};

/// Earliest private connection deadline, including retained requests with no waiters.
/// Caller supplies independently resolved human history authority in the same snapshot.
/// # Errors
/// Returns persistence errors without changing request state or retention.
pub async fn next_connection_deadline(
    tx: &mut coral_db::Transaction<'_>,
    authority: &coral_workspace::HistoryAuthority,
    now: i64,
) -> Result<Option<i64>, crate::Error> {
    use super::repository::AccountConnectionRequests as C;
    use sea_query::{IntoColumnRef as _, Query};
    let mut query = Query::select()
        .expr(Expr::col(C::ExpiresAtUnixNanos).min())
        .from(C::Table)
        .and_where(Expr::col(C::State).eq("pending"))
        .and_where(Expr::col(C::ExpiresAtUnixNanos).gt(now))
        .to_owned();
    coral_workspace::history_repository::restrict_user_rows(
        &mut query,
        authority,
        (C::Table, C::WorkspaceId).into_column_ref(),
        (C::Table, C::UserId).into_column_ref(),
    );
    let result: Option<(Option<i64>,)> = tx.fetch_optional(query).await?;
    Ok(result.and_then(|row| row.0))
}

/// Latest connection or human decline while an exact Invocation waited on a shared request.
/// Automatic expiry/configuration invalidation can be materialized by inspection;
/// those failed outcomes qualify through the Invocation's actual transition instead.
/// Historical references alone cannot renew a detached waiter's retention.
/// The owning execution query supplies Workspace-scoped request and waiting evidence.
#[must_use]
pub fn connection_completion(
    workspace: ColumnRef,
    request: ColumnRef,
    started: ColumnRef,
    ended: ColumnRef,
) -> SimpleExpr {
    use sea_query::{Alias, Query};
    let col = |name: &str| Expr::col((Alias::new("connection_event"), Alias::new(name)));
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            Query::select()
                .expr(col("occurred_at_unix_nanos").max())
                .from_as(
                    Alias::new("provider_lifecycle_events"),
                    Alias::new("connection_event"),
                )
                .and_where(col("workspace_id").eq(Expr::col(workspace)))
                .and_where(col("target_id").eq(Expr::col(request)))
                .and_where(col("action").eq("account_connection.completed"))
                .and_where(col("outcome").is_in(["accepted", "succeeded"]))
                .and_where(col("occurred_at_unix_nanos").gte(Expr::col(started)))
                .and_where(
                    Expr::col(ended.clone())
                        .is_null()
                        .or(col("occurred_at_unix_nanos").lte(Expr::col(ended))),
                )
                .to_owned(),
        )),
    )
}

/// Deadline of one exact pending connection, for independently authorized Run summaries.
#[must_use]
pub fn pending_connection_deadline(
    workspace: ColumnRef,
    principal: ColumnRef,
    request: ColumnRef,
    now: i64,
) -> SimpleExpr {
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            super::repository::pending_connection_statement(
                Expr::col(workspace),
                Expr::col(principal),
                Expr::col(request),
                now,
            ),
        )),
    )
}

/// Whether one exact attached request is currently pending for its invoking User.
/// This shares the predicate used by Run's exact required-action snapshot. It does
/// not grant request inspection or resolution authority. Call only inside an
/// authorized execution query, using fully qualified columns from its outer row.
/// The Run owner additionally checks Run/Invocation state and Run deadlines.
#[must_use]
pub fn has_pending_connection(
    workspace: ColumnRef,
    principal: ColumnRef,
    kind: ColumnRef,
    request: ColumnRef,
    now: i64,
) -> SimpleExpr {
    Expr::col(kind)
        .eq(PrincipalKind::User.as_storage_str())
        .and(Expr::exists(
            super::repository::pending_connection_statement(
                Expr::col(workspace),
                Expr::col(principal),
                Expr::col(request),
                now,
            ),
        ))
}
