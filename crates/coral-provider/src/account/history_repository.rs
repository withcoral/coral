//! Account-owned repository facet for execution summary selection.
use coral_workspace::PrincipalKind;
use sea_query::{ColumnRef, Expr, ExprTrait as _, SimpleExpr};

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

/// Deadline of one exact pending connection, as recorded, for independently
/// authorized Run summaries.
#[must_use]
pub fn pending_connection_deadline(
    workspace: ColumnRef,
    principal: ColumnRef,
    request: ColumnRef,
) -> SimpleExpr {
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            super::repository::pending_connection_statement(
                Expr::col(workspace),
                Expr::col(principal),
                Expr::col(request),
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
) -> SimpleExpr {
    Expr::col(kind)
        .eq(PrincipalKind::User.as_storage_str())
        .and(Expr::exists(
            super::repository::pending_connection_statement(
                Expr::col(workspace),
                Expr::col(principal),
                Expr::col(request),
            ),
        ))
}
