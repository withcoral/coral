//! Safe Provider presentation facets for independently authorized retained history.
use sea_query::{Alias, ColumnRef, Expr, ExprTrait as _, Query, SimpleExpr};

/// Current display label of the exact Workspace-qualified Provider. A removed Provider
/// returns NULL; callers keep its retained stable identity and show an unavailable label.
/// This does not authorize history or load configuration, Accounts, or Generations.
#[must_use]
pub fn display_label(workspace: ColumnRef, provider: ColumnRef) -> SimpleExpr {
    Expr::SubQuery(
        None,
        Box::new(sea_query::SubQueryStatement::SelectStatement(
            Query::select()
                .column((Alias::new("history_provider"), Alias::new("display_name")))
                .from_as(
                    Alias::new("provider_records"),
                    Alias::new("history_provider"),
                )
                .and_where(
                    Expr::col((Alias::new("history_provider"), Alias::new("workspace_id")))
                        .eq(Expr::col(workspace)),
                )
                .and_where(
                    Expr::col((Alias::new("history_provider"), Alias::new("id")))
                        .eq(Expr::col(provider)),
                )
                .to_owned(),
        )),
    )
}
