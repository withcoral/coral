//! Exact policy selection for payload-free pending-approval projections.
use crate::{ToolAccessState, default_access_state};
use coral_catalog::{HttpMethod, ToolHints};
use sea_query::{
    Alias, ColumnRef, Expr, ExprTrait as _, Func, Query, SimpleExpr, SubQueryStatement,
};

/// Applies the same exact-rule/default selection as Invocation policy. Retained
/// Generation evidence fixes the default even after catalog publication changes.
#[must_use]
pub fn approval_required(
    workspace: ColumnRef,
    provider: ColumnRef,
    generation: ColumnRef,
    tool: ColumnRef,
) -> SimpleExpr {
    let col = |name: &str| Expr::col((Alias::new("approval_policy"), Alias::new(name)));
    let explicit = Expr::SubQuery(
        None,
        Box::new(SubQueryStatement::SelectStatement(
            Query::select()
                .expr(col("access_state"))
                .from_as(
                    Alias::new("tool_access_rules"),
                    Alias::new("approval_policy"),
                )
                .and_where(col("workspace_id").eq(Expr::col(workspace)))
                .and_where(col("provider_id").eq(Expr::col(provider)))
                .and_where(col("tool_key").eq(Expr::col(tool.clone())))
                .to_owned(),
        )),
    );
    let read_only = ToolHints::new(Some(true), None, None, None);
    let undeclared = ToolHints::default();
    let fallback: SimpleExpr = Expr::case(
        coral_catalog::history_repository::declares_read_only(generation.clone(), tool.clone()),
        Expr::val(default_access_state(read_only, None).as_storage_str()),
    )
    // GET stands for every safe method; the default does not tell them apart.
    .case(
        coral_catalog::history_repository::has_safe_http_method(generation, tool),
        Expr::val(default_access_state(undeclared, Some(HttpMethod::Get)).as_storage_str()),
    )
    .finally(Expr::val(
        default_access_state(undeclared, None).as_storage_str(),
    ))
    .into();
    Expr::expr(Func::coalesce([explicit, fallback]))
        .eq(ToolAccessState::ApprovalRequired.as_storage_str())
}
