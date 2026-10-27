//! Payload-free retained Tool facts for owning execution repositories.
use sea_query::{Alias, ColumnRef, Expr, ExprTrait as _, Query, SimpleExpr};

use crate::HttpMethod;

/// Whether the exact retained Tool declared that it changes nothing.
/// The caller supplies a trusted Generation reference after Workspace authorization.
#[must_use]
pub fn declares_read_only(generation: ColumnRef, tool: ColumnRef) -> SimpleExpr {
    retained_tool_where(generation, tool, col("read_only_hint").eq(true))
}

/// Whether the exact retained Tool is HTTP with a safe method; see [`HttpMethod::is_safe`].
/// The caller supplies a trusted Generation reference after Workspace authorization.
#[must_use]
pub fn has_safe_http_method(generation: ColumnRef, tool: ColumnRef) -> SimpleExpr {
    retained_tool_where(
        generation,
        tool,
        col("method").is_in(HttpMethod::SAFE.map(HttpMethod::as_str)),
    )
}

fn col(name: &str) -> Expr {
    Expr::col((Alias::new("retained_tool"), Alias::new(name)))
}

fn retained_tool_where(
    generation: ColumnRef,
    tool: ColumnRef,
    condition: SimpleExpr,
) -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("materialized_tools"),
                Alias::new("retained_tool"),
            )
            .and_where(col("provider_generation_id").eq(Expr::col(generation)))
            .and_where(col("tool_key").eq(Expr::col(tool)))
            .and_where(condition)
            .to_owned(),
    )
}
