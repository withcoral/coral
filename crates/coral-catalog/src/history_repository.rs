//! Payload-free retained Operation facts for owning execution repositories.
use sea_query::{Alias, ColumnRef, Expr, ExprTrait as _, Query, SimpleExpr};

use crate::HttpMethod;

/// Whether the exact retained Operation declared that it changes nothing.
/// The caller supplies a trusted Generation reference after Workspace authorization.
#[must_use]
pub fn declares_read_only(generation: ColumnRef, operation: ColumnRef) -> SimpleExpr {
    retained_operation_where(generation, operation, col("read_only_hint").eq(true))
}

/// Whether the exact retained Operation is HTTP with a safe method; see [`HttpMethod::is_safe`].
/// The caller supplies a trusted Generation reference after Workspace authorization.
#[must_use]
pub fn has_safe_http_method(generation: ColumnRef, operation: ColumnRef) -> SimpleExpr {
    retained_operation_where(
        generation,
        operation,
        col("method").is_in(HttpMethod::SAFE.map(HttpMethod::as_str)),
    )
}

fn col(name: &str) -> Expr {
    Expr::col((Alias::new("retained_operation"), Alias::new(name)))
}

fn retained_operation_where(
    generation: ColumnRef,
    operation: ColumnRef,
    condition: SimpleExpr,
) -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("materialized_operations"),
                Alias::new("retained_operation"),
            )
            .and_where(col("provider_generation_id").eq(Expr::col(generation)))
            .and_where(col("operation_key").eq(Expr::col(operation)))
            .and_where(condition)
            .to_owned(),
    )
}
