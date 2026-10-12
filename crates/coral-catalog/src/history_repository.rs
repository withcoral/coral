//! Payload-free retained Operation facts for owning execution repositories.
use sea_query::{Alias, ColumnRef, Expr, ExprTrait as _, Query, SimpleExpr};

/// Whether the exact retained Operation declared that it changes nothing.
/// The caller supplies a trusted Generation reference after Workspace authorization.
#[must_use]
pub fn declares_read_only(generation: ColumnRef, operation: ColumnRef) -> SimpleExpr {
    let col = |name: &str| Expr::col((Alias::new("retained_operation"), Alias::new(name)));
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(
                Alias::new("materialized_operations"),
                Alias::new("retained_operation"),
            )
            .and_where(col("provider_generation_id").eq(Expr::col(generation)))
            .and_where(col("operation_key").eq(Expr::col(operation)))
            .and_where(col("read_only_hint").eq(true))
            .to_owned(),
    )
}
