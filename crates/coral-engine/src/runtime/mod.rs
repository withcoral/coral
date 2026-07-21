//! Source-runtime orchestration: registration into `DataFusion`, system catalog
//! tables, and schema plumbing.

use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;

pub(crate) const DATAFUSION_DEFAULT_CATALOG: &str = "datafusion";

/// Removes `DataFusion`'s synthetic default catalog from a table qualifier.
/// Schema-backed Coral tables store an empty catalog name in metadata, even
/// when `DataFusion` expands an explicit reference to `datafusion.schema.table`.
pub(crate) fn non_default_catalog_name(catalog_name: Option<&str>) -> Option<&str> {
    catalog_name.filter(|name| !name.eq_ignore_ascii_case(DATAFUSION_DEFAULT_CATALOG))
}

pub(crate) mod catalog;
pub(crate) mod dependent_join;
pub(crate) mod error;
pub(crate) mod json;
pub(crate) mod memory;
pub(crate) mod pattern_validator;
pub(crate) mod query;
pub(crate) mod query_planner;
pub(crate) mod registry;
pub(crate) mod schema_provider;
pub(crate) mod scoped_table_functions;
pub(crate) mod source_functions;
pub(crate) mod udf_calls;
pub(crate) mod udfs;

fn literal_scalar_value(expr: &Expr) -> Result<Option<ScalarValue>> {
    match unalias(expr) {
        Expr::Literal(value, _) => Ok(Some(value.clone())),
        Expr::Cast(cast) => Ok(literal_scalar_value(&cast.expr)?
            .map(|value| value.cast_to(cast.field.data_type()))
            .transpose()?),
        Expr::TryCast(cast) => {
            let Some(value) = literal_scalar_value(&cast.expr)? else {
                return Ok(None);
            };
            Ok(Some(value.cast_to(cast.field.data_type()).unwrap_or(
                ScalarValue::try_new_null(cast.field.data_type())?,
            )))
        }
        Expr::Negative(expr) => Ok(literal_scalar_value(expr)?
            .map(|value| value.arithmetic_negate())
            .transpose()?),
        _ => Ok(None),
    }
}

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}
