//! Shared runtime-error normalization for source compilation and registration.

use datafusion::common::{Column, SchemaError, TableReference};
use datafusion::error::DataFusionError;

use crate::backends::http::ProviderQueryError;
use crate::contracts::{ColumnParts, StructuredQueryError};
use crate::{CoreError, SourceDecoratorError, TableInfo};

pub(crate) fn datafusion_to_core(error: &DataFusionError, tables: &[TableInfo]) -> CoreError {
    // Unwrap Context/Shared/Diagnostic wrappers so wrapped schema errors
    // get classified by their root variant instead of all landing in the
    // `Internal` bucket. Without `find_root()`, `SELECT bogus FROM wide`
    // surfaces as `CoreError::Internal` because DataFusion wraps the
    // SchemaError in `Context`/`Execution`, hiding the structured variant
    // from the match arms below.
    match error.find_root() {
        DataFusionError::SQL(detail, _) => CoreError::InvalidInput(detail.to_string()),
        DataFusionError::Plan(detail) => plan_error_to_core(detail, tables),
        DataFusionError::SchemaError(schema_error, _) => schema_error_to_core(schema_error),
        DataFusionError::NotImplemented(detail) => CoreError::Unimplemented(detail.clone()),
        DataFusionError::External(inner) => {
            if let Some(provider_error) = inner.downcast_ref::<ProviderQueryError>() {
                return provider_error_to_core(provider_error);
            }
            if let Some(source_decorator_error) = inner.downcast_ref::<SourceDecoratorError>() {
                return source_decorator_error_to_core(source_decorator_error);
            }
            CoreError::internal(inner.to_string())
        }
        DataFusionError::ObjectStore(err) => CoreError::Unavailable(err.to_string()),
        DataFusionError::ResourcesExhausted(detail) => CoreError::Unavailable(detail.clone()),
        other => CoreError::internal(other.to_string()),
    }
}

pub(crate) fn source_decorator_error_to_core(error: &SourceDecoratorError) -> CoreError {
    match error {
        SourceDecoratorError::InvalidInput(detail) => CoreError::InvalidInput(detail.clone()),
        SourceDecoratorError::FailedPrecondition(detail) => {
            CoreError::FailedPrecondition(detail.clone())
        }
    }
}

fn plan_error_to_core(detail: &str, tables: &[TableInfo]) -> CoreError {
    if let Some(table_ref) = extract_table_not_found(detail) {
        return CoreError::QueryFailure(Box::new(StructuredQueryError::table_not_found(
            table_ref, tables,
        )));
    }
    CoreError::InvalidInput(detail.to_string())
}

fn schema_error_to_core(schema_error: &SchemaError) -> CoreError {
    if let SchemaError::FieldNotFound {
        field,
        valid_fields,
    } = schema_error
    {
        let missing = column_to_parts(field);
        let valid: Vec<ColumnParts> = valid_fields.iter().map(column_to_parts).collect();
        return CoreError::QueryFailure(Box::new(StructuredQueryError::unknown_column(
            &missing, &valid,
        )));
    }
    CoreError::InvalidInput(schema_error.to_string())
}

/// Converts a `DataFusion` `Column` into structure-preserving parts.
///
/// `Column` carries its qualifier as a `TableReference` (Bare / Partial /
/// Full) and the bare name as a plain `String` — literal dots inside the
/// name stay inside the name. Preserving that separation here is what lets
/// downstream hint rendering distinguish `.` as a qualifier from `.` as a
/// character in a quoted identifier.
fn column_to_parts(column: &Column) -> ColumnParts {
    let relation: Vec<String> = column
        .relation
        .as_ref()
        .map(|reference| match reference {
            TableReference::Bare { table } => vec![table.to_string()],
            TableReference::Partial { schema, table } => {
                vec![schema.to_string(), table.to_string()]
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
        })
        .unwrap_or_default();
    ColumnParts {
        relation,
        name: column.name.clone(),
    }
}

/// Extracts the table reference from a `"table 'xxx' not found"` plan error.
///
/// `DataFusion` raises this literal form from both the SQL-level relation
/// resolver and the session-state catalog lookup when a referenced table
/// isn't in the catalog. `DataFusionError::source` for the `Diagnostic`
/// variant returns the inner error, so `find_root()` unwraps any
/// diagnostic decoration before we match on the `Plan` string.
///
/// The exact prefix/suffix are load-bearing: the integration tests in
/// `tests/engine/structured_error_tests.rs` exercise this path end-to-end,
/// so any upstream wording change surfaces as a concrete test failure
/// rather than silent mis-classification.
fn extract_table_not_found(detail: &str) -> Option<&str> {
    let rest = detail.strip_prefix("table '")?;
    rest.strip_suffix("' not found")
}

fn provider_error_to_core(error: &ProviderQueryError) -> CoreError {
    CoreError::QueryFailure(Box::new(error.to_structured()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::UNKNOWN_COLUMN_REASON;

    #[test]
    fn datafusion_to_core_unwraps_context_wrapped_schema_error_to_structured() {
        let schema_err = Box::new(SchemaError::FieldNotFound {
            field: Box::new(Column::new_unqualified("user_login")),
            valid_fields: vec![
                Column::new_unqualified("user__login"),
                Column::new_unqualified("title"),
            ],
        });
        let inner = DataFusionError::SchemaError(schema_err, Box::new(None));
        let wrapped = DataFusionError::Context("wrapping context".to_string(), Box::new(inner));

        let core = datafusion_to_core(&wrapped, &[]);

        match core {
            CoreError::QueryFailure(sqe) => {
                assert_eq!(sqe.reason(), UNKNOWN_COLUMN_REASON);
                assert!(sqe.summary().contains("user_login"));
            }
            other => panic!("expected CoreError::QueryFailure, got {other:?}"),
        }
    }

    #[test]
    fn extract_table_not_found_matches_datafusion_format() {
        assert_eq!(
            extract_table_not_found("table 'hockey.master' not found"),
            Some("hockey.master")
        );
        assert_eq!(
            extract_table_not_found("table 'foo' not found"),
            Some("foo")
        );
        assert_eq!(extract_table_not_found("something else"), None);
    }

    #[test]
    fn plan_error_without_table_prefix_is_invalid_input() {
        let core = plan_error_to_core("syntax error at position 12", &[]);
        match core {
            CoreError::InvalidInput(detail) => assert!(detail.contains("syntax error")),
            other => panic!("expected CoreError::InvalidInput, got {other:?}"),
        }
    }
}
