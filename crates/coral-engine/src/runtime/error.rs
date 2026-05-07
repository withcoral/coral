//! Shared runtime-error normalization for source compilation and registration.

use datafusion::common::{Column, SchemaError, Span, TableReference};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{ObjectName, ObjectNamePart};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::backends::http::ProviderQueryError;
use crate::contracts::{ColumnParts, StructuredQueryError, TableRefParts};
use crate::{CoreError, SourceDecoratorError, TableInfo};

pub(crate) fn datafusion_to_core(error: &DataFusionError, tables: &[TableInfo]) -> CoreError {
    datafusion_to_core_with_sql(error, tables, None)
}

pub(crate) fn datafusion_to_core_with_sql(
    error: &DataFusionError,
    tables: &[TableInfo],
    sql: Option<&str>,
) -> CoreError {
    // Unwrap Context/Shared/Diagnostic wrappers so wrapped schema errors
    // get classified by their root variant instead of all landing in the
    // `Internal` bucket. Without `find_root()`, `SELECT bogus FROM wide`
    // surfaces as `CoreError::Internal` because DataFusion wraps the
    // SchemaError in `Context`/`Execution`, hiding the structured variant
    // from the match arms below.
    match error.find_root() {
        DataFusionError::SQL(detail, _) => CoreError::InvalidInput(detail.to_string()),
        DataFusionError::Plan(detail) => plan_error_to_core(detail, error, tables, sql),
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

fn plan_error_to_core(
    detail: &str,
    error: &DataFusionError,
    tables: &[TableInfo],
    sql: Option<&str>,
) -> CoreError {
    if let Some(table_ref) = table_not_found_ref(error, detail, sql) {
        return CoreError::QueryFailure(Box::new(StructuredQueryError::table_not_found(
            &table_ref, tables,
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

/// Extracts the missing table reference from a `DataFusion` table-not-found
/// planning error.
///
/// `DataFusion` 53 does not expose a structured missing-relation variant; it
/// currently emits a `Plan` error and, for SQL relation resolution, attaches a
/// diagnostic span covering the table reference. Prefer reparsing that exact
/// SQL span so quoted identifiers containing dots remain one component. The
/// formatted-message parser is retained only for `DataFusion` paths that do not
/// carry a span, such as direct session catalog lookup.
fn table_not_found_ref(
    error: &DataFusionError,
    detail: &str,
    sql: Option<&str>,
) -> Option<TableRefParts> {
    let spanned_sql_ref = sql
        .zip(error.diagnostic().and_then(|diagnostic| diagnostic.span))
        .and_then(|(sql, span)| sql_span(sql, span))
        .and_then(table_ref_parts_from_sql_object);

    if let Some(table_ref) = spanned_sql_ref
        && looks_like_table_not_found(detail)
    {
        return Some(table_ref);
    }

    if !is_legacy_table_not_found_plan(detail) {
        return None;
    }

    extract_table_not_found(detail)
        .and_then(table_ref_parts_from_sql_object)
        .or_else(|| {
            extract_table_not_found(detail)
                .map(|raw| TableRefParts::new(raw.split('.').map(ToString::to_string).collect()))
        })
}

fn looks_like_table_not_found(detail: &str) -> bool {
    let lowered = detail.to_lowercase();
    lowered.contains("table")
        && lowered.contains("not found")
        && !lowered.contains("table function")
}

fn is_legacy_table_not_found_plan(detail: &str) -> bool {
    extract_table_not_found(detail).is_some()
}

fn extract_table_not_found(detail: &str) -> Option<&str> {
    let rest = detail.strip_prefix("table '")?;
    rest.strip_suffix("' not found")
}

fn sql_span(sql: &str, span: Span) -> Option<&str> {
    if span.start.line != span.end.line || span.start.line == 0 {
        return None;
    }
    let line_index = usize::try_from(span.start.line - 1).ok()?;
    let start = usize::try_from(span.start.column - 1).ok()?;
    let end = usize::try_from(span.end.column - 1).ok()?;
    let line = sql.lines().nth(line_index)?;
    if start >= end {
        return None;
    }
    byte_range_for_char_range(line, start, end).and_then(|range| line.get(range))
}

fn byte_range_for_char_range(
    value: &str,
    start: usize,
    end: usize,
) -> Option<std::ops::Range<usize>> {
    let start_byte = value
        .char_indices()
        .nth(start)
        .map_or(value.len(), |(index, _)| index);
    let end_byte = value
        .char_indices()
        .nth(end)
        .map_or(value.len(), |(index, _)| index);
    (start_byte < end_byte && end_byte <= value.len()).then_some(start_byte..end_byte)
}

fn table_ref_parts_from_sql_object(raw: &str) -> Option<TableRefParts> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(raw).ok()?;
    let object_name = parser.parse_object_name(true).ok()?;
    table_ref_parts_from_object_name(object_name)
}

fn table_ref_parts_from_object_name(object_name: ObjectName) -> Option<TableRefParts> {
    let parts = object_name
        .0
        .into_iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(match ident.quote_style {
                Some(_) => ident.value,
                None => ident.value.to_lowercase(),
            }),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some(TableRefParts::new(parts))
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
        let error = DataFusionError::Plan("syntax error at position 12".to_string());
        let core = plan_error_to_core("syntax error at position 12", &error, &[], None);
        match core {
            CoreError::InvalidInput(detail) => assert!(detail.contains("syntax error")),
            other => panic!("expected CoreError::InvalidInput, got {other:?}"),
        }
    }
}
