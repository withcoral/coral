//! Persistence encoding of authored source locations on Invocations and Events.
//!
//! Both tables keep the same four nullable columns. All four are NULL when no
//! location was recorded, including every record written before them.

use sea_query::{Alias, Expr, Value};

use crate::{Error, SourceDocument, SourceLocation};

const PROGRAM: &str = "program";
const SAVED_FUNCTION: &str = "saved_function";

/// Column names, in the order [`values`] returns them.
pub(crate) const COLUMNS: [&str; 4] = [
    "source_document",
    "source_saved_function_id",
    "source_line",
    "source_column",
];

/// Columns for one insert; NULL in every column when `location` is absent.
pub(crate) fn values(location: Option<&SourceLocation>) -> [(Alias, Expr); 4] {
    let [
        document_column,
        saved_function_column,
        line_column,
        column_column,
    ] = COLUMNS;
    let [document, saved_function_id, line, column] = row_values(location);
    [
        (Alias::new(document_column), Expr::val(document)),
        (
            Alias::new(saved_function_column),
            Expr::val(saved_function_id),
        ),
        (Alias::new(line_column), Expr::val(line)),
        (Alias::new(column_column), Expr::val(column)),
    ]
}

/// The [`COLUMNS`] values of one row, in order. An absent location is a NULL
/// of each column's own type, so a multi-row `VALUES` insert whose rows all
/// lack a location still types every column.
pub(crate) fn row_values(location: Option<&SourceLocation>) -> [Value; 4] {
    let (document, saved_function_id, line, column) = match location {
        None => (None, None, None, None),
        Some(location) => {
            let (document, saved_function_id) = match &location.document {
                SourceDocument::Program => (PROGRAM, None),
                SourceDocument::SavedFunction { saved_function_id } => {
                    (SAVED_FUNCTION, Some(saved_function_id.clone()))
                }
            };
            (
                Some(document),
                saved_function_id,
                Some(i64::from(location.line)),
                Some(i64::from(location.column)),
            )
        }
    };
    [
        Value::from(document.map(str::to_string)),
        Value::from(saved_function_id),
        Value::from(line),
        Value::from(column),
    ]
}

/// The four location columns of one selected row.
#[derive(Debug, sqlx::FromRow)]
#[expect(
    clippy::struct_field_names,
    reason = "fields are the shared column names that every row query selects"
)]
pub(crate) struct StoredSourceLocation {
    source_document: Option<String>,
    source_saved_function_id: Option<String>,
    source_line: Option<i64>,
    source_column: Option<i64>,
}

impl StoredSourceLocation {
    pub(crate) fn decode(self) -> Result<Option<SourceLocation>, Error> {
        let corrupt = || Error::CorruptData("record has an invalid source location".to_string());
        let Some(document) = self.source_document else {
            return if self.source_saved_function_id.is_none()
                && self.source_line.is_none()
                && self.source_column.is_none()
            {
                Ok(None)
            } else {
                Err(corrupt())
            };
        };
        let document = match (document.as_str(), self.source_saved_function_id) {
            (PROGRAM, None) => SourceDocument::Program,
            (SAVED_FUNCTION, Some(saved_function_id)) if !saved_function_id.is_empty() => {
                SourceDocument::SavedFunction { saved_function_id }
            }
            _ => return Err(corrupt()),
        };
        let position = |value: Option<i64>| {
            value
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(corrupt)
        };
        Ok(Some(SourceLocation {
            document,
            line: position(self.source_line)?,
            column: position(self.source_column)?,
        }))
    }
}
