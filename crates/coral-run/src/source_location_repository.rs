//! Persistence encoding of authored source locations on Invocations and Events.
//!
//! Both tables keep the same five nullable columns. All five are NULL when no
//! location was recorded, including every record written before them.

use sea_query::{Alias, Expr, Value};

use crate::{Error, SourceDocument, SourceLocation};

const PROGRAM: &str = "program";
const PRIVATE_TOOL: &str = "private_tool";
const SHARED_TOOL: &str = "shared_tool";

/// Column names, in the order [`values`] returns them.
pub(crate) const COLUMNS: [&str; 5] = [
    "source_document",
    "source_private_tool_id",
    "source_shared_tool_id",
    "source_line",
    "source_column",
];

/// Columns for one insert; NULL in every column when `location` is absent.
pub(crate) fn values(location: Option<&SourceLocation>) -> [(Alias, Expr); 5] {
    let mut values = row_values(location).into_iter();
    COLUMNS.map(|column| {
        (
            Alias::new(column),
            Expr::val(values.next().unwrap_or(Value::String(None))),
        )
    })
}

/// The [`COLUMNS`] values of one row, in order. An absent location is a NULL
/// of each column's own type, so a multi-row `VALUES` insert whose rows all
/// lack a location still types every column.
pub(crate) fn row_values(location: Option<&SourceLocation>) -> [Value; 5] {
    let (document, private_tool_id, shared_tool_id, line, column) = match location {
        None => (None, None, None, None, None),
        Some(location) => {
            let (document, private_tool_id, shared_tool_id) = match &location.document {
                SourceDocument::Program => (PROGRAM, None, None),
                SourceDocument::PrivateTool { private_tool_id } => {
                    (PRIVATE_TOOL, Some(private_tool_id.clone()), None)
                }
                SourceDocument::SharedTool { shared_tool_id } => {
                    (SHARED_TOOL, None, Some(shared_tool_id.clone()))
                }
            };
            (
                Some(document),
                private_tool_id,
                shared_tool_id,
                Some(i64::from(location.line)),
                Some(i64::from(location.column)),
            )
        }
    };
    [
        Value::from(document.map(str::to_string)),
        Value::from(private_tool_id),
        Value::from(shared_tool_id),
        Value::from(line),
        Value::from(column),
    ]
}

/// The five location columns of one selected row.
#[derive(Debug, sqlx::FromRow)]
#[expect(
    clippy::struct_field_names,
    reason = "fields are the shared column names that every row query selects"
)]
pub(crate) struct StoredSourceLocation {
    source_document: Option<String>,
    source_private_tool_id: Option<String>,
    source_shared_tool_id: Option<String>,
    source_line: Option<i64>,
    source_column: Option<i64>,
}

impl StoredSourceLocation {
    pub(crate) fn decode(self) -> Result<Option<SourceLocation>, Error> {
        let corrupt = || Error::CorruptData("record has an invalid source location".to_string());
        let Some(document) = self.source_document else {
            return if self.source_private_tool_id.is_none()
                && self.source_shared_tool_id.is_none()
                && self.source_line.is_none()
                && self.source_column.is_none()
            {
                Ok(None)
            } else {
                Err(corrupt())
            };
        };
        let document = match (
            document.as_str(),
            self.source_private_tool_id,
            self.source_shared_tool_id,
        ) {
            (PROGRAM, None, None) => SourceDocument::Program,
            (PRIVATE_TOOL, Some(private_tool_id), None) if !private_tool_id.is_empty() => {
                SourceDocument::PrivateTool { private_tool_id }
            }
            (SHARED_TOOL, None, Some(shared_tool_id)) if !shared_tool_id.is_empty() => {
                SourceDocument::SharedTool { shared_tool_id }
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
