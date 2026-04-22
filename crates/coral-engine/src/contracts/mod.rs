//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;
mod query_error;

pub use catalog::{ColumnInfo, TableInfo};
pub use error::{CoreError, StatusCode, StructuredQueryError};
pub use query::{
    QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource, QueryTestFailure,
    QueryTestResult, QueryTestSuccess, SourceValidationReport,
};
pub(crate) use query_error::ColumnParts;

#[cfg(test)]
pub(crate) use query_error::UNKNOWN_COLUMN_REASON;
