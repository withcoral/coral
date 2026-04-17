//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;
mod query_error;

pub use catalog::{ColumnInfo, TableInfo};
<<<<<<< HEAD
pub use error::{CoreError, StatusCode, StructuredQueryError};
pub use query::{QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource};
||||||| parent of 68e2039 (Make validation-query results explicit success/failure variants)
pub use error::{CoreError, StatusCode};
pub use query::{
    QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource, QueryTestResult,
    SourceValidationOutcome,
};
=======
pub use error::{CoreError, StatusCode};
pub use query::{
    QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource, QueryTestOutcome,
    QueryTestResult, SourceValidationOutcome,
};
>>>>>>> 68e2039 (Make validation-query results explicit success/failure variants)
