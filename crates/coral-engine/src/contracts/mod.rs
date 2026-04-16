//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;

pub use catalog::{ColumnInfo, TableInfo};
pub use error::{CoreError, StatusCode};
pub use query::{QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource};
