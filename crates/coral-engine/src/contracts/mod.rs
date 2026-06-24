//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;
mod query_error;
mod saved_functions;

pub use catalog::{
    CatalogInfo, ColumnInfo, DescribeTableInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
pub use error::{CoreError, StatusCode, StructuredQueryError};
pub use query::{
    DependentJoinConfig, DependentJoinSourceConfig, EffectiveDependentJoinConfig, MemorySize,
    QueryExecution, QueryMemoryConfig, QueryParameterValue, QueryParameters, QueryPlan,
    QueryRuntimeConfig, QueryRuntimeContext, QuerySource, QueryTestFailure, QueryTestResult,
    QueryTestSuccess, RuntimeSourceComponent, RuntimeSourcePackage, SourceValidationReport,
};
pub(crate) use query_error::{ColumnParts, TableRefParts};
pub use saved_functions::{
    SavedFunctionRuntimeArgument, SavedFunctionRuntimeArgumentType, SavedFunctionRuntimeDefinition,
    SavedFunctionRuntimeImplementation,
};

#[cfg(test)]
pub(crate) use query_error::UNKNOWN_COLUMN_REASON;
