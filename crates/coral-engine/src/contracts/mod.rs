//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;
mod query_error;
mod udfs;

pub use catalog::{
    CatalogInfo, ColumnInfo, DescribeTableInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo, UniversalSearchAuthorizationDecision,
    UniversalSearchAuthorizationInfo, UniversalSearchAuthorizationOrigin,
};
pub use error::{CoreError, StatusCode, StructuredQueryError};
pub use query::{
    DependentJoinConfig, DependentJoinSourceConfig, EffectiveDependentJoinConfig, MemorySize,
    QueryCancellationToken, QueryExecution, QueryExecutionControls, QueryExecutionFailureKind,
    QueryExecutionProvenance, QueryMemoryConfig, QueryPaginationPolicy, QueryParameterValue,
    QueryParameters, QueryPlan, QueryRetryPolicy, QueryRuntimeConfig, QueryRuntimeContext,
    QuerySource, QueryTableFunctionUsage, QueryTableUsage, QueryTestFailure, QueryTestResult,
    QueryTestSuccess, ResolvedQueryResources, RuntimeSourceComponent, RuntimeSourcePackage,
    RuntimeTableFunctionAuthorizationInfo, SourceValidationReport,
};
pub(crate) use query_error::{ColumnParts, TableRefParts};
pub use udfs::{
    UdfRuntimeArgument, UdfRuntimeDefinition, UdfRuntimeImplementation, UdfRuntimePublish,
    UdfRuntimeResultColumn, UdfRuntimeSignature, UdfRuntimeSqlDefinition,
    UdfRuntimeTableFunctionPublish,
};

#[cfg(test)]
pub(crate) use query_error::UNKNOWN_COLUMN_REASON;
