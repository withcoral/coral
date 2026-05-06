//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;
mod query_error;
mod statistics;

pub use catalog::{
    CatalogInfo, ColumnInfo, DescribeTableInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
pub use error::{CoreError, StatusCode, StructuredQueryError};
pub use query::{
    DependentJoinConfig, DependentJoinSourceConfig, EffectiveDependentJoinConfig, MemorySize,
    QueryExecution, QueryExecutionProvenance, QueryMemoryConfig, QueryParameterValue,
    QueryParameters, QueryPlan, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    QueryTableFunctionUsage, QueryTableUsage, QueryTestFailure, QueryTestResult, QueryTestSuccess,
    RuntimeSourceComponent, RuntimeSourcePackage, SourceValidationReport,
};
pub(crate) use query_error::{ColumnParts, TableRefParts};
pub use statistics::{
    ColumnSchemaSignature, ColumnStatistics, ColumnStatisticsObservation, SourceStatistics,
    StatisticPrecision, StatisticValue, StatisticsObservation, StatisticsObservationScope,
    StatisticsProfile, TableSchemaSignature, TableStatistics,
};

#[cfg(test)]
pub(crate) use query_error::UNKNOWN_COLUMN_REASON;
