//! Reviewable contracts for the management-plane to data-plane seam.

mod catalog;
mod error;
mod query;
mod query_error;
mod runtime_catalog;
mod udfs;

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
    ResolvedQueryResources, RuntimeSourcePackage, SourceValidationReport,
};
pub(crate) use query_error::{ColumnParts, TableRefParts};
pub(crate) use runtime_catalog::RuntimeBackendManifest;
pub use runtime_catalog::{
    DatabaseProviderDiscoveredRuntimeCatalog, DatabaseRuntimeBackend, DeclaredRuntimeCatalog,
    FileDeclaredRuntimeCatalog, FileRuntimeBackend, FileRuntimeRelation, FileRuntimeTableRelation,
    HttpDeclaredRuntimeCatalog, HttpRuntimeBackend, HttpRuntimeRelation,
    HttpRuntimeTableFunctionRelation, HttpRuntimeTableRelation, McpDeclaredRuntimeCatalog,
    McpRuntimeBackend, McpRuntimeRelation, McpRuntimeTableFunctionRelation,
    McpRuntimeTableRelation, ProviderDiscoveredRuntimeCatalog, RuntimeCatalog, RuntimeRelationKind,
    RuntimeRelationRef,
};
pub use udfs::{
    UdfRuntimeArgument, UdfRuntimeDefinition, UdfRuntimeImplementation, UdfRuntimePublish,
    UdfRuntimeResultColumn, UdfRuntimeSignature, UdfRuntimeSqlDefinition,
    UdfRuntimeTableFunctionPublish,
};

#[cfg(test)]
pub(crate) use query_error::UNKNOWN_COLUMN_REASON;
