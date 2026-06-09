//! SQL projection contributor and SQL runtime ownership boundary.
//!
//! `DataFusion` lives here because SQL is one export consumer/runtime, not the
//! central Coral engine.

#![allow(
    missing_docs,
    reason = "Serializable SQL projection contract fields live in coral-exports; this crate documents behavior through tests."
)]

mod error;
mod info;
mod metadata;
mod projection;
mod runtime;
mod table_provider;
mod validation;

pub use error::{SqlError, SqlResult, StatusCode};
pub use metadata::{
    ColumnInfo, QueryExecution, QueryPlan, QueryTestFailure, QueryTestResult, QueryTestSuccess,
    SourceValidationReport, SqlMetadataInfo, SqlTableLookup, TableFunctionArgumentInfo,
    TableFunctionInfo, TableFunctionResultColumnInfo, TableInfo,
};
pub use projection::{
    SqlBindingContributor, datafusion_runtime_type_name, generate_sql_bindings,
    upstream_plan_type_name,
};
pub use runtime::{SqlProviderInvocation, SqlProviderInvoker, SqlRuntimeBinding, SqlWorkspace};
pub use validation::validate_read_only_sql;

#[cfg(test)]
mod tests;
