//! Engine runtime UDF contracts supplied by the app layer.
//!
//! These are not the user-authored UDF file format. The app/spec layers
//! parse and validate authored UDFs, then supply these runtime definitions
//! to the engine for planning and execution.

use coral_spec::ManifestDataType;

/// Typed UDF signature inferred by planning its SQL body.
#[derive(Debug, Clone)]
pub struct UdfRuntimeSignature {
    /// Arguments referenced by the UDF SQL body.
    pub arguments: Vec<UdfRuntimeArgument>,
    /// Columns returned by the UDF SQL body.
    pub result_columns: Vec<UdfRuntimeResultColumn>,
}

/// One SQL UDF body to validate before runtime registration.
#[derive(Debug, Clone)]
pub struct UdfRuntimeSqlDefinition {
    /// Stable UDF id within one workspace.
    pub name: String,
    /// Executable UDF implementation.
    pub implementation: UdfRuntimeImplementation,
}

/// One validated UDF made available to the query runtime.
#[derive(Debug, Clone)]
pub struct UdfRuntimeDefinition {
    /// Stable UDF id within one workspace.
    pub name: String,
    /// Typed arguments accepted by the UDF.
    pub arguments: Vec<UdfRuntimeArgument>,
    /// Executable UDF implementation.
    pub implementation: UdfRuntimeImplementation,
    /// Columns inferred by planning the UDF SQL body.
    pub result_columns: Vec<UdfRuntimeResultColumn>,
}

/// One typed UDF argument.
#[derive(Debug, Clone)]
pub struct UdfRuntimeArgument {
    /// Argument name.
    pub name: String,
    /// Declared argument type in manifest spelling. `Json` and `Timestamp`
    /// can only originate from source-function arg bindings; parameters
    /// inferred from casts or comparisons are always `Utf8`/`Int64`/
    /// `Float64`/`Boolean`. All string-shaped types bind as string SQL
    /// parameters at execution.
    pub data_type: ManifestDataType,
}

/// One column returned by a UDF table function.
#[derive(Debug, Clone)]
pub struct UdfRuntimeResultColumn {
    /// Column name.
    pub name: String,
    /// Arrow/DataFusion type expected for the column.
    pub data_type: arrow::datatypes::DataType,
    /// Whether the column can contain null values.
    pub nullable: bool,
}

/// Executable UDF implementation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum UdfRuntimeImplementation {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    CoralSql {
        /// SQL query executed by Coral after typed argument binding.
        query: String,
    },
}
