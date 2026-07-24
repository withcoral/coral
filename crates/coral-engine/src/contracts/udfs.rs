//! Engine runtime UDF contracts supplied by the app layer.

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

impl UdfRuntimeSqlDefinition {
    /// Returns the SQL body for this definition.
    #[must_use]
    pub fn sql(&self) -> &str {
        let UdfRuntimeImplementation::CoralSql { query } = &self.implementation;
        query
    }
}

/// One validated UDF made available to the query runtime.
#[derive(Debug, Clone)]
pub struct UdfRuntimeDefinition {
    /// Stable UDF id within one workspace.
    pub name: String,
    /// User-facing UDF description.
    pub description: String,
    /// Typed arguments accepted by the UDF.
    pub arguments: Vec<UdfRuntimeArgument>,
    /// Executable UDF implementation.
    pub implementation: UdfRuntimeImplementation,
    /// Public surfaces requested by the UDF.
    pub publish: UdfRuntimePublish,
    /// Columns inferred by planning the UDF SQL body.
    pub result_columns: Vec<UdfRuntimeResultColumn>,
}

/// One typed UDF argument.
#[derive(Debug, Clone)]
pub struct UdfRuntimeArgument {
    /// Argument name.
    pub name: String,
    /// Argument type in manifest spelling.
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

/// Public surfaces requested by one UDF.
#[derive(Debug, Clone)]
pub struct UdfRuntimePublish {
    /// Canonical public SQL table-function wrapper.
    pub table_function: UdfRuntimeTableFunctionPublish,
}

/// Canonical SQL table-function surface for one UDF.
#[derive(Debug, Clone)]
pub struct UdfRuntimeTableFunctionPublish {
    /// SQL schema.
    pub schema: String,
    /// SQL function name.
    pub name: String,
    /// Optional publish-target-specific description.
    pub description: String,
    /// Query guidance for the published table function.
    pub guide: String,
}
