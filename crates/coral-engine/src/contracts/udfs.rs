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
