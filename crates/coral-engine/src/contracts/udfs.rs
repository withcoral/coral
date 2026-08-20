//! Executable Coral SQL function contracts supplied by the app layer.

use coral_spec::ManifestDataType;

/// Typed function signature inferred by planning its Coral SQL query.
#[derive(Debug, Clone)]
pub struct CoralSqlFunctionSignature {
    /// Arguments referenced by the function query.
    pub arguments: Vec<CoralSqlFunctionArgument>,
    /// Columns returned by the function query.
    pub result_columns: Vec<CoralSqlResultColumn>,
    /// Canonical installed source names referenced by the function query.
    pub source_names: Vec<String>,
}

/// One Coral SQL query to validate before runtime registration.
#[derive(Debug, Clone)]
pub struct CoralSqlFunctionInferenceDefinition {
    /// Stable function id within one workspace.
    pub name: String,
    /// Read-only Coral SQL query using `DataFusion` value parameters like `$argument`.
    pub query: String,
}

/// One executable Coral SQL function made available to the query runtime.
#[derive(Debug, Clone)]
pub struct CoralSqlFunctionDefinition {
    /// Stable function id within one workspace.
    pub name: String,
    /// User-facing function description.
    pub description: String,
    /// Typed arguments accepted by the function.
    pub arguments: Vec<CoralSqlFunctionArgument>,
    /// Read-only Coral SQL query executed after typed argument binding.
    pub query: String,
    /// Public SQL table-function target.
    pub publish: CoralSqlTableFunctionPublish,
    /// Columns inferred by planning the function query.
    pub result_columns: Vec<CoralSqlResultColumn>,
    /// Canonical installed source names referenced by the function query.
    pub source_names: Vec<String>,
}

/// One typed Coral SQL function argument.
#[derive(Debug, Clone)]
pub struct CoralSqlFunctionArgument {
    /// Argument name.
    pub name: String,
    /// Argument type in manifest spelling.
    pub data_type: ManifestDataType,
}

/// One column returned by a Coral SQL table function.
#[derive(Debug, Clone)]
pub struct CoralSqlResultColumn {
    /// Column name.
    pub name: String,
    /// Arrow/DataFusion type expected for the column.
    pub data_type: arrow::datatypes::DataType,
    /// Whether the column can contain null values.
    pub nullable: bool,
}

/// Canonical SQL table-function surface for one Coral SQL function.
#[derive(Debug, Clone)]
pub struct CoralSqlTableFunctionPublish {
    /// SQL schema.
    pub schema: String,
    /// SQL function name.
    pub name: String,
    /// Optional publish-target-specific description.
    pub description: String,
    /// Query guidance for the published table function.
    pub guide: String,
}
