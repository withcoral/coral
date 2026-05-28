//! Typed query-visible catalog metadata.

/// Describes one queryable column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Data type rendered in `Arrow`/`DataFusion` string form.
    pub data_type: String,
    /// Whether the column can contain null values.
    pub nullable: bool,
    /// Whether the column is provider-derived metadata, such as a filter or computed column.
    pub is_virtual: bool,
    /// Whether the column must be constrained before querying the table.
    pub is_required_filter: bool,
    /// User-facing column description.
    pub description: String,
    /// Zero-based position of the column within the table.
    pub ordinal_position: u32,
}

/// Describes one queryable table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// `SQL` schema name.
    pub schema_name: String,
    /// Table name within the schema.
    pub table_name: String,
    /// User-facing table description.
    pub description: String,
    /// User-facing query guidance.
    pub guide: String,
    /// Exposed columns for the table.
    pub columns: Vec<ColumnInfo>,
    /// Required filter names for the table.
    pub required_filters: Vec<String>,
}

/// Describes the queryable catalog exposed by one runtime snapshot.
#[derive(Debug, Clone)]
pub struct CatalogInfo {
    /// Queryable tables.
    pub tables: Vec<TableInfo>,
    /// Source-scoped table functions.
    pub table_functions: Vec<TableFunctionInfo>,
    /// Source-declared prepared SQL statements.
    pub prepared_statements: Vec<PreparedStatementInfo>,
}

/// Describes one argument accepted by a source-declared prepared statement.
#[derive(Debug, Clone)]
pub struct PreparedStatementArgumentInfo {
    /// Argument name from the source spec.
    pub name: String,
    /// Data type rendered in source-spec form.
    pub data_type: String,
}

/// Describes one source-declared prepared SQL statement.
#[derive(Debug, Clone)]
pub struct PreparedStatementInfo {
    /// `SQL` schema/source name.
    pub schema_name: String,
    /// Public source-scoped prepared statement name.
    pub statement_name: String,
    /// Runtime `DataFusion` prepared statement name used with `EXECUTE`.
    pub execute_name: String,
    /// User-facing statement description.
    pub description: String,
    /// Positional arguments accepted by this statement.
    pub arguments: Vec<PreparedStatementArgumentInfo>,
    /// SQL text prepared by the source.
    pub sql: String,
}

/// Describes one argument accepted by a source-scoped table function.
#[derive(Debug, Clone)]
pub struct TableFunctionArgumentInfo {
    /// Argument name as used in a named SQL function call.
    pub name: String,
    /// Whether callers must provide this argument.
    pub required: bool,
    /// Allowed values, if the source declares an enum-like value set.
    pub values: Vec<String>,
}

/// Describes one result column returned by a source-scoped table function.
#[derive(Debug, Clone)]
pub struct TableFunctionResultColumnInfo {
    /// Column name returned by the table function.
    pub name: String,
    /// Data type rendered in `Arrow`/`DataFusion` string form.
    pub data_type: String,
    /// Whether the column can contain null values.
    pub nullable: bool,
    /// User-facing column description.
    pub description: String,
}

/// Describes one source-scoped table function.
#[derive(Debug, Clone)]
pub struct TableFunctionInfo {
    /// `SQL` schema name.
    pub schema_name: String,
    /// Function name within the schema.
    pub function_name: String,
    /// User-facing table function description.
    pub description: String,
    /// Accepted function arguments.
    pub arguments: Vec<TableFunctionArgumentInfo>,
    /// Columns returned by the function.
    pub result_columns: Vec<TableFunctionResultColumnInfo>,
}
