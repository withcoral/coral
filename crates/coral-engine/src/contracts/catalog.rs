//! Typed query-visible catalog metadata.

/// Describes one queryable column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Copy-pasteable SQL reference for this column name.
    pub sql_column_ref: String,
    /// Copy-pasteable SQL reference qualified by schema and table when known.
    pub sql_qualified_column_ref: String,
    /// Data type rendered in `Arrow`/`DataFusion` string form.
    pub data_type: String,
    /// Whether the column can contain null values.
    pub nullable: bool,
}

/// Describes one queryable table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// `SQL` schema name.
    pub schema_name: String,
    /// Table name within the schema.
    pub table_name: String,
    /// Copy-pasteable fully qualified SQL reference for this table.
    pub sql_table_ref: String,
    /// User-facing table description.
    pub description: String,
    /// Exposed columns for the table.
    pub columns: Vec<ColumnInfo>,
    /// Required filter names for the table.
    pub required_filters: Vec<String>,
}
