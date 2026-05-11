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
