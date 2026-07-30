//! Typed query-visible catalog metadata.

use coral_spec::{SearchLimitsSpec, SourceTableFunctionKind};

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
    /// `SQL` catalog name. Absent for two-part table references.
    pub catalog_name: Option<String>,
    /// `SQL` schema name.
    pub schema_name: String,
    /// Table name within the schema.
    pub table_name: String,
    /// User-facing table description.
    pub description: String,
    /// User-facing query guidance.
    pub guide: String,
    /// Whether MCP SQL must surface the guide before first use in a task.
    pub require_guide_read: bool,
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
}

/// Result of a table lookup from one runtime snapshot.
#[derive(Debug, Clone)]
pub struct DescribeTableInfo {
    /// Exact table match, when present.
    pub table: Option<TableInfo>,
    /// Lightweight table metadata for missing-table context.
    pub missing_context_tables: Vec<TableInfo>,
}

/// Describes one argument accepted by a table function.
#[derive(Debug, Clone)]
pub struct TableFunctionArgumentInfo {
    /// Argument name as used in a named SQL function call.
    pub name: String,
    /// Argument type in source-manifest spelling.
    pub data_type: String,
    /// Whether callers must provide this argument.
    pub required: bool,
    /// Allowed values, if the source declares an enum-like value set.
    pub values: Vec<String>,
    /// Authored typed default encoded as JSON, or `None` when no default was declared.
    pub default_json: Option<String>,
}

/// How a passive Universal Search authorization was selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalSearchAuthorizationOrigin {
    /// The source authored an explicit Universal Search route.
    Explicit,
    /// Coral selected the route through canonical inference.
    Inferred,
}

impl UniversalSearchAuthorizationOrigin {
    /// Returns the stable catalog spelling of this origin.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Inferred => "inferred",
        }
    }
}

/// Passive authorization decision for one source table function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalSearchAuthorizationDecision {
    /// The route is eligible if the independent runtime feature is enabled.
    Eligible,
    /// The source explicitly denied execution of the route.
    Denied,
}

impl UniversalSearchAuthorizationDecision {
    /// Returns the stable catalog spelling of this decision.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::Denied => "denied",
        }
    }
}

/// Passive Universal Search authorization attached to one query-visible function.
///
/// This metadata does not enable execution. The app owns policy resolution and
/// the independent runtime feature gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniversalSearchAuthorizationInfo {
    /// Canonical installed source identity, separate from the SQL schema.
    pub source_name: String,
    /// Stable source-authored route id, when the route was explicit.
    pub route_id: Option<String>,
    /// Whether the route was explicitly authored or canonically inferred.
    pub origin: UniversalSearchAuthorizationOrigin,
    /// Passive eligibility or explicit-denial decision.
    pub decision: UniversalSearchAuthorizationDecision,
    /// Query-visible argument receiving the Universal Search query, when present.
    pub query_argument: Option<String>,
    /// Original DSL v4 operation identity.
    pub operation_id: String,
}

/// Describes one result column returned by a table function.
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

/// Describes one table function.
#[derive(Debug, Clone)]
pub struct TableFunctionInfo {
    /// `SQL` schema name.
    pub schema_name: String,
    /// Function name within the schema.
    pub function_name: String,
    /// User-facing table function description.
    pub description: String,
    /// User-facing query guidance.
    pub guide: String,
    /// Whether MCP SQL must surface the guide before first use in a task.
    pub require_guide_read: bool,
    /// Accepted function arguments.
    pub arguments: Vec<TableFunctionArgumentInfo>,
    /// Columns returned by the function.
    pub result_columns: Vec<TableFunctionResultColumnInfo>,
    /// Function role. Search functions perform provider-native retrieval.
    pub kind: SourceTableFunctionKind,
    /// Provider search limit metadata, when declared by the source.
    pub search_limits: Option<SearchLimitsSpec>,
    /// Passive Universal Search authorization, when one resolves to this function.
    pub universal_search: Option<UniversalSearchAuthorizationInfo>,
}
