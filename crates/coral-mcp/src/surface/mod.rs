//! Focused helpers for the Coral MCP surface.

mod discovery;
mod errors;
mod resources;
mod tools;

pub(crate) use discovery::{
    ColumnSummary, Pagination, TableFunctionSummary, TableSummary, compile_metadata_regex,
    page_items, paged_value, parse_pagination, parse_pagination_with_limits,
};
pub(crate) use errors::{
    internal_status, status_to_error_data, tool_error_from_status, tool_error_result,
};
pub(crate) use resources::{
    format_schema_table_equivalent, guide_resource, guide_resource_content, initial_instructions,
    list_tables_value, tables_resource, tables_resource_content,
};
pub(crate) use tools::{
    build_tool_result, describe_table_arguments, describe_table_tool, feedback_tool,
    list_columns_arguments, list_columns_tool, list_table_functions_arguments,
    list_table_functions_tool, list_tables_arguments, list_tables_tool, required_string_argument,
    search_table_functions_arguments, search_table_functions_tool, search_tables_arguments,
    search_tables_tool, sql_tool,
};
