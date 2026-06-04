//! Focused helpers for the Coral MCP surface.

mod catalog;
mod discovery;
mod errors;
mod resources;
mod tools;
mod values;

pub(crate) use catalog::{
    describe_table_value, list_catalog_value, list_columns_value, search_catalog_value,
};
pub(crate) use discovery::{Pagination, parse_pagination, parse_pagination_with_limits};
pub(crate) use errors::{status_to_error_data, tool_error_from_status, tool_error_result};
pub(crate) use resources::{
    guide_resource, guide_resource_content, initial_instructions, tables_resource,
    tables_resource_content,
};
pub(crate) use tools::{
    CatalogToolKind, SqlParameterArgument, SqlParametersArgument, build_tool_result,
    describe_table_arguments, describe_table_tool, feedback_tool, list_catalog_arguments,
    list_catalog_tool, list_columns_arguments, list_columns_tool, required_string_argument,
    search_catalog_arguments, search_catalog_tool, sql_arguments, sql_tool,
};
#[cfg(feature = "code-mode")]
pub(crate) use tools::{
    ExecArguments, MAX_CODE_MODE_YIELD_TIME_MS, WaitArguments, exec_arguments, exec_tool,
    wait_arguments, wait_tool,
};
