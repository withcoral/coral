//! Focused helpers for the Coral MCP surface.

mod errors;
mod resources;
mod tools;

pub(crate) use errors::{
    internal_status, status_to_error_data, tool_error_from_status, tool_error_result,
};
pub(crate) use resources::{
    guide_resource, guide_resource_content, initial_instructions, list_tables_value,
    tables_resource, tables_resource_content,
};
pub(crate) use tools::{build_tool_result, list_tables_tool, required_string_argument, sql_tool};
