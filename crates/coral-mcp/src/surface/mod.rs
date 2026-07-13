//! Focused helpers for the Coral MCP surface.

mod arguments;
mod catalog;
mod context;
mod discovery;
mod errors;
mod feedback;
mod resources;
mod schema;
mod search;
mod source_names;
mod sql;
mod tool_names;
mod tools;
mod values;

pub(crate) use catalog::{
    CatalogToolKind, describe_table_arguments, describe_table_value, list_catalog_arguments,
    list_catalog_value, list_columns_arguments, list_columns_table_fallback_value,
    list_columns_value,
};
pub(crate) use context::ToolDescriptionContext;
pub(crate) use errors::{status_to_error_data, tool_error_from_status, tool_error_result};
pub(crate) use feedback::{FeedbackStoredValue, feedback_arguments};
pub(crate) use resources::{
    guide_resource, guide_resource_content, initial_instructions, tables_resource,
    tables_resource_content,
};
pub(crate) use search::search_arguments;
pub(crate) use sql::{SqlBatchValue, SqlQueryResultValue, sql_arguments};
pub(crate) use tool_names::ToolName;
pub(crate) use tools::{available_tools, build_tool_result};
