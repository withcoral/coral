//! Focused helpers for the Coral MCP surface.

mod arguments;
mod catalog;
mod context;
mod discovery;
mod errors;
mod feedback;
mod function;
mod resources;
mod schema;
mod search;
mod source_names;
mod sql;
mod task;
mod tool_names;
mod tools;
mod values;

pub(crate) use catalog::{
    CatalogToolKind, describe_arguments, describe_value, list_catalog_arguments,
    list_catalog_value, list_columns_arguments, list_columns_value,
};
pub(crate) use context::ToolDescriptionContext;
pub(crate) use errors::{status_to_error_data, tool_error_from_status, tool_error_result};
pub(crate) use feedback::{FeedbackStoredValue, feedback_arguments};
pub(crate) use function::{
    AddFunctionArguments, add_function_arguments, function_added_value, render_function_artifact,
};
pub(crate) use resources::{
    guide_resource, guide_resource_content, initial_instructions, tables_resource,
    tables_resource_content,
};
pub(crate) use search::search_arguments;
pub(crate) use sql::{
    SqlBatchValue, SqlGuideBlockValue, SqlGuideValue, SqlQueryResultValue, sql_arguments,
};
pub(crate) use task::{
    EndTaskArguments, StartTaskArguments, TaskEndedValue, TaskId, TaskStartedValue, TaskStatus,
    end_task_arguments, required_task_id_argument, required_tool_intent_argument,
    start_task_arguments,
};
pub(crate) use tool_names::ToolName;
pub(crate) use tools::{ToolAvailability, available_tools, build_tool_result};
