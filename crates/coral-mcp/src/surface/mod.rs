//! Focused helpers for the Coral MCP surface.

mod errors;
mod instructions;
mod tools;

pub(crate) use errors::{status_to_error_data, tool_error_from_status, tool_error_result};
pub(crate) use instructions::initial_instructions;
pub(crate) use tools::{
    DescribeView, build_tool_result, describe_arguments, describe_tool, exec_arguments, exec_tool,
    feedback_tool, required_string_argument, search_arguments, search_tool, wait_arguments,
    wait_tool,
};
