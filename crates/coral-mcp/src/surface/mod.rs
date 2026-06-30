//! Focused helpers for the Coral MCP surface.

mod catalog;
mod discovery;
mod errors;
mod resources;
mod schema;
mod source_names;
mod sql;
mod tools;
mod values;

pub(crate) use catalog::{
    CatalogToolKind, describe_table_output_schema, describe_table_value,
    list_catalog_output_schema, list_catalog_value, list_columns_output_schema, list_columns_value,
    search_catalog_output_schema, search_catalog_value,
};
pub(crate) use discovery::{
    DEFAULT_PAGINATION_LIMIT, DEFAULT_PAGINATION_OFFSET, DEFAULT_SEARCH_PAGINATION_LIMIT,
    MAX_PAGINATION_LIMIT, MAX_SEARCH_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT, Pagination,
    parse_pagination, parse_search_pagination,
};
pub(crate) use errors::{
    ToolError, ToolErrorWithData, status_to_error_data, tool_error_from_status, tool_error_result,
};
pub(crate) use resources::{
    guide_resource, guide_resource_content, initial_instructions, tables_resource,
    tables_resource_content,
};
pub(crate) use source_names::connected_source_names_text;
pub(crate) use sql::{
    SqlBatchValue, SqlQueryResultValue, sql_arguments, sql_input_schema, sql_output_schema,
};
pub(crate) use tools::{
    EpisodeOpenedValue, FeedbackStoredValue, ToolDescriptionContext, build_tool_result,
    describe_table_arguments, describe_table_tool, feedback_arguments, feedback_tool,
    list_catalog_arguments, list_catalog_tool, list_columns_arguments, list_columns_tool,
    open_episode_arguments, open_episode_tool, optional_episode_id_argument,
    search_catalog_arguments, search_catalog_tool, sql_tool, with_episode_id_argument,
};
