//! Focused helpers for the Coral MCP surface.

use std::sync::Arc;

use serde_json::{Map, Value};

mod catalog;
mod discovery;
mod errors;
mod resources;
mod tools;

pub(crate) use catalog::{CatalogItem, catalog_output_schema, catalog_value};
pub(crate) use discovery::{
    ColumnSummary, Pagination, TableSummary, compile_metadata_regex, page_items,
    paged_serialized_value, paged_value, parse_pagination, parse_pagination_with_limits,
};
pub(crate) use errors::{
    internal_status, status_to_error_data, tool_error_from_status, tool_error_result,
};
pub(crate) use resources::{
    format_schema_table_equivalent, guide_resource, guide_resource_content, initial_instructions,
    tables_resource, tables_resource_content,
};
pub(crate) use tools::{
    build_tool_result, describe_table_arguments, describe_table_tool, feedback_tool,
    list_catalog_arguments, list_catalog_tool, list_columns_arguments, list_columns_tool,
    required_string_argument, search_tables_arguments, search_tables_tool, sql_tool,
};

fn json_object_schema(value: &Value) -> Arc<Map<String, Value>> {
    Arc::new(
        value
            .as_object()
            .cloned()
            .expect("tool schemas should be JSON objects"),
    )
}
