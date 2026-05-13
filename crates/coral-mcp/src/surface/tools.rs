use std::sync::Arc;

use coral_api::v1::Source;
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content, Tool, ToolAnnotations},
};
use serde_json::{Map, Value, json};

use super::{Pagination, parse_pagination, parse_pagination_with_limits};

pub(crate) struct ListTablesArguments {
    pub(crate) schema: Option<String>,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

pub(crate) struct SearchTablesArguments {
    pub(crate) pattern: String,
    pub(crate) schema: Option<String>,
    pub(crate) ignore_case: bool,
    pub(crate) pagination: Pagination,
}

pub(crate) struct DescribeTableArguments {
    pub(crate) schema: String,
    pub(crate) table: String,
}

pub(crate) struct ListColumnsArguments {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) pattern: Option<String>,
    pub(crate) ignore_case: bool,
    pub(crate) required_only: bool,
    pub(crate) pagination: Pagination,
}

pub(crate) fn sql_tool(sources: &[Source], visible_table_count: usize) -> Tool {
    Tool::new(
        "sql",
        sql_tool_description(sources, visible_table_count),
        json_object_schema(&json!({
            "type": "object",
            "required": ["sql"],
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A single SQL statement to execute."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Run SQL")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(true),
    )
}

pub(crate) fn list_tables_tool(visible_table_count: usize) -> Tool {
    Tool::new(
        "list_tables",
        list_tables_description(visible_table_count),
        json_object_schema(&json!({
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Optional exact schema/source name to list."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum tables to return, from 1 to 200. Defaults to 50.",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 50
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of matching tables to skip. Defaults to 0.",
                    "minimum": 0,
                    "maximum": u32::MAX,
                    "default": 0
                }
            }
        })),
    )
    .with_raw_output_schema(list_tables_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Tables")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn search_tables_tool(visible_table_count: usize) -> Tool {
    Tool::new(
        "search_tables",
        search_tables_description(visible_table_count),
        json_object_schema(&json!({
            "type": "object",
            "required": ["pattern"],
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Rust regex pattern to match table metadata."
                },
                "schema": {
                    "type": "string",
                    "description": "Optional exact schema/source name to search."
                },
                "ignore_case": {
                    "type": "boolean",
                    "description": "Whether regex matching is case-insensitive. Defaults to true."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum tables to return, from 1 to 100. Defaults to 20.",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 20
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of matching tables to skip. Defaults to 0.",
                    "minimum": 0,
                    "maximum": u32::MAX,
                    "default": 0
                }
            }
        })),
    )
    .with_raw_output_schema(search_tables_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Search Tables")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn describe_table_tool() -> Tool {
    Tool::new(
        "describe_table",
        "Describe one queryable table without returning full column definitions.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["schema", "table"],
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Exact schema/source name."
                },
                "table": {
                    "type": "string",
                    "description": "Exact table name within the schema."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Describe Table")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn list_columns_tool() -> Tool {
    Tool::new(
        "list_columns",
        "List columns for one table with optional regex and required-filter narrowing.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["schema", "table"],
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Exact schema/source name."
                },
                "table": {
                    "type": "string",
                    "description": "Exact table name within the schema."
                },
                "pattern": {
                    "type": "string",
                    "description": "Optional Rust regex matched against column names, descriptions, and data types."
                },
                "ignore_case": {
                    "type": "boolean",
                    "description": "Whether regex matching is case-insensitive. Defaults to true."
                },
                "required_only": {
                    "type": "boolean",
                    "description": "Only return columns that are required filters. Defaults to false."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum columns to return, from 1 to 200. Defaults to 50.",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 50
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of matching columns to skip. Defaults to 0.",
                    "minimum": 0,
                    "maximum": u32::MAX,
                    "default": 0
                }
            }
        })),
    )
    .with_raw_output_schema(list_columns_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Columns")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn feedback_tool() -> Tool {
    Tool::new(
        "feedback",
        "Submit feedback when you are blocked. Coral stores the report locally and uploads an anonymous copy, without user identifiers, to Coral's hosted feedback service to improve Coral's performance.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["trying_to_do", "tried", "stuck"],
            "properties": {
                "trying_to_do": {
                    "type": "string",
                    "description": "What you were trying to do."
                },
                "tried": {
                    "type": "string",
                    "description": "What you already tried."
                },
                "stuck": {
                    "type": "string",
                    "description": "Where you got blocked."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Store Feedback Report")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(true),
    )
}

pub(crate) fn required_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<String, ErrorData> {
    let value = arguments
        .and_then(|arguments| arguments.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(format!("missing string argument '{key}'"), None)
        })?;
    Ok(value.to_string())
}

pub(crate) fn list_tables_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListTablesArguments, ErrorData> {
    let pagination = parse_pagination(arguments)?;
    Ok(ListTablesArguments {
        schema: optional_string_argument(arguments, "schema")?,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub(crate) fn search_tables_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchTablesArguments, ErrorData> {
    Ok(SearchTablesArguments {
        pattern: required_string_argument(arguments, "pattern")?,
        schema: optional_string_argument(arguments, "schema")?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", true)?,
        pagination: parse_pagination_with_limits(arguments, 20, 100)?,
    })
}

pub(crate) fn describe_table_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<DescribeTableArguments, ErrorData> {
    Ok(DescribeTableArguments {
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
    })
}

pub(crate) fn list_columns_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListColumnsArguments, ErrorData> {
    Ok(ListColumnsArguments {
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
        pattern: optional_non_empty_string_argument(arguments, "pattern")?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", true)?,
        required_only: optional_bool_argument(arguments, "required_only", false)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn build_tool_result(value: Value) -> Result<CallToolResult, ErrorData> {
    let pretty = serde_json::to_string_pretty(&value)
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
    let mut result = CallToolResult::structured(value);
    result.content = vec![Content::text(pretty)];
    Ok(result)
}

fn sql_tool_description(sources: &[Source], visible_table_count: usize) -> String {
    if visible_table_count == 0 {
        format!(
            "Run a SQL query against local Coral sources. {} configured source(s), but no visible SQL tables are currently available.",
            sources.len()
        )
    } else {
        format!(
            "Run a SQL query against local Coral sources. {visible_table_count} table(s) are currently visible."
        )
    }
}

fn list_tables_description(visible_table_count: usize) -> String {
    format!(
        "List queryable fully qualified tables. {visible_table_count} table(s) are currently visible."
    )
}

fn search_tables_description(visible_table_count: usize) -> String {
    format!(
        "Search queryable table metadata with a Rust regex. {visible_table_count} table(s) are currently visible."
    )
}

fn list_tables_output_schema() -> Arc<Map<String, Value>> {
    paginated_table_output_schema(&json!({
        "type": "object",
        "required": [
            "schema_name",
            "table_name",
            "name",
            "sql_reference",
            "description",
            "guide",
            "required_filters"
        ],
        "additionalProperties": false,
        "properties": {
            "schema_name": { "type": "string" },
            "table_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "guide": { "type": "string" },
            "required_filters": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    }))
}

fn search_tables_output_schema() -> Arc<Map<String, Value>> {
    paginated_table_output_schema(&json!({
        "type": "object",
        "required": [
            "schema_name",
            "table_name",
            "name",
            "sql_reference",
            "description",
            "guide",
            "required_filters",
            "matched_fields"
        ],
        "additionalProperties": false,
        "properties": {
            "schema_name": { "type": "string" },
            "table_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "guide": { "type": "string" },
            "required_filters": {
                "type": "array",
                "items": { "type": "string" }
            },
            "matched_fields": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "schema_name",
                        "table_name",
                        "name",
                        "description",
                        "guide",
                        "required_filters"
                    ]
                }
            }
        }
    }))
}

fn list_columns_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "oneOf": [
            list_columns_page_output_schema(),
            missing_table_output_schema()
        ]
    }))
}

fn list_columns_page_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_name", "table_name", "columns", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "schema_name": { "type": "string" },
            "table_name": { "type": "string" },
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "is_virtual",
                        "is_required_filter",
                        "description",
                        "ordinal_position"
                    ],
                    "additionalProperties": false,
                    "properties": {
                        "column_name": { "type": "string" },
                        "data_type": { "type": "string" },
                        "is_nullable": { "type": "boolean" },
                        "is_virtual": { "type": "boolean" },
                        "is_required_filter": { "type": "boolean" },
                        "description": { "type": "string" },
                        "ordinal_position": {
                            "type": "integer",
                            "minimum": 0
                        },
                        "matched_fields": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "string",
                                "enum": ["column_name", "description", "data_type"]
                            }
                        }
                    }
                }
            },
            "total": {
                "type": "integer",
                "minimum": 0
            },
            "limit": {
                "type": "integer",
                "minimum": 1
            },
            "offset": {
                "type": "integer",
                "minimum": 0
            },
            "has_more": { "type": "boolean" },
            "next_offset": {
                "type": "integer",
                "minimum": 0
            }
        }
    })
}

fn missing_table_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["found", "requested", "available_schemas", "same_schema_tables", "suggested_calls"],
        "additionalProperties": false,
        "properties": {
            "found": { "enum": [false] },
            "requested": {
                "type": "object",
                "required": ["schema", "table"],
                "additionalProperties": false,
                "properties": {
                    "schema": { "type": "string" },
                    "table": { "type": "string" }
                }
            },
            "available_schemas": {
                "type": "array",
                "items": { "type": "string" }
            },
            "same_schema_tables": {
                "type": "array",
                "items": missing_table_summary_output_schema()
            },
            "suggested_calls": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["tool", "arguments"],
                    "additionalProperties": false,
                    "properties": {
                        "tool": {
                            "type": "string",
                            "enum": ["search_tables", "list_tables"]
                        },
                        "arguments": { "type": "object" }
                    }
                }
            }
        }
    })
}

fn missing_table_summary_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_name", "table_name", "name", "description", "required_filters"],
        "additionalProperties": false,
        "properties": {
            "schema_name": { "type": "string" },
            "table_name": { "type": "string" },
            "name": { "type": "string" },
            "description": { "type": "string" },
            "required_filters": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

fn paginated_table_output_schema(table_item_schema: &Value) -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["tables", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "tables": {
                "type": "array",
                "items": table_item_schema
            },
            "total": {
                "type": "integer",
                "minimum": 0
            },
            "limit": {
                "type": "integer",
                "minimum": 1
            },
            "offset": {
                "type": "integer",
                "minimum": 0
            },
            "has_more": { "type": "boolean" },
            "next_offset": {
                "type": "integer",
                "minimum": 0
            }
        }
    }))
}

pub(crate) fn optional_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn optional_non_empty_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    let value = value.trim();
    if value.is_empty() {
        Err(ErrorData::invalid_params(
            format!("argument '{key}' must not be empty"),
            None,
        ))
    } else {
        Ok(Some(value.to_string()))
    }
}

fn optional_bool_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: bool,
) -> Result<bool, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    value.as_bool().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a boolean"), None)
    })
}

fn json_object_schema(value: &Value) -> Arc<Map<String, Value>> {
    Arc::new(
        value
            .as_object()
            .cloned()
            .expect("tool schemas should be JSON objects"),
    )
}
