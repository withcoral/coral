use std::sync::Arc;

use coral_api::v1::Source;
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content, Tool, ToolAnnotations},
};
use serde_json::{Map, Value, json};

use super::{Pagination, parse_pagination, parse_pagination_with_limits};

#[cfg(feature = "code-mode")]
pub(crate) const MAX_CODE_MODE_YIELD_TIME_MS: u64 = 60_000;

pub(crate) struct SqlArguments {
    pub(crate) sql: String,
    pub(crate) params: Option<SqlParametersArgument>,
}

#[cfg(feature = "code-mode")]
pub(crate) struct ExecArguments {
    pub(crate) source: String,
    pub(crate) yield_time_ms: Option<u64>,
    pub(crate) max_output_tokens: Option<usize>,
}

#[cfg(feature = "code-mode")]
pub(crate) struct WaitArguments {
    pub(crate) cell_id: String,
    pub(crate) yield_time_ms: Option<u64>,
    pub(crate) terminate: bool,
}

pub(crate) enum SqlParametersArgument {
    Positional(Vec<SqlParameterArgument>),
    Named(std::collections::BTreeMap<String, SqlParameterArgument>),
}

pub(crate) enum SqlParameterArgument {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
}

pub(crate) struct ListCatalogArguments {
    pub(crate) schema: Option<String>,
    pub(crate) kind: Option<CatalogToolKind>,
    pub(crate) pagination: Pagination,
}

pub(crate) struct SearchCatalogArguments {
    pub(crate) pattern: String,
    pub(crate) schema: Option<String>,
    pub(crate) kind: Option<CatalogToolKind>,
    pub(crate) ignore_case: bool,
    pub(crate) pagination: Pagination,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogToolKind {
    Table,
    TableFunction,
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
                },
                "params": {
                    "description": "Optional bound SQL parameters. Arrays bind positional placeholders such as $1 and $2. Objects bind named placeholders such as $name.",
                    "anyOf": [
                        {
                            "type": "array",
                            "items": sql_parameter_value_schema()
                        },
                        {
                            "type": "object",
                            "propertyNames": {
                                "type": "string",
                                "minLength": 1,
                                "pattern": "^[^$]"
                            },
                            "additionalProperties": sql_parameter_value_schema()
                        }
                    ]
                }
            }
        })),
    )
    .with_raw_output_schema(sql_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Run SQL")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(true),
    )
}

pub(crate) fn list_catalog_tool(visible_table_count: usize, visible_function_count: usize) -> Tool {
    Tool::new(
        "list_catalog",
        format!(
            "List queryable catalog items. {visible_table_count} table(s) and {visible_function_count} table function(s) are currently visible."
        ),
        json_object_schema(&json!({
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Optional exact schema/source name to list."
                },
                "kind": {
                    "description": "Optional item kind to list. Omit or pass null to list all catalog items.",
                    "anyOf": [
                        {
                            "type": "string",
                            "enum": ["table", "table_function"]
                        },
                        {
                            "type": "null"
                        }
                    ]
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum catalog items to return, from 1 to 200. Defaults to 50.",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 50
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of matching catalog items to skip. Defaults to 0.",
                    "minimum": 0,
                    "maximum": u32::MAX,
                    "default": 0
                }
            }
        })),
    )
    .with_raw_output_schema(list_catalog_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Catalog")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn search_catalog_tool(
    visible_table_count: usize,
    visible_function_count: usize,
) -> Tool {
    Tool::new(
        "search_catalog",
        search_catalog_description(visible_table_count, visible_function_count),
        json_object_schema(&json!({
            "type": "object",
            "required": ["pattern"],
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Rust regex pattern to match catalog metadata."
                },
                "schema": {
                    "type": "string",
                    "description": "Optional exact schema/source name to search."
                },
                "kind": {
                    "description": "Optional item kind to search. Omit or pass null to search all catalog items.",
                    "anyOf": [
                        {
                            "type": "string",
                            "enum": ["table", "table_function"]
                        },
                        {
                            "type": "null"
                        }
                    ]
                },
                "ignore_case": {
                    "type": "boolean",
                    "description": "Whether regex matching is case-insensitive. Defaults to true."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum catalog items to return, from 1 to 100. Defaults to 20.",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 20
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of matching catalog items to skip. Defaults to 0.",
                    "minimum": 0,
                    "maximum": u32::MAX,
                    "default": 0
                }
            }
        })),
    )
    .with_raw_output_schema(search_catalog_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Search Catalog")
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
    .with_raw_output_schema(describe_table_output_schema())
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

#[cfg(feature = "code-mode")]
pub(crate) fn exec_tool(description: String, open_world_hint: bool) -> Tool {
    Tool::new(
        "exec",
        description,
        json_object_schema(&json!({
            "type": "object",
            "required": ["source"],
            "additionalProperties": false,
            "properties": {
                "source": {
                    "type": "string",
                    "description": "JavaScript source to run in Code Mode. Return the JSON-serializable value you want exec to return."
                },
                "yield_time_ms": {
                    "type": "integer",
                    "description": "Optional milliseconds to run before yielding a still-running cell.",
                    "minimum": 0,
                    "maximum": MAX_CODE_MODE_YIELD_TIME_MS
                },
                "max_output_tokens": {
                    "type": "integer",
                    "description": "Optional output token budget for this exec result.",
                    "minimum": 0
                }
            }
        })),
    )
    .with_raw_output_schema(code_mode_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Run Code Mode")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(open_world_hint),
    )
}

#[cfg(feature = "code-mode")]
pub(crate) fn wait_tool(description: &'static str, open_world_hint: bool) -> Tool {
    Tool::new(
        "wait",
        description,
        json_object_schema(&json!({
            "type": "object",
            "required": ["cell_id"],
            "additionalProperties": false,
            "properties": {
                "cell_id": {
                    "type": "string",
                    "description": "Code Mode cell id returned by exec."
                },
                "yield_time_ms": {
                    "type": "integer",
                    "description": "Optional milliseconds to wait before yielding again.",
                    "minimum": 0,
                    "maximum": MAX_CODE_MODE_YIELD_TIME_MS
                },
                "terminate": {
                    "type": "boolean",
                    "description": "Terminate the cell instead of waiting for more output.",
                    "default": false
                }
            }
        })),
    )
    .with_raw_output_schema(code_mode_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Wait For Code Mode")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(open_world_hint),
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

pub(crate) fn sql_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SqlArguments, ErrorData> {
    Ok(SqlArguments {
        sql: required_string_argument(arguments, "sql")?,
        params: optional_sql_parameters_argument(arguments)?,
    })
}

#[cfg(feature = "code-mode")]
pub(crate) fn exec_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ExecArguments, ErrorData> {
    Ok(ExecArguments {
        source: required_string_argument(arguments, "source")?,
        yield_time_ms: optional_code_mode_yield_time_ms_argument(arguments, "yield_time_ms")?,
        max_output_tokens: optional_usize_argument(arguments, "max_output_tokens")?,
    })
}

#[cfg(feature = "code-mode")]
pub(crate) fn wait_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<WaitArguments, ErrorData> {
    Ok(WaitArguments {
        cell_id: required_string_argument(arguments, "cell_id")?,
        yield_time_ms: optional_code_mode_yield_time_ms_argument(arguments, "yield_time_ms")?,
        terminate: optional_bool_argument(arguments, "terminate", false)?,
    })
}

fn optional_sql_parameters_argument(
    arguments: Option<&Map<String, Value>>,
) -> Result<Option<SqlParametersArgument>, ErrorData> {
    let Some(params) = arguments.and_then(|arguments| arguments.get("params")) else {
        return Ok(None);
    };
    match params {
        Value::Array(values) => values
            .iter()
            .map(sql_parameter_argument)
            .collect::<Result<Vec<_>, _>>()
            .map(SqlParametersArgument::Positional)
            .map(Some),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                if key.is_empty() {
                    return Err(ErrorData::invalid_params(
                        "SQL parameter names must not be empty",
                        None,
                    ));
                }
                if key.starts_with('$') {
                    return Err(ErrorData::invalid_params(
                        "SQL parameter names must not include the leading '$'",
                        None,
                    ));
                }
                sql_parameter_argument(value).map(|value| (key.clone(), value))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()
            .map(SqlParametersArgument::Named)
            .map(Some),
        _ => Err(ErrorData::invalid_params(
            "argument 'params' must be an array or object",
            None,
        )),
    }
}

fn sql_parameter_argument(value: &Value) -> Result<SqlParameterArgument, ErrorData> {
    match value {
        Value::Null => Ok(SqlParameterArgument::Null),
        Value::Bool(value) => Ok(SqlParameterArgument::Boolean(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqlParameterArgument::Int64(value))
            } else if let Some(value) = value.as_u64().and_then(|value| i64::try_from(value).ok()) {
                Ok(SqlParameterArgument::Int64(value))
            } else if is_json_integer_number(value) {
                Err(ErrorData::invalid_params(
                    "SQL integer parameters must fit in the signed 64-bit range",
                    None,
                ))
            } else {
                let value = value.as_f64().ok_or_else(|| {
                    ErrorData::invalid_params("SQL numeric parameters must be finite", None)
                })?;
                Ok(SqlParameterArgument::Float64(value))
            }
        }
        Value::String(value) => Ok(SqlParameterArgument::String(value.clone())),
        Value::Array(_) | Value::Object(_) => Err(ErrorData::invalid_params(
            "SQL parameter values must be null, boolean, number, or string",
            None,
        )),
    }
}

fn is_json_integer_number(value: &serde_json::Number) -> bool {
    let raw = value.to_string();
    !raw.contains('.') && !raw.contains('e') && !raw.contains('E')
}

pub(crate) fn list_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListCatalogArguments, ErrorData> {
    Ok(ListCatalogArguments {
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn search_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchCatalogArguments, ErrorData> {
    Ok(SearchCatalogArguments {
        pattern: required_string_argument(arguments, "pattern")?,
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", true)?,
        pagination: parse_pagination_with_limits(arguments, 20, 100)?,
    })
}

fn optional_catalog_kind_argument(
    arguments: Option<&Map<String, Value>>,
) -> Result<Option<CatalogToolKind>, ErrorData> {
    let Some(kind) = optional_string_argument(arguments, "kind")? else {
        return Ok(None);
    };
    match kind.as_str() {
        "table" => Ok(Some(CatalogToolKind::Table)),
        "table_function" => Ok(Some(CatalogToolKind::TableFunction)),
        _ => Err(ErrorData::invalid_params(
            "argument 'kind' must be 'table' or 'table_function'",
            None,
        )),
    }
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

fn search_catalog_description(visible_table_count: usize, visible_function_count: usize) -> String {
    format!(
        "Search queryable catalog metadata with a Rust regex. {visible_table_count} table(s) and {visible_function_count} table function(s) are currently visible."
    )
}

fn sql_output_schema() -> Arc<Map<String, Value>> {
    success_or_error_output_schema(&json!({
        "type": "object",
        "required": ["columns", "rows", "row_count"],
        "additionalProperties": false,
        "properties": {
            "columns": {
                "type": "array",
                "items": sql_column_schema()
            },
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": true
                }
            },
            "row_count": {
                "type": "integer",
                "minimum": 0
            }
        }
    }))
}

#[cfg(feature = "code-mode")]
fn code_mode_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["status"],
        "additionalProperties": false,
        "properties": {
            "status": {
                "type": "string",
                "enum": ["completed", "running", "terminated", "failed"]
            },
            "cell_id": {
                "type": "string"
            },
            "result": {
                "description": "JSON-serializable value returned by the Code Mode cell."
            },
            "error": {
                "type": "object",
                "required": ["message"],
                "additionalProperties": true,
                "properties": {
                    "message": { "type": "string" }
                }
            }
        }
    }))
}

fn sql_column_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "data_type", "nullable"],
        "additionalProperties": false,
        "properties": {
            "name": { "type": "string" },
            "data_type": {
                "type": "object",
                "required": ["kind"],
                "additionalProperties": true,
                "properties": {
                    "kind": { "type": "string" }
                }
            },
            "nullable": { "type": "boolean" }
        }
    })
}

fn sql_parameter_value_schema() -> Value {
    json!({
        "anyOf": [
            { "type": "null" },
            { "type": "boolean" },
            { "type": "number" },
            { "type": "string" }
        ]
    })
}

fn list_catalog_output_schema() -> Arc<Map<String, Value>> {
    success_or_error_output_schema(&json!({
        "type": "object",
        "required": ["items", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "oneOf": [
                        catalog_table_item_output_schema(),
                        catalog_table_function_item_output_schema()
                    ]
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
    }))
}

fn catalog_table_item_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "schema_name", "name", "sql_reference", "description", "table"],
        "additionalProperties": false,
        "properties": {
            "kind": { "enum": ["table"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "table": {
                "type": "object",
                "required": ["table_name", "guide", "required_filters"],
                "additionalProperties": false,
                "properties": {
                    "table_name": { "type": "string" },
                    "guide": { "type": "string" },
                    "required_filters": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                }
            }
        }
    })
}

fn catalog_table_function_item_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "kind",
            "schema_name",
            "name",
            "sql_reference",
            "sql_call_example",
            "description",
            "table_function"
        ],
        "additionalProperties": false,
        "properties": {
            "kind": { "enum": ["table_function"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "sql_call_example": { "type": "string" },
            "description": { "type": "string" },
            "table_function": {
                "type": "object",
                "required": ["function_name", "arguments", "result_columns"],
                "additionalProperties": false,
                "properties": {
                    "function_name": { "type": "string" },
                    "arguments": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "required", "values"],
                            "additionalProperties": false,
                            "properties": {
                                "name": { "type": "string" },
                                "required": { "type": "boolean" },
                                "values": {
                                    "type": "array",
                                    "items": { "type": "string" }
                                }
                            }
                        }
                    },
                    "result_columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["column_name", "data_type", "is_nullable", "description"],
                            "additionalProperties": false,
                            "properties": {
                                "column_name": { "type": "string" },
                                "data_type": { "type": "string" },
                                "is_nullable": { "type": "boolean" },
                                "description": { "type": "string" }
                            }
                        }
                    }
                }
            }
        }
    })
}

fn search_catalog_output_schema() -> Arc<Map<String, Value>> {
    success_or_error_output_schema(&json!({
        "type": "object",
        "required": ["items", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "oneOf": [
                        catalog_search_item_output_schema(catalog_table_item_output_schema()),
                        catalog_search_item_output_schema(catalog_table_function_item_output_schema())
                    ]
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
    }))
}

fn catalog_search_item_output_schema(mut schema: Value) -> Value {
    let object = schema
        .as_object_mut()
        .expect("catalog item schema is an object");
    object
        .get_mut("required")
        .and_then(Value::as_array_mut)
        .expect("catalog item schema has required array")
        .push(json!("matched_fields"));
    object
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .expect("catalog item schema has properties object")
        .insert(
            "matched_fields".to_string(),
            json!({
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "schema_name",
                        "table_name",
                        "function_name",
                        "name",
                        "description",
                        "guide",
                        "required_filters",
                        "arguments",
                        "result_columns"
                    ]
                }
            }),
        );
    schema
}

fn list_columns_output_schema() -> Arc<Map<String, Value>> {
    success_or_error_output_schema(&json!({
        "type": "object",
        "oneOf": [
            list_columns_page_output_schema(),
            missing_table_output_schema()
        ]
    }))
}

fn describe_table_output_schema() -> Arc<Map<String, Value>> {
    success_or_error_output_schema(&json!({
        "type": "object",
        "oneOf": [
            {
                "type": "object",
                "required": [
                    "found",
                    "schema_name",
                    "table_name",
                    "name",
                    "description",
                    "guide",
                    "required_filters",
                    "column_count",
                    "columns_hint"
                ],
                "additionalProperties": false,
                "properties": {
                    "found": { "enum": [true] },
                    "schema_name": { "type": "string" },
                    "table_name": { "type": "string" },
                    "name": { "type": "string" },
                    "description": { "type": "string" },
                    "guide": { "type": "string" },
                    "required_filters": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "column_count": {
                        "type": "integer",
                        "minimum": 0
                    },
                    "columns_hint": { "type": "string" }
                }
            },
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
        "required": ["found", "requested", "available_schemas", "same_schema_tables", "suggestions", "suggested_calls"],
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
            "suggestions": {
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
                            "enum": ["search_catalog", "list_catalog"]
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

pub(crate) fn optional_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
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

#[cfg(feature = "code-mode")]
fn optional_u64_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<u64>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    value.as_u64().map(Some).ok_or_else(|| {
        ErrorData::invalid_params(
            format!("argument '{key}' must be a non-negative integer"),
            None,
        )
    })
}

#[cfg(feature = "code-mode")]
fn optional_code_mode_yield_time_ms_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<u64>, ErrorData> {
    let Some(value) = optional_u64_argument(arguments, key)? else {
        return Ok(None);
    };
    if value > MAX_CODE_MODE_YIELD_TIME_MS {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be at most {MAX_CODE_MODE_YIELD_TIME_MS}"),
            None,
        ));
    }
    Ok(Some(value))
}

#[cfg(feature = "code-mode")]
fn optional_usize_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<usize>, ErrorData> {
    optional_u64_argument(arguments, key).and_then(|value| {
        value.map(usize::try_from).transpose().map_err(|_error| {
            ErrorData::invalid_params(format!("argument '{key}' is too large"), None)
        })
    })
}

fn success_or_error_output_schema(success_schema: &Value) -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "oneOf": [
            success_schema,
            tool_error_output_schema()
        ]
    }))
}

fn tool_error_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["error"],
        "additionalProperties": false,
        "properties": {
            "error": {
                "type": "object",
                "required": ["summary", "detail", "grpc_code", "retryable", "metadata"],
                "additionalProperties": false,
                "properties": {
                    "summary": { "type": "string" },
                    "detail": { "type": "string" },
                    "hint": { "type": "string" },
                    "grpc_code": { "type": "string" },
                    "reason": { "type": "string" },
                    "retryable": { "type": "boolean" },
                    "metadata": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value, json};

    #[cfg(feature = "code-mode")]
    use super::{exec_arguments, wait_arguments};
    use super::{list_catalog_arguments, search_catalog_arguments, sql_arguments};

    #[test]
    fn catalog_kind_argument_accepts_null_as_all_kinds() {
        let mut arguments = Map::new();
        arguments.insert("kind".to_string(), Value::Null);
        let list = list_catalog_arguments(Some(&arguments)).expect("list arguments");
        assert_eq!(list.kind, None);

        arguments.insert("pattern".to_string(), Value::String("issue".to_string()));
        let search = search_catalog_arguments(Some(&arguments)).expect("search arguments");
        assert_eq!(search.kind, None);
    }

    #[test]
    fn sql_params_reject_integer_outside_i64_range() {
        let mut arguments = Map::new();
        arguments.insert("sql".to_string(), json!("SELECT $1"));
        arguments.insert("params".to_string(), json!([9_223_372_036_854_775_808_u64]));

        let Err(error) = sql_arguments(Some(&arguments)) else {
            panic!("expected oversized integer param to fail");
        };

        assert!(
            error
                .message
                .contains("SQL integer parameters must fit in the signed 64-bit range"),
            "unexpected error: {error:?}"
        );
    }

    #[cfg(feature = "code-mode")]
    #[test]
    fn code_mode_yield_time_ms_rejects_over_limit() {
        let mut exec = Map::new();
        exec.insert("source".to_string(), json!("return 1;"));
        exec.insert("yield_time_ms".to_string(), json!(60_001));

        let Err(error) = exec_arguments(Some(&exec)) else {
            panic!("expected oversized exec yield_time_ms to fail");
        };
        assert!(
            error
                .message
                .contains("argument 'yield_time_ms' must be at most 60000"),
            "unexpected error: {error:?}"
        );

        let mut wait = Map::new();
        wait.insert("cell_id".to_string(), json!("1"));
        wait.insert("yield_time_ms".to_string(), json!(60_001));

        let Err(error) = wait_arguments(Some(&wait)) else {
            panic!("expected oversized wait yield_time_ms to fail");
        };
        assert!(
            error
                .message
                .contains("argument 'yield_time_ms' must be at most 60000"),
            "unexpected error: {error:?}"
        );
    }
}
