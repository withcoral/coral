use std::sync::Arc;

use rmcp::{
    ErrorData,
    model::{CallToolResult, Content, Tool, ToolAnnotations},
};
use serde_json::{Map, Value, json};

use super::{Pagination, parse_pagination, parse_pagination_with_limits};

pub(crate) struct ListCatalogArguments {
    pub(crate) schema: Option<String>,
    pub(crate) kind: Option<CatalogToolKind>,
    pub(crate) detail: CatalogToolDetail,
    pub(crate) pagination: Pagination,
}

pub(crate) struct SearchCatalogArguments {
    pub(crate) pattern: String,
    pub(crate) schema: Option<String>,
    pub(crate) kind: Option<CatalogToolKind>,
    pub(crate) detail: CatalogToolDetail,
    pub(crate) ignore_case: bool,
    pub(crate) pagination: Pagination,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogToolKind {
    Table,
    TableFunction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogToolDetail {
    Summary,
    Full,
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

pub(crate) struct SearchColumnsArguments {
    pub(crate) pattern: String,
    pub(crate) schema: Option<String>,
    pub(crate) ignore_case: bool,
    pub(crate) required_only: bool,
    pub(crate) pagination: Pagination,
}

pub(crate) fn sql_tool() -> Tool {
    Tool::new(
        "sql",
        sql_tool_description(),
        json_object_schema(&json!({
            "type": "object",
            "required": ["sql"],
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "One read-only SQL statement to execute against the Coral database."
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

pub(crate) fn list_catalog_tool() -> Tool {
    Tool::new(
        "list_catalog",
        "List source context and compact database catalog summaries for currently configured sources. Prefer search_catalog when you know the entity or task; use detail='full' only for small result sets that need guides, function result columns, or full argument metadata.",
        json_object_schema(&json!({
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Optional exact SQL schema name to list."
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
                "detail": {
                    "type": "string",
                    "description": "Output detail level. Defaults to compact summaries. Use 'full' only with a narrow schema/kind/limit when bulky guides, function result columns, and full argument metadata are required.",
                    "enum": ["summary", "full"],
                    "default": "summary"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum catalog items to return, from 1 to 50. Defaults to 50.",
                    "minimum": 1,
                    "maximum": 50,
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

pub(crate) fn search_catalog_tool() -> Tool {
    Tool::new(
        "search_catalog",
        search_catalog_description(),
        json_object_schema(&json!({
            "type": "object",
            "required": ["pattern"],
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Rust regex pattern to match database catalog metadata."
                },
                "schema": {
                    "type": "string",
                    "description": "Optional exact SQL schema name to search."
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
                "detail": {
                    "type": "string",
                    "description": "Output detail level. Defaults to compact summaries. Use 'full' only for small result sets that need guides, function result columns, or full argument metadata.",
                    "enum": ["summary", "full"],
                    "default": "summary"
                },
                "ignore_case": {
                    "type": "boolean",
                    "description": "Whether regex matching is case-insensitive. Defaults to true."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum catalog items to return, from 1 to 50. Defaults to 20.",
                    "minimum": 1,
                    "maximum": 50,
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
        "Describe one database table, including required filters and compact column metadata.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["schema", "table"],
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Exact SQL schema name."
                },
                "table": {
                    "type": "string",
                    "description": "Exact table name within the SQL schema."
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
        "List columns for one database table with optional regex and required-filter narrowing.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["schema", "table"],
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Exact SQL schema name."
                },
                "table": {
                    "type": "string",
                    "description": "Exact table name within the SQL schema."
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

pub(crate) fn search_columns_tool() -> Tool {
    Tool::new(
        "search_columns",
        "Search columns across database tables. Use this when you know a column, field, or data type but not the exact table.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["pattern"],
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Rust regex matched against column names, descriptions, and data types."
                },
                "schema": {
                    "type": "string",
                    "description": "Optional exact SQL schema name to search."
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
                    "description": "Maximum column matches to return, from 1 to 50. Defaults to 20.",
                    "minimum": 1,
                    "maximum": 50,
                    "default": 20
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
    .with_raw_output_schema(search_columns_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Search Columns")
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

pub(crate) fn list_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListCatalogArguments, ErrorData> {
    Ok(ListCatalogArguments {
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        detail: optional_catalog_detail_argument(arguments)?,
        pagination: parse_pagination_with_limits(arguments, 50, 50)?,
    })
}

pub(crate) fn search_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchCatalogArguments, ErrorData> {
    Ok(SearchCatalogArguments {
        pattern: required_string_argument(arguments, "pattern")?,
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        detail: optional_catalog_detail_argument(arguments)?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", true)?,
        pagination: parse_pagination_with_limits(arguments, 20, 50)?,
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

fn optional_catalog_detail_argument(
    arguments: Option<&Map<String, Value>>,
) -> Result<CatalogToolDetail, ErrorData> {
    let Some(detail) = optional_string_argument(arguments, "detail")? else {
        return Ok(CatalogToolDetail::Summary);
    };
    match detail.as_str() {
        "summary" => Ok(CatalogToolDetail::Summary),
        "full" => Ok(CatalogToolDetail::Full),
        _ => Err(ErrorData::invalid_params(
            "argument 'detail' must be 'summary' or 'full'",
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

pub(crate) fn search_columns_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchColumnsArguments, ErrorData> {
    Ok(SearchColumnsArguments {
        pattern: required_string_argument(arguments, "pattern")?,
        schema: optional_string_argument(arguments, "schema")?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", true)?,
        required_only: optional_bool_argument(arguments, "required_only", false)?,
        pagination: parse_pagination_with_limits(arguments, 20, 50)?,
    })
}

pub(crate) fn build_tool_result(value: Value) -> Result<CallToolResult, ErrorData> {
    let pretty = serde_json::to_string_pretty(&value)
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
    let mut result = CallToolResult::structured(value);
    result.content = vec![Content::text(pretty)];
    Ok(result)
}

fn sql_tool_description() -> &'static str {
    "Execute read-only SQL against the Coral database. Use JOIN, CROSS JOIN, CTEs, subqueries, and aggregates to combine tables in one statement; use catalog tools or coral.* metadata tables for discovery."
}

fn search_catalog_description() -> &'static str {
    "Search source context and compact database catalog summaries for currently configured sources with a Rust regex. Strong identifier matches are ranked first. Use this before list_catalog when you know the entity or task; use detail='full' only for small result sets."
}

fn sql_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["rows", "row_count", "columns"],
        "additionalProperties": false,
        "properties": {
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": true
                }
            },
            "row_count": {
                "type": "integer",
                "minimum": 0,
                "description": "Number of rows returned by this SQL statement."
            },
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "data_type", "is_nullable"],
                    "additionalProperties": false,
                    "properties": {
                        "name": { "type": "string" },
                        "data_type": { "type": "string" },
                        "is_nullable": { "type": "boolean" }
                    }
                }
            }
        }
    }))
}

fn list_catalog_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["sources", "items", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "sources": {
                "type": "array",
                "items": catalog_source_output_schema()
            },
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

fn catalog_source_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_name", "description"],
        "additionalProperties": false,
        "properties": {
            "schema_name": { "type": "string" },
            "description": { "type": "string" },
            "onboarding_instructions": { "type": "string" }
        }
    })
}

fn catalog_table_item_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "schema_name", "name", "sql_reference", "description"],
        "additionalProperties": false,
        "properties": {
            "kind": { "enum": ["table"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "required_filters": {
                "type": "array",
                "items": { "type": "string" }
            },
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
            "description"
        ],
        "additionalProperties": false,
        "properties": {
            "kind": { "enum": ["table_function"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "sql_call_example": { "type": "string" },
            "description": { "type": "string" },
            "arguments": {
                "type": "array",
                "items": table_function_argument_output_schema()
            },
            "result_column_count": {
                "type": "integer",
                "minimum": 0
            },
            "table_function": {
                "type": "object",
                "required": ["function_name", "arguments", "result_columns"],
                "additionalProperties": false,
                "properties": {
                    "function_name": { "type": "string" },
                    "arguments": {
                        "type": "array",
                        "items": table_function_argument_output_schema()
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

fn table_function_argument_output_schema() -> Value {
    json!({
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
    })
}

fn search_catalog_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["sources", "items", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "sources": {
                "type": "array",
                "items": catalog_source_output_schema()
            },
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
    json_object_schema(&json!({
        "type": "object",
        "oneOf": [
            list_columns_page_output_schema(),
            missing_table_output_schema()
        ]
    }))
}

fn search_columns_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["columns", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "columns": {
                "type": "array",
                "items": table_column_search_result_output_schema()
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

fn table_column_search_result_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "schema_name",
            "table_name",
            "sql_reference",
            "table_description",
            "required_filters",
            "column_name",
            "data_type",
            "is_nullable",
            "is_virtual",
            "is_required_filter",
            "description",
            "ordinal_position",
            "matched_fields"
        ],
        "additionalProperties": false,
        "properties": {
            "schema_name": { "type": "string" },
            "table_name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "table_description": { "type": "string" },
            "required_filters": {
                "type": "array",
                "items": { "type": "string" }
            },
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
    })
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
    use serde_json::{Map, Value};

    use super::{CatalogToolDetail, list_catalog_arguments, search_catalog_arguments};

    #[test]
    fn catalog_kind_argument_accepts_null_as_all_kinds() {
        let mut arguments = Map::new();
        arguments.insert("kind".to_string(), Value::Null);
        let list = list_catalog_arguments(Some(&arguments)).expect("list arguments");
        assert_eq!(list.kind, None);
        assert_eq!(list.detail, CatalogToolDetail::Summary);

        arguments.insert("pattern".to_string(), Value::String("issue".to_string()));
        let search = search_catalog_arguments(Some(&arguments)).expect("search arguments");
        assert_eq!(search.kind, None);
        assert_eq!(search.detail, CatalogToolDetail::Summary);

        arguments.insert("detail".to_string(), Value::String("full".to_string()));
        let search = search_catalog_arguments(Some(&arguments)).expect("full search arguments");
        assert_eq!(search.detail, CatalogToolDetail::Full);
    }
}
