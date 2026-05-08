use std::sync::Arc;

use coral_api::v1::Source;
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content, Tool, ToolAnnotations},
};
use serde_json::{Map, Value, json};

pub(crate) struct ListTablesArguments {
    pub(crate) schema: Option<String>,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
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
    .with_annotations(
        ToolAnnotations::with_title("List Tables")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn feedback_tool() -> Tool {
    Tool::new(
        "feedback",
        "Submit feedback when you are blocked or stuck in an unproductive pattern",
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
            .open_world(false),
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
    Ok(ListTablesArguments {
        schema: optional_string_argument(arguments, "schema")?,
        limit: optional_u32_argument(arguments, "limit", 50, 1, 200)?,
        offset: optional_u32_argument(arguments, "offset", 0, 0, u32::MAX)?,
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

fn optional_string_argument(
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

fn optional_u32_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    let value = value.as_i64().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be an integer"), None)
    })?;
    if value < i64::from(min) || value > i64::from(max) {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        ));
    }
    u32::try_from(value).map_err(|_| {
        ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        )
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
