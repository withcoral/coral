use serde_json::Value;

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrOperationInput, IrOperationOutput, IrScalarType, OutputCardinality};
use crate::v4::surfaces::json_schema::json_schema_type_contains;

use super::import::MAX_SCHEMA_DEPTH;

pub(super) fn infer_mcp_pagination_contracts(
    inputs: &[IrOperationInput],
    output: &IrOperationOutput,
    output_schema: Option<&Value>,
    input_schema: &Value,
    surface_id: &str,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> (Option<McpPaginationSpec>, Option<McpOffsetPaginationSpec>) {
    let pagination = infer_mcp_pagination(
        inputs,
        output,
        output_schema,
        surface_id,
        operation_id,
        diagnostics,
    );
    let offset_pagination = pagination
        .is_none()
        .then(|| infer_mcp_offset_pagination(inputs, output, input_schema))
        .flatten();
    (pagination, offset_pagination)
}

fn infer_mcp_pagination(
    inputs: &[IrOperationInput],
    output: &IrOperationOutput,
    output_schema: Option<&Value>,
    surface_id: &str,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<McpPaginationSpec> {
    if !matches!(
        output.cardinality,
        OutputCardinality::List | OutputCardinality::WrappedList
    ) {
        return None;
    }
    let cursor_arg = cursor_input_name(inputs)?;
    let response_cursor_path =
        find_response_cursor_path_or_warn(output_schema?, surface_id, operation_id, diagnostics)?;
    Some(McpPaginationSpec {
        cursor_arg: cursor_arg.to_string(),
        response_cursor_path,
        max_pages: None,
    })
}

fn infer_mcp_offset_pagination(
    inputs: &[IrOperationInput],
    output: &IrOperationOutput,
    input_schema: &Value,
) -> Option<McpOffsetPaginationSpec> {
    if !matches!(
        output.cardinality,
        OutputCardinality::List | OutputCardinality::WrappedList
    ) {
        return None;
    }
    let properties = input_schema.get("properties").and_then(Value::as_object)?;
    let limit = offset_pagination_input(inputs, properties.get("limit")?, "limit")?;
    if limit.default == 0
        || limit.maximum.is_none_or(|maximum| maximum == 0)
        || !schema_minimum_rejects_values_below(properties.get("limit")?, 1)
    {
        return None;
    }
    let offset = offset_pagination_input(inputs, properties.get("offset")?, "offset")?;
    if offset.default != 0 || !schema_allows_unsigned_value(properties.get("offset")?, 0) {
        return None;
    }
    Some(McpOffsetPaginationSpec {
        limit_arg: limit.name.to_string(),
        default_limit: limit.default,
        max_limit: limit.maximum?,
        offset_arg: offset.name.to_string(),
        offset_start: offset.default,
        max_pages: None,
    })
}

struct OffsetPaginationInput<'a> {
    name: &'a str,
    default: usize,
    maximum: Option<usize>,
}

fn offset_pagination_input<'a>(
    inputs: &'a [IrOperationInput],
    schema: &Value,
    name: &str,
) -> Option<OffsetPaginationInput<'a>> {
    let input = inputs.iter().find(|input| {
        input.name == name && !input.required && input.data_type == IrScalarType::Integer
    })?;
    Some(OffsetPaginationInput {
        name: input.name.as_str(),
        default: input
            .default_value
            .as_deref()
            .and_then(|value| value.parse::<usize>().ok())
            .or_else(|| schema_unsigned_integer(schema, "default"))?,
        maximum: schema_unsigned_integer(schema, "maximum"),
    })
}

fn schema_minimum_rejects_values_below(schema: &Value, expected: usize) -> bool {
    schema_unsigned_integer(schema, "minimum").is_some_and(|minimum| minimum >= expected)
}

fn schema_allows_unsigned_value(schema: &Value, value: usize) -> bool {
    schema_unsigned_integer(schema, "minimum").is_none_or(|minimum| minimum <= value)
        && schema_unsigned_integer(schema, "maximum").is_none_or(|maximum| maximum >= value)
}

fn schema_unsigned_integer(schema: &Value, key: &str) -> Option<usize> {
    let value = schema.get(key)?;
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .or_else(|| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .and_then(|value| usize::try_from(value).ok())
        })
}

fn cursor_input_name(inputs: &[IrOperationInput]) -> Option<&str> {
    const CURSOR_INPUTS: &[&str] = &[
        "cursor",
        "after",
        "page_token",
        "pagetoken",
        "next_cursor",
        "nextcursor",
        "next_token",
        "nexttoken",
    ];
    inputs
        .iter()
        .filter(|input| !input.required)
        .find(|input| {
            let normalized = cursor_token(&input.name);
            CURSOR_INPUTS.contains(&normalized.as_str())
        })
        .map(|input| input.name.as_str())
}

pub(super) fn find_response_cursor_path_or_warn(
    schema: &Value,
    surface_id: &str,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Vec<String>> {
    match find_response_cursor_path(schema) {
        CursorPathSearch::Found(path) => Some(path),
        CursorPathSearch::NotFound => None,
        CursorPathSearch::DepthExceeded => {
            diagnostics.push(Diagnostic::warning(
                "MCP_RESPONSE_SCHEMA_DEPTH_EXCEEDED",
                format!("MCP response schema exceeds maximum depth of {MAX_SCHEMA_DEPTH}"),
                surface_id.to_string(),
                Some(operation_id.to_string()),
            ));
            None
        }
    }
}

pub(super) enum CursorPathSearch {
    Found(Vec<String>),
    NotFound,
    DepthExceeded,
}

pub(super) fn find_response_cursor_path(schema: &Value) -> CursorPathSearch {
    find_response_cursor_path_at_depth(schema, 0)
}

fn find_response_cursor_path_at_depth(schema: &Value, depth: usize) -> CursorPathSearch {
    if depth > MAX_SCHEMA_DEPTH {
        return CursorPathSearch::DepthExceeded;
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return CursorPathSearch::NotFound;
    };
    for (name, property) in properties {
        if is_response_cursor_property(name, property) {
            return CursorPathSearch::Found(vec![name.clone()]);
        }
    }
    let mut depth_exceeded = false;
    for (name, property) in properties {
        if !json_schema_type_contains(property, "object") {
            continue;
        }
        match find_response_cursor_path_at_depth(property, depth + 1) {
            CursorPathSearch::Found(mut path) => {
                path.insert(0, name.clone());
                return CursorPathSearch::Found(path);
            }
            CursorPathSearch::NotFound => {}
            CursorPathSearch::DepthExceeded => {
                depth_exceeded = true;
            }
        }
    }
    if depth_exceeded {
        CursorPathSearch::DepthExceeded
    } else {
        CursorPathSearch::NotFound
    }
}

fn is_response_cursor_property(name: &str, schema: &Value) -> bool {
    const RESPONSE_CURSORS: &[&str] = &["nextcursor", "nextpagetoken", "nexttoken", "endcursor"];
    RESPONSE_CURSORS.contains(&cursor_token(name).as_str())
        && (json_schema_type_contains(schema, "string") || schema.get("type").is_none())
}

fn cursor_token(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}
