//! Response interpretation for MCP tool calls.
//!
//! Converts an `rmcp` `CallToolResult` into the JSON payload Coral's row
//! extractor consumes, or into a structured `McpProviderQueryError` when the
//! tool signals an error. Generic JSON-to-row extraction lives in
//! `backends/shared/response_rows.rs`.

use datafusion::error::{DataFusionError, Result};
use rmcp::model::CallToolResult;
use serde_json::Value;

use super::error::McpProviderQueryError;

pub(super) fn normalize_tool_result(
    source_schema: &str,
    relation: &str,
    tool_name: &str,
    result: CallToolResult,
) -> Result<Value> {
    if result.is_error.unwrap_or(false) {
        let detail = result
            .content
            .iter()
            .find_map(|content| content.as_text().map(|text| text.text.clone()))
            .or_else(|| {
                result.structured_content.as_ref().map(|value| match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                })
            })
            .unwrap_or_else(|| "tool reported isError=true with no content".to_string());
        return Err(DataFusionError::External(Box::new(
            McpProviderQueryError::ToolReturnedError {
                source_schema: source_schema.to_string(),
                relation: relation.to_string(),
                tool: tool_name.to_string(),
                detail,
            },
        )));
    }
    if let Some(value) = result.structured_content {
        return Ok(value);
    }
    let mut saw_non_text_content = false;
    for content in &result.content {
        if let Some(text) = content.as_text() {
            return serde_json::from_str(&text.text).map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::ResultDecode {
                    source_schema: source_schema.to_string(),
                    relation: relation.to_string(),
                    tool: tool_name.to_string(),
                    detail: error.to_string(),
                }))
            });
        }
        saw_non_text_content = true;
    }
    if saw_non_text_content {
        return Err(DataFusionError::External(Box::new(
            McpProviderQueryError::ResultDecode {
                source_schema: source_schema.to_string(),
                relation: relation.to_string(),
                tool: tool_name.to_string(),
                detail: "tool returned only non-text content (e.g. image, audio, resource) with no structured_content; Coral cannot decode this into rows".to_string(),
            },
        )));
    }
    // Empty `content` with no `structured_content` and `is_error` unset is a
    // legitimate "tool succeeded with no output" — surface as zero rows.
    Ok(Value::Null)
}
