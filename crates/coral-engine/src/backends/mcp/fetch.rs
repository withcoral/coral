use async_trait::async_trait;
use coral_spec::ResponseSpec;
use coral_spec::backends::mcp::McpPaginationSpec;
use datafusion::error::{DataFusionError, Result};
use rmcp::model::JsonObject;
use serde_json::Value;

use super::client::McpSourceClient;
use super::error::McpProviderQueryError;
use crate::backends::shared::json_exec::RowFetcher;
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;

const DEFAULT_MCP_MAX_PAGES: usize = 100;

#[derive(Debug)]
pub(super) struct McpFetchPlan {
    pub(super) backend: McpSourceClient,
    pub(super) source_schema: String,
    pub(super) relation: String,
    pub(super) tool_name: String,
    pub(super) arguments: JsonObject,
    pub(super) response: ResponseSpec,
    pub(super) pagination: Option<McpPaginationSpec>,
    pub(super) limit: Option<usize>,
}

#[async_trait]
impl RowFetcher for McpFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        let mut all_rows = Vec::new();
        let mut next_cursor: Option<Value> = None;
        let mut page_count = 0usize;
        let max_pages = self
            .pagination
            .as_ref()
            .and_then(|pagination| pagination.max_pages)
            .unwrap_or(DEFAULT_MCP_MAX_PAGES);

        loop {
            page_count += 1;
            if page_count > max_pages {
                return Err(DataFusionError::External(Box::new(
                    McpProviderQueryError::Pagination {
                        source_schema: self.source_schema.clone(),
                        relation: self.relation.clone(),
                        tool: self.tool_name.clone(),
                        detail: format!("exceeded pagination max_pages={max_pages}"),
                    },
                )));
            }

            let arguments = self.arguments_for_cursor(next_cursor.as_ref());
            let payload = self
                .backend
                .call_tool(&self.relation, &self.tool_name, arguments)
                .await?;
            if let Some(detail) = detect_payload_error(&self.response, &payload) {
                return Err(DataFusionError::External(Box::new(
                    McpProviderQueryError::ToolReturnedError {
                        source_schema: self.source_schema.clone(),
                        relation: self.relation.clone(),
                        tool: self.tool_name.clone(),
                        detail,
                    },
                )));
            }
            let mut rows = extract_rows(&self.response, &payload);
            all_rows.append(&mut rows);
            if let Some(limit) = self.limit
                && all_rows.len() >= limit
            {
                all_rows.truncate(limit);
                break;
            }

            let Some(pagination) = &self.pagination else {
                break;
            };
            match next_page_cursor(pagination, &payload) {
                Some(cursor) => next_cursor = Some(cursor),
                None => break,
            }
        }
        Ok(all_rows)
    }
}

impl McpFetchPlan {
    fn arguments_for_cursor(&self, cursor: Option<&Value>) -> JsonObject {
        let Some((pagination, cursor)) = self.pagination.as_ref().zip(cursor) else {
            return self.arguments.clone();
        };
        let mut arguments = self.arguments.clone();
        arguments.insert(pagination.cursor_arg.clone(), cursor.clone());
        arguments
    }
}

/// Returns the error detail when `response.error_path` resolves to a non-null
/// value in `payload`. Lets a tool that signals failure inside a successful
/// MCP response (e.g. `ClickHouse`'s `{ "result": { "status": "error",
/// "message": "..." } }` shape) be surfaced as a structured engine error
/// instead of being silently extracted as zero rows.
fn detect_payload_error(response: &ResponseSpec, payload: &Value) -> Option<String> {
    if response.error_path.is_empty() {
        return None;
    }
    let value = get_path_value(payload, &response.error_path)?;
    if value.is_null() {
        return None;
    }
    Some(match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    })
}

fn next_page_cursor(pagination: &McpPaginationSpec, payload: &Value) -> Option<Value> {
    let value = get_path_value(payload, &pagination.response_cursor_path)?;
    match value {
        Value::Null => None,
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| Value::String(trimmed.to_string()))
        }
        other => Some(other.clone()),
    }
}
