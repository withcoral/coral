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
        let mut next_cursor: Option<String> = None;
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

            let arguments = self.arguments_for_cursor(next_cursor.as_deref());
            let payload = self
                .backend
                .call_tool(&self.relation, &self.tool_name, arguments)
                .await?;
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
    fn arguments_for_cursor(&self, cursor: Option<&str>) -> JsonObject {
        let Some((pagination, cursor)) = self.pagination.as_ref().zip(cursor) else {
            return self.arguments.clone();
        };
        let mut arguments = self.arguments.clone();
        arguments.insert(
            pagination.cursor_arg.clone(),
            Value::String(cursor.to_string()),
        );
        arguments
    }
}

fn next_page_cursor(pagination: &McpPaginationSpec, payload: &Value) -> Option<String> {
    get_path_value(payload, &pagination.response_cursor_path)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|cursor| !cursor.is_empty())
        .map(ToOwned::to_owned)
}
