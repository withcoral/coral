use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::ResponseSpec;
use coral_spec::ValueSourceSpec;
use coral_spec::backends::mcp::McpPaginationSpec;
use datafusion::error::{DataFusionError, Result};
use rmcp::model::JsonObject;
use serde_json::Value;

use super::error::McpProviderQueryError;
use super::{McpSourceInputs, McpToolCaller};
use crate::backends::shared::json_exec::RowFetcher;
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::{
    ResponseErrorPolicy, detect_response_error, extract_rows,
};
use crate::backends::shared::template::{RenderContext, resolve_value_source};

const DEFAULT_MCP_MAX_PAGES: usize = 100;

#[derive(Debug)]
pub(super) struct McpFetchPlan {
    pub(super) backend: Arc<dyn McpToolCaller>,
    pub(super) source_schema: String,
    pub(super) relation: String,
    pub(super) tool_name: String,
    pub(super) arguments: JsonObject,
    pub(super) source_inputs: Option<Arc<McpSourceInputs>>,
    pub(super) source_tool_args: Arc<BTreeMap<String, ValueSourceSpec>>,
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

            let arguments = self.arguments_for_cursor(next_cursor.as_ref()).await?;
            let payload = self
                .backend
                .call_tool(&self.relation, &self.tool_name, arguments)
                .await?;
            if let Some(detail) = detect_response_error(
                &self.response,
                &payload,
                ResponseErrorPolicy::OkPathOrErrorPath(
                    "tool reported failure via ok_path but no error_path detail was provided",
                ),
            ) {
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
    async fn arguments_for_cursor(&self, cursor: Option<&Value>) -> Result<JsonObject> {
        let mut arguments = JsonObject::new();
        if !self.source_tool_args.is_empty() {
            let source_inputs = self.source_inputs.as_ref().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{}.{} has MCP tool args but no source input resolver state",
                    self.source_schema, self.relation
                ))
            })?;
            let resolved_inputs = source_inputs.resolve_for_request().await?;
            let render_context = RenderContext::source_scoped(&resolved_inputs);
            for (name, source) in self.source_tool_args.iter() {
                if let Some(value) = resolve_value_source(source, &render_context)? {
                    arguments.insert(name.clone(), value);
                }
            }
        }
        arguments.extend(self.arguments.clone());
        if let Some((pagination, cursor)) = self.pagination.as_ref().zip(cursor) {
            arguments.insert(pagination.cursor_arg.clone(), cursor.clone());
        }
        Ok(arguments)
    }
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
