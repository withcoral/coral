use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::ResponseSpec;
use coral_spec::ValueSourceSpec;
use coral_spec::backends::mcp::McpPaginationSpec;
use datafusion::error::{DataFusionError, Result};
use rmcp::model::JsonObject;
use serde_json::Value;

use super::McpSourceInputs;
use super::client::McpSourceClient;
use super::error::McpProviderQueryError;
use crate::backends::shared::json_exec::RowFetcher;
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{RenderContext, resolve_value_source};

const DEFAULT_MCP_MAX_PAGES: usize = 100;

#[derive(Debug)]
pub(super) struct McpFetchPlan {
    pub(super) backend: McpSourceClient,
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

/// Returns an error detail string when the payload signals failure.
///
/// Two manifest conventions are supported, matching the shared `ResponseSpec`
/// shape used by HTTP sources:
///
/// 1. **`ok_path` discriminator** — when `ok_path` is set, its boolean value
///    decides success/failure. The same semantics HTTP uses: a non-`true`
///    value (including missing, non-bool, or `false`) triggers a failure,
///    and `error_path` is read only to populate the detail. Manifests that
///    declare both `ok_path` and a permanently-present `error_path` field
///    therefore do not misclassify successful responses.
/// 2. **`error_path`-only sentinel** — when `ok_path` is empty, the presence
///    of a non-null value at `error_path` is itself the failure signal. This
///    is the shape `ClickHouse`'s MCP server uses (`{ "result": { "status":
///    "error", "message": "..." } }`), where `message` only appears on the
///    error branch.
fn detect_payload_error(response: &ResponseSpec, payload: &Value) -> Option<String> {
    if !response.ok_path.is_empty() {
        let ok = get_path_value(payload, &response.ok_path)
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if ok {
            return None;
        }
        return Some(error_path_detail(response, payload).unwrap_or_else(|| {
            "tool reported failure via ok_path but no error_path detail was provided".to_string()
        }));
    }
    if response.error_path.is_empty() {
        return None;
    }
    error_path_detail(response, payload)
}

/// Returns the value at `response.error_path` rendered as a string, if it is
/// present and non-null. Returns `None` when `error_path` is empty, missing
/// from the payload, or explicitly null.
fn error_path_detail(response: &ResponseSpec, payload: &Value) -> Option<String> {
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
