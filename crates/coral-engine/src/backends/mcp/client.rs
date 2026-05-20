use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::McpServerSpec;
use datafusion::error::{DataFusionError, Result};
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientInfo, Implementation, JsonObject};
use rmcp::transport::ConfigureCommandExt;
use rmcp::{ClientHandler, ServiceExt};
use serde_json::Value;
use tokio::process::Command;

use super::error::McpProviderQueryError;
use crate::backends::shared::template::{RenderContext, resolve_value_source};

#[derive(Clone)]
pub(super) struct McpSourceClient {
    caller: Arc<dyn McpToolCaller>,
}

impl McpSourceClient {
    pub(super) fn new(caller: Arc<dyn McpToolCaller>) -> Self {
        Self { caller }
    }

    pub(super) async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        self.caller.call_tool(relation, tool_name, arguments).await
    }
}

impl std::fmt::Debug for McpSourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpSourceClient").finish_non_exhaustive()
    }
}

#[async_trait]
pub(super) trait McpToolCaller: std::fmt::Debug + Send + Sync {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value>;
}

#[derive(Debug)]
pub(super) struct StdioMcpToolCaller {
    pub(super) source_name: String,
    pub(super) server: McpServerSpec,
    pub(super) resolved_inputs: Arc<BTreeMap<String, String>>,
}

#[async_trait]
impl McpToolCaller for StdioMcpToolCaller {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        let mut command = Command::new(&self.server.command);
        command.args(&self.server.args);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let render_context = RenderContext::source_scoped(&self.resolved_inputs);
        for env in &self.server.env {
            let Some(value) = resolve_value_source(&env.value, &render_context)? else {
                continue;
            };
            command.env(&env.name, value_to_env_string(value));
        }

        let transport = rmcp::transport::TokioChildProcess::new(command.configure(|cmd| {
            cmd.kill_on_drop(true);
        }))
        .map_err(|error| {
            DataFusionError::External(Box::new(McpProviderQueryError::ServerStart {
                source_schema: self.source_name.clone(),
                detail: error.to_string(),
            }))
        })?;
        let client = McpClientHandler::new(&self.source_name)
            .serve(transport)
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::Initialize {
                    source_schema: self.source_name.clone(),
                    detail: error.to_string(),
                }))
            })?;
        let result = client
            .call_tool(CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments))
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::ToolCall {
                    source_schema: self.source_name.clone(),
                    relation: relation.to_string(),
                    tool: tool_name.to_string(),
                    detail: error.to_string(),
                }))
            })?;
        normalize_tool_result(&self.source_name, relation, tool_name, result)
    }
}

#[derive(Debug, Clone)]
struct McpClientHandler {
    client_info: ClientInfo,
}

impl McpClientHandler {
    fn new(source_name: &str) -> Self {
        let mut client_info = ClientInfo::default();
        client_info.client_info = Implementation::new(
            format!("coral-engine/{source_name}"),
            env!("CARGO_PKG_VERSION"),
        );
        Self { client_info }
    }
}

impl ClientHandler for McpClientHandler {
    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }
}

fn normalize_tool_result(
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
    }
    Ok(Value::Null)
}

fn value_to_env_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}
