//! Per-instance MCP transport implementations.
//!
//! Today only stdio (`StdioMcpToolCaller`) is supported — it spawns the MCP
//! server as a child process for each tool call. A future HTTP-streaming
//! transport would slot in alongside it as another `McpToolCaller` impl.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::McpServerSpec;
use datafusion::error::{DataFusionError, Result};
use rmcp::model::{CallToolRequestParams, ClientInfo, Implementation, JsonObject};
use rmcp::transport::ConfigureCommandExt;
use rmcp::{ClientHandler, ServiceExt};
use serde_json::Value;
use tokio::process::Command;

use super::McpSourceInputs;
use super::client::McpToolCaller;
use super::error::McpProviderQueryError;
use super::response::normalize_tool_result;
use crate::backends::shared::template::{RenderContext, resolve_value_source};

#[derive(Debug)]
pub(super) struct StdioMcpToolCaller {
    pub(super) source_name: String,
    pub(super) server: McpServerSpec,
    pub(super) source_inputs: Arc<McpSourceInputs>,
}

impl StdioMcpToolCaller {
    pub(super) async fn resolved_server_env(&self) -> Result<Vec<(String, String)>> {
        if self.server.env.is_empty() {
            return Ok(Vec::new());
        }
        let resolved_inputs = self.source_inputs.resolve_for_request().await?;
        let render_context = RenderContext::source_scoped(&resolved_inputs);
        let mut env = Vec::with_capacity(self.server.env.len());
        for spec in &self.server.env {
            let Some(value) = resolve_value_source(&spec.value, &render_context)? else {
                continue;
            };
            env.push((spec.name.clone(), value_to_env_string(value)));
        }
        Ok(env)
    }
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

        for (name, value) in self.resolved_server_env().await? {
            command.env(name, value);
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

fn value_to_env_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}
