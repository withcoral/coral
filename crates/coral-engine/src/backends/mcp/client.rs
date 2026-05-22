//! MCP client wrapper and the `McpToolCaller` transport interface.
//!
//! `McpSourceClient` is the configured stateful object the rest of the
//! backend talks to; it delegates the actual tool call to an
//! `McpToolCaller`. Concrete transport implementations live in
//! `transport.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result;
use rmcp::model::JsonObject;
use serde_json::Value;

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
