use std::sync::Arc;

use coral_spec::backends::mcp::McpServerSpec;
use coral_spec::v4::{McpToolCatalog, McpToolDescriptor};
use datafusion::error::Result;
use rmcp::model::Tool;

use super::McpSourceInputs;
use super::trace::McpBodyCapture;
use super::transport::{StdioMcpToolCaller, StreamableHttpMcpToolCaller};

pub(super) async fn inspect_tools(
    source_name: String,
    server: McpServerSpec,
    source_inputs: Arc<McpSourceInputs>,
) -> Result<McpToolCatalog> {
    let body_capture = McpBodyCapture::new(None);
    let tools = match &server {
        McpServerSpec::Stdio { .. } => {
            StdioMcpToolCaller {
                source_name: source_name.clone(),
                server: server.clone(),
                source_inputs,
                body_capture,
            }
            .list_tools()
            .await
        }
        McpServerSpec::StreamableHttp { .. } => {
            StreamableHttpMcpToolCaller::new(source_name, server, source_inputs, body_capture)
                .list_tools()
                .await
        }
    }?;
    let mut tools = tools.into_iter().map(tool_descriptor).collect::<Vec<_>>();
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(McpToolCatalog { tools })
}

fn tool_descriptor(tool: Tool) -> McpToolDescriptor {
    McpToolDescriptor {
        name: tool.name.into_owned(),
        title: tool.title,
        description: tool.description.map(std::borrow::Cow::into_owned),
        input_schema: serde_json::Value::Object((*tool.input_schema).clone()),
        output_schema: tool
            .output_schema
            .map(|schema| serde_json::Value::Object((*schema).clone())),
        read_only_hint: tool
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.read_only_hint),
        idempotent_hint: tool
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.idempotent_hint),
    }
}
