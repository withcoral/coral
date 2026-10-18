//! The tools Coral's agent calls: this Adapter's own tools, bound to the caller.
//!
//! The agent sees the same schemas, descriptions and results an outside agent
//! sees, and acts only with the caller's authority. It never sees `agent`, Task
//! lifecycle or Saved Function tools: the caller owns the Task and decides what
//! to save.

use async_trait::async_trait;
use coral_agent::{RunObserved, ToolOutcome, ToolSpec, Toolbox};
use rmcp::model::{CallToolRequestParams, CallToolResponse};
use serde_json::{Map, Value, json};

use crate::arguments::{
    CancelArguments, DescribeArguments, ExecArguments, SearchArguments, ToolArguments as _,
    WaitArguments,
};
use crate::prompts::Channel;
use crate::server::{DispatchError, McpServer};

const AGENT_TOOLS: [&str; 5] = [
    SearchArguments::NAME,
    DescribeArguments::NAME,
    ExecArguments::NAME,
    WaitArguments::NAME,
    CancelArguments::NAME,
];

pub(crate) struct McpToolbox {
    server: McpServer,
}

impl McpToolbox {
    pub(crate) const fn new(server: McpServer) -> Self {
        Self { server }
    }
}

#[async_trait]
impl Toolbox for McpToolbox {
    fn instructions(&self) -> &str {
        crate::instructions::INSTRUCTIONS
    }

    fn tools(&self) -> Vec<ToolSpec> {
        crate::arguments::tools(false)
            .into_iter()
            .filter(|tool| AGENT_TOOLS.contains(&tool.name.as_ref()))
            .map(|tool| {
                let mut schema = (*tool.input_schema).clone();
                // Turns are the caller's; the agent observes only its own Runs.
                if let Some(Value::Object(properties)) = schema.get_mut("properties") {
                    properties.remove("turn_id");
                }
                ToolSpec {
                    name: tool.name.to_string(),
                    description: tool
                        .description
                        .map(|description| description.to_string())
                        .unwrap_or_default(),
                    input_schema: Value::Object(schema),
                }
            })
            .collect()
    }

    async fn call(&self, name: &str, input: Value) -> ToolOutcome {
        if !AGENT_TOOLS.contains(&name) {
            return error(
                &json!({"code": "unknown_tool", "message": format!("tool '{name}' is not available")}),
            );
        }
        let arguments = match input {
            Value::Object(arguments) => arguments,
            Value::Null => Map::new(),
            _ => {
                return error(
                    &json!({"code": "invalid_params", "message": "tool input must be an object"}),
                );
            }
        };
        let request = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
        match self.server.dispatch(request, Channel::Unavailable).await {
            Ok(CallToolResponse::Complete(result)) => {
                let structured = result.structured_content.clone().unwrap_or(Value::Null);
                let text = result
                    .content
                    .first()
                    .and_then(|block| block.as_text())
                    .map_or_else(|| structured.to_string(), |text| text.text.clone());
                ToolOutcome {
                    text,
                    is_error: result.is_error.unwrap_or(false),
                    run: run_observed(&structured),
                }
            }
            Ok(_other) => {
                error(&json!({"code": "unsupported", "message": "this result needs a person"}))
            }
            Err(DispatchError::Tool(failure)) => error(
                &serde_json::to_value(&failure)
                    .unwrap_or_else(|_error| json!({"code": "internal_error"})),
            ),
            Err(DispatchError::Protocol(failure)) => {
                error(&json!({"code": "invalid_params", "message": failure.message}))
            }
        }
    }
}

fn error(value: &Value) -> ToolOutcome {
    ToolOutcome {
        text: value.to_string(),
        is_error: true,
        run: None,
    }
}

/// Reads the Run page this Adapter projected, so the turn can track actions and evidence.
fn run_observed(page: &Value) -> Option<RunObserved> {
    let run_id = page.get("run_id")?.as_str()?.to_owned();
    let state = page.get("state")?.as_str()?;
    let has_more = page
        .get("has_more")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(RunObserved {
        run_id,
        succeeded: state == "succeeded" && !has_more,
        required_actions: page
            .get("required_actions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    })
}
