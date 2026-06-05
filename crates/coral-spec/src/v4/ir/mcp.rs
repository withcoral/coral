use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpExecutionAttachment {
    pub tool_name: String,
}
