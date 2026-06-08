use serde::{Deserialize, Serialize};

use crate::backends::mcp::McpPaginationSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpExecutionAttachment {
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<McpPaginationSpec>,
}
