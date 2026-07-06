use serde::{Deserialize, Serialize};

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpExecutionAttachment {
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<McpPaginationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_pagination: Option<McpOffsetPaginationSpec>,
}
