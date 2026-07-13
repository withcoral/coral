use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};

#[derive(Debug, Clone)]
pub struct McpExecutionAttachment {
    pub tool_name: String,
    pub pagination: Option<McpPaginationSpec>,
    pub offset_pagination: Option<McpOffsetPaginationSpec>,
}
