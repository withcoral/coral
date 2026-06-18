use serde::{Deserialize, Serialize};

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::pagination::PaginationProvenance;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpExecutionAttachment {
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<McpPaginationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_pagination: Option<McpOffsetPaginationSpec>,
    #[serde(default)]
    pub pagination_provenance: PaginationProvenance,
}
