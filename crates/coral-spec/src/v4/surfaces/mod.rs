pub(in crate::v4) mod json_schema;
mod mcp;
mod openapi;

pub use mcp::{McpToolCatalog, McpToolDescriptor, import_mcp_surface, normalize_mcp_tool_catalog};
pub use openapi::{
    OpenApiDocumentMetadata, import_openapi_surface, normalize_source_document,
    openapi_document_metadata,
};
