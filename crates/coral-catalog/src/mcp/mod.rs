mod binding;
mod input;
mod materialize;
mod projection;

pub use binding::{
    McpExecutionBinding, McpInvocationMethod, McpTaskSupport, McpToolAnnotations, McpTransport,
};
pub use input::{MCP_DISCOVERY_SNAPSHOT_MAX_BYTES, McpDiscoverySnapshot};
pub(super) use materialize::{MATERIALIZER_VERSION, materialize};
pub(crate) use projection::{declares_input, typescript_names};
