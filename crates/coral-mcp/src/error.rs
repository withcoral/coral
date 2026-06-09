//! Error surface for the Coral MCP stdio adapter.

/// Errors surfaced by the `MCP` stdio server.
#[derive(Debug, thiserror::Error)]
#[expect(
    clippy::large_enum_variant,
    reason = "Top-level process error returned once from run_stdio_with_client; boxing the rmcp init error would drop the ergonomic #[from]/? conversion for no runtime benefit."
)]
pub enum McpError {
    /// The RMCP server failed before entering its main service loop.
    #[error(transparent)]
    Initialize(#[from] rmcp::service::ServerInitializeError),
    /// The RMCP server task failed while waiting for shutdown.
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}
