//! Error surface for the Coral MCP stdio adapter.

/// Errors surfaced by the `MCP` stdio server.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// Building or using the Coral client failed.
    #[error(transparent)]
    Client(#[from] coral_client::ClientError),
    /// The RMCP server lifecycle failed.
    #[error(transparent)]
    Rmcp(#[from] rmcp::RmcpError),
    /// The RMCP server failed before entering its main service loop.
    #[error(transparent)]
    Initialize(#[from] rmcp::service::ServerInitializeError),
    /// The local gRPC layer returned a transport error.
    #[error(transparent)]
    Grpc(#[from] tonic::Status),
    /// `JSON` encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The RMCP server task failed while waiting for shutdown.
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}
