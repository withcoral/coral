//! Error surfaces for Coral client bootstrap and query result decoding.

/// Errors surfaced while bootstrapping a Coral client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Building the local server failed.
    #[error(transparent)]
    App(#[from] coral_app::AppError),
    /// Connecting the generated gRPC client failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Errors surfaced while decoding or rendering query results.
#[derive(Debug, thiserror::Error)]
pub enum QueryResultError {
    /// Arrow IPC decoding or rendering failed.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// `JSON` encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// `UTF-8` decoding failed.
    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
    /// The server returned an invalid query result payload.
    #[error("invalid query result: {0}")]
    InvalidResponse(String),
}
