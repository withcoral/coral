//! Error surfaces for Coral client bootstrap and query result decoding.

/// Errors surfaced while bootstrapping a Coral client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// A bearer token was empty or could not be encoded as gRPC metadata.
    #[error("invalid bearer token: {0}")]
    InvalidBearerToken(String),
    /// Sending authorization metadata to the endpoint would expose it over plaintext.
    #[error(
        "authorization metadata requires an HTTPS endpoint, or an HTTP loopback endpoint for local development"
    )]
    InsecureAuthorizationEndpoint,
    /// Endpoint credentials could leak through logs or authority handling.
    #[error("endpoint URLs must not include credentials")]
    EndpointCredentials,
    /// Caller-supplied request metadata was invalid.
    #[error("invalid client metadata: {0}")]
    InvalidMetadata(String),
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
