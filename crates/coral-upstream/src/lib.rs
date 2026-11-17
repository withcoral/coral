//! One-use HTTP and MCP dispatch for Coral Tools, plus bounded MCP discovery.
//!
//! Protocol adapters own planning, serialization, bounded sending, and complete
//! result capture. They never retry, traverse pagination, or evaluate Coral
//! authority. The Invocation owner supplies retained recipes and a Run-local
//! resource registry; no policy or Policy Enforcement Point permit crosses this
//! boundary.

mod dispatch;
mod exchange;
mod http;
mod mcp;
mod span_field;

pub use dispatch::{OutcomeClass, ReadyDispatch, UpstreamOutcome};
pub use http::{HttpDecodeDiagnostic, HttpOutcome, HttpRedirect, HttpUpstream, ReadyHttpDispatch};
pub use mcp::{
    AuthorizationDiscoveryError, AuthorizationRequirement, BearerChallenge, McpOutcome,
    McpRejection, McpResources, McpUpstream, ReadyMcpDispatch, RegisteredClient,
    discover_authorization, discover_mcp, register_client,
};

#[cfg(test)]
use opentelemetry as _;
#[cfg(test)]
use opentelemetry_sdk as _;
#[cfg(test)]
use tracing_opentelemetry as _;
#[cfg(test)]
use wiremock as _;

fn media_type_essence(value: &str) -> String {
    value
        .split(';')
        .next()
        .map_or_default(str::trim)
        .to_ascii_lowercase()
}

fn is_json_media_type(essence: &str) -> bool {
    essence == "application/json" || essence.ends_with("+json")
}

fn http_client(
    policy: coral_outbound::DestinationPolicy,
) -> Result<coral_outbound::Client, ClientError> {
    coral_outbound::Client::new(policy, std::time::Duration::from_secs(30)).map_err(ClientError)
}

/// `request` with only the trace context of `span`, the client span it is
/// sent in (`coral_telemetry::set_outbound_trace_context`).
fn traced(
    request: reqwest::RequestBuilder,
    span: &tracing::Span,
) -> Result<reqwest::RequestBuilder, SendError> {
    let (client, request) = request.build_split();
    let mut request = request.map_err(SendError::send)?;
    coral_telemetry::set_outbound_trace_context(span, request.headers_mut());
    Ok(reqwest::RequestBuilder::from_parts(client, request))
}

async fn read_bounded_body(
    response: &mut reqwest::Response,
    result_max_bytes: usize,
    content_length_describes_body: bool,
) -> Result<Vec<u8>, SendError> {
    // A limit no `Content-Length` can state is one no body reaches.
    let result_max_bytes_u64 = u64::try_from(result_max_bytes).unwrap_or(u64::MAX);
    if content_length_describes_body
        && response
            .content_length()
            .is_some_and(|length| length > result_max_bytes_u64)
    {
        return Err(SendError::ResultTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| SendError::Body(error.without_url()))?
    {
        let next_length = body
            .len()
            .checked_add(chunk.len())
            .ok_or(SendError::ResultTooLarge)?;
        if next_length > result_max_bytes {
            return Err(SendError::ResultTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The HTTP client of a sender could not be initialized.
#[derive(Debug, thiserror::Error)]
#[error("HTTP client initialization failed: {0}")]
pub struct ClientError(#[source] reqwest::Error);

/// Why a Tool call could not be planned. Nothing was sent.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    /// The cataloged Tool cannot be invoked faithfully.
    #[error("Tool is unavailable")]
    ToolUnavailable,
    /// The retained binding does not match the selected protocol Adapter.
    #[error("Tool does not use the selected execution protocol")]
    UnsupportedProtocol,
    /// An available Tool has no selected HTTP server.
    #[error("available Tool has no selected HTTP server")]
    MissingServer,
    /// The retained HTTP server and path do not form a URL.
    #[error("invalid retained HTTP URL: {0}")]
    InvalidUrl(#[source] url::ParseError),
    /// The retained MCP binding calls a tool and names none.
    #[error("retained MCP binding names no tool")]
    MissingToolName,
    /// Logical arguments do not satisfy the retained wire recipe. The reason is
    /// Coral's own and reaches the Program: it may name an argument, never hold
    /// an argument value or upstream content.
    #[error("invalid Tool arguments: {0}")]
    InvalidArguments(String),
    /// A content-based parameter cannot use this media type.
    #[error("unsupported HTTP request media type: {0}")]
    UnsupportedRequestMedia(String),
    /// The encoded request crossed the accepted finite bound.
    #[error("Tool input exceeded its limit")]
    InputTooLarge,
    /// A JSON request value could not be encoded.
    #[error("HTTP Tool JSON encoding failed: {0}")]
    EncodeJson(#[source] serde_json::Error),
    /// The Adapter found its own state broken, such as a poisoned lock.
    #[error("upstream Adapter state is broken")]
    Internal,
}

impl PlanError {
    pub(crate) fn invalid_value(error: coral_value::ValueError) -> Self {
        match error {
            coral_value::ValueError::TooLarge => Self::InputTooLarge,
            coral_value::ValueError::Invalid => {
                Self::InvalidArguments("invalid Binary Value".to_string())
            }
        }
    }
}

/// Why a host credential could not join a planned request. Nothing was sent,
/// and the dispatch is unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizeError {
    /// The credential that the contract requires was not supplied, or the
    /// contract takes none.
    #[error("upstream credential required")]
    CredentialRequired,
    /// A supplied host credential cannot occupy its position safely.
    #[error("upstream credential cannot be sent")]
    CredentialUnusable,
    /// The request with its credential crossed the accepted finite bound.
    #[error("Tool input exceeded its limit")]
    InputTooLarge,
    /// The dispatch already holds its credential.
    #[error("dispatch is already authorized")]
    AlreadyAuthorized,
    /// The planned request already holds a header or a query parameter that
    /// the credential is sent in.
    #[error("upstream credential collides with the planned request")]
    CredentialCollision,
}

/// Why a dispatch produced no complete, bounded result.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The outbound destination is outside the deployment policy.
    #[error("outbound destination is not permitted")]
    DestinationRejected,
    /// The dispatch requires a credential and holds none that can be sent.
    #[error("upstream credential required")]
    CredentialRequired,
    /// A redirect target already holds a query parameter that a credential is
    /// sent in.
    #[error("upstream credential cannot be sent")]
    CredentialUnusable,
    /// No complete HTTP response was received.
    #[error("upstream HTTP send failed: {0}")]
    Send(#[source] reqwest::Error),
    /// The complete response body could not be read.
    #[error("upstream HTTP body read failed: {0}")]
    Body(#[source] reqwest::Error),
    /// The Tool Result could not be encoded.
    #[error("HTTP Tool JSON encoding failed: {0}")]
    EncodeJson(#[source] serde_json::Error),
    /// The MCP request crossed the accepted finite bound on the wire.
    #[error("Tool input exceeded its limit")]
    InputTooLarge,
    /// The complete body crossed the accepted finite bound.
    #[error("upstream result exceeded its limit")]
    ResultTooLarge,
    /// MCP initialization or a single request did not produce a complete result.
    #[error("upstream MCP request failed")]
    McpRequest,
    /// The MCP server demanded authentication; a Bearer challenge is typed when present.
    #[error("upstream MCP server requires authentication")]
    McpUnauthorized(Option<BearerChallenge>),
    /// The peer explicitly advertises an authentication method Coral cannot use.
    #[error("upstream MCP authentication method is unsupported")]
    McpAuthenticationUnsupported,
    /// The upstream protocol no longer matches the materialized recipe.
    #[error("upstream MCP protocol differs from the retained binding")]
    McpProtocolChanged,
    /// The MCP peer answered the Tool request with a JSON-RPC error instead
    /// of a result, so the request was received and refused rather than lost.
    #[error("upstream MCP peer rejected the request: {0}")]
    McpRejected(McpRejection),
    /// The upstream answered with a result that is malformed, or one that
    /// this method does not support.
    #[error("upstream result is malformed or unsupported")]
    MalformedResult,
    /// The Adapter found its own state broken, such as a poisoned lock.
    #[error("upstream Adapter state is broken")]
    Internal,
}

impl SendError {
    pub(crate) fn send(error: reqwest::Error) -> Self {
        match coral_outbound::FetchError::from(error) {
            coral_outbound::FetchError::DestinationRejected => Self::DestinationRejected,
            // reqwest names the URL in its error, and a query credential can be in it.
            coral_outbound::FetchError::Http(error) => Self::Send(error.without_url()),
        }
    }

    pub(crate) fn invalid_result(error: coral_value::ValueError) -> Self {
        match error {
            coral_value::ValueError::TooLarge => Self::ResultTooLarge,
            coral_value::ValueError::Invalid => Self::MalformedResult,
        }
    }
}

impl From<coral_outbound::DestinationRejected> for SendError {
    fn from(_: coral_outbound::DestinationRejected) -> Self {
        Self::DestinationRejected
    }
}

/// Why MCP discovery captured no catalog snapshot.
#[derive(Debug, thiserror::Error)]
pub enum McpDiscoveryError {
    /// The HTTP client could not be initialized.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// The outbound destination is outside the deployment policy.
    #[error("outbound destination is not permitted")]
    DestinationRejected,
    /// The MCP server demanded authentication; a Bearer challenge is typed when present.
    #[error("upstream MCP server requires authentication")]
    Unauthorized(Option<BearerChallenge>),
    /// The peer explicitly advertises an authentication method Coral cannot use.
    #[error("upstream MCP authentication method is unsupported")]
    AuthenticationUnsupported,
    /// The peer answered with a protocol version that Coral does not support.
    #[error("upstream MCP protocol version is unsupported")]
    UnsupportedProtocol,
    /// Initialization or a listing request did not produce a complete result.
    #[error("upstream MCP request failed")]
    Request,
    /// Discovery did not finish before its deadline.
    #[error("MCP discovery exceeded its deadline")]
    Deadline,
    /// A listing request crossed the snapshot bound on the wire.
    #[error("MCP discovery request exceeded its limit")]
    RequestTooLarge,
    /// The catalog crossed the snapshot bound, in bytes or in pages, or the
    /// snapshot it forms was refused.
    #[error("MCP discovery snapshot exceeded its limit")]
    SnapshotTooLarge,
    /// The peer answered with a result that is malformed.
    #[error("upstream result is malformed or unsupported")]
    MalformedResult,
    /// The snapshot could not be encoded.
    #[error("MCP discovery snapshot JSON encoding failed: {0}")]
    EncodeJson(#[source] serde_json::Error),
    /// The Adapter found its own state broken, such as a poisoned lock.
    #[error("upstream Adapter state is broken")]
    Internal,
}

/// A failure of any phase of a Tool dispatch, for a caller that reports one
/// without deciding on it.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A sender could not be built.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// The Tool call could not be planned.
    #[error(transparent)]
    Plan(#[from] PlanError),
    /// The host credential could not join the planned request.
    #[error(transparent)]
    Authorize(#[from] AuthorizeError),
    /// The dispatch produced no complete result.
    #[error(transparent)]
    Send(#[from] SendError),
}
