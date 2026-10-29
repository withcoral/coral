//! One-use HTTP and MCP dispatch for Coral Tools, plus bounded MCP discovery.
//!
//! Protocol adapters own planning, serialization, bounded sending, and complete
//! result capture. They never retry, traverse pagination, or evaluate Coral
//! authority. The Invocation owner supplies retained recipes and a Run-local
//! resource registry; no policy or fenced-dispatch permit crosses this boundary.

mod dispatch;
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
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn is_json_media_type(essence: &str) -> bool {
    essence == "application/json" || essence.ends_with("+json")
}

fn http_client(policy: coral_outbound::DestinationPolicy) -> Result<coral_outbound::Client, Error> {
    coral_outbound::Client::new(policy, std::time::Duration::from_secs(30)).map_err(Error::Client)
}

/// Records one dispatch to an Upstream Service over `protocol` that began
/// at `started` and ended `sent`, with the body sizes it carried.
fn record_exchange(
    protocol: coral_telemetry::metric::ProviderProtocol,
    sent: Result<(), &Error>,
    started: std::time::Instant,
    request_bytes: Option<u64>,
    response_bytes: Option<u64>,
) {
    use coral_telemetry::metric::UpstreamOutcome;
    let outcome = match sent {
        Ok(()) => UpstreamOutcome::Response,
        Err(Error::ResultTooLarge) => UpstreamOutcome::TooLarge,
        Err(_) => UpstreamOutcome::Failed,
    };
    coral_telemetry::metric::upstream_exchanged(
        protocol,
        outcome,
        started.elapsed(),
        request_bytes,
        response_bytes,
    );
}

/// `request` with only the trace context of `span`, the client span it is
/// sent in (`coral_telemetry::set_outbound_trace_context`).
fn traced(
    request: reqwest::RequestBuilder,
    span: &tracing::Span,
) -> Result<reqwest::RequestBuilder, Error> {
    let (client, request) = request.build_split();
    let mut request = request.map_err(Error::send)?;
    coral_telemetry::set_outbound_trace_context(span, request.headers_mut());
    Ok(reqwest::RequestBuilder::from_parts(client, request))
}

async fn read_bounded_body(
    response: &mut reqwest::Response,
    result_max_bytes: usize,
    content_length_describes_body: bool,
) -> Result<Vec<u8>, Error> {
    let result_max_bytes_u64 =
        u64::try_from(result_max_bytes).map_err(|_error| Error::InvalidLimit)?;
    if content_length_describes_body
        && response
            .content_length()
            .is_some_and(|length| length > result_max_bytes_u64)
    {
        return Err(Error::ResultTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Error::Body(error.without_url()))?
    {
        let next_length = body
            .len()
            .checked_add(chunk.len())
            .ok_or(Error::ResultTooLarge)?;
        if next_length > result_max_bytes {
            return Err(Error::ResultTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl From<coral_outbound::DestinationRejected> for Error {
    fn from(_: coral_outbound::DestinationRejected) -> Self {
        Self::DestinationRejected
    }
}

impl Error {
    pub(crate) fn send(error: reqwest::Error) -> Self {
        match coral_outbound::FetchError::from(error) {
            coral_outbound::FetchError::DestinationRejected => Self::DestinationRejected,
            // reqwest names the URL in its error, and a query credential can be in it.
            coral_outbound::FetchError::Http(error) => Self::Send(error.without_url()),
        }
    }

    pub(crate) fn invalid_value(error: coral_value::ValueError) -> Self {
        match error {
            coral_value::ValueError::TooLarge => Self::InputTooLarge,
            coral_value::ValueError::Invalid => {
                Self::InvalidArguments("invalid Binary Value".to_string())
            }
        }
    }

    pub(crate) fn invalid_result(error: coral_value::ValueError) -> Self {
        match error {
            coral_value::ValueError::TooLarge => Self::ResultTooLarge,
            coral_value::ValueError::Invalid => Self::McpResult,
        }
    }
}

/// Upstream planning, host credential injection, or send failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The outbound destination is outside the deployment policy.
    #[error("outbound destination is not permitted")]
    DestinationRejected,
    /// The exact required host credential was not supplied safely.
    #[error("upstream credential required")]
    CredentialRequired,
    /// A supplied host credential cannot occupy its position safely.
    #[error("upstream credential cannot be sent")]
    CredentialUnusable,
    /// The cataloged Tool cannot be invoked faithfully.
    #[error("Tool is unavailable")]
    ToolUnavailable,
    /// The retained binding does not match the selected protocol Adapter.
    #[error("Tool does not use the selected execution protocol")]
    UnsupportedProtocol,
    /// A finite result limit must be positive.
    #[error("upstream result limit must be positive")]
    InvalidLimit,
    /// An available Tool has no selected HTTP server.
    #[error("available Tool has no selected HTTP server")]
    MissingServer,
    /// The retained HTTP server and path do not form a URL.
    #[error("invalid retained HTTP URL: {0}")]
    InvalidUrl(#[source] url::ParseError),
    /// The HTTP client could not be initialized.
    #[error("HTTP client initialization failed: {0}")]
    Client(#[source] reqwest::Error),
    /// No complete HTTP response was received.
    #[error("upstream HTTP send failed: {0}")]
    Send(#[source] reqwest::Error),
    /// The complete response body could not be read.
    #[error("upstream HTTP body read failed: {0}")]
    Body(#[source] reqwest::Error),
    /// Logical arguments do not satisfy the retained wire recipe. The reason is
    /// Coral's own and reaches the Program: it may name an argument, never hold
    /// an argument value or upstream content.
    #[error("invalid Tool arguments: {0}")]
    InvalidArguments(String),
    /// A JSON request value could not be encoded.
    #[error("HTTP Tool JSON encoding failed: {0}")]
    EncodeJson(#[source] serde_json::Error),
    /// A content-based parameter cannot use this media type.
    #[error("unsupported HTTP request media type: {0}")]
    UnsupportedRequestMedia(String),
    /// The encoded request crossed the accepted finite bound before dispatch.
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
    /// An MCP peer did not return the complete result required by this method.
    #[error("upstream MCP result is incomplete or unsupported")]
    McpResult,
}
