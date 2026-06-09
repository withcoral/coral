use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use coral_capabilities::{GraphqlOperationKind, HttpMethod, ResponseTrust};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use url::Url;

/// Upstream result type.
pub type Result<T> = std::result::Result<T, UpstreamError>;

pub const MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES: usize = 64 * 1024;

/// Upstream runtime errors.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// The provider returned an error response.
    #[error("provider error: {kind}")]
    Provider {
        kind: ProviderErrorKind,
        detail: String,
    },
    /// Coral cannot invoke this provider shape yet.
    #[error("unsupported upstream invocation: {0}")]
    Unsupported(String),
    /// The provider response could not be decoded.
    #[error("invalid upstream response: {0}")]
    InvalidResponse(String),
    /// The provider transport failed before Coral could classify a response.
    #[error("upstream transport error: {0}")]
    Transport(String),
}

/// Provider error kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    HttpError,
    ProtocolError,
    ToolError,
    GraphqlError,
    InvalidResponse,
}

impl fmt::Display for ProviderErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = serde_json::to_value(self).map_err(|_error| fmt::Error)?;
        f.write_str(value.as_str().unwrap_or("unknown"))
    }
}

pub(crate) fn structured_provider_error_detail(
    message: impl Into<String>,
    mut detail: Map<String, Value>,
) -> String {
    detail.insert("message".to_string(), Value::String(message.into()));
    Value::Object(detail).to_string()
}

#[must_use]
pub fn bounded_provider_diagnostic_value(value: Value) -> Value {
    let Ok(serialized) = serde_json::to_string(&value) else {
        return value;
    };
    if serialized.len() <= MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES {
        return value;
    }
    let preview = truncate_utf8_bytes(serialized, MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES);
    let preview_bytes = preview.text.len();
    serde_json::json!({
        "truncated": true,
        "json_bytes": preview.original_bytes,
        "json_preview_bytes": preview_bytes,
        "json_preview": preview.text,
    })
}

struct TruncatedText {
    text: String,
    original_bytes: usize,
}

fn truncate_utf8_bytes(mut text: String, max_bytes: usize) -> TruncatedText {
    let original_bytes = text.len();
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    TruncatedText {
        text,
        original_bytes,
    }
}

/// Secret-bearing string that redacts in debug/display contexts.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactableString {
    value: String,
}

impl RedactableString {
    /// Creates a redactable string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    /// Returns the secret-bearing value for transport code.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.value
    }

    /// Returns a redacted marker.
    #[must_use]
    pub fn redacted(&self) -> &'static str {
        "[REDACTED]"
    }
}

impl fmt::Debug for RedactableString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.redacted())
    }
}

impl fmt::Display for RedactableString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.redacted())
    }
}

/// Neutral upstream invocation plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpstreamInvocationPlan {
    Http(HttpRequestPlan),
    McpToolCall(McpToolCallPlan),
    Graphql(GraphqlRequestPlan),
}

/// HTTP request plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpRequestPlan {
    pub method: HttpMethod,
    pub url: Url,
    pub headers: Vec<(String, RedactableString)>,
    pub body: Option<UpstreamRequestBody>,
    #[serde(skip)]
    pub timeout: Option<Duration>,
    pub trace_labels: BTreeMap<String, String>,
}

/// MCP tool call plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolCallPlan {
    pub server: McpConnectionTarget,
    pub tool_name: String,
    pub arguments: Map<String, Value>,
    #[serde(skip)]
    pub timeout: Option<Duration>,
    pub trace_labels: BTreeMap<String, String>,
}

/// GraphQL request plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphqlRequestPlan {
    pub endpoint: Url,
    pub headers: Vec<(String, RedactableString)>,
    pub operation_name: String,
    pub graphql_operation_kind: GraphqlOperationKind,
    pub document: String,
    pub variables: Map<String, Value>,
    #[serde(skip)]
    pub timeout: Option<Duration>,
    pub trace_labels: BTreeMap<String, String>,
}

/// MCP connection target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpConnectionTarget {
    Stdio {
        command: String,
        args: Vec<String>,
        env: Vec<(String, RedactableString)>,
    },
    StreamableHttp {
        url: Url,
        headers: Vec<(String, RedactableString)>,
    },
}

/// Upstream response envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpstreamResponseEnvelope {
    Http(HttpUpstreamResponse),
    Mcp(McpUpstreamResponse),
    Graphql(GraphqlUpstreamResponse),
}

/// HTTP response envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpUpstreamResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub media_type: Option<String>,
    pub body: UpstreamResponseBody,
    pub response_trust: ResponseTrust,
}

/// MCP response envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpUpstreamResponse {
    pub structured_content: Option<Value>,
    pub content: Vec<McpContentBlock>,
    pub is_error: bool,
    pub meta: Option<Value>,
    pub response_trust: ResponseTrust,
}

impl McpUpstreamResponse {
    /// Converts MCP tool-result error flags into structured provider errors.
    ///
    /// # Errors
    ///
    /// Returns [`UpstreamError`] when `is_error` is true.
    pub fn into_success(self) -> Result<Self> {
        if self.is_error {
            let detail = mcp_tool_error_detail(self.structured_content, self.content, self.meta);
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::ToolError,
                detail,
            });
        }
        Ok(self)
    }
}

fn mcp_tool_error_detail(
    structured_content: Option<Value>,
    content: Vec<McpContentBlock>,
    meta: Option<Value>,
) -> String {
    let mut tool_result = serde_json::Map::new();
    if let Some(structured_content) = structured_content {
        tool_result.insert("structuredContent".to_string(), structured_content);
    }
    tool_result.insert(
        "content".to_string(),
        serde_json::to_value(content).unwrap_or(Value::Null),
    );
    tool_result.insert("isError".to_string(), Value::Bool(true));
    if let Some(meta) = meta {
        tool_result.insert("_meta".to_string(), meta);
    }

    structured_provider_error_detail(
        "upstream MCP tool returned isError=true",
        Map::from_iter([(
            "mcp_tool_result".to_string(),
            bounded_provider_diagnostic_value(Value::Object(tool_result)),
        )]),
    )
}

/// MCP content block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpContentBlock {
    Text { text: String },
    Image { data: String, mime_type: String },
    Resource { resource: Value },
}

/// GraphQL response envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphqlUpstreamResponse {
    pub http_status: u16,
    pub headers: BTreeMap<String, String>,
    pub media_type: Option<String>,
    pub data: Option<Value>,
    pub errors: Vec<Value>,
    pub extensions: Option<Value>,
    pub partial_data: Option<Value>,
    pub response_trust: ResponseTrust,
}

/// Request body variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum UpstreamRequestBody {
    Json(Value),
    Empty,
}

/// Response body variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum UpstreamResponseBody {
    Json(Value),
    Text(String),
    Bytes(Vec<u8>),
    Empty,
}
