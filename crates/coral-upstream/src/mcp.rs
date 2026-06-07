use std::collections::BTreeMap;
use std::future::Future;
use std::process::Stdio;
use std::time::Duration;

use coral_capabilities::ResponseTrust;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use rmcp::model::{CallToolRequestParams, ClientInfo, Implementation};
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{ClientHandler, ServiceExt};
use serde_json::{Map, Value};
use tokio::process::Command;
use url::Url;

use crate::http::{
    MAX_PROVIDER_RESPONSE_BYTES, limited_response_bytes, response_headers, response_media_type,
    upstream_http_client,
};
use crate::{
    McpConnectionTarget, McpContentBlock, McpToolCallPlan, McpUpstreamResponse, ProviderErrorKind,
    RedactableString, Result, UpstreamError,
};

pub(crate) const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
pub(crate) const MCP_PROTOCOL_VERSION_HEADER: &str = "MCP-Protocol-Version";
pub(crate) const MCP_SESSION_ID_HEADER: &str = "Mcp-Session-Id";
const MCP_ACCEPT: &str = "application/json, text/event-stream";
const MAX_MCP_TOOLS_LIST_PAGES: usize = 16;

pub(crate) struct McpHttpSession<'a> {
    pub(crate) client: reqwest::Client,
    pub(crate) url: &'a Url,
    pub(crate) headers: &'a [(String, RedactableString)],
    pub(crate) timeout: Option<Duration>,
    pub(crate) session_id: Option<String>,
    pub(crate) next_id: u64,
}

pub(crate) struct McpJsonResponse {
    pub(crate) value: Option<Value>,
    pub(crate) headers: BTreeMap<String, String>,
}

/// Lists tools from an upstream MCP server using Coral's provider transport
/// semantics.
///
/// # Errors
///
/// Returns [`UpstreamError`] when the server cannot be initialized, rejects
/// `tools/list`, returns malformed MCP payloads, or times out.
pub async fn list_mcp_tools(
    server: &McpConnectionTarget,
    timeout: Option<Duration>,
) -> Result<Value> {
    match server {
        McpConnectionTarget::Stdio { command, args, env } => {
            with_timeout(
                timeout,
                "MCP stdio tools/list",
                list_stdio_mcp_tools(command, args, env),
            )
            .await
        }
        McpConnectionTarget::StreamableHttp { url, headers } => {
            with_timeout(
                timeout,
                "MCP Streamable HTTP tools/list",
                list_streamable_http_mcp_tools(url, headers, timeout),
            )
            .await
        }
    }
}

pub(crate) async fn execute_mcp_tool_call_plan(
    plan: &McpToolCallPlan,
) -> Result<McpUpstreamResponse> {
    match &plan.server {
        McpConnectionTarget::Stdio { command, args, env } => {
            return with_timeout(
                plan.timeout,
                "MCP stdio tools/call",
                execute_stdio_mcp_tool_call(
                    command,
                    args,
                    env,
                    &plan.tool_name,
                    plan.arguments.clone(),
                ),
            )
            .await;
        }
        McpConnectionTarget::StreamableHttp { url, headers } => {
            return with_timeout(
                plan.timeout,
                "MCP Streamable HTTP tools/call",
                execute_streamable_http_mcp_tool_call(url, headers, plan),
            )
            .await;
        }
    }
}

async fn execute_streamable_http_mcp_tool_call(
    url: &Url,
    headers: &[(String, RedactableString)],
    plan: &McpToolCallPlan,
) -> Result<McpUpstreamResponse> {
    let client = upstream_http_client()?;
    let mut session = McpHttpSession::initialize(client, url, headers, plan.timeout).await?;
    let params = serde_json::json!({
        "name": &plan.tool_name,
        "arguments": &plan.arguments,
    });
    let value = session.post_request("tools/call", &params).await?;
    mcp_tool_response_from_json_rpc(value)?.into_success()
}

async fn execute_stdio_mcp_tool_call(
    command: &str,
    args: &[String],
    env: &[(String, RedactableString)],
    tool_name: &str,
    arguments: Map<String, Value>,
) -> Result<McpUpstreamResponse> {
    let mut client = connect_stdio_mcp(command, args, env).await?;
    let raw = client
        .call_tool(CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments))
        .await
        .map_err(|error| UpstreamError::Provider {
            kind: ProviderErrorKind::ToolError,
            detail: error.to_string(),
        })?;
    let raw_value = serde_json::to_value(raw)
        .map_err(|error| UpstreamError::InvalidResponse(error.to_string()))?;
    let response = mcp_tool_response_from_result_value(&raw_value);
    let _close_result = client
        .close_with_timeout(Duration::from_secs(1))
        .await
        .map_err(|error| {
            UpstreamError::Transport(format!("failed to close MCP stdio session: {error}"))
        })?;
    response?.into_success()
}

async fn list_stdio_mcp_tools(
    command: &str,
    args: &[String],
    env: &[(String, RedactableString)],
) -> Result<Value> {
    let mut client = connect_stdio_mcp(command, args, env).await?;
    let tools = client
        .list_all_tools()
        .await
        .map_err(|error| UpstreamError::Provider {
            kind: ProviderErrorKind::ProtocolError,
            detail: error.to_string(),
        })?;
    let value = serde_json::json!({ "tools": tools });
    let _close_result = client
        .close_with_timeout(Duration::from_secs(1))
        .await
        .map_err(|error| {
            UpstreamError::Transport(format!("failed to close MCP stdio session: {error}"))
        })?;
    Ok(value)
}

async fn list_streamable_http_mcp_tools(
    url: &Url,
    headers: &[(String, RedactableString)],
    timeout: Option<Duration>,
) -> Result<Value> {
    let client = upstream_http_client()?;
    let mut session = McpHttpSession::initialize(client, url, headers, timeout).await?;
    let mut tools = Vec::new();
    let mut cursor = None;
    for _ in 0..MAX_MCP_TOOLS_LIST_PAGES {
        let mut params = Map::new();
        if let Some(value) = cursor.take() {
            params.insert("cursor".to_string(), value);
        }
        let response = session
            .post_request("tools/list", &Value::Object(params))
            .await?;
        let mut result = json_rpc_result("tools/list", response)?;
        let Some(result_object) = result.as_object_mut() else {
            return Err(UpstreamError::InvalidResponse(
                "MCP tools/list result must be a JSON object".to_string(),
            ));
        };
        let Some(Value::Array(page_tools)) = result_object.remove("tools") else {
            return Err(UpstreamError::InvalidResponse(
                "MCP tools/list result did not contain a tools array".to_string(),
            ));
        };
        tools.extend(page_tools);
        cursor = next_tools_list_cursor(result_object.remove("nextCursor"))?;
        if cursor.is_none() {
            return Ok(serde_json::json!({ "tools": tools }));
        }
    }
    Err(UpstreamError::InvalidResponse(format!(
        "MCP tools/list exceeded {MAX_MCP_TOOLS_LIST_PAGES} pages"
    )))
}

fn next_tools_list_cursor(value: Option<Value>) -> Result<Option<Value>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(cursor)) => Ok(Some(Value::String(cursor))),
        Some(other) => Err(UpstreamError::InvalidResponse(format!(
            "MCP tools/list nextCursor must be a string when present, got {}",
            json_type_name(&other)
        ))),
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

async fn connect_stdio_mcp(
    command: &str,
    args: &[String],
    env: &[(String, RedactableString)],
) -> Result<rmcp::service::RunningService<rmcp::RoleClient, McpClientHandler>> {
    let mut process = Command::new(command);
    process.args(args);
    process.env_clear();
    process
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    for (name, value) in env {
        process.env(name, value.expose_secret());
    }
    let transport = TokioChildProcess::new(process.configure(|cmd| {
        cmd.kill_on_drop(true);
    }))
    .map_err(|error| {
        UpstreamError::Transport(format!("failed to start MCP stdio server: {error}"))
    })?;
    McpClientHandler::new()
        .serve(transport)
        .await
        .map_err(|error| UpstreamError::Provider {
            kind: ProviderErrorKind::ProtocolError,
            detail: format!("failed to initialize MCP stdio server: {error}"),
        })
}

async fn with_timeout<T, F>(timeout: Option<Duration>, context: &str, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if let Some(timeout_duration) = timeout {
        return tokio::time::timeout(timeout_duration, future)
            .await
            .map_err(|_elapsed| {
                UpstreamError::Transport(format!("{context} timed out after {timeout_duration:?}"))
            })?;
    }
    future.await
}

#[derive(Debug, Clone)]
struct McpClientHandler {
    client_info: ClientInfo,
}

impl McpClientHandler {
    fn new() -> Self {
        let mut client_info = ClientInfo::default();
        client_info.client_info = Implementation::new("coral-upstream", env!("CARGO_PKG_VERSION"));
        Self { client_info }
    }
}

impl ClientHandler for McpClientHandler {
    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }
}

impl<'a> McpHttpSession<'a> {
    async fn initialize(
        client: reqwest::Client,
        url: &'a Url,
        headers: &'a [(String, RedactableString)],
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let mut session = Self {
            client,
            url,
            headers,
            timeout,
            session_id: None,
            next_id: 1,
        };
        let initialize = serde_json::json!({
            "jsonrpc": "2.0",
            "id": session.next_request_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "coral",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });
        let response = session.post_json(&initialize, true).await?;
        session.remember_session_id(&response);
        let value = response.value.ok_or_else(|| {
            UpstreamError::InvalidResponse("MCP initialize response was empty".to_string())
        })?;
        json_rpc_result("initialize", value)?;
        session
            .post_notification("notifications/initialized")
            .await?;
        Ok(session)
    }

    async fn post_request(&mut self, method: &str, params: &Value) -> Result<Value> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.next_request_id(),
            "method": method,
            "params": params,
        });
        let response = self.post_json(&request, true).await?;
        self.remember_session_id(&response);
        response.value.ok_or_else(|| {
            UpstreamError::InvalidResponse(format!("MCP {method} response was empty"))
        })
    }

    async fn post_notification(&mut self, method: &str) -> Result<()> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        let response = self.post_json(&request, false).await?;
        self.remember_session_id(&response);
        if let Some(value) = response.value
            && let Some(error) = value.get("error")
        {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::ProtocolError,
                detail: format!("MCP {method} returned JSON-RPC error: {error}"),
            });
        }
        Ok(())
    }

    pub(crate) async fn post_json(
        &self,
        body: &Value,
        require_body: bool,
    ) -> Result<McpJsonResponse> {
        let mut request = self
            .client
            .post(self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, MCP_ACCEPT)
            .header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION)
            .json(body);
        for (name, value) in self.headers {
            request = request.header(name, value.expose_secret());
        }
        if let Some(session_id) = &self.session_id {
            request = request.header(MCP_SESSION_ID_HEADER, session_id);
        }
        if let Some(timeout) = self.timeout {
            request = request.timeout(timeout);
        }
        let response = request
            .send()
            .await
            .map_err(|error| UpstreamError::Transport(error.to_string()))?;
        let status = response.status().as_u16();
        let headers = response_headers(response.headers());
        let media_type = response_media_type(&headers);
        if !(200..300).contains(&status) {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::HttpError,
                detail: format!("MCP endpoint returned HTTP {status}"),
            });
        }
        let value = if media_type
            .as_deref()
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            if require_body {
                read_mcp_sse_json_response(response, body.get("id")).await?
            } else {
                None
            }
        } else {
            let bytes = limited_response_bytes(response, "MCP provider").await?;
            if bytes.is_empty() {
                None
            } else {
                Some(decode_mcp_json_response(&bytes)?)
            }
        };
        if require_body && value.is_none() {
            return Err(UpstreamError::InvalidResponse(
                "MCP response was empty".to_string(),
            ));
        }
        Ok(McpJsonResponse { value, headers })
    }

    fn remember_session_id(&mut self, response: &McpJsonResponse) {
        if let Some(session_id) = response
            .headers
            .get(&MCP_SESSION_ID_HEADER.to_ascii_lowercase())
        {
            self.session_id = Some(session_id.clone());
        }
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_id;
        self.next_id += 1;
        request_id
    }
}

pub(crate) fn json_rpc_result(method: &str, response: Value) -> Result<Value> {
    let Value::Object(mut object) = response else {
        return Err(UpstreamError::InvalidResponse(format!(
            "MCP {method} response must be a JSON-RPC object"
        )));
    };
    if let Some(error) = object.remove("error") {
        return Err(UpstreamError::Provider {
            kind: ProviderErrorKind::ProtocolError,
            detail: format!("MCP {method} returned JSON-RPC error: {error}"),
        });
    }
    object.remove("result").ok_or_else(|| {
        UpstreamError::InvalidResponse(format!("MCP {method} response did not include result"))
    })
}

fn decode_mcp_json_response(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(|error| UpstreamError::InvalidResponse(error.to_string()))
}

async fn read_mcp_sse_json_response(
    mut response: reqwest::Response,
    request_id: Option<&Value>,
) -> Result<Option<Value>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROVIDER_RESPONSE_BYTES as u64)
    {
        return Err(UpstreamError::Provider {
            kind: ProviderErrorKind::InvalidResponse,
            detail: format!("MCP provider response exceeds {MAX_PROVIDER_RESPONSE_BYTES} bytes"),
        });
    }
    let mut bytes_read = 0usize;
    let mut buffer = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| UpstreamError::Transport(error.to_string()))?
    {
        bytes_read = bytes_read.saturating_add(chunk.len());
        if bytes_read > MAX_PROVIDER_RESPONSE_BYTES {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::InvalidResponse,
                detail: format!(
                    "MCP provider response exceeds {MAX_PROVIDER_RESPONSE_BYTES} bytes"
                ),
            });
        }
        buffer.extend_from_slice(&chunk);
        while let Some((boundary, separator_len)) = find_sse_event_boundary(&buffer) {
            let event = buffer
                .get(..boundary)
                .expect("SSE boundary must be within the buffered response")
                .to_vec();
            buffer.drain(..boundary + separator_len);
            if let Some(value) = decode_mcp_sse_event(&event, request_id)? {
                return Ok(Some(value));
            }
        }
    }
    if !buffer.is_empty()
        && let Some(value) = decode_mcp_sse_event(&buffer, request_id)?
    {
        return Ok(Some(value));
    }
    Err(UpstreamError::InvalidResponse(
        "MCP SSE response did not include a matching JSON data event".to_string(),
    ))
}

fn find_sse_event_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if crlf < lf => Some((crlf, 4)),
        (Some(lf), _) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

fn decode_mcp_sse_event(event: &[u8], request_id: Option<&Value>) -> Result<Option<Value>> {
    let text = std::str::from_utf8(event)
        .map_err(|error| UpstreamError::InvalidResponse(error.to_string()))?;
    let mut data = String::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim_start());
            data.push('\n');
        }
    }
    let data = data.trim();
    if data.is_empty() {
        return Ok(None);
    }
    let value = serde_json::from_str::<Value>(data)
        .map_err(|error| UpstreamError::InvalidResponse(error.to_string()))?;
    if request_id_matches(&value, request_id) {
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn request_id_matches(value: &Value, expected: Option<&Value>) -> bool {
    match expected {
        Some(expected) => value.get("id") == Some(expected),
        None => true,
    }
}

fn mcp_tool_response_from_json_rpc(value: Value) -> Result<McpUpstreamResponse> {
    let Value::Object(mut object) = value else {
        return Err(UpstreamError::InvalidResponse(
            "MCP tools/call response must be a JSON-RPC object".to_string(),
        ));
    };
    if let Some(error) = object.remove("error") {
        return Err(UpstreamError::Provider {
            kind: ProviderErrorKind::ToolError,
            detail: error.to_string(),
        });
    }
    let Some(Value::Object(mut result)) = object.remove("result") else {
        return Err(UpstreamError::InvalidResponse(
            "MCP tools/call response did not include result object".to_string(),
        ));
    };
    let structured_content = result.remove("structuredContent");
    let content = result
        .remove("content")
        .and_then(|value| match value {
            Value::Array(values) => Some(values),
            _ => None,
        })
        .unwrap_or_default()
        .into_iter()
        .filter_map(mcp_content_block_from_value)
        .collect();
    let is_error = result
        .remove("isError")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let meta = result.remove("_meta");
    Ok(McpUpstreamResponse {
        structured_content,
        content,
        is_error,
        meta,
        response_trust: ResponseTrust::UntrustedProviderData,
    })
}

fn mcp_tool_response_from_result_value(result: &Value) -> Result<McpUpstreamResponse> {
    mcp_tool_response_from_json_rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 0,
        "result": result,
    }))
}

fn mcp_content_block_from_value(value: Value) -> Option<McpContentBlock> {
    let Value::Object(object) = value else {
        return None;
    };
    match object.get("type").and_then(Value::as_str)? {
        "text" => object
            .get("text")
            .and_then(Value::as_str)
            .map(|text| McpContentBlock::Text {
                text: text.to_string(),
            }),
        "image" => {
            let data = object.get("data").and_then(Value::as_str)?;
            let mime_type = object
                .get("mimeType")
                .or_else(|| object.get("mime_type"))
                .and_then(Value::as_str)?;
            Some(McpContentBlock::Image {
                data: data.to_string(),
                mime_type: mime_type.to_string(),
            })
        }
        "resource" => object
            .get("resource")
            .cloned()
            .map(|resource| McpContentBlock::Resource { resource }),
        _ => None,
    }
}
