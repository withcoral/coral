//! Per-instance MCP transport implementations.
//!
//! Both stdio (`StdioMcpToolCaller`) and Streamable HTTP
//! (`StreamableHttpMcpToolCaller`) are supported. Each implementation
//! creates a fresh MCP client session for every tool call; pooling is a
//! future optimization.
//!
//! Each `call_tool` is wrapped in an `mcp.tool.call` parent span and
//! optionally emits child preview spans for the JSON arguments and the
//! normalized response payload (see `McpBodyCapture`). For the Streamable
//! HTTP transport, the parent span's W3C trace context is injected as
//! custom HTTP headers so an instrumented MCP server can continue the
//! trace.

use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::McpServerSpec;
use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rmcp::model::{CallToolRequestParams, ClientInfo, Implementation, JsonObject};
use rmcp::transport::ConfigureCommandExt;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientHandler, ServiceExt};
use serde_json::Value;
use tokio::process::Command;
use tracing::Instrument as _;
use tracing::field;

use super::client::McpToolCaller;
use super::error::McpProviderQueryError;
use super::response::normalize_tool_result;
use super::trace::{McpBodyCapture, mcp_error_type, next_mcp_request_id};
use crate::backends::shared::template::{RenderContext, resolve_value_source};
use crate::backends::shared::trace::{
    inject_trace_context, record_processing_error, record_trace_http_endpoint, sanitize_trace_url,
    trace_http_endpoint,
};

#[derive(Debug)]
pub(super) struct StdioMcpToolCaller {
    pub(super) source_name: String,
    pub(super) server: McpServerSpec,
    pub(super) resolved_inputs: Arc<BTreeMap<String, String>>,
    pub(super) body_capture: McpBodyCapture,
}

#[async_trait]
impl McpToolCaller for StdioMcpToolCaller {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        let McpServerSpec::Stdio {
            command: program,
            args,
            env,
        } = &self.server
        else {
            unreachable!("StdioMcpToolCaller requires a stdio MCP server spec");
        };

        let request_id = next_mcp_request_id();
        let request_span = tracing::info_span!(
            target: "coral_engine::mcp",
            "mcp.tool.call",
            coral.mcp.command = program.as_str(),
            coral.mcp.args.count = i64::try_from(args.len()).unwrap_or(i64::MAX),
            coral.mcp.relation = relation,
            coral.mcp.request_id = request_id,
            coral.mcp.tool = tool_name,
            coral.mcp.transport = "stdio",
            coral.source = self.source_name.as_str(),
            error = field::Empty,
            error.type = field::Empty,
            exception.message = field::Empty,
            otel.kind = "client",
            otel.name = tool_name,
            otel.status_code = field::Empty,
            otel.status_description = field::Empty,
        );

        let result = self
            .call_tool_inner(
                program, args, env, relation, tool_name, arguments, request_id,
            )
            .instrument(request_span.clone())
            .await;
        if let Err(error) = &result {
            record_mcp_error(&request_span, error);
        }
        result
    }
}

impl StdioMcpToolCaller {
    #[expect(
        clippy::too_many_arguments,
        reason = "Internal helper threading stdio spec fields + per-call IDs from call_tool"
    )]
    async fn call_tool_inner(
        &self,
        program: &str,
        args: &[String],
        env: &[coral_spec::backends::mcp::McpEnvSpec],
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
        request_id: u64,
    ) -> Result<Value> {
        let mut command = Command::new(program);
        command.args(args);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let render_context = RenderContext::source_scoped(&self.resolved_inputs);
        for env in env {
            let Some(value) = resolve_value_source(&env.value, &render_context)? else {
                continue;
            };
            command.env(&env.name, value_to_env_string(value));
        }

        let span = tracing::Span::current();
        self.body_capture
            .record_request(&span, request_id, &arguments);

        let transport = rmcp::transport::TokioChildProcess::new(command.configure(|cmd| {
            cmd.kill_on_drop(true);
        }))
        .map_err(|error| {
            DataFusionError::External(Box::new(McpProviderQueryError::ServerStart {
                source_schema: self.source_name.clone(),
                detail: error.to_string(),
            }))
        })?;
        let client = McpClientHandler::new(&self.source_name)
            .serve(transport)
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::Initialize {
                    source_schema: self.source_name.clone(),
                    detail: error.to_string(),
                }))
            })?;
        let raw = client
            .call_tool(CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments))
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::ToolCall {
                    source_schema: self.source_name.clone(),
                    relation: relation.to_string(),
                    tool: tool_name.to_string(),
                    detail: error.to_string(),
                }))
            })?;
        let payload = normalize_tool_result(&self.source_name, relation, tool_name, raw)?;
        self.body_capture
            .record_response(&span, request_id, &payload);
        Ok(payload)
    }
}

#[derive(Debug)]
pub(super) struct StreamableHttpMcpToolCaller {
    pub(super) source_name: String,
    pub(super) server: McpServerSpec,
    pub(super) resolved_inputs: Arc<BTreeMap<String, String>>,
    pub(super) body_capture: McpBodyCapture,
}

#[async_trait]
impl McpToolCaller for StreamableHttpMcpToolCaller {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        let McpServerSpec::StreamableHttp { url, auth } = &self.server else {
            unreachable!("StreamableHttpMcpToolCaller requires a Streamable HTTP MCP server spec");
        };

        let request_id = next_mcp_request_id();
        let traced_url = sanitize_trace_url(url);
        let endpoint = trace_http_endpoint(&traced_url);
        let request_span = tracing::info_span!(
            target: "coral_engine::mcp",
            "mcp.tool.call",
            coral.mcp.relation = relation,
            coral.mcp.request_id = request_id,
            coral.mcp.tool = tool_name,
            coral.mcp.transport = "streamable_http",
            coral.source = self.source_name.as_str(),
            error = field::Empty,
            error.type = field::Empty,
            exception.message = field::Empty,
            http.host = field::Empty,
            net.peer.name = field::Empty,
            otel.kind = "client",
            otel.name = tool_name,
            otel.status_code = field::Empty,
            otel.status_description = field::Empty,
            peer.service = field::Empty,
            server.address = field::Empty,
            server.port = field::Empty,
            url.full = %traced_url,
        );
        record_trace_http_endpoint(&request_span, &endpoint);

        let result = self
            .call_tool_inner(
                url,
                auth.as_ref(),
                relation,
                tool_name,
                arguments,
                request_id,
            )
            .instrument(request_span.clone())
            .await;
        if let Err(error) = &result {
            record_mcp_error(&request_span, error);
        }
        result
    }
}

/// Install the `ring` `rustls::CryptoProvider` as the process-wide
/// default for rustls 0.23. rmcp's reqwest 0.13 is configured with the
/// `rustls-no-provider` feature so it does not pull `aws-lc-rs` (whose
/// `aws-lc-sys` carries an `OpenSSL` license clause our workspace
/// rejects). reqwest 0.12 in the same workspace links `ring` already;
/// this just registers it as the rustls default. Idempotent — a second
/// call is a no-op and any error from a competing initializer is
/// ignored.
fn ensure_default_crypto_provider() {
    static PROVIDER_INIT: std::sync::Once = std::sync::Once::new();
    PROVIDER_INIT.call_once(|| {
        // `install_default` returns `Err` if a provider is already
        // installed by another part of the workspace (for example,
        // reqwest 0.12 elsewhere in the process). Either way the rustls
        // 0.23 default is now set; ignore the Result deliberately rather
        // than panic.
        match rustls::crypto::ring::default_provider().install_default() {
            Ok(()) | Err(_) => {}
        }
    });
}

impl StreamableHttpMcpToolCaller {
    async fn call_tool_inner(
        &self,
        url: &str,
        auth: Option<&coral_spec::backends::mcp::McpHttpAuthSpec>,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
        request_id: u64,
    ) -> Result<Value> {
        ensure_default_crypto_provider();
        let span = tracing::Span::current();

        let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_string())
            .reinit_on_expired_session(true);
        if let Some(auth) = auth
            && let Some(token) = resolve_value_source(
                auth.bearer_token(),
                &RenderContext::source_scoped(&self.resolved_inputs),
            )?
        {
            config = config.auth_header(value_to_env_string(token));
        }

        // Propagate the current span's W3C trace context to the MCP server
        // via the transport's custom header hook so an instrumented server
        // can continue the trace.
        let mut header_map = HeaderMap::new();
        inject_trace_context(&span, &mut header_map);
        if !header_map.is_empty() {
            let custom_headers: HashMap<HeaderName, HeaderValue> = header_map
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            config = config.custom_headers(custom_headers);
        }

        self.body_capture
            .record_request(&span, request_id, &arguments);

        let transport = StreamableHttpClientTransport::from_config(config);
        let client = McpClientHandler::new(&self.source_name)
            .serve(transport)
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(mcp_http_initialize_error(
                    &self.source_name,
                    &error,
                )))
            })?;
        let raw = client
            .call_tool(CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments))
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(mcp_http_tool_call_error(
                    &self.source_name,
                    relation,
                    tool_name,
                    &error,
                )))
            })?;
        let payload = normalize_tool_result(&self.source_name, relation, tool_name, raw)?;
        self.body_capture
            .record_response(&span, request_id, &payload);
        Ok(payload)
    }
}

/// Annotate the parent `mcp.tool.call` span with the structured fields of
/// an [`McpProviderQueryError`] so the surfaced error and the span agree
/// on category, message, and `OTel` status.
fn record_mcp_error(span: &tracing::Span, error: &DataFusionError) {
    if let DataFusionError::External(boxed) = error
        && let Some(mcp_error) = boxed.downcast_ref::<McpProviderQueryError>()
    {
        record_processing_error(span, mcp_error_type(mcp_error), mcp_error);
    }
}

#[derive(Debug, Clone)]
struct McpClientHandler {
    client_info: ClientInfo,
}

impl McpClientHandler {
    fn new(source_name: &str) -> Self {
        let mut client_info = ClientInfo::default();
        client_info.client_info = Implementation::new(
            format!("coral-engine/{source_name}"),
            env!("CARGO_PKG_VERSION"),
        );
        Self { client_info }
    }
}

impl ClientHandler for McpClientHandler {
    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }
}

fn value_to_env_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}

fn mcp_http_initialize_error(
    source_schema: &str,
    error: &(dyn std::error::Error + 'static),
) -> McpProviderQueryError {
    classify_streamable_http_error(source_schema, None, error)
}

fn mcp_http_tool_call_error(
    source_schema: &str,
    relation: &str,
    tool: &str,
    error: &(dyn std::error::Error + 'static),
) -> McpProviderQueryError {
    classify_streamable_http_error(source_schema, Some((relation, tool)), error)
}

/// Walk the rmcp error chain looking for a `StreamableHttpError` and map
/// its variants to the matching `McpProviderQueryError`. Falls back to the
/// generic Initialize/ToolCall variants when the chain doesn't expose a
/// typed transport error we recognize.
/// Classify an rmcp `ClientInitializeError` / `ServiceError` raised by the
/// Streamable HTTP transport into a structured `McpProviderQueryError`.
///
/// rmcp wraps the underlying `StreamableHttpError` in a `DynamicTransportError`
/// whose inner `Box<dyn Error>` we cannot typed-downcast: rmcp parameterizes
/// the box with its own `reqwest::Error` (rmcp 1.6 pulls reqwest 0.13) while
/// coral-engine uses reqwest 0.12, so the `StreamableHttpError<reqwest::Error>`
/// `TypeId`s do not match across the crate boundary. Instead we match on the
/// inner error's stable `#[error("...")]` display prefix from
/// `rmcp::transport::streamable_http_client::StreamableHttpError`.
fn classify_streamable_http_error(
    source_schema: &str,
    relation_and_tool: Option<(&str, &str)>,
    error: &(dyn std::error::Error + 'static),
) -> McpProviderQueryError {
    let inner_detail = match error.downcast_ref::<rmcp::service::ClientInitializeError>() {
        Some(rmcp::service::ClientInitializeError::TransportError { error: dyn_err, .. }) => {
            Some(dyn_err.error.to_string())
        }
        _ => None,
    }
    .or_else(
        || match error.downcast_ref::<rmcp::service::ServiceError>() {
            Some(rmcp::service::ServiceError::TransportSend(dyn_err)) => {
                Some(dyn_err.error.to_string())
            }
            _ => None,
        },
    );

    let full_detail = error.to_string();

    if let Some(inner) = inner_detail.as_deref()
        && let Some(variant) = classify_streamable_http_inner(inner)
    {
        return match variant {
            StreamableVariant::AuthRequired => McpProviderQueryError::AuthRequired {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            StreamableVariant::AuthFailed => McpProviderQueryError::AuthFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            StreamableVariant::SessionExpired => McpProviderQueryError::SessionExpired {
                source_schema: source_schema.to_string(),
            },
            StreamableVariant::HttpStatusFailed => McpProviderQueryError::HttpStatusFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            StreamableVariant::HttpSseDecodeFailed => McpProviderQueryError::HttpSseDecodeFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            StreamableVariant::HttpRequestFailed => McpProviderQueryError::HttpRequestFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
        };
    }

    match relation_and_tool {
        Some((relation, tool)) => McpProviderQueryError::ToolCall {
            source_schema: source_schema.to_string(),
            relation: relation.to_string(),
            tool: tool.to_string(),
            detail: full_detail,
        },
        None => McpProviderQueryError::Initialize {
            source_schema: source_schema.to_string(),
            detail: full_detail,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamableVariant {
    AuthRequired,
    AuthFailed,
    SessionExpired,
    HttpStatusFailed,
    HttpSseDecodeFailed,
    HttpRequestFailed,
}

/// Match the `Display` output of an `rmcp::transport::streamable_http_client::
/// StreamableHttpError` to one of our structured variants. Prefixes come from
/// the `#[error("...")]` thiserror derives on each enum variant and are stable
/// per rmcp 1.6.
fn classify_streamable_http_inner(detail: &str) -> Option<StreamableVariant> {
    if detail.starts_with("Auth required") {
        Some(StreamableVariant::AuthRequired)
    } else if detail.starts_with("Insufficient scope") {
        Some(StreamableVariant::AuthFailed)
    } else if detail.starts_with("Session expired") {
        Some(StreamableVariant::SessionExpired)
    } else if detail.starts_with("unexpected server response") {
        Some(StreamableVariant::HttpStatusFailed)
    } else if detail.starts_with("SSE error")
        || detail.starts_with("Unexpected content type")
        || detail.starts_with("Deserialize error")
        || detail.starts_with("Server does not support SSE")
    {
        Some(StreamableVariant::HttpSseDecodeFailed)
    } else if detail.starts_with("Client error") || detail.starts_with("Io error") {
        Some(StreamableVariant::HttpRequestFailed)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use opentelemetry::Value as OtelValue;
    use opentelemetry::trace::{SpanKind, Status, TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use rmcp::model::JsonObject;
    use serde_json::{Value, json};
    use tracing::subscriber::DefaultGuard;

    use super::{StreamableVariant, classify_streamable_http_inner};

    #[test]
    fn classify_streamable_http_inner_matches_known_prefixes() {
        // Pin the prefix→variant map against the rmcp 1.6 `#[error("...")]`
        // strings. If rmcp bumps and renames a variant's display message,
        // this test fails — we'd update both this table and
        // `classify_streamable_http_inner` together.
        let cases = &[
            ("Auth required", Some(StreamableVariant::AuthRequired)),
            ("Insufficient scope", Some(StreamableVariant::AuthFailed)),
            (
                "Session expired (HTTP 404)",
                Some(StreamableVariant::SessionExpired),
            ),
            (
                "unexpected server response: HTTP 502",
                Some(StreamableVariant::HttpStatusFailed),
            ),
            (
                "Unexpected content type: \"text/plain\"",
                Some(StreamableVariant::HttpSseDecodeFailed),
            ),
            (
                "SSE error: invalid frame",
                Some(StreamableVariant::HttpSseDecodeFailed),
            ),
            (
                "Deserialize error: expected value",
                Some(StreamableVariant::HttpSseDecodeFailed),
            ),
            (
                "Server does not support SSE",
                Some(StreamableVariant::HttpSseDecodeFailed),
            ),
            (
                "Client error: connection refused",
                Some(StreamableVariant::HttpRequestFailed),
            ),
            (
                "Io error: broken pipe",
                Some(StreamableVariant::HttpRequestFailed),
            ),
            ("Transport channel closed", None),
            ("Missing session id in HTTP response", None),
        ];
        for (detail, expected) in cases {
            assert_eq!(
                classify_streamable_http_inner(detail),
                *expected,
                "unexpected classification for `{detail}`"
            );
        }
    }
    use tracing_subscriber::layer::SubscriberExt;
    use wiremock::matchers::{body_partial_json, header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    struct TraceCapture {
        memory: InMemorySpanExporter,
        provider: SdkTracerProvider,
        _guard: DefaultGuard,
    }

    impl TraceCapture {
        fn install() -> Self {
            let memory = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(memory.clone())
                .build();
            let tracer = provider.tracer("mcp-telemetry-test");
            let layer = tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_target(true)
                .with_level(true);
            let subscriber = tracing_subscriber::Registry::default().with(layer);
            let guard = tracing::subscriber::set_default(subscriber);
            Self {
                memory,
                provider,
                _guard: guard,
            }
        }

        fn finished_spans(&self) -> Vec<SpanData> {
            self.provider.force_flush().expect("flush spans");
            self.memory.get_finished_spans().expect("finished spans")
        }
    }

    fn span_attr_string(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                OtelValue::String(value) => Some(value.to_string()),
                OtelValue::I64(value) => Some(value.to_string()),
                _ => None,
            })
    }

    fn span_attr_bool(span: &SpanData, key: &str) -> Option<bool> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                OtelValue::Bool(value) => Some(*value),
                _ => None,
            })
    }

    fn streamable_http_manifest(url: &str) -> coral_spec::McpSourceManifest {
        let manifest = coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "remote_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "inputs": {
                "MCP_ACCESS_TOKEN": { "kind": "secret" }
            },
            "server": {
                "transport": "streamable_http",
                "url": url,
                "auth": {
                    "type": "bearer",
                    "from": "input",
                    "key": "MCP_ACCESS_TOKEN"
                }
            },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "columns": [{ "name": "title", "type": "Utf8" }]
            }]
        }))
        .expect("manifest should parse");
        manifest.as_mcp().expect("expected mcp manifest").clone()
    }

    fn initialize_response() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .append_header("Content-Type", "application/json")
            .set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 0,
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "serverInfo": {
                        "name": "fixture",
                        "version": "0.1.0"
                    }
                }
            }))
    }

    #[tokio::test]
    async fn streamable_http_caller_sends_bearer_token_and_decodes_tool_result() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response())
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/initialized" }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(header("authorization", "Bearer secret-token"))
            .and(body_partial_json(json!({
                "method": "tools/call",
                "params": {
                    "name": "list_issues",
                    "arguments": { "state": "open" }
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "application/json")
                    .set_body_json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "structuredContent": {
                                "issues": [{ "title": "Bug A" }]
                            }
                        }
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let manifest = streamable_http_manifest(&server.uri());
        let mut secrets = BTreeMap::new();
        secrets.insert("MCP_ACCESS_TOKEN".to_string(), "secret-token".to_string());
        let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
            &manifest.declared_inputs,
            &secrets,
            &BTreeMap::new(),
        ));
        let caller = StreamableHttpMcpToolCaller {
            source_name: manifest.common.name,
            server: manifest.server,
            resolved_inputs,
            body_capture: McpBodyCapture::default(),
        };
        let mut arguments = JsonObject::new();
        arguments.insert("state".to_string(), Value::String("open".to_string()));

        let payload = caller
            .call_tool("issues", "list_issues", arguments)
            .await
            .expect("tool call should succeed");

        let title = payload
            .get("issues")
            .and_then(Value::as_array)
            .and_then(|issues| issues.first())
            .and_then(|issue| issue.get("title"))
            .and_then(Value::as_str);
        assert_eq!(title, Some("Bug A"));
    }

    /// Helper: wire up a wiremock server that successfully serves
    /// initialize → notifications/initialized → tools/call with the
    /// supplied tool-call body.
    async fn mock_success_server(tool_response: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response())
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/initialized" }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "application/json")
                    .set_body_json(tool_response),
            )
            .mount(&server)
            .await;
        server
    }

    fn make_caller(
        manifest: coral_spec::McpSourceManifest,
        body_capture: McpBodyCapture,
    ) -> StreamableHttpMcpToolCaller {
        let mut secrets = BTreeMap::new();
        secrets.insert("MCP_ACCESS_TOKEN".to_string(), "token".to_string());
        let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
            &manifest.declared_inputs,
            &secrets,
            &BTreeMap::new(),
        ));
        StreamableHttpMcpToolCaller {
            source_name: manifest.common.name,
            server: manifest.server,
            resolved_inputs,
            body_capture,
        }
    }

    /// Find the `mcp.tool.call` span by `coral.source` attribute rather
    /// than name — `otel.name = tool_name` overrides the tracing-level
    /// span name in `OTel` exports (matches the HTTP backend's convention
    /// of `otel.name = method_label`).
    fn find_call_span<'a>(spans: &'a [SpanData], source: &str) -> Option<&'a SpanData> {
        spans
            .iter()
            .find(|span| span_attr_string(span, "coral.source").as_deref() == Some(source))
    }

    #[tokio::test]
    async fn streamable_http_caller_emits_parent_span_with_otel_attributes() {
        let capture = TraceCapture::install();
        let server = mock_success_server(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "structuredContent": { "issues": [] } }
        }))
        .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        caller
            .call_tool("issues", "list_issues", JsonObject::new())
            .await
            .expect("tool call should succeed");
        drop(caller);
        drop(server);
        // Yield + sleep so rmcp's background worker drops its cloned span
        // and the parent span's `on_close` fires before we read.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let spans = capture.finished_spans();
        let parent = find_call_span(&spans, "remote_mcp").expect("parent mcp.tool.call span");

        // `otel.name = tool_name` overrides the tracing-level span name in
        // exports, matching the HTTP backend's convention.
        assert_eq!(parent.name.as_ref(), "list_issues");
        assert_eq!(parent.span_kind, SpanKind::Client);
        assert_eq!(
            span_attr_string(parent, "coral.source").as_deref(),
            Some("remote_mcp")
        );
        assert_eq!(
            span_attr_string(parent, "coral.mcp.tool").as_deref(),
            Some("list_issues")
        );
        assert_eq!(
            span_attr_string(parent, "coral.mcp.relation").as_deref(),
            Some("issues")
        );
        assert_eq!(
            span_attr_string(parent, "coral.mcp.transport").as_deref(),
            Some("streamable_http")
        );
        assert!(
            span_attr_string(parent, "url.full")
                .as_deref()
                .is_some_and(|url| url.starts_with("http://")),
            "url.full should be recorded"
        );
        assert!(
            span_attr_string(parent, "server.address")
                .as_deref()
                .is_some_and(|address| !address.is_empty()),
            "server.address should be recorded"
        );
        // Success path should not annotate error fields.
        assert_eq!(span_attr_bool(parent, "error"), None);
        assert_eq!(span_attr_string(parent, "error.type"), None);
        assert!(matches!(parent.status, Status::Unset | Status::Ok));

        capture.provider.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn streamable_http_caller_emits_body_capture_child_spans_when_enabled() {
        let capture = TraceCapture::install();
        let server = mock_success_server(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "structuredContent": { "issues": [{ "title": "x" }] } }
        }))
        .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::new(Some(1024)));
        let mut arguments = JsonObject::new();
        arguments.insert("state".to_string(), Value::String("open".to_string()));

        caller
            .call_tool("issues", "list_issues", arguments)
            .await
            .expect("tool call should succeed");

        let spans = capture.finished_spans();
        let request_body = spans
            .iter()
            .find(|span| span.name == "coral.mcp.request.body")
            .expect("request body child span");
        assert_eq!(
            span_attr_string(request_body, "coral.mcp.body.direction").as_deref(),
            Some("request")
        );
        assert!(
            span_attr_string(request_body, "coral.mcp.request.body")
                .as_deref()
                .is_some_and(|body| body.contains("\"state\":\"open\"")),
            "request body preview should include the argument JSON"
        );

        let response_body = spans
            .iter()
            .find(|span| span.name == "coral.mcp.response.body")
            .expect("response body child span");
        assert!(
            span_attr_string(response_body, "coral.mcp.response.body")
                .as_deref()
                .is_some_and(|body| body.contains("\"title\":\"x\"")),
            "response body preview should include the normalized payload"
        );

        capture.provider.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn streamable_http_caller_records_auth_required_on_initialize_401() {
        let capture = TraceCapture::install();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).append_header("WWW-Authenticate", "Bearer"))
            .mount(&server)
            .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new())
            .await
            .expect_err("auth required should surface as error");
        let message = error.to_string();
        assert!(
            message.contains("Auth required") || message.contains("authorization"),
            "expected auth-required error message, got: {message}"
        );
        drop(caller);
        drop(server);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let spans = capture.finished_spans();
        let parent = find_call_span(&spans, "remote_mcp").expect("parent span");

        assert_eq!(span_attr_bool(parent, "error"), Some(true));
        assert_eq!(
            span_attr_string(parent, "error.type").as_deref(),
            Some("MCP_AUTH_REQUIRED")
        );
        assert!(
            matches!(parent.status, Status::Error { .. }),
            "expected Status::Error, got {:?}",
            parent.status
        );
        assert!(
            span_attr_string(parent, "exception.message")
                .as_deref()
                .is_some_and(|message| !message.is_empty()),
            "exception.message should carry the underlying error"
        );

        capture.provider.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn streamable_http_caller_classifies_non_auth_5xx_as_http_status_failed() {
        let capture = TraceCapture::install();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(502).set_body_string("bad gateway"))
            .mount(&server)
            .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new())
            .await
            .expect_err("5xx should surface as error");
        assert!(
            error.to_string().contains("unexpected status")
                || error.to_string().contains("unexpected server response"),
            "expected http-status-failed error message, got: {error}"
        );
        drop(caller);
        drop(server);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let spans = capture.finished_spans();
        let parent = find_call_span(&spans, "remote_mcp").expect("parent span");
        assert_eq!(
            span_attr_string(parent, "error.type").as_deref(),
            Some("MCP_HTTP_STATUS_FAILED")
        );
        capture.provider.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn streamable_http_caller_classifies_unexpected_content_type_as_sse_decode_failed() {
        let capture = TraceCapture::install();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/plain")
                    .set_body_string("hello"),
            )
            .mount(&server)
            .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new())
            .await
            .expect_err("unexpected content type should surface as error");
        assert!(
            error.to_string().contains("undecodable SSE stream")
                || error.to_string().contains("Unexpected content type"),
            "expected sse-decode-failed error message, got: {error}"
        );
        drop(caller);
        drop(server);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let spans = capture.finished_spans();
        let parent = find_call_span(&spans, "remote_mcp").expect("parent span");
        assert_eq!(
            span_attr_string(parent, "error.type").as_deref(),
            Some("MCP_HTTP_SSE_DECODE_FAILED")
        );
        capture.provider.shutdown().expect("shutdown");
    }
}
