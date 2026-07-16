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

use std::collections::{BTreeSet, HashMap};
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::McpServerSpec;
use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rmcp::model::{
    CallToolRequestParams, CallToolResult, CancelledNotificationParam, ClientInfo, ClientRequest,
    Implementation, JsonObject, PaginatedRequestParams, Request, ServerResult, Tool,
};
use rmcp::transport::ConfigureCommandExt;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::common::client_side_sse::NeverRetry;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientHandler, RoleClient, ServiceExt};
use serde_json::Value;
use tokio::process::Command;
use tracing::Instrument as _;
use tracing::field;

use super::McpSourceInputs;
use super::client::McpToolCaller;
use super::controlled_http::{CoralStreamableHttpClient, HttpFailureSignal, HttpOperationTracker};
use super::controlled_stdio::{ChildTransportTracker, CoralChildTransport};
use super::error::McpProviderQueryError;
use super::response::normalize_tool_result;
use super::trace::{McpBodyCapture, mcp_error_type, next_mcp_request_id};
use crate::backends::shared::template::{RenderContext, resolve_value_source};
use crate::backends::shared::trace::{
    inject_trace_context, record_processing_error, record_trace_http_endpoint, sanitize_trace_url,
    trace_http_endpoint,
};
use crate::{QueryExecutionControls, QueryExecutionFailureKind, QueryRetryPolicy};

const MAX_MCP_DISCOVERY_PAGES: usize = 100;

#[derive(Debug)]
pub(super) struct StdioMcpToolCaller {
    pub(super) source_name: String,
    pub(super) server: McpServerSpec,
    pub(super) source_inputs: Arc<McpSourceInputs>,
    pub(super) body_capture: McpBodyCapture,
}

impl StdioMcpToolCaller {
    pub(super) async fn resolved_server_env(&self) -> Result<Vec<(String, String)>> {
        let server_env = match &self.server {
            McpServerSpec::Stdio { env, .. } => env,
            McpServerSpec::StreamableHttp { .. } => {
                return Ok(Vec::new());
            }
        };
        if server_env.is_empty() {
            return Ok(Vec::new());
        }
        let resolved_inputs = self.source_inputs.resolve_for_request().await?;
        let render_context = RenderContext::source_scoped(&resolved_inputs);
        let mut env = Vec::with_capacity(server_env.len());
        for spec in server_env {
            let Some(value) = resolve_value_source(&spec.value, &render_context)? else {
                continue;
            };
            env.push((spec.name.clone(), value_to_env_string(value)));
        }
        Ok(env)
    }

    pub(super) async fn list_tools(&self) -> Result<Vec<Tool>> {
        let McpServerSpec::Stdio {
            command: program,
            args,
            ..
        } = &self.server
        else {
            unreachable!("StdioMcpToolCaller requires a stdio MCP server spec");
        };

        let mut command = Command::new(program);
        command.args(args);
        command.stdin(Stdio::piped()).stdout(Stdio::piped());

        for (name, value) in self.resolved_server_env().await? {
            command.env(name, value);
        }

        let transport = rmcp::transport::TokioChildProcess::new(command.configure(|cmd| {
            cmd.kill_on_drop(true);
        }))
        .map_err(|error| {
            DataFusionError::External(Box::new(McpProviderQueryError::ServerStart {
                source_schema: self.source_name.clone(),
                detail: error.to_string(),
            }))
        })?;
        let transport = CoralChildTransport::raw(transport);
        let client = McpClientHandler::new(&self.source_name)
            .serve(transport)
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::Initialize {
                    source_schema: self.source_name.clone(),
                    detail: error.to_string(),
                    failure_kind: QueryExecutionFailureKind::UpstreamUnavailable,
                }))
            })?;
        list_tools_bounded(client.peer(), &self.source_name, |source_name, error| {
            McpProviderQueryError::Initialize {
                source_schema: source_name.to_string(),
                detail: error.to_string(),
                failure_kind: QueryExecutionFailureKind::UpstreamUnavailable,
            }
        })
        .await
    }
}

#[async_trait]
impl McpToolCaller for StdioMcpToolCaller {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
        controls: &QueryExecutionControls,
    ) -> Result<Value> {
        let McpServerSpec::Stdio {
            command: program,
            args,
            ..
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

        let result = Box::pin(
            self.call_tool_inner(
                program, args, relation, tool_name, arguments, request_id, controls,
            )
            .instrument(request_span.clone()),
        )
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
        clippy::too_many_lines,
        reason = "The isolated stdio call keeps transport, trace identity, execution controls, and owned teardown explicit."
    )]
    async fn call_tool_inner(
        &self,
        program: &str,
        args: &[String],
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
        request_id: u64,
        controls: &QueryExecutionControls,
    ) -> Result<Value> {
        let mut command = Command::new(program);
        command.args(args);
        command.stdin(Stdio::piped()).stdout(Stdio::piped());

        let server_env = controls
            .run_until_stopped(self.resolved_server_env())
            .await
            .map_err(|kind| self.execution_stopped(relation, tool_name, kind))??;
        for (name, value) in server_env {
            command.env(name, value);
        }

        let span = tracing::Span::current();
        self.body_capture
            .record_request(&span, request_id, &arguments);

        let enforce_transport_stop = controls.requires_transport_enforcement();
        let child_tracker = ChildTransportTracker::default();
        let transport = if enforce_transport_stop {
            controls
                .check_active()
                .map_err(|kind| self.execution_stopped(relation, tool_name, kind))?;
            CoralChildTransport::spawn_controlled(command, child_tracker.clone(), controls.clone())
        } else {
            rmcp::transport::TokioChildProcess::new(command.configure(|cmd| {
                cmd.kill_on_drop(true);
            }))
            .map(CoralChildTransport::raw)
        }
        .map_err(|error| {
            if let Some(kind) = error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<QueryExecutionFailureKind>())
                .copied()
            {
                return self.execution_stopped(relation, tool_name, kind);
            }
            DataFusionError::External(Box::new(McpProviderQueryError::ServerStart {
                source_schema: self.source_name.clone(),
                detail: error.to_string(),
            }))
        })?;
        let mut initialization = Box::pin(
            controls.run_until_stopped(McpClientHandler::new(&self.source_name).serve(transport)),
        );
        let initialized = if enforce_transport_stop {
            tokio::select! {
                biased;
                failure_kind = child_tracker.wait_failure() => {
                    drop(initialization);
                    if let Err(reap_error) = child_tracker.wait_reaped().await {
                        tracing::debug!(%reap_error, "failed to reap MCP child after transport failure");
                    }
                    return Err(DataFusionError::External(Box::new(
                        McpProviderQueryError::Initialize {
                            source_schema: self.source_name.clone(),
                            detail: "controlled MCP child transport failed".to_string(),
                            failure_kind,
                        },
                    )));
                }
                initialized = &mut initialization => initialized,
            }
        } else {
            initialization.await
        };
        let mut client = match initialized {
            Err(kind) => {
                if enforce_transport_stop {
                    await_stopped_child(&child_tracker, controls, kind).await;
                }
                return Err(self.execution_stopped(relation, tool_name, kind));
            }
            Ok(Err(error)) => {
                let failure_kind = child_tracker
                    .failure_kind()
                    .unwrap_or_else(|| initialize_failure_kind(&error));
                if enforce_transport_stop && let Err(reap_error) = child_tracker.wait_reaped().await
                {
                    tracing::debug!(%reap_error, "failed to reap MCP child after initialization failure");
                }
                return Err(DataFusionError::External(Box::new(
                    McpProviderQueryError::Initialize {
                        source_schema: self.source_name.clone(),
                        detail: error.to_string(),
                        failure_kind,
                    },
                )));
            }
            Ok(Ok(client)) => client,
        };
        let raw = match call_tool_cancellable(
            client.peer(),
            CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments),
            controls,
            None,
            enforce_transport_stop.then_some(&child_tracker),
        )
        .await
        {
            Ok(raw) => raw,
            Err(ControlledMcpCallError::Stopped(kind)) => {
                if enforce_transport_stop {
                    close_stopped_stdio_service(&mut client, controls, kind, &child_tracker).await;
                } else {
                    close_stopped_service(&mut client).await;
                }
                return Err(self.execution_stopped(relation, tool_name, kind));
            }
            Err(ControlledMcpCallError::Service(error)) => {
                if enforce_transport_stop {
                    close_controlled_stdio_service(&mut client, &child_tracker).await;
                }
                let failure_kind = child_tracker
                    .failure_kind()
                    .unwrap_or_else(|| service_failure_kind(&error));
                return Err(DataFusionError::External(Box::new(
                    McpProviderQueryError::ToolCall {
                        source_schema: self.source_name.clone(),
                        relation: relation.to_string(),
                        tool: tool_name.to_string(),
                        detail: error.to_string(),
                        failure_kind,
                    },
                )));
            }
            Err(ControlledMcpCallError::StreamFailed(kind)) => {
                if enforce_transport_stop {
                    close_controlled_stdio_service(&mut client, &child_tracker).await;
                }
                return Err(DataFusionError::External(Box::new(
                    McpProviderQueryError::ToolCall {
                        source_schema: self.source_name.clone(),
                        relation: relation.to_string(),
                        tool: tool_name.to_string(),
                        detail: "controlled MCP response stream failed".to_string(),
                        failure_kind: kind,
                    },
                )));
            }
        };
        if enforce_transport_stop {
            close_controlled_stdio_service(&mut client, &child_tracker).await;
        }
        controls
            .check_active()
            .map_err(|kind| self.execution_stopped(relation, tool_name, kind))?;
        let payload = normalize_tool_result(&self.source_name, relation, tool_name, raw)?;
        controls
            .check_active()
            .map_err(|kind| self.execution_stopped(relation, tool_name, kind))?;
        self.body_capture
            .record_response(&span, request_id, &payload);
        Ok(payload)
    }

    fn execution_stopped(
        &self,
        relation: &str,
        tool: &str,
        kind: QueryExecutionFailureKind,
    ) -> DataFusionError {
        execution_stopped(&self.source_name, relation, tool, kind)
    }
}

#[derive(Debug)]
pub(super) struct StreamableHttpMcpToolCaller {
    pub(super) source_name: String,
    pub(super) server: McpServerSpec,
    pub(super) source_inputs: Arc<McpSourceInputs>,
    pub(super) body_capture: McpBodyCapture,
    http_clients: McpHttpClients,
}

impl StreamableHttpMcpToolCaller {
    pub(super) fn new(
        source_name: String,
        server: McpServerSpec,
        source_inputs: Arc<McpSourceInputs>,
        body_capture: McpBodyCapture,
    ) -> Self {
        Self {
            source_name,
            server,
            source_inputs,
            body_capture,
            // Build clients while assembling the runtime. Client construction can
            // consult system proxy configuration and must not consume a query's
            // execution deadline before its cancellation future starts polling.
            http_clients: McpHttpClients::new(),
        }
    }

    /// Resolve the configured bearer token through the source-input
    /// resolver, picking up any refreshed OAuth access token before each
    /// `tools/call`. Returns `None` when no auth is configured or when the
    /// `auth_token` value source resolves to an empty value.
    pub(super) async fn resolved_bearer_token(&self) -> Result<Option<String>> {
        let McpServerSpec::StreamableHttp { auth, .. } = &self.server else {
            return Ok(None);
        };
        let Some(auth) = auth else {
            return Ok(None);
        };
        let resolved_inputs = self.source_inputs.resolve_for_request().await?;
        let render_context = RenderContext::source_scoped(&resolved_inputs);
        let Some(token) = resolve_value_source(auth.bearer_token(), &render_context)? else {
            return Ok(None);
        };
        Ok(Some(value_to_env_string(token)))
    }

    pub(super) async fn list_tools(&self) -> Result<Vec<Tool>> {
        let McpServerSpec::StreamableHttp { url, .. } = &self.server else {
            unreachable!("StreamableHttpMcpToolCaller requires a Streamable HTTP MCP server spec");
        };

        let mut config = StreamableHttpClientTransportConfig::with_uri(url.clone())
            .reinit_on_expired_session(true);
        if let Some(token) = self.resolved_bearer_token().await? {
            config = config.auth_header(token);
        }

        let (transport, failures) = streamable_http_transport(
            config,
            QueryRetryPolicy::SourceDefault,
            &self.http_clients,
            None,
        )
        .map_err(|detail| {
            DataFusionError::External(Box::new(McpProviderQueryError::HttpRequestFailed {
                source_schema: self.source_name.clone(),
                detail,
                failure_kind: QueryExecutionFailureKind::UpstreamUnavailable,
            }))
        })?;
        let client = McpClientHandler::new(&self.source_name)
            .serve(transport)
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(mcp_http_initialize_error(
                    &self.source_name,
                    &error,
                    &failures,
                )))
            })?;
        list_tools_bounded(client.peer(), &self.source_name, |source_name, error| {
            mcp_http_initialize_error(source_name, error, &failures)
        })
        .await
    }

    #[cfg(test)]
    fn active_http_operations(&self) -> usize {
        self.http_clients.operations.active()
    }
}

#[async_trait]
impl McpToolCaller for StreamableHttpMcpToolCaller {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
        controls: &QueryExecutionControls,
    ) -> Result<Value> {
        let McpServerSpec::StreamableHttp { url, .. } = &self.server else {
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
            .call_tool_inner(url, relation, tool_name, arguments, request_id, controls)
            .instrument(request_span.clone())
            .await;
        if let Err(error) = &result {
            record_mcp_error(&request_span, error);
        }
        result
    }
}

impl StreamableHttpMcpToolCaller {
    #[expect(
        clippy::too_many_lines,
        reason = "The isolated HTTP call keeps initialization, causal classification, execution controls, and owned teardown explicit."
    )]
    async fn call_tool_inner(
        &self,
        url: &str,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
        request_id: u64,
        controls: &QueryExecutionControls,
    ) -> Result<Value> {
        let span = tracing::Span::current();

        let mut config = streamable_http_config(url, controls.retry_policy());
        let bearer_token = controls
            .run_until_stopped(self.resolved_bearer_token())
            .await
            .map_err(|kind| self.execution_stopped(relation, tool_name, kind))??;
        if let Some(token) = bearer_token {
            config = config.auth_header(token);
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

        let (transport, failures) = streamable_http_transport(
            config,
            controls.retry_policy(),
            &self.http_clients,
            Some(controls),
        )
        .map_err(|detail| {
            DataFusionError::External(Box::new(McpProviderQueryError::HttpRequestFailed {
                source_schema: self.source_name.clone(),
                detail,
                failure_kind: QueryExecutionFailureKind::UpstreamUnavailable,
            }))
        })?;
        let enforce_transport_stop = controls.requires_transport_enforcement();
        let mut initialization = Box::pin(
            controls.run_until_stopped(McpClientHandler::new(&self.source_name).serve(transport)),
        );
        let initialized = if enforce_transport_stop {
            tokio::select! {
                biased;
                failure_kind = failures.wait_stream_failure() => {
                    failures.shutdown_transport();
                    drop(initialization);
                    if !self
                        .http_clients
                        .operations
                        .wait_idle_until(controls.cleanup_deadline(failure_kind))
                        .await
                    {
                        tracing::debug!(
                            "controlled MCP HTTP initialize stream remained active at cleanup cutoff"
                        );
                    }
                    return Err(DataFusionError::External(Box::new(
                        McpProviderQueryError::Initialize {
                            source_schema: self.source_name.clone(),
                            detail: "controlled MCP initialization stream failed".to_string(),
                            failure_kind,
                        },
                    )));
                }
                initialized = &mut initialization => initialized,
            }
        } else {
            initialization.await
        };
        let mut client = match initialized {
            Err(kind) => {
                if controls.requires_transport_enforcement() {
                    failures.shutdown_transport();
                    if !self
                        .http_clients
                        .operations
                        .wait_idle_until(controls.cleanup_deadline(kind))
                        .await
                    {
                        tracing::debug!(
                            "controlled MCP HTTP initialize remained active at cleanup cutoff"
                        );
                    }
                }
                return Err(self.execution_stopped(relation, tool_name, kind));
            }
            Ok(Err(error)) => {
                if enforce_transport_stop {
                    failures.shutdown_transport();
                }
                return Err(DataFusionError::External(Box::new(
                    mcp_http_initialize_error(&self.source_name, &error, &failures),
                )));
            }
            Ok(Ok(client)) => client,
        };
        let raw = match call_tool_cancellable(
            client.peer(),
            CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments),
            controls,
            Some(&failures),
            None,
        )
        .await
        {
            Ok(raw) => raw,
            Err(ControlledMcpCallError::Stopped(kind)) => {
                if enforce_transport_stop {
                    failures.shutdown_transport();
                }
                close_stopped_service(&mut client).await;
                return Err(self.execution_stopped(relation, tool_name, kind));
            }
            Err(ControlledMcpCallError::Service(error)) => {
                let classified = mcp_http_tool_call_error(
                    &self.source_name,
                    relation,
                    tool_name,
                    &error,
                    &failures,
                );
                if enforce_transport_stop {
                    failures.shutdown_transport();
                    close_stopped_service(&mut client).await;
                }
                return Err(DataFusionError::External(Box::new(classified)));
            }
            Err(ControlledMcpCallError::StreamFailed(kind)) => {
                if enforce_transport_stop {
                    failures.shutdown_transport();
                    close_stopped_service(&mut client).await;
                }
                return Err(DataFusionError::External(Box::new(
                    McpProviderQueryError::ToolCall {
                        source_schema: self.source_name.clone(),
                        relation: relation.to_string(),
                        tool: tool_name.to_string(),
                        detail: "controlled MCP response stream failed".to_string(),
                        failure_kind: kind,
                    },
                )));
            }
        };
        if enforce_transport_stop {
            let ping_cleanup_deadline = controls.deadline().map_or_else(
                || controls.cleanup_deadline(QueryExecutionFailureKind::Execution),
                |deadline| {
                    deadline.min(controls.cleanup_deadline(QueryExecutionFailureKind::Execution))
                },
            );
            let _completed = failures
                .wait_server_request_until(ping_cleanup_deadline)
                .await;
            failures.shutdown_transport();
            close_stopped_service(&mut client).await;
        }
        controls
            .check_active()
            .map_err(|kind| self.execution_stopped(relation, tool_name, kind))?;
        let payload = normalize_tool_result(&self.source_name, relation, tool_name, raw)?;
        controls
            .check_active()
            .map_err(|kind| self.execution_stopped(relation, tool_name, kind))?;
        self.body_capture
            .record_response(&span, request_id, &payload);
        Ok(payload)
    }

    fn execution_stopped(
        &self,
        relation: &str,
        tool: &str,
        kind: QueryExecutionFailureKind,
    ) -> DataFusionError {
        execution_stopped(&self.source_name, relation, tool, kind)
    }
}

#[derive(Debug)]
enum ControlledMcpCallError {
    Stopped(QueryExecutionFailureKind),
    Service(rmcp::service::ServiceError),
    StreamFailed(QueryExecutionFailureKind),
}

async fn call_tool_cancellable(
    peer: &rmcp::service::Peer<RoleClient>,
    params: CallToolRequestParams,
    controls: &QueryExecutionControls,
    http_failures: Option<&HttpFailureSignal>,
    child_failures: Option<&ChildTransportTracker>,
) -> std::result::Result<CallToolResult, ControlledMcpCallError> {
    let request = ClientRequest::CallToolRequest(Request::new(params));
    let mut handle = controls
        .run_until_stopped(
            peer.send_cancellable_request(request, rmcp::service::PeerRequestOptions::no_options()),
        )
        .await
        .map_err(ControlledMcpCallError::Stopped)?
        .map_err(ControlledMcpCallError::Service)?;
    let cancellation_peer = handle.peer.clone();
    let cancellation_request_id = handle.id.clone();

    tokio::select! {
        biased;
        kind = wait_until_stopped(controls) => {
            let cleanup_deadline = controls.cleanup_deadline(kind);
            let notification = cancellation_peer.notify_cancelled(CancelledNotificationParam {
                request_id: cancellation_request_id,
                reason: Some(kind.to_string()),
            });
            match tokio::time::timeout_at(cleanup_deadline, notification).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::debug!(%error, "failed to send MCP cancellation notification"),
                Err(_elapsed) => tracing::debug!("timed out sending MCP cancellation notification"),
            }
            Err(ControlledMcpCallError::Stopped(kind))
        }
        response = &mut handle.rx => {
            let response = match response {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    if matches!(error, rmcp::service::ServiceError::TransportClosed)
                        && let Some(kind) = http_failures.and_then(HttpFailureSignal::stream_kind)
                    {
                        return Err(ControlledMcpCallError::StreamFailed(kind));
                    }
                    return Err(ControlledMcpCallError::Service(error));
                }
                Err(_closed) => {
                    if let Some(kind) = http_failures.and_then(HttpFailureSignal::stream_kind) {
                        return Err(ControlledMcpCallError::StreamFailed(kind));
                    }
                    return Err(ControlledMcpCallError::Service(
                        rmcp::service::ServiceError::TransportClosed,
                    ));
                }
            };
            match response {
                ServerResult::CallToolResult(result) => Ok(result),
                _ => Err(ControlledMcpCallError::Service(
                    rmcp::service::ServiceError::UnexpectedResponse,
                )),
            }
        }
        kind = wait_for_http_stream_failure(http_failures) => {
            Err(ControlledMcpCallError::StreamFailed(kind))
        }
        kind = wait_for_child_failure(child_failures) => {
            Err(ControlledMcpCallError::StreamFailed(kind))
        }
    }
}

async fn wait_for_child_failure(
    failures: Option<&ChildTransportTracker>,
) -> QueryExecutionFailureKind {
    match failures {
        Some(failures) => failures.wait_failure().await,
        None => std::future::pending().await,
    }
}

async fn wait_for_http_stream_failure(
    failures: Option<&HttpFailureSignal>,
) -> QueryExecutionFailureKind {
    match failures {
        Some(failures) => failures.wait_stream_failure().await,
        None => std::future::pending().await,
    }
}

async fn wait_until_stopped(controls: &QueryExecutionControls) -> QueryExecutionFailureKind {
    match controls
        .run_until_stopped(std::future::pending::<std::convert::Infallible>())
        .await
    {
        Err(kind) => kind,
        Ok(never) => match never {},
    }
}

fn streamable_http_config(
    url: &str,
    retry_policy: QueryRetryPolicy,
) -> StreamableHttpClientTransportConfig {
    let retry_enabled = matches!(retry_policy, QueryRetryPolicy::SourceDefault);
    let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_string())
        .reinit_on_expired_session(retry_enabled);
    if !retry_enabled {
        config.retry_config = Arc::new(NeverRetry::default());
    }
    config
}

fn streamable_http_transport(
    config: StreamableHttpClientTransportConfig,
    retry_policy: QueryRetryPolicy,
    clients: &McpHttpClients,
    controls: Option<&QueryExecutionControls>,
) -> std::result::Result<
    (
        StreamableHttpClientTransport<CoralStreamableHttpClient>,
        HttpFailureSignal,
    ),
    String,
> {
    let failures = HttpFailureSignal::default();
    let client = clients.client(retry_policy)?;
    let client = match controls.filter(|controls| controls.requires_transport_enforcement()) {
        Some(controls) => CoralStreamableHttpClient::controlled(
            client,
            controls.clone(),
            failures.clone(),
            clients.operations.clone(),
        ),
        None => CoralStreamableHttpClient::raw(client),
    };
    Ok((
        StreamableHttpClientTransport::with_client(client, config),
        failures,
    ))
}

#[derive(Debug)]
struct McpHttpClients {
    source_default: std::result::Result<reqwest::Client, String>,
    single_attempt: std::result::Result<reqwest::Client, String>,
    operations: HttpOperationTracker,
}

impl McpHttpClients {
    fn new() -> Self {
        Self {
            source_default: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .map_err(|error| error.to_string()),
            single_attempt: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| error.to_string()),
            operations: HttpOperationTracker::default(),
        }
    }

    fn client(
        &self,
        retry_policy: QueryRetryPolicy,
    ) -> std::result::Result<reqwest::Client, String> {
        match retry_policy {
            QueryRetryPolicy::SourceDefault => &self.source_default,
            QueryRetryPolicy::Disabled => &self.single_attempt,
        }
        .as_ref()
        .cloned()
        .map_err(Clone::clone)
    }
}

async fn close_stopped_service<R>(client: &mut rmcp::service::RunningService<RoleClient, R>)
where
    R: rmcp::Service<RoleClient>,
{
    // The controlled transports bound every request, cancellation, stream,
    // session-delete, and child-process operation by the one absolute cleanup
    // cutoff. Await rmcp's service task to completion after cancelling it;
    // `close_with_timeout` drops its owned JoinHandle on timeout, which would
    // detach that task past Coral's hard execution boundary.
    match client.close().await {
        Ok(_) => {}
        Err(error) => tracing::debug!(%error, "failed to close stopped MCP client service"),
    }
}

async fn close_stopped_stdio_service<R>(
    client: &mut rmcp::service::RunningService<RoleClient, R>,
    controls: &QueryExecutionControls,
    kind: QueryExecutionFailureKind,
    child_tracker: &ChildTransportTracker,
) where
    R: rmcp::Service<RoleClient>,
{
    let cleanup_deadline = controls.cleanup_deadline(kind);
    let close = close_stopped_service(client);
    let reap = child_tracker.wait_reaped_until(cleanup_deadline);
    let ((), reap_result) = tokio::join!(close, reap);
    if let Err(error) = reap_result {
        tracing::debug!(%error, "failed to reap stopped MCP child before cleanup cutoff");
    }
}

async fn close_controlled_stdio_service<R>(
    client: &mut rmcp::service::RunningService<RoleClient, R>,
    child_tracker: &ChildTransportTracker,
) where
    R: rmcp::Service<RoleClient>,
{
    close_stopped_service(client).await;
    if let Err(error) = child_tracker.wait_reaped().await {
        tracing::debug!(%error, "failed to reap completed MCP child service");
    }
}

async fn await_stopped_child(
    child_tracker: &ChildTransportTracker,
    controls: &QueryExecutionControls,
    kind: QueryExecutionFailureKind,
) {
    if let Err(error) = child_tracker
        .wait_reaped_until(controls.cleanup_deadline(kind))
        .await
    {
        tracing::debug!(%error, "failed to reap MCP child after stopped initialization");
    }
}

fn service_failure_kind(error: &rmcp::service::ServiceError) -> QueryExecutionFailureKind {
    match error {
        rmcp::service::ServiceError::Timeout { .. } => QueryExecutionFailureKind::Timeout,
        rmcp::service::ServiceError::Cancelled { .. } => QueryExecutionFailureKind::Cancelled,
        rmcp::service::ServiceError::UnexpectedResponse => {
            QueryExecutionFailureKind::InvalidResponse
        }
        rmcp::service::ServiceError::TransportSend(_)
        | rmcp::service::ServiceError::TransportClosed => {
            QueryExecutionFailureKind::UpstreamUnavailable
        }
        // Includes typed MCP application errors and future non-exhaustive
        // service variants, which are ordinary execution failures.
        _ => QueryExecutionFailureKind::Execution,
    }
}

fn initialize_failure_kind(
    error: &rmcp::service::ClientInitializeError,
) -> QueryExecutionFailureKind {
    use rmcp::service::ClientInitializeError as InitializeError;

    match error {
        InitializeError::ExpectedInitResponse(_)
        | InitializeError::ExpectedInitResult(_)
        | InitializeError::ConflictInitResponseId(_, _) => {
            QueryExecutionFailureKind::InvalidResponse
        }
        InitializeError::Cancelled => QueryExecutionFailureKind::Cancelled,
        InitializeError::JsonRpcError(_) => QueryExecutionFailureKind::Execution,
        InitializeError::ConnectionClosed(_) | InitializeError::TransportError { .. } => {
            QueryExecutionFailureKind::UpstreamUnavailable
        }
        _ => QueryExecutionFailureKind::UpstreamUnavailable,
    }
}

fn execution_stopped(
    source_schema: &str,
    relation: &str,
    tool: &str,
    kind: QueryExecutionFailureKind,
) -> DataFusionError {
    DataFusionError::External(Box::new(McpProviderQueryError::ExecutionStopped {
        source_schema: source_schema.to_string(),
        relation: relation.to_string(),
        tool: tool.to_string(),
        kind,
    }))
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

async fn list_tools_bounded(
    peer: &rmcp::service::Peer<rmcp::RoleClient>,
    source_name: &str,
    classify_error: impl Fn(&str, &(dyn std::error::Error + 'static)) -> McpProviderQueryError,
) -> Result<Vec<Tool>> {
    let mut tools = Vec::new();
    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();

    for _page in 0..MAX_MCP_DISCOVERY_PAGES {
        let result = peer
            .list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor)))
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(classify_error(source_name, &error)))
            })?;
        tools.extend(result.tools);
        let Some(next_cursor) = result.next_cursor else {
            return Ok(tools);
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(DataFusionError::External(Box::new(
                McpProviderQueryError::Initialize {
                    source_schema: source_name.to_string(),
                    detail: format!("MCP tools/list returned repeated next cursor '{next_cursor}'"),
                    failure_kind: QueryExecutionFailureKind::InvalidResponse,
                },
            )));
        }
        cursor = Some(next_cursor);
    }

    Err(DataFusionError::External(Box::new(
        McpProviderQueryError::Initialize {
            source_schema: source_name.to_string(),
            detail: format!("MCP tools/list exceeded max_pages={MAX_MCP_DISCOVERY_PAGES}"),
            failure_kind: QueryExecutionFailureKind::InvalidResponse,
        },
    )))
}

fn mcp_http_initialize_error(
    source_schema: &str,
    error: &(dyn std::error::Error + 'static),
    failures: &HttpFailureSignal,
) -> McpProviderQueryError {
    classify_streamable_http_error(source_schema, None, error, failures)
}

fn mcp_http_tool_call_error(
    source_schema: &str,
    relation: &str,
    tool: &str,
    error: &(dyn std::error::Error + 'static),
    failures: &HttpFailureSignal,
) -> McpProviderQueryError {
    classify_streamable_http_error(source_schema, Some((relation, tool)), error, failures)
}

/// Classify an rmcp `ClientInitializeError` / `ServiceError` raised by the
/// Streamable HTTP transport into a structured `McpProviderQueryError`.
///
/// rmcp wraps the underlying transport error in a `DynamicTransportError`
/// whose inner `Box<dyn Error>` is typed-downcast back to
/// `StreamableHttpError<reqwest::Error>`. Unrecognized errors fall back to
/// the generic `Initialize`/`ToolCall` variants.
#[expect(
    clippy::too_many_lines,
    reason = "The exhaustive rmcp transport classification is kept in one match so legacy structured shapes and typed internal kinds remain auditable together."
)]
fn classify_streamable_http_error(
    source_schema: &str,
    relation_and_tool: Option<(&str, &str)>,
    error: &(dyn std::error::Error + 'static),
    failures: &HttpFailureSignal,
) -> McpProviderQueryError {
    let service_error = error.downcast_ref::<rmcp::service::ServiceError>();
    let initialize_error = error.downcast_ref::<rmcp::service::ClientInitializeError>();
    let dyn_err = match initialize_error {
        Some(rmcp::service::ClientInitializeError::TransportError { error: dyn_err, .. }) => {
            Some(dyn_err)
        }
        _ => None,
    }
    .or_else(
        || match error.downcast_ref::<rmcp::service::ServiceError>() {
            Some(rmcp::service::ServiceError::TransportSend(dyn_err)) => Some(dyn_err),
            _ => None,
        },
    );

    let full_detail = error.to_string();

    if let Some(dyn_err) = dyn_err
        && let Some(streamable_err) = dyn_err
            .error
            .downcast_ref::<rmcp::transport::streamable_http_client::StreamableHttpError<
            reqwest::Error,
        >>()
    {
        use rmcp::transport::streamable_http_client::StreamableHttpError as SHE;
        // rmcp 1.8 retains non-success HTTP status only inside
        // `UnexpectedServerResponse` display text. The guarded branch below
        // preserves Coral's legacy structured-error shape, but intentionally
        // does not derive typed 429/401/403 classifications from that text.
        return match streamable_err {
            SHE::AuthRequired(_) => McpProviderQueryError::AuthRequired {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            SHE::InsufficientScope(_) => McpProviderQueryError::AuthFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            SHE::SessionExpired => McpProviderQueryError::SessionExpired {
                source_schema: source_schema.to_string(),
            },
            SHE::UnexpectedServerResponse(detail) if is_legacy_http_status_detail(detail) => {
                McpProviderQueryError::HttpStatusFailed {
                    source_schema: source_schema.to_string(),
                    detail: full_detail,
                    failure_kind: failures
                        .get()
                        .unwrap_or(QueryExecutionFailureKind::UpstreamUnavailable),
                }
            }
            SHE::Sse(_)
            | SHE::UnexpectedContentType(_)
            | SHE::Deserialize(_)
            | SHE::ServerDoesNotSupportSse => McpProviderQueryError::HttpSseDecodeFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
            },
            SHE::Client(error) => McpProviderQueryError::HttpRequestFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
                failure_kind: failures
                    .get()
                    .unwrap_or_else(|| reqwest_failure_kind(error)),
            },
            SHE::Io(error) => McpProviderQueryError::HttpRequestFailed {
                source_schema: source_schema.to_string(),
                detail: full_detail,
                failure_kind: error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<QueryExecutionFailureKind>())
                    .copied()
                    .or_else(|| failures.get())
                    .unwrap_or(QueryExecutionFailureKind::UpstreamUnavailable),
            },
            SHE::UnexpectedServerResponse(_)
            | SHE::UnexpectedEndOfStream
            | SHE::MissingSessionIdInResponse => {
                let failure_kind = failures
                    .get()
                    .unwrap_or(QueryExecutionFailureKind::InvalidResponse);
                match relation_and_tool {
                    Some((relation, tool)) => McpProviderQueryError::ToolCall {
                        source_schema: source_schema.to_string(),
                        relation: relation.to_string(),
                        tool: tool.to_string(),
                        detail: full_detail,
                        failure_kind,
                    },
                    None => McpProviderQueryError::Initialize {
                        source_schema: source_schema.to_string(),
                        detail: full_detail,
                        failure_kind,
                    },
                }
            }
            _ => match relation_and_tool {
                Some((relation, tool)) => McpProviderQueryError::ToolCall {
                    source_schema: source_schema.to_string(),
                    relation: relation.to_string(),
                    tool: tool.to_string(),
                    detail: full_detail,
                    failure_kind: service_error.map_or(
                        QueryExecutionFailureKind::UpstreamUnavailable,
                        service_failure_kind,
                    ),
                },
                None => McpProviderQueryError::Initialize {
                    source_schema: source_schema.to_string(),
                    detail: full_detail,
                    failure_kind: failures
                        .get()
                        .unwrap_or(QueryExecutionFailureKind::UpstreamUnavailable),
                },
            },
        };
    }

    if let (Some(service_error), Some((relation, tool))) = (service_error, relation_and_tool) {
        return McpProviderQueryError::ToolCall {
            source_schema: source_schema.to_string(),
            relation: relation.to_string(),
            tool: tool.to_string(),
            detail: full_detail,
            failure_kind: failures
                .get()
                .unwrap_or_else(|| service_failure_kind(service_error)),
        };
    }

    match relation_and_tool {
        Some((relation, tool)) => McpProviderQueryError::ToolCall {
            source_schema: source_schema.to_string(),
            relation: relation.to_string(),
            tool: tool.to_string(),
            detail: full_detail,
            failure_kind: QueryExecutionFailureKind::UpstreamUnavailable,
        },
        None => McpProviderQueryError::Initialize {
            source_schema: source_schema.to_string(),
            detail: full_detail,
            failure_kind: failures
                .get()
                .or_else(|| initialize_error.map(initialize_failure_kind))
                .unwrap_or(QueryExecutionFailureKind::UpstreamUnavailable),
        },
    }
}

fn reqwest_failure_kind(error: &reqwest::Error) -> QueryExecutionFailureKind {
    if error.is_timeout() {
        return QueryExecutionFailureKind::Timeout;
    }
    match error.status().map(|status| status.as_u16()) {
        Some(401) => QueryExecutionFailureKind::Authentication,
        Some(403) => QueryExecutionFailureKind::PermissionDenied,
        Some(429) => QueryExecutionFailureKind::RateLimited,
        Some(500..=599) | None => QueryExecutionFailureKind::UpstreamUnavailable,
        Some(_) => QueryExecutionFailureKind::Execution,
    }
}

fn is_legacy_http_status_detail(detail: &str) -> bool {
    // Compatibility only: rmcp 1.8 erased the typed HTTP status before Coral
    // receives this error, and the pre-control path used this prefix solely to
    // retain MCP_HTTP_STATUS_FAILED. Do not derive 429/auth classifications.
    let detail = detail.trim_start();
    detail.starts_with("HTTP ") || detail.starts_with("unexpected status")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use opentelemetry::Value as OtelValue;
    use opentelemetry::trace::{SpanKind, Status, TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use rmcp::model::JsonObject;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};
    use tracing::subscriber::DefaultGuard;

    use tracing_subscriber::layer::SubscriberExt;
    use wiremock::http::HeaderValue as WiremockHeaderValue;
    use wiremock::matchers::{body_partial_json, header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::backends::CONTROLLED_RESPONSE_BODY_LIMIT_BYTES;
    use crate::{
        CoralQuery, QueryCancellationToken, QueryPaginationPolicy, QueryParameters,
        QueryRuntimeConfig, QuerySource,
    };

    fn unexpected_server_response_error(detail: &'static str) -> rmcp::service::ServiceError {
        let error = rmcp::transport::streamable_http_client::StreamableHttpError::<
            reqwest::Error,
        >::UnexpectedServerResponse(detail.into());
        rmcp::service::ServiceError::TransportSend(
            rmcp::transport::DynamicTransportError::from_parts(
                "test-streamable-http",
                std::any::TypeId::of::<()>(),
                Box::new(error),
            ),
        )
    }

    fn typed_mcp_error(error: &DataFusionError) -> &McpProviderQueryError {
        let DataFusionError::External(inner) = error.find_root() else {
            panic!("expected external MCP error, got {error}");
        };
        inner
            .downcast_ref::<McpProviderQueryError>()
            .expect("typed MCP provider error")
    }

    #[test]
    fn bare_http_status_keeps_legacy_shape_without_inventing_typed_rate_limit() {
        let error = unexpected_server_response_error("HTTP 429 Too Many Requests: slow down");
        let classified = classify_streamable_http_error(
            "remote_mcp",
            Some(("issues", "list_issues")),
            &error,
            &HttpFailureSignal::default(),
        );

        assert!(matches!(
            classified,
            McpProviderQueryError::HttpStatusFailed { .. }
        ));
        assert_eq!(
            classified.execution_failure_kind(),
            QueryExecutionFailureKind::UpstreamUnavailable
        );
        assert_eq!(
            classified.to_structured().reason(),
            "MCP_HTTP_STATUS_FAILED"
        );
    }

    #[test]
    fn classify_unexpected_server_response_does_not_treat_protocol_errors_as_status_failures() {
        let error = unexpected_server_response_error("empty sse stream");
        let classified = classify_streamable_http_error(
            "remote_mcp",
            Some(("issues", "list_issues")),
            &error,
            &HttpFailureSignal::default(),
        );

        assert!(matches!(classified, McpProviderQueryError::ToolCall { .. }));
    }

    #[test]
    fn malformed_initialize_protocol_shapes_are_invalid_responses() {
        use rmcp::model::RequestId;
        use rmcp::service::ClientInitializeError as InitializeError;

        let errors = [
            InitializeError::ExpectedInitResponse(None),
            InitializeError::ExpectedInitResult(None),
            InitializeError::ConflictInitResponseId(RequestId::Number(1), RequestId::Number(2)),
        ];

        for error in errors {
            assert_eq!(
                initialize_failure_kind(&error),
                QueryExecutionFailureKind::InvalidResponse
            );
            let classified =
                mcp_http_initialize_error("remote_mcp", &error, &HttpFailureSignal::default());
            assert_eq!(
                classified.execution_failure_kind(),
                QueryExecutionFailureKind::InvalidResponse
            );
        }
    }

    #[test]
    fn typed_service_error_keeps_default_tool_call_shape_and_safe_classification() {
        let service_error = rmcp::service::ServiceError::UnexpectedResponse;
        let classified = classify_streamable_http_error(
            "remote_mcp",
            Some(("issues", "list_issues")),
            &service_error,
            &HttpFailureSignal::default(),
        );

        assert_eq!(
            classified.execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        let structured = classified.to_structured();
        assert_eq!(structured.reason(), "MCP_TOOL_CALL_FAILED");
        assert!(structured.detail().contains("Unexpected response type"));
    }

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

    fn streamable_http_validated_manifest(url: &str) -> coral_spec::ValidatedSourceManifest {
        coral_spec::parse_source_manifest_value(json!({
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
                "filters": [{ "name": "mode", "tool_arg": "mode" }],
                "response": { "rows_path": ["issues"] },
                "columns": [
                    { "name": "title", "type": "Utf8" },
                    {
                        "name": "mode",
                        "type": "Utf8",
                        "virtual": true,
                        "expr": { "kind": "from_filter", "key": "mode" }
                    }
                ]
            }]
        }))
        .expect("manifest should parse")
    }

    fn streamable_http_manifest(url: &str) -> coral_spec::McpSourceManifest {
        streamable_http_validated_manifest(url)
            .as_mcp()
            .expect("expected mcp manifest")
            .clone()
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

    fn json_rpc_result_response(id: Value, result: Value) -> ResponseTemplate {
        let mut body = serde_json::Map::new();
        body.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        body.insert("id".to_string(), id);
        body.insert("result".to_string(), result);
        ResponseTemplate::new(200)
            .append_header("Content-Type", "application/json")
            .set_body_json(Value::Object(body))
    }

    fn json_rpc_request_id(body: &Value) -> Value {
        body.get("id").cloned().unwrap_or(Value::Null)
    }

    async fn read_http_request(socket: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = socket.read(&mut buffer).await.expect("read HTTP request");
            assert!(read > 0, "HTTP client closed before request completed");
            bytes.extend(buffer.iter().copied().take(read));
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(bytes.get(..header_end).unwrap_or_default());
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .and_then(|value| value.parse::<usize>().ok())
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + 4 + content_length {
                return String::from_utf8_lossy(&bytes).into_owned();
            }
        }
    }

    async fn spawn_stalled_status_mcp_server(status: u16) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled MCP server");
        let addr = listener.local_addr().expect("MCP server address");
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("accept MCP request");
                let request = read_http_request(&mut socket).await;
                if request.contains("\"method\":\"initialize\"") {
                    let body = json!({
                        "jsonrpc": "2.0",
                        "id": 0,
                        "result": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "serverInfo": { "name": "fixture", "version": "0.1.0" }
                        }
                    })
                    .to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket
                        .write_all(response.as_bytes())
                        .await
                        .expect("write initialize response");
                } else if request.contains("\"method\":\"notifications/initialized\"") {
                    socket
                        .write_all(
                            b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .expect("write initialized acknowledgement");
                } else if request.contains("\"method\":\"tools/call\"") {
                    let response = format!(
                        "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n10\r\nprovider-private"
                    );
                    socket
                        .write_all(response.as_bytes())
                        .await
                        .expect("write stalled status response");
                    socket.flush().await.expect("flush stalled status response");
                    std::future::pending::<()>().await;
                } else {
                    panic!("unexpected MCP HTTP request: {request}");
                }
            }
        });
        (format!("http://{addr}"), task)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "The raw test server keeps its coordinated HTTP branches together so the Ping-reply-before-result ordering remains auditable."
    )]
    async fn spawn_coordinated_stalled_ping_reply_server() -> (String, tokio::task::JoinHandle<()>)
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind coordinated MCP server");
        let addr = listener.local_addr().expect("MCP server address");
        let ping_reply_started = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.expect("accept MCP request");
                        let ping_reply_started = Arc::clone(&ping_reply_started);
                        handlers.spawn(async move {
                            let request = read_http_request(&mut socket).await;
                            if request.starts_with("GET ") {
                                socket
                                    .write_all(
                                        b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    )
                                    .await
                                    .expect("reject common SSE");
                            } else if request.starts_with("DELETE ") {
                                socket
                                    .write_all(
                                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    )
                                    .await
                                    .expect("acknowledge session deletion");
                            } else if request.contains("\"method\":\"initialize\"") {
                                let body = json!({
                                    "jsonrpc": "2.0",
                                    "id": 0,
                                    "result": {
                                        "protocolVersion": "2025-06-18",
                                        "capabilities": {},
                                        "serverInfo": { "name": "fixture", "version": "0.1.0" }
                                    }
                                })
                                .to_string();
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                socket
                                    .write_all(response.as_bytes())
                                    .await
                                    .expect("write initialize response");
                            } else if request.contains("\"method\":\"notifications/initialized\"") {
                                socket
                                    .write_all(
                                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    )
                                    .await
                                    .expect("write initialized acknowledgement");
                            } else if request.contains("\"method\":\"tools/call\"") {
                                socket
                                    .write_all(
                                        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
                                    )
                                    .await
                                    .expect("write tool SSE headers");
                                let ping = json!({
                                    "jsonrpc": "2.0",
                                    "id": 7,
                                    "method": "ping"
                                });
                                let ping_event = format!("data: {ping}\n\n");
                                let ping_chunk = format!(
                                    "{:X}\r\n{ping_event}\r\n",
                                    ping_event.len()
                                );
                                socket
                                    .write_all(ping_chunk.as_bytes())
                                    .await
                                    .expect("write server Ping");
                                socket.flush().await.expect("flush server Ping");

                                // Do not reveal the valid result until Coral's
                                // Ping reply is definitely in flight and the
                                // other handler is deliberately stalling it.
                                ping_reply_started.notified().await;
                                let result = json!({
                                    "jsonrpc": "2.0",
                                    "id": 1,
                                    "result": {
                                        "structuredContent": {
                                            "issues": [{ "title": "cleanup won" }]
                                        }
                                    }
                                });
                                let result_event = format!("data: {result}\n\n");
                                let result_chunk = format!(
                                    "{:X}\r\n{result_event}\r\n0\r\n\r\n",
                                    result_event.len()
                                );
                                socket
                                    .write_all(result_chunk.as_bytes())
                                    .await
                                    .expect("write coordinated tool result");
                                socket.flush().await.expect("flush coordinated tool result");
                            } else if request.contains("\"id\":7") {
                                ping_reply_started.notify_one();
                                // Hold response headers open until Coral's
                                // transport-local shutdown cancels this POST.
                                std::future::pending::<()>().await;
                            } else {
                                panic!("unexpected coordinated MCP request: {request}");
                            }
                        });
                    }
                    Some(result) = handlers.join_next(), if !handlers.is_empty() => {
                        result.expect("coordinated MCP request handler");
                    }
                }
            }
        });
        (format!("http://{addr}"), task)
    }

    fn tool_result(name: &str) -> Value {
        json!({
            "name": name,
            "description": format!("Tool {name}"),
            "inputSchema": { "type": "object", "properties": {} }
        })
    }

    async fn mount_streamable_http_tools_list_responder(
        server: &MockServer,
        tools_list_response: impl Fn(&Value) -> ResponseTemplate + Send + Sync + 'static,
    ) {
        Mock::given(method("POST"))
            .respond_with(move |request: &wiremock::Request| {
                let body: Value = request.body_json().expect("JSON-RPC request body");
                match body.get("method").and_then(Value::as_str) {
                    Some("initialize") => initialize_response(),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some("tools/list") => tools_list_response(&body),
                    other => ResponseTemplate::new(404)
                        .set_body_string(format!("unexpected MCP method {other:?}")),
                }
            })
            .mount(server)
            .await;
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
        let source_inputs = Arc::new(McpSourceInputs::static_inputs(resolved_inputs));
        let caller = StreamableHttpMcpToolCaller::new(
            manifest.common.name,
            manifest.server,
            source_inputs,
            McpBodyCapture::default(),
        );
        let mut arguments = JsonObject::new();
        arguments.insert("state".to_string(), Value::String("open".to_string()));

        let payload = caller
            .call_tool(
                "issues",
                "list_issues",
                arguments,
                &QueryExecutionControls::default(),
            )
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

    #[test]
    fn disabled_retry_policy_disables_session_reinit_and_sse_reconnect() {
        let ordinary =
            streamable_http_config("https://example.com/mcp", QueryRetryPolicy::SourceDefault);
        assert!(ordinary.reinit_on_expired_session);
        assert!(ordinary.retry_config.retry(0).is_some());

        let disabled =
            streamable_http_config("https://example.com/mcp", QueryRetryPolicy::Disabled);
        assert!(!disabled.reinit_on_expired_session);
        assert!(disabled.retry_config.retry(0).is_none());
    }

    #[tokio::test]
    async fn disabled_retry_policy_does_not_replay_post_through_redirects() {
        let redirect_target = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(initialize_response())
            .expect(0)
            .mount(&redirect_target)
            .await;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                ResponseTemplate::new(307).append_header("Location", redirect_target.uri()),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::SourceDefault,
            QueryRetryPolicy::Disabled,
        );

        caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("single-attempt MCP call must reject POST redirects");

        server.verify().await;
        redirect_target.verify().await;
    }

    async fn mount_concurrent_execution_responses(server: &MockServer) {
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "concurrent-session"),
            )
            .expect(2)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(405))
            .mount(server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/initialized" }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .expect(2)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({
                "method": "tools/call",
                "params": { "arguments": { "mode": "stall" } }
            })))
            .respond_with(
                json_rpc_result_response(
                    Value::from(1),
                    json!({ "structuredContent": { "issues": [] } }),
                )
                .set_delay(std::time::Duration::from_secs(5)),
            )
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({
                "method": "tools/call",
                "params": { "arguments": { "mode": "ordinary" } }
            })))
            .respond_with(json_rpc_result_response(
                Value::from(1),
                json!({
                    "structuredContent": { "issues": [{ "title": "ordinary survived" }] }
                }),
            ))
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/cancelled" }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .mount(server)
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controlled_timeout_cancels_only_its_session_while_ordinary_call_succeeds() {
        let server = MockServer::start().await;
        mount_concurrent_execution_responses(&server).await;
        let source = QuerySource::new(
            streamable_http_validated_manifest(&server.uri()),
            BTreeMap::new(),
            BTreeMap::from([("MCP_ACCESS_TOKEN".to_string(), "secret-token".to_string())]),
        );
        let runtime = Arc::new(
            CoralQuery::prepare(&[source], QueryRuntimeConfig::default())
                .await
                .expect("runtime should prepare"),
        );
        let controlled = QueryExecutionControls::new(
            Some(tokio::time::Instant::now() + std::time::Duration::from_millis(150)),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );
        let stalled_runtime = Arc::clone(&runtime);
        let stalled = tokio::spawn(async move {
            let started = tokio::time::Instant::now();
            let result = stalled_runtime
                .execute_sql_with_controls(
                    "SELECT title FROM remote_mcp.issues WHERE mode = 'stall'",
                    QueryParameters::new(),
                    controlled,
                )
                .await;
            (result, started.elapsed())
        });
        let ordinary_runtime = Arc::clone(&runtime);
        let ordinary = tokio::spawn(async move {
            ordinary_runtime
                .execute_sql("SELECT title FROM remote_mcp.issues WHERE mode = 'ordinary'")
                .await
        });
        let (stalled, ordinary) = tokio::join!(stalled, ordinary);

        let (stalled, stalled_elapsed) = stalled.expect("stalled call task");
        assert!(
            stalled_elapsed < std::time::Duration::from_secs(1),
            "stalled call elapsed {stalled_elapsed:?}"
        );
        assert_eq!(
            stalled.expect_err("controlled call should time out"),
            QueryExecutionFailureKind::Timeout
        );
        let ordinary = ordinary
            .expect("ordinary call task")
            .expect("ordinary call should survive");
        assert_eq!(ordinary.row_count(), 1);
        server.verify().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controlled_session_teardown_has_no_http_operation_past_cleanup_cutoff() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "controlled-session"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(405))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                json_rpc_result_response(
                    Value::from(1),
                    json!({ "structuredContent": { "issues": [] } }),
                )
                .set_delay(std::time::Duration::from_secs(5)),
            )
            .expect(1)
            .mount(&server)
            .await;
        // Both best-effort protocol cleanup operations deliberately outlive
        // the grace period. The controlled adapter must drop them at the one
        // shared cleanup cutoff rather than inheriting rmcp's five seconds.
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/cancelled" }),
            ))
            .respond_with(ResponseTemplate::new(202).set_delay(std::time::Duration::from_secs(5)))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202).set_delay(std::time::Duration::from_secs(5)))
            .mount(&server)
            .await;

        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(125);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("controlled session should stop at its deadline");
        let mcp_error = typed_mcp_error(&error);
        assert_eq!(
            mcp_error.execution_failure_kind(),
            QueryExecutionFailureKind::Timeout
        );
        assert!(
            tokio::time::Instant::now() <= deadline + std::time::Duration::from_millis(100),
            "controlled teardown exceeded its cleanup cutoff"
        );
        assert_eq!(caller.active_http_operations(), 0);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controlled_cancellation_uses_one_shared_cleanup_cutoff() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "cancelled-session"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(405))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                json_rpc_result_response(
                    Value::from(1),
                    json!({ "structuredContent": { "issues": [] } }),
                )
                .set_delay(std::time::Duration::from_secs(5)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/cancelled" }),
            ))
            .respond_with(ResponseTemplate::new(202).set_delay(std::time::Duration::from_secs(5)))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202).set_delay(std::time::Duration::from_secs(5)))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let cancellation = QueryCancellationToken::new();
        let controls = QueryExecutionControls::new(
            Some(tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
            cancellation.clone(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );
        let call = caller.call_tool("issues", "list_issues", JsonObject::new(), &controls);
        let cancel = async {
            tokio::time::sleep(std::time::Duration::from_millis(75)).await;
            cancellation.cancel();
            tokio::time::Instant::now()
        };

        let (result, cancelled_at) = tokio::join!(call, cancel);
        let error = result.expect_err("cancelled tool call must fail");

        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::Cancelled
        );
        assert!(
            tokio::time::Instant::now()
                <= controls.cleanup_deadline(QueryExecutionFailureKind::Cancelled)
                    + std::time::Duration::from_millis(100),
            "cleanup exceeded the cancellation's shared cutoff from {cancelled_at:?}"
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_success_does_not_wait_for_stalled_session_delete() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "successful-session"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(405))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(json_rpc_result_response(
                Value::from(1),
                json!({
                    "structuredContent": { "issues": [{ "title": "result survived" }] }
                }),
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202).set_delay(std::time::Duration::from_secs(5)))
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );
        let started = tokio::time::Instant::now();

        let payload = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect("successful tool result must survive stalled session deletion");

        assert_eq!(
            payload
                .get("issues")
                .and_then(Value::as_array)
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("title"))
                .and_then(Value::as_str),
            Some("result survived")
        );
        assert!(started.elapsed() < std::time::Duration::from_millis(250));
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_stalled_initialize_drops_http_operation_at_deadline() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response().set_delay(std::time::Duration::from_secs(5)))
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(125);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("controlled initialize should stop at its deadline");
        let mcp_error = typed_mcp_error(&error);
        assert_eq!(
            mcp_error.execution_failure_kind(),
            QueryExecutionFailureKind::Timeout
        );
        assert_eq!(caller.active_http_operations(), 0);
        assert!(controls.upstream_started());
        assert!(
            tokio::time::Instant::now() <= deadline + std::time::Duration::from_millis(100),
            "controlled initialize exceeded its cleanup cutoff"
        );
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_malformed_json_is_invalid_response_without_provider_body_detail() {
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "application/json")
                    .set_body_raw("provider-private-malformed-body{", "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("malformed controlled response must fail");
        let mcp_error = typed_mcp_error(&error);
        assert_eq!(
            mcp_error.execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        let structured = mcp_error.to_structured();
        assert_eq!(structured.reason(), "MCP_TOOL_CALL_FAILED");
        assert!(structured.detail().contains("malformed JSON response"));
        assert!(
            !structured
                .detail()
                .contains("provider-private-malformed-body")
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_sse_progress_then_result_succeeds() {
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
        let progress = json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": { "progressToken": "fanout", "progress": 0.5 }
        });
        let result = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "structuredContent": { "issues": [{ "title": "SSE result" }] }
            }
        });
        let body = format!("data: {progress}\n\ndata: {result}\n\n");
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw(body, "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let payload = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect("valid SSE result should survive a progress notification");

        assert_eq!(
            payload
                .get("issues")
                .and_then(Value::as_array)
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("title"))
                .and_then(Value::as_str),
            Some("SSE result")
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_sse_ping_then_result_succeeds() {
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
            .and(body_partial_json(json!({ "id": 7 })))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        let ping = json!({ "jsonrpc": "2.0", "id": 7, "method": "ping" });
        let result = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "structuredContent": { "issues": [{ "title": "ping survived" }] }
            }
        });
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw(
                        format!("data: {ping}\n\ndata: {result}\n\n"),
                        "text/event-stream",
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let payload = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect("one protocol-valid server ping should remain compatible");
        assert_eq!(
            payload
                .get("issues")
                .and_then(Value::as_array)
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("title"))
                .and_then(Value::as_str),
            Some("ping survived")
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_success_cancels_stalled_ping_reply_transport() {
        let (url, server) = spawn_coordinated_stalled_ping_reply_server().await;
        let caller = make_caller(streamable_http_manifest(&url), McpBodyCapture::default());
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let started = tokio::time::Instant::now();
        let payload = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("stalled Ping reply must not keep the controlled transport alive")
        .expect("the valid tool result should win before transport shutdown");
        assert_eq!(
            payload
                .get("issues")
                .and_then(Value::as_array)
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("title"))
                .and_then(Value::as_str),
            Some("cleanup won")
        );
        assert_eq!(caller.active_http_operations(), 0);
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "transport shutdown waited for rmcp's multi-second service drain"
        );
        server.abort();
    }

    #[tokio::test]
    async fn controlled_direct_sse_ignores_failed_common_stream() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "direct-sse-session"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
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
        let result = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "structuredContent": { "issues": [{ "title": "direct SSE won" }] }
            }
        });
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_delay(std::time::Duration::from_millis(50))
                    .set_body_raw(format!("data: {result}\n\n"), "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let payload = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect("a failed common stream must not poison a direct tool SSE response");
        assert_eq!(
            payload
                .get("issues")
                .and_then(Value::as_array)
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("title"))
                .and_then(Value::as_str),
            Some("direct SSE won")
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_empty_initialize_sse_is_invalid_without_deadline() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw("", "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("empty initialize SSE should terminate without a query deadline")
        .expect_err("empty initialize SSE must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_initialize_sse_rejects_pre_handshake_ping() {
        let server = MockServer::start().await;
        let ping = json!({ "jsonrpc": "2.0", "id": 7, "method": "ping" });
        let initialize = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": { "name": "fixture", "version": "0.1.0" }
            }
        });
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw(
                        format!("data: {ping}\n\ndata: {initialize}\n\n"),
                        "text/event-stream",
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("pre-handshake Ping should terminate initialization")
        .expect_err("rmcp cannot service a server Ping before initialization");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_initialize_sse_error_is_terminal_execution_failure() {
        let server = MockServer::start().await;
        let error_response = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "error": { "code": -32000, "message": "initialization rejected" }
        });
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw(format!("data: {error_response}\n\n"), "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("matching initialize Error should terminate its SSE stream")
        .expect_err("JSON-RPC initialize Error must fail initialization");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::Execution
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_empty_tool_sse_is_invalid_without_deadline() {
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw("", "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("empty tool SSE should terminate without a query deadline")
        .expect_err("empty tool SSE must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_direct_json_wrong_response_id_is_invalid_without_deadline() {
        let server = mock_success_server(json!({
            "jsonrpc": "2.0",
            "id": 999,
            "result": { "structuredContent": { "issues": [] } }
        }))
        .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("wrong direct response id should not wait for a query deadline")
        .expect_err("wrong direct response id must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
    }

    #[tokio::test]
    async fn controlled_sse_wrong_response_id_is_invalid_without_deadline() {
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
        let wrong = json!({
            "jsonrpc": "2.0",
            "id": 999,
            "result": { "structuredContent": { "issues": [] } }
        });
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw(format!("data: {wrong}\n\n"), "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("wrong SSE response id should not wait for a query deadline")
        .expect_err("wrong SSE response id must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_initialized_notification_accepts_json_acknowledgement() {
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
            .respond_with(json_rpc_result_response(Value::from(999), json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(json_rpc_result_response(
                Value::from(1),
                json!({ "structuredContent": { "issues": [] } }),
            ))
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let payload = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect("JSON acknowledgement for initialized notification should remain compatible");
        assert_eq!(payload, json!({ "issues": [] }));
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_initialized_notification_rejects_sse_without_panicking() {
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
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw("data: malformed-ack\n\n", "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("malformed initialized acknowledgement should terminate")
        .expect_err("initialized notification must reject an SSE acknowledgement");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_oversized_json_is_invalid_without_decoding_private_body() {
        let private = "provider-private"
            .repeat(CONTROLLED_RESPONSE_BODY_LIMIT_BYTES / "provider-private".len() + 2);
        let server = mock_success_server(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "structuredContent": { "issues": [{ "title": private }] } }
        }))
        .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("oversized controlled JSON must fail before decoding");
        let mcp_error = typed_mcp_error(&error);
        assert_eq!(
            mcp_error.execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert!(
            !mcp_error
                .to_structured()
                .detail()
                .contains("provider-private")
        );
        assert_eq!(caller.active_http_operations(), 0);
    }

    #[tokio::test]
    async fn controlled_oversized_sse_is_invalid_without_waiting_for_deadline() {
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
        let body = format!(
            "data: {}",
            "provider-private"
                .repeat(CONTROLLED_RESPONSE_BODY_LIMIT_BYTES / "provider-private".len() + 2)
        );
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "text/event-stream")
                    .set_body_raw(body, "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("oversized SSE should terminate without a query deadline")
        .expect_err("oversized controlled SSE must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_accepted_tool_uses_common_stream_authentication_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "auth-failure-session"),
            )
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(401).set_delay(std::time::Duration::from_millis(50)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("common stream authentication failure should terminate the call")
        .expect_err("common stream authentication failure must fail the tool call");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::Authentication
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_accepted_tool_rejects_unavailable_common_stream() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response().append_header("mcp-session-id", "no-sse-session"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(405))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(202).set_delay(std::time::Duration::from_millis(50)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("a rejected common stream should terminate without a query deadline")
        .expect_err("202 cannot complete after the server rejected common SSE");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_accepted_tool_without_session_is_invalid() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response())
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("a stateless 202 should terminate without a query deadline")
        .expect_err("202 cannot complete a request without a common stream session");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_common_stream_not_found_is_upstream_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "expired-get-session"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(202).set_delay(std::time::Duration::from_millis(50)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("a missing common stream should terminate without a query deadline")
        .expect_err("404 common stream must fail the tool call");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::UpstreamUnavailable
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_common_stream_rejects_json_content_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response().append_header("mcp-session-id", "json-get-session"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Type", "application/json")
                    .set_body_raw("provider-private-open-body", "application/json"),
            )
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(202).set_delay(std::time::Duration::from_millis(50)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("invalid common-stream content type should terminate")
        .expect_err("standalone GET must not accept a JSON response body");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn controlled_http_statuses_have_typed_failure_kinds_and_legacy_shape() {
        for (status, expected_kind) in [
            (401, QueryExecutionFailureKind::Authentication),
            (403, QueryExecutionFailureKind::PermissionDenied),
            (429, QueryExecutionFailureKind::RateLimited),
            (503, QueryExecutionFailureKind::UpstreamUnavailable),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(body_partial_json(json!({ "method": "initialize" })))
                .respond_with(
                    ResponseTemplate::new(status)
                        .set_body_string(format!("provider-private-status-{status}")),
                )
                .expect(1)
                .mount(&server)
                .await;
            let caller = make_caller(
                streamable_http_manifest(&server.uri()),
                McpBodyCapture::default(),
            );
            let controls = QueryExecutionControls::new(
                None,
                QueryCancellationToken::new(),
                QueryPaginationPolicy::FirstPageOnly,
                QueryRetryPolicy::Disabled,
            );

            let error = caller
                .call_tool("issues", "list_issues", JsonObject::new(), &controls)
                .await
                .expect_err("controlled status should fail initialization");
            let mcp_error = typed_mcp_error(&error);
            assert_eq!(mcp_error.execution_failure_kind(), expected_kind);
            let structured = mcp_error.to_structured();
            assert_eq!(structured.reason(), "MCP_HTTP_STATUS_FAILED");
            assert!(
                !structured
                    .detail()
                    .contains(&format!("provider-private-status-{status}"))
            );
            assert_eq!(caller.active_http_operations(), 0);
            server.verify().await;
        }
    }

    #[tokio::test]
    async fn controlled_malformed_authenticate_header_keeps_known_status_kind() {
        for (status, expected_kind) in [
            (401, QueryExecutionFailureKind::Authentication),
            (403, QueryExecutionFailureKind::PermissionDenied),
        ] {
            let server = MockServer::start().await;
            let malformed = WiremockHeaderValue::from_bytes(&[0xFF])
                .expect("non-UTF8 header value should be wire-valid");
            Mock::given(method("POST"))
                .and(body_partial_json(json!({ "method": "initialize" })))
                .respond_with(
                    ResponseTemplate::new(status).append_header("WWW-Authenticate", malformed),
                )
                .expect(1)
                .mount(&server)
                .await;
            let caller = make_caller(
                streamable_http_manifest(&server.uri()),
                McpBodyCapture::default(),
            );
            let controls = QueryExecutionControls::new(
                None,
                QueryCancellationToken::new(),
                QueryPaginationPolicy::FirstPageOnly,
                QueryRetryPolicy::Disabled,
            );

            let error = caller
                .call_tool("issues", "list_issues", JsonObject::new(), &controls)
                .await
                .expect_err("malformed authenticate header should fail initialization");
            assert_eq!(
                typed_mcp_error(&error).execution_failure_kind(),
                expected_kind
            );
            assert_eq!(caller.active_http_operations(), 0);
            server.verify().await;
        }
    }

    #[tokio::test]
    async fn controlled_status_headers_do_not_wait_for_stalled_mcp_body() {
        let (url, task) = spawn_stalled_status_mcp_server(429).await;
        let caller = make_caller(streamable_http_manifest(&url), McpBodyCapture::default());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("known MCP status should not wait for its stalled body")
        .expect_err("HTTP 429 must fail the controlled tool call");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::RateLimited
        );
        assert!(tokio::time::Instant::now() < deadline);
        assert_eq!(caller.active_http_operations(), 0);
        task.abort();
    }

    #[tokio::test]
    async fn controlled_tool_json_rpc_status_uses_the_causal_post_classification() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(
                initialize_response().append_header("mcp-session-id", "causal-status-session"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(
                ResponseTemplate::new(429)
                    .append_header("Content-Type", "application/json")
                    .set_delay(std::time::Duration::from_millis(50))
                    .set_body_json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "error": { "code": -32000, "message": "provider-private-rate-limit" }
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("JSON-RPC error at HTTP 429 should fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::RateLimited
        );
        assert_eq!(caller.active_http_operations(), 0);
        server.verify().await;
    }

    #[tokio::test]
    async fn disabled_retry_policy_does_not_reinitialize_expired_session() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response().append_header("mcp-session-id", "expired-session"))
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
            .and(body_partial_json(json!({ "method": "tools/call" })))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let caller = make_caller(
            streamable_http_manifest(&server.uri()),
            McpBodyCapture::default(),
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::SourceDefault,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("expired session should not be replayed");
        let DataFusionError::External(inner) = error.find_root() else {
            panic!("expected typed MCP error, got {error}");
        };
        let mcp_error = inner
            .downcast_ref::<McpProviderQueryError>()
            .expect("MCP provider error");
        assert_eq!(
            mcp_error.execution_failure_kind(),
            QueryExecutionFailureKind::UpstreamUnavailable
        );
        server.verify().await;
    }

    #[tokio::test]
    async fn streamable_http_list_tools_bounded_follows_cursor_pages() {
        let server = MockServer::start().await;
        let list_calls = Arc::new(AtomicUsize::new(0));
        let responder_calls = Arc::clone(&list_calls);
        mount_streamable_http_tools_list_responder(&server, move |body| {
            responder_calls.fetch_add(1, Ordering::SeqCst);
            match body.pointer("/params/cursor").and_then(Value::as_str) {
                None => json_rpc_result_response(
                    json_rpc_request_id(body),
                    json!({
                        "tools": [tool_result("first_page_tool")],
                        "nextCursor": "second-page"
                    }),
                ),
                Some("second-page") => json_rpc_result_response(
                    json_rpc_request_id(body),
                    json!({
                        "tools": [tool_result("second_page_tool")]
                    }),
                ),
                other => ResponseTemplate::new(400)
                    .set_body_string(format!("unexpected cursor {other:?}")),
            }
        })
        .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        let tools = caller
            .list_tools()
            .await
            .expect("tools/list should succeed");

        let tool_names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(tool_names, ["first_page_tool", "second_page_tool"]);
        assert_eq!(list_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn streamable_http_list_tools_bounded_rejects_repeated_cursor() {
        let server = MockServer::start().await;
        let list_calls = Arc::new(AtomicUsize::new(0));
        let responder_calls = Arc::clone(&list_calls);
        mount_streamable_http_tools_list_responder(&server, move |body| {
            responder_calls.fetch_add(1, Ordering::SeqCst);
            json_rpc_result_response(
                json_rpc_request_id(body),
                json!({
                    "tools": [tool_result("looping_tool")],
                    "nextCursor": "same-cursor"
                }),
            )
        })
        .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        let error = caller
            .list_tools()
            .await
            .expect_err("repeated cursor should fail");

        assert!(
            error
                .to_string()
                .contains("MCP tools/list returned repeated next cursor 'same-cursor'"),
            "unexpected repeated-cursor error: {error}"
        );
        assert_eq!(list_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn streamable_http_list_tools_bounded_rejects_max_page_overrun() {
        let server = MockServer::start().await;
        let list_calls = Arc::new(AtomicUsize::new(0));
        let responder_calls = Arc::clone(&list_calls);
        mount_streamable_http_tools_list_responder(&server, move |body| {
            let page = responder_calls.fetch_add(1, Ordering::SeqCst) + 1;
            json_rpc_result_response(
                json_rpc_request_id(body),
                json!({
                    "tools": [tool_result(&format!("tool_{page}"))],
                    "nextCursor": format!("page-{page}")
                }),
            )
        })
        .await;
        let manifest = streamable_http_manifest(&server.uri());
        let caller = make_caller(manifest, McpBodyCapture::default());

        let error = caller
            .list_tools()
            .await
            .expect_err("max page overrun should fail");

        assert!(
            error.to_string().contains(&format!(
                "MCP tools/list exceeded max_pages={MAX_MCP_DISCOVERY_PAGES}"
            )),
            "unexpected max-page error: {error}"
        );
        assert_eq!(list_calls.load(Ordering::SeqCst), MAX_MCP_DISCOVERY_PAGES);
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
        let source_inputs = Arc::new(McpSourceInputs::static_inputs(resolved_inputs));
        StreamableHttpMcpToolCaller::new(
            manifest.common.name,
            manifest.server,
            source_inputs,
            body_capture,
        )
    }

    #[cfg(unix)]
    fn make_stdio_caller(command: &str, args: &[String]) -> StdioMcpToolCaller {
        let manifest = coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "stdio_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "server": {
                "transport": "stdio",
                "command": command,
                "args": args
            },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "response": { "rows_path": ["issues"] },
                "columns": [{ "name": "title", "type": "Utf8" }]
            }]
        }))
        .expect("stdio manifest should parse")
        .as_mcp()
        .expect("MCP manifest")
        .clone();
        let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
            &manifest.declared_inputs,
            &BTreeMap::new(),
            &BTreeMap::new(),
        ));
        StdioMcpToolCaller {
            source_name: manifest.common.name,
            server: manifest.server,
            source_inputs: Arc::new(McpSourceInputs::static_inputs(resolved_inputs)),
            body_capture: McpBodyCapture::default(),
        }
    }

    #[cfg(unix)]
    fn assert_child_pid_gone(pid_file: &std::path::Path) {
        let pid = std::fs::read_to_string(pid_file)
            .expect("read child PID")
            .parse::<u32>()
            .expect("parse child PID");
        let probe = std::process::Command::new("/bin/kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("probe child PID");
        assert!(
            !probe.success(),
            "controlled MCP child PID {pid} remained alive or zombie"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stalled_stdio_initialize_kills_and_reaps_child_before_cutoff() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(125);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("stalled stdio initialize should time out");
        let mcp_error = typed_mcp_error(&error);
        assert_eq!(
            mcp_error.execution_failure_kind(),
            QueryExecutionFailureKind::Timeout
        );
        assert!(controls.upstream_started());
        assert!(
            tokio::time::Instant::now() <= deadline + std::time::Duration::from_millis(100),
            "stdio initialization teardown exceeded its cleanup cutoff"
        );

        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stalled_stdio_tool_call_kills_and_reaps_child_before_cutoff() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}'
IFS= read -r _initialized
IFS= read -r _tool_call
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(125);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("stalled stdio tool call should time out");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::Timeout
        );
        assert!(controls.upstream_started());
        assert!(
            tokio::time::Instant::now() <= deadline + std::time::Duration::from_millis(100),
            "stdio tool teardown exceeded its cleanup cutoff"
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stdio_blank_flood_observes_deadline_and_reaps_child() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}'
IFS= read -r _initialized
IFS= read -r _tool_call
while :; do
  printf '\n'
  /bin/sleep 0.01
done"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(125);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("blank-line flood must not monopolize controlled receive");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::Timeout
        );
        assert!(
            tokio::time::Instant::now() <= deadline + std::time::Duration::from_millis(100),
            "stdio flood teardown exceeded its cleanup cutoff"
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_malformed_stdio_initialize_reaps_child_before_returning() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":999,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}'
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect_err("conflicting initialize response id should fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stdio_initialize_rejects_pre_handshake_ping_and_reaps_child() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"ping"}'
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("pre-handshake stdio Ping should terminate initialization")
        .expect_err("rmcp cannot service a stdio Ping before initialization");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stdio_wrong_tool_response_id_is_invalid_and_reaped() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}'
IFS= read -r _initialized
IFS= read -r _tool_call
printf '%s\n' '{"jsonrpc":"2.0","id":999,"result":{"structuredContent":{"issues":[]}}}'
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("wrong stdio response id should not wait for a query deadline")
        .expect_err("wrong stdio response id must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stdio_sequential_pings_then_tool_result_succeeds() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}'
IFS= read -r _initialized
IFS= read -r _tool_call
printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"ping"}'
IFS= read -r _first_ping_response
printf '%s\n' '{"jsonrpc":"2.0","id":8,"method":"ping"}'
IFS= read -r _second_ping_response
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"structuredContent":{"issues":[{"title":"ping survived"}]}}}'
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let payload = caller
            .call_tool("issues", "list_issues", JsonObject::new(), &controls)
            .await
            .expect("sequential protocol-valid server pings should remain compatible");
        assert_eq!(
            payload
                .get("issues")
                .and_then(Value::as_array)
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("title"))
                .and_then(Value::as_str),
            Some("ping survived")
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_oversized_unterminated_stdio_line_is_invalid_and_reaped() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
chunk=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
i=0
while [ "$i" -lt 260 ]; do
  printf '%s' "$chunk"
  i=$((i + 1))
done
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("oversized unterminated line should not wait for a newline or deadline")
        .expect_err("oversized controlled stdio response must fail");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_child_pid_gone(pid_file.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_stdio_server_ping_flood_is_bounded_and_reaped() {
        let pid_file = tempfile::NamedTempFile::new().expect("PID file");
        let pid_path = pid_file.path().to_string_lossy().into_owned();
        let script = r#"printf '%s' "$$" > "$1"
IFS= read -r _initialize
i=0
while [ "$i" -lt 100 ]; do
  printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"ping"}'
  i=$((i + 1))
done
exec /bin/sleep 30"#;
        let caller = make_stdio_caller(
            "/bin/sh",
            &[
                "-c".to_string(),
                script.to_string(),
                "coral-controlled-mcp-test".to_string(),
                pid_path,
            ],
        );
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.call_tool("issues", "list_issues", JsonObject::new(), &controls),
        )
        .await
        .expect("server ping flood should not enter rmcp's handler drain")
        .expect_err("more than one server ping per request must be bounded");
        assert_eq!(
            typed_mcp_error(&error).execution_failure_kind(),
            QueryExecutionFailureKind::InvalidResponse
        );
        assert_child_pid_gone(pid_file.path());
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
            .call_tool(
                "issues",
                "list_issues",
                JsonObject::new(),
                &QueryExecutionControls::default(),
            )
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
            .call_tool(
                "issues",
                "list_issues",
                arguments,
                &QueryExecutionControls::default(),
            )
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
            .call_tool(
                "issues",
                "list_issues",
                JsonObject::new(),
                &QueryExecutionControls::default(),
            )
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
            .call_tool(
                "issues",
                "list_issues",
                JsonObject::new(),
                &QueryExecutionControls::default(),
            )
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
            .call_tool(
                "issues",
                "list_issues",
                JsonObject::new(),
                &QueryExecutionControls::default(),
            )
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

    #[derive(Debug)]
    struct RotatingResolver {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::SourceInputResolver for RotatingResolver {
        async fn resolve_inputs(
            &self,
            _source: &crate::SourceInputResolutionContext,
        ) -> std::result::Result<BTreeMap<String, String>, crate::SourceInputResolverError>
        {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Ok(BTreeMap::from([(
                "MCP_ACCESS_TOKEN".to_string(),
                format!("fresh-token-{call}"),
            )]))
        }
    }

    async fn mount_token_rotation_mocks(server: &MockServer) {
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "initialize" })))
            .respond_with(initialize_response())
            .expect(2)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({ "method": "notifications/initialized" }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .expect(2)
            .mount(server)
            .await;
        for token in ["fresh-token-1", "fresh-token-2"] {
            Mock::given(method("POST"))
                .and(header("authorization", format!("Bearer {token}")))
                .and(body_partial_json(json!({ "method": "tools/call" })))
                .respond_with(
                    ResponseTemplate::new(200)
                        .append_header("Content-Type", "application/json")
                        .set_body_json(json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": { "structuredContent": { "issues": [] } }
                        })),
                )
                .expect(1)
                .mount(server)
                .await;
        }
    }

    /// Each `tools/call` re-resolves the bearer token through the source
    /// input resolver, so a fresh OAuth access token is picked up between
    /// calls without recompiling the source.
    #[tokio::test]
    async fn streamable_http_caller_re_resolves_bearer_token_for_each_tool_call() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let server = MockServer::start().await;
        mount_token_rotation_mocks(&server).await;

        let validated = coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "remote_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "inputs": { "MCP_ACCESS_TOKEN": { "kind": "secret" } },
            "server": {
                "transport": "streamable_http",
                "url": server.uri(),
                "auth": { "type": "bearer", "from": "input", "key": "MCP_ACCESS_TOKEN" }
            },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "columns": [{ "name": "title", "type": "Utf8" }]
            }]
        }))
        .expect("manifest should parse");
        let manifest = validated.as_mcp().expect("mcp manifest").clone();
        let variables = BTreeMap::new();
        let secrets = BTreeMap::from([("MCP_ACCESS_TOKEN".to_string(), "stale-token".to_string())]);
        let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
            &manifest.declared_inputs,
            &secrets,
            &variables,
        ));
        let resolver_calls = Arc::new(AtomicUsize::new(0));
        let source = crate::QuerySource::new(validated, variables, secrets);
        let source_input_resolution =
            crate::SourceInputResolutionContext::from_query_source(&source);
        let source_inputs = Arc::new(McpSourceInputs::with_resolver(
            resolved_inputs,
            source_input_resolution,
            Arc::new(RotatingResolver {
                calls: Arc::clone(&resolver_calls),
            }),
        ));
        let caller = StreamableHttpMcpToolCaller::new(
            manifest.common.name,
            manifest.server,
            source_inputs,
            McpBodyCapture::default(),
        );

        caller
            .call_tool(
                "issues",
                "list_issues",
                JsonObject::new(),
                &QueryExecutionControls::default(),
            )
            .await
            .expect("first call_tool should succeed");
        caller
            .call_tool(
                "issues",
                "list_issues",
                JsonObject::new(),
                &QueryExecutionControls::default(),
            )
            .await
            .expect("second call_tool should succeed");

        assert_eq!(resolver_calls.load(Ordering::SeqCst), 2);
        // wiremock verifies on drop: each `.expect(1)` mock above must have
        // matched exactly once, which fails the test if either call sent
        // the wrong Authorization header.
    }
}
