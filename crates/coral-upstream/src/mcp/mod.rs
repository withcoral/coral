//! One-use Streamable HTTP MCP invocation and bounded setup discovery, without
//! retries or Tasks.

mod authorization;
mod discovery;
mod resources;
mod transport;

pub use authorization::{
    AuthorizationDiscoveryError, AuthorizationRequirement, RegisteredClient,
    discover_authorization, register_client,
};
pub use discovery::{BearerChallenge, discover_mcp};
pub use resources::McpResources;
pub use transport::McpRejection;

use coral_catalog::AuthenticationConfiguration;
use coral_telemetry::safe;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use tracing::Instrument as _;

use coral_catalog::{
    ExecutionBinding, MaterializedTool, McpExecutionBinding, McpInvocationMethod, ToolAvailability,
};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ClientConfig, ReadResourceRequestParams,
    ReadResourceResponse,
};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::common::client_side_sse::NeverRetry;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt as _};
use serde_json::{Value, json};

use crate::Error;
use crate::span_field::{self, Declared};

/// A validated, one-use request against an exact retained MCP binding.
pub struct ReadyMcpDispatch {
    credential: Option<coral_credentials::Secret>,
    binding: McpExecutionBinding,
    arguments: serde_json::Map<String, Value>,
    limits: coral_value::ValueLimits,
    provider: String,
    resources: Arc<McpResources>,
    resource_uri: Option<String>,
}

impl ReadyMcpDispatch {
    /// Plans a direct tool call without contacting the upstream server.
    ///
    /// # Errors
    /// Returns an error for unavailable Tools, unsupported bindings, invalid
    /// logical arguments, or a non-positive result limit.
    pub fn plan(
        tool: &MaterializedTool,
        arguments: &Value,
        provider: &str,
        resources: Arc<McpResources>,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        if tool.availability() != ToolAvailability::Available {
            return Err(Error::ToolUnavailable);
        }
        limits.validate(arguments).map_err(Error::invalid_value)?;
        let ExecutionBinding::Mcp(binding) = tool.execution_binding() else {
            return Err(Error::UnsupportedProtocol);
        };
        let arguments = arguments
            .as_object()
            .ok_or_else(|| Error::InvalidArguments("Tool arguments must be an object".to_string()))?
            .clone();
        let resource_uri = if binding.invocation_method() == McpInvocationMethod::ReadResource {
            if arguments.len() != 1 {
                return Err(Error::InvalidArguments(
                    "resources.read requires only a reference".to_string(),
                ));
            }
            Some(resources.resolve(
                provider,
                arguments.get("reference").ok_or_else(|| {
                    Error::InvalidArguments("resources.read requires a reference".to_string())
                })?,
            )?)
        } else {
            None
        };
        validate_request_size(
            binding,
            &arguments,
            resource_uri.as_deref(),
            limits.upstream_bytes(),
        )?;
        Ok(Self {
            credential: None,
            binding: binding.clone(),
            arguments,
            limits,
            provider: provider.to_string(),
            resources,
            resource_uri,
        })
    }

    /// Supplies the host-selected bearer credential without changing logical arguments.
    ///
    /// # Errors
    /// Rejects a credential-free contract, an HTTP-header contract, or a second
    /// credential for the same dispatch.
    pub fn authorize(&mut self, values: &coral_credentials::CredentialValues) -> Result<(), Error> {
        let secret = values
            .get(coral_credentials::CredentialValues::TOKEN)
            .and_then(|token| coral_credentials::Secret::new(token.expose().to_owned()).ok())
            .ok_or(Error::CredentialRequired)?;
        if self.credential.is_some()
            || !matches!(
                self.binding.credential_contract().authentication(),
                AuthenticationConfiguration::Bearer
                    | AuthenticationConfiguration::OAuthAuthorizationCode { .. }
                    | AuthenticationConfiguration::OAuthDeviceCode { .. }
            )
        {
            return Err(Error::CredentialRequired);
        }
        self.credential = Some(secret);
        Ok(())
    }

    fn credential_header(&self) -> Result<Option<String>, Error> {
        let required =
            self.binding.credential_contract() != &coral_catalog::ProviderCredentialContract::NONE;
        if required != self.credential.is_some() {
            return Err(Error::CredentialRequired);
        }
        Ok(self
            .credential
            .as_ref()
            .map(|secret| secret.expose().to_owned()))
    }
}

fn validate_request_size(
    binding: &McpExecutionBinding,
    arguments: &serde_json::Map<String, Value>,
    resource_uri: Option<&str>,
    maximum: usize,
) -> Result<(), Error> {
    let (method, params) = if let Some(uri) = resource_uri {
        (
            "resources/read",
            serde_json::to_value(ReadResourceRequestParams::new(uri.to_string())),
        )
    } else {
        (
            "tools/call",
            serde_json::to_value(
                CallToolRequestParams::new(
                    binding.tool_name().ok_or(Error::McpResult)?.to_string(),
                )
                .with_arguments(arguments.clone()),
            ),
        )
    };
    let mut params = params.map_err(Error::EncodeJson)?;
    // Each dispatch initializes a fresh SDK session (request 0), then sends one
    // request (1) with its first progress token (0). The wire-boundary test pins
    // these SDK facts so dependency changes cannot silently undercount requests.
    params
        .as_object_mut()
        .ok_or(Error::McpResult)?
        .insert("_meta".to_string(), json!({"progressToken":0}));
    let request = json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params});
    if serde_json::to_vec(&request)
        .map_err(Error::EncodeJson)?
        .len()
        > maximum
    {
        return Err(Error::InputTooLarge);
    }
    Ok(())
}

/// One complete MCP result, already projected without transport envelopes.
#[derive(Debug, Clone, PartialEq)]
pub struct McpOutcome {
    result: Value,
    method: McpInvocationMethod,
}

impl McpOutcome {
    fn from_data(
        mut data: Value,
        method: McpInvocationMethod,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        let object = data.as_object_mut().ok_or(Error::McpResult)?;
        let mut metadata = serde_json::Map::from_iter([("kind".to_string(), json!("mcp"))]);
        if let Some(meta) = object.remove("_meta") {
            metadata.insert("_meta".to_string(), meta);
        }
        let result = json!({"data": data, "metadata": metadata});
        let encoded = serde_json::to_string(&result).map_err(Error::EncodeJson)?;
        if encoded.len() > limits.upstream_bytes() {
            return Err(Error::ResultTooLarge);
        }
        limits
            .validate_json(&encoded)
            .map_err(Error::invalid_result)?;
        Ok(Self { result, method })
    }

    /// Returns distinct provider-native data and safe MCP metadata.
    #[must_use]
    pub const fn tool_result(&self) -> &Value {
        &self.result
    }

    /// Returns [`OutcomeClass::ErrorResponse`] for a tool result with
    /// `isError: true`, and a success otherwise. A resource read has no
    /// `isError`; a field of that name there is a provider extension.
    #[must_use]
    pub fn class(&self) -> crate::OutcomeClass {
        if self.method == McpInvocationMethod::CallTool
            && self.result.pointer("/data/isError") == Some(&Value::Bool(true))
        {
            crate::OutcomeClass::ErrorResponse
        } else {
            crate::OutcomeClass::Success
        }
    }
}

/// Whether the peer currently lists a tool by this name.
async fn lists_tool(
    client: &RunningService<RoleClient, ClientConfig>,
    captured: &Mutex<Option<transport::Captured>>,
    name: &str,
) -> Result<bool, Error> {
    let mut listed = false;
    discovery::visit_tool_pages(client, captured, |tools| {
        listed = tools
            .iter()
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some(name));
        if listed {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .await?;
    Ok(listed)
}

fn request_failure(captured: &Mutex<Option<transport::Captured>>) -> Error {
    match captured
        .lock()
        .ok()
        .as_deref()
        .and_then(|slot| slot.as_ref())
    {
        Some(transport::Captured::DestinationRejected) => Error::DestinationRejected,
        Some(transport::Captured::InputTooLarge) => Error::InputTooLarge,
        Some(transport::Captured::ResultTooLarge) => Error::ResultTooLarge,
        Some(transport::Captured::AuthenticationUnsupported) => Error::McpAuthenticationUnsupported,
        Some(transport::Captured::Unauthorized(challenge)) => {
            Error::McpUnauthorized(challenge.clone())
        }
        Some(transport::Captured::Rejected(rejection)) => Error::McpRejected(*rejection),
        _ => Error::McpRequest,
    }
}

/// Host-authorized MCP sender. Each dispatch owns a fresh, bounded session.
#[derive(Clone)]
pub struct McpUpstream {
    client: coral_outbound::Client,
}

impl McpUpstream {
    /// Builds a sender with the deployment-selected outbound policy.
    /// # Errors
    /// Returns an HTTP client initialization error.
    pub fn new(policy: coral_outbound::DestinationPolicy) -> Result<Self, Error> {
        Ok(Self {
            client: crate::http_client(policy)?,
        })
    }

    /// Consumes one request; an explicit tool `isError: true` still resolves.
    ///
    /// # Errors
    /// Returns an error when no complete bounded MCP result is available.
    pub async fn send(&self, dispatch: ReadyMcpDispatch) -> Result<McpOutcome, Error> {
        let span = client_span(&dispatch);
        let measure = Arc::new(crate::exchange::ExchangeMeasure::start(
            coral_telemetry::metric::ProviderProtocol::Mcp,
        ));
        let sent = self
            .send_session(dispatch, span.clone(), measure.clone())
            .instrument(span.clone())
            .await;
        measure.end(sent.as_ref().map(|_outcome| ()));
        match &sent {
            Ok(outcome) if outcome.class() == crate::OutcomeClass::ErrorResponse => {
                coral_telemetry::record_failure(&span, "tool_error");
            }
            Ok(_) => {}
            // The JSON-RPC error code, as the MCP conventions name it.
            Err(Error::McpRejected(rejection)) => coral_telemetry::record_failure(
                &span,
                safe::json_rpc_code(match rejection {
                    McpRejection::InvalidParams | McpRejection::UnknownTool => -32602,
                    McpRejection::Other(code) => *code,
                }),
            ),
            Err(_) => coral_telemetry::record_failure(&span, "incomplete"),
        }
        sent
    }

    /// One session that sends `dispatch`, each of its requests carrying the
    /// trace context of `span`, its client span.
    async fn send_session(
        &self,
        dispatch: ReadyMcpDispatch,
        span: tracing::Span,
        measure: Arc<crate::exchange::ExchangeMeasure>,
    ) -> Result<McpOutcome, Error> {
        let mut config = StreamableHttpClientTransportConfig::with_uri(
            dispatch.binding.server_url().to_string(),
        )
        .reinit_on_expired_session(false)
        .max_sse_event_size(dispatch.limits.upstream_bytes());
        config.retry_config = Arc::new(NeverRetry::default());
        config.auth_header = dispatch.credential_header()?;
        let captured = Arc::new(Mutex::new(None));
        let request_error = || request_failure(&captured);
        let transport = StreamableHttpClientTransport::with_client(
            transport::McpHttpClient {
                request_headers: dispatch.binding.request_headers().clone(),
                client: self.client.clone(),
                limit: dispatch.limits.upstream_bytes(),
                result: captured.clone(),
                span,
                measure: Some(measure),
            },
            config,
        );
        let mut info = ClientConfig::default();
        info.protocol_version = serde_json::from_value(json!(dispatch.binding.protocol_version()))
            .map_err(|_invalid_version| Error::McpProtocolChanged)?;
        let client = info
            .serve(transport)
            .await
            .map_err(|_initialization| request_error())?;
        if !client.peer_info().is_some_and(|info| {
            info.protocol_version.as_str() == dispatch.binding.protocol_version()
        }) {
            return Err(Error::McpProtocolChanged);
        }
        // The SDK's multi-round convenience API can repeat a tool call. Never use it here.
        let required_content = match dispatch.binding.invocation_method() {
            McpInvocationMethod::CallTool => {
                let tool_name = dispatch.binding.tool_name().ok_or(Error::McpResult)?;
                let call = client
                    .call_tool_once(
                        CallToolRequestParams::new(tool_name.to_string())
                            .with_arguments(dispatch.arguments),
                    )
                    .await;
                let response = match call {
                    Ok(response) => response,
                    Err(_request) => {
                        let error = request_error();
                        // MCP answers an unknown tool with the same code as invalid
                        // arguments. A listing that cannot be read keeps the peer's answer.
                        if matches!(error, Error::McpRejected(McpRejection::InvalidParams))
                            && matches!(lists_tool(&client, &captured, tool_name).await, Ok(false))
                        {
                            return Err(Error::McpRejected(McpRejection::UnknownTool));
                        }
                        return Err(error);
                    }
                };
                if !matches!(response, CallToolResponse::Complete(_)) {
                    return Err(Error::McpResult);
                }
                "content"
            }
            McpInvocationMethod::ReadResource => {
                let response = client
                    .read_resource_once(ReadResourceRequestParams::new(
                        dispatch.resource_uri.ok_or(Error::McpResult)?,
                    ))
                    .await
                    .map_err(|_request| request_error())?;
                if !matches!(response, ReadResourceResponse::Complete(_)) {
                    return Err(Error::McpResult);
                }
                "contents"
            }
        };
        let mut data = captured
            .lock()
            .map_err(|_poisoned| Error::McpResult)?
            .take()
            .ok_or(Error::McpResult)?
            .into_result()?;
        if !data.get(required_content).is_some_and(Value::is_array) {
            return Err(Error::McpResult);
        }
        if dispatch.binding.invocation_method() == McpInvocationMethod::ReadResource
            && let Some(contents) = data.get("contents").and_then(Value::as_array)
        {
            for resource in contents {
                dispatch
                    .limits
                    .validate_mcp_resource(resource)
                    .map_err(Error::invalid_result)?;
            }
        }
        dispatch.resources.project(
            &dispatch.provider,
            dispatch.binding.invocation_method(),
            &mut data,
        )?;
        McpOutcome::from_data(data, dispatch.binding.invocation_method(), dispatch.limits)
    }
}

/// The OpenTelemetry MCP client span of one dispatch: its method, the
/// upstream tool name, and the server's scheme, address and port. Never the
/// arguments, the result, a session identifier or the full URL.
fn client_span(dispatch: &ReadyMcpDispatch) -> tracing::Span {
    let (method, target) = match dispatch.binding.invocation_method() {
        McpInvocationMethod::CallTool => ("tools/call", Declared::tool_name(&dispatch.binding)),
        McpInvocationMethod::ReadResource => ("resources/read", None),
    };
    let server = url::Url::parse(dispatch.binding.server_url()).ok();
    tracing::info_span!(
        "mcp.client",
        otel.name = safe::span_name(method, target.as_ref()),
        otel.kind = "client",
        mcp.method.name = safe::kind(method),
        gen_ai.tool.name = target.as_ref().map(safe::id),
        url.scheme = server.as_ref().map(span_field::scheme).map(safe::kind),
        server.address = server.as_ref().and_then(span_field::host).as_ref().map(safe::id),
        server.port = server.as_ref().and_then(url::Url::port_or_known_default).map(safe::number),
        error.type = tracing::field::Empty,
    )
}
