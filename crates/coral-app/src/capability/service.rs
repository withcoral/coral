//! Implements the gRPC `CapabilityService`.

#![expect(
    clippy::result_large_err,
    reason = "internal invocation helpers return the protobuf response as structured user-facing error control flow"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use coral_api::v1::capability_service_server::CapabilityService as CapabilityServiceApi;
use coral_api::v1::{InvokeCapabilityError, InvokeCapabilityRequest, InvokeCapabilityResponse};
use coral_capabilities::{
    Capability, CapabilityId, FileArtifactRef, FileFormatDescriptor, FileScanBinding,
    GraphqlOperationBinding, McpToolUpstreamBinding, RestParameterBinding, RestParameterLocation,
    RestRequestBody, RestUpstreamBinding, ResultShapeHint, UpstreamBinding,
    code_mode_tool_input_schema, executable_schema_unresolved_refs,
};
use coral_exports::{Binding, CapabilityExport};
use coral_spec::{
    AuthDescriptor, GraphqlInterface, ManifestInputKind, ManifestInputSpec, McpEnvBinding,
    McpInterface, McpTransportDescriptor, OpenApiInterface, ParsedTemplate, SourceInterface,
    SourceSpec, TemplateNamespace, TemplatePart, openapi_document_metadata,
    parse_source_manifest_yaml,
};
use coral_upstream::{
    GraphqlRequestPlan, HttpRequestPlan, McpConnectionTarget, McpToolCallPlan, RedactableString,
    UpstreamError, UpstreamInvocationPlan, UpstreamRequestBody, UpstreamResponseBody,
    UpstreamResponseEnvelope, bounded_provider_diagnostic_value, execute_plan,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tonic::{Request, Response, Status};
use url::Url;

use crate::RuntimeExposureMode;
use crate::bootstrap::{app_status, stdio_path_env};
use crate::credentials::{CredentialManager, CredentialSetId, CredentialStorageKind};
use crate::discovery::manager::{DiscoveryManager, LoadedWorkspaceExports};
use crate::graphql_documents::operation_document_path;
use crate::sources::SourceName;
use crate::transport::{
    grpc_span, instrument_grpc, json_value_to_proto, proto_json_value_to_json,
    workspace_name_from_proto,
};
use crate::workspaces::WorkspaceName;

const MAX_FILE_READ_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_FILE_READ_LIMIT: usize = 1_000;
const MAX_FILE_READ_LIMIT: usize = 10_000;
const DEFAULT_PROVIDER_INVOCATION_TIMEOUT: Duration = Duration::from_mins(1);

#[derive(Clone)]
pub(crate) struct CapabilityService {
    invoker: CapabilityInvoker,
}

impl CapabilityService {
    pub(crate) fn new(
        discovery: DiscoveryManager,
        credentials: CredentialManager,
        runtime_exposure: RuntimeExposureMode,
    ) -> Self {
        Self {
            invoker: CapabilityInvoker::new(discovery, credentials, runtime_exposure),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CapabilityInvoker {
    discovery: DiscoveryManager,
    credentials: CredentialManager,
    runtime_exposure: RuntimeExposureMode,
}

impl CapabilityInvoker {
    pub(crate) fn new(
        discovery: DiscoveryManager,
        credentials: CredentialManager,
        runtime_exposure: RuntimeExposureMode,
    ) -> Self {
        Self {
            discovery,
            credentials,
            runtime_exposure,
        }
    }

    pub(crate) async fn invoke(
        &self,
        workspace_name: &WorkspaceName,
        request: CapabilityInvocationRequest,
    ) -> Result<InvokeCapabilityResponse, crate::bootstrap::AppError> {
        if !self.runtime_exposure.exposes_typescript() {
            return Ok(error_response(
                "runtime_exposure_disabled",
                "TypeScript invocation is disabled by the active runtime exposure; start Coral with runtime exposure 'typescript' or 'both' to invoke capabilities.",
                json!({ "runtime_exposure": self.runtime_exposure.as_str() }),
            ));
        }
        let args = match parse_args_json(&request.args_json) {
            Ok(args) => args,
            Err(response) => return Ok(response),
        };
        if request.capability_id.trim().is_empty() {
            return Ok(error_response(
                "invalid_request",
                "capability_id is required",
                JsonValue::Null,
            ));
        }
        let workspace = self
            .discovery
            .load_workspace_exports_for_capability_id(workspace_name, &request.capability_id)?;
        let proto_request = InvokeCapabilityRequest {
            workspace: None,
            capability_id: request.capability_id,
            binding_ref: request.binding_ref,
            binding_path: request.binding_path,
            args_json: request.args_json,
        };
        let resolved = match resolve_invocation(&workspace, &proto_request) {
            Ok(resolved) => resolved,
            Err(response) => return Ok(response),
        };
        Ok(invoke_resolved(
            resolved,
            args,
            Some(InvocationRuntime {
                workspace_name,
                credentials: &self.credentials,
            }),
        )
        .await)
    }
}

pub(crate) struct CapabilityInvocationRequest {
    pub(crate) capability_id: String,
    pub(crate) binding_ref: String,
    pub(crate) binding_path: Vec<String>,
    pub(crate) args_json: String,
}

pub(crate) struct SqlProviderCapabilityInvocation {
    pub(crate) entry: CapabilityExport,
    pub(crate) capability: Capability,
    pub(crate) source_materialized_dir: PathBuf,
    pub(crate) source_name: SourceName,
    pub(crate) credential_storage: CredentialStorageKind,
    pub(crate) source_variables: BTreeMap<String, String>,
    pub(crate) args: JsonMap<String, JsonValue>,
}

pub(crate) async fn invoke_sql_provider_capability(
    workspace_name: &WorkspaceName,
    credentials: &CredentialManager,
    invocation: SqlProviderCapabilityInvocation,
) -> Result<JsonValue, String> {
    let resolved = ResolvedInvocation {
        entry: invocation.entry,
        capability: invocation.capability,
        source_materialized_dir: invocation.source_materialized_dir,
        source_name: invocation.source_name,
        credential_storage: invocation.credential_storage,
        source_variables: invocation.source_variables,
        binding_ref: None,
        binding_path: None,
    };
    let response = invoke_resolved(
        resolved,
        invocation.args,
        Some(InvocationRuntime {
            workspace_name,
            credentials,
        }),
    )
    .await;
    if response.ok {
        return Ok(response
            .value
            .map_or(JsonValue::Null, proto_json_value_to_json));
    }
    let Some(error) = response.error else {
        return Err("provider invocation failed without a structured error".to_string());
    };
    let details = error.details.map(proto_json_value_to_json);
    Err(json!({
        "kind": error.kind,
        "message": error.message,
        "details": details.unwrap_or(JsonValue::Null),
    })
    .to_string())
}

#[tonic::async_trait]
impl CapabilityServiceApi for CapabilityService {
    async fn invoke(
        &self,
        request: Request<InvokeCapabilityRequest>,
    ) -> Result<Response<InvokeCapabilityResponse>, Status> {
        let span = grpc_span(&request);
        let invoker = self.invoker.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let response = Box::pin(invoker.invoke(
                &workspace_name,
                CapabilityInvocationRequest {
                    capability_id: request.capability_id,
                    binding_ref: request.binding_ref,
                    binding_path: request.binding_path,
                    args_json: request.args_json,
                },
            ))
            .await
            .map_err(app_status)?;
            Ok(Response::new(response))
        }))
        .await
    }
}

#[derive(Debug, Clone)]
struct ResolvedInvocation {
    entry: CapabilityExport,
    capability: Capability,
    source_materialized_dir: PathBuf,
    source_name: SourceName,
    credential_storage: CredentialStorageKind,
    source_variables: BTreeMap<String, String>,
    binding_ref: Option<String>,
    binding_path: Option<Vec<String>>,
}

#[derive(Clone, Copy)]
struct InvocationRuntime<'a> {
    workspace_name: &'a WorkspaceName,
    credentials: &'a CredentialManager,
}

fn parse_args_json(raw: &str) -> Result<JsonMap<String, JsonValue>, InvokeCapabilityResponse> {
    if raw.trim().is_empty() {
        return Ok(JsonMap::new());
    }
    match serde_json::from_str::<JsonValue>(raw) {
        Ok(JsonValue::Object(args)) => Ok(args),
        Ok(_) => Err(error_response(
            "invalid_args",
            "args_json must be a JSON object",
            json!({ "args_json_type": "non_object" }),
        )),
        Err(error) => Err(error_response(
            "invalid_args",
            format!("args_json is not valid JSON: {error}"),
            json!({ "parse_error": error.to_string() }),
        )),
    }
}

fn resolve_invocation(
    workspace: &LoadedWorkspaceExports,
    request: &InvokeCapabilityRequest,
) -> Result<ResolvedInvocation, InvokeCapabilityResponse> {
    let capability_id = request.capability_id.trim();
    if capability_id.is_empty() {
        return Err(error_response(
            "invalid_request",
            "capability_id is required",
            JsonValue::Null,
        ));
    }
    let expected_id = CapabilityId(capability_id.to_string());

    let binding_ref = trimmed_nonempty(&request.binding_ref);
    let binding_path = normalized_binding_path(&request.binding_path)?;
    let entry = match (&binding_ref, &binding_path) {
        (Some(binding_ref), _) => resolve_by_binding_ref(workspace, binding_ref)?,
        (None, Some(binding_path)) => resolve_by_binding_path(workspace, binding_path)?,
        (None, None) => resolve_by_capability_id(workspace, &expected_id)?,
    };

    if entry.capability_id != expected_id {
        return Err(error_response(
            "stale_binding",
            format!(
                "binding resolved to capability '{}', but request expected '{}'",
                entry.capability_id, expected_id
            ),
            json!({
                "expected_capability_id": expected_id.as_str(),
                "resolved_capability_id": entry.capability_id.as_str(),
                "binding_ref": binding_ref,
                "binding_path": binding_path.as_ref().map(|path| path.join(".")),
            }),
        ));
    }

    let capability = workspace
        .capability_by_id
        .get(&entry.capability_id)
        .cloned()
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                format!(
                    "workspace exports reference capability '{}' but capabilities.yaml does not contain it",
                    entry.capability_id
                ),
                json!({ "capability_id": entry.capability_id.as_str() }),
            )
        })?;
    let source_materialized_dir = workspace
        .source_materialized_dir_by_id
        .get(&entry.source_id)
        .cloned()
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                format!(
                    "workspace exports reference source '{}' but no installed source directory was found",
                    entry.source_id
                ),
                json!({ "source_id": entry.source_id.as_str() }),
            )
        })?;
    let source_runtime = workspace
        .source_runtime_by_id
        .get(&entry.source_id)
        .cloned()
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                format!(
                    "workspace exports reference source '{}' but no runtime source metadata was found",
                    entry.source_id
                ),
                json!({ "source_id": entry.source_id.as_str() }),
            )
        })?;

    Ok(ResolvedInvocation {
        entry,
        capability,
        source_materialized_dir,
        source_name: source_runtime.name,
        credential_storage: source_runtime.credential_storage,
        source_variables: source_runtime.variables,
        binding_ref,
        binding_path,
    })
}

fn resolve_by_binding_ref(
    workspace: &LoadedWorkspaceExports,
    binding_ref: &str,
) -> Result<CapabilityExport, InvokeCapabilityResponse> {
    resolve_unique_entry(
        workspace
            .exports
            .entries
            .iter()
            .filter(|entry| {
                entry
                    .bindings
                    .iter()
                    .any(|binding| binding.ref_().value == binding_ref)
            })
            .collect(),
        "binding_ref",
        binding_ref,
    )
}

fn resolve_by_binding_path(
    workspace: &LoadedWorkspaceExports,
    binding_path: &[String],
) -> Result<CapabilityExport, InvokeCapabilityResponse> {
    let alias = binding_path.join(".");
    resolve_unique_entry(
        workspace
            .exports
            .entries
            .iter()
            .filter(|entry| {
                entry.bindings.iter().any(|binding| match binding {
                    Binding::Typescript(binding) => binding.path == binding_path,
                    Binding::Sql(_) => false,
                })
            })
            .collect(),
        "binding_path",
        &alias,
    )
}

fn resolve_by_capability_id(
    workspace: &LoadedWorkspaceExports,
    capability_id: &CapabilityId,
) -> Result<CapabilityExport, InvokeCapabilityResponse> {
    let Some(entry) = workspace
        .exports
        .entries
        .iter()
        .find(|entry| &entry.capability_id == capability_id)
        .cloned()
    else {
        return Err(error_response(
            "not_found",
            format!("capability '{capability_id}' was not found in workspace exports"),
            json!({ "capability_id": capability_id.as_str() }),
        ));
    };
    Ok(entry)
}

fn resolve_unique_entry(
    mut entries: Vec<&CapabilityExport>,
    selector_name: &str,
    selector_value: &str,
) -> Result<CapabilityExport, InvokeCapabilityResponse> {
    entries.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    match entries.len() {
        0 => Err(error_response(
            "not_found",
            format!("no capability binding matched {selector_name} '{selector_value}'"),
            json!({ selector_name: selector_value }),
        )),
        1 => Ok(entries
            .pop()
            .expect("entries length was checked before pop")
            .clone()),
        _ => Err(error_response(
            "ambiguous_binding",
            format!("multiple capability bindings matched {selector_name} '{selector_value}'"),
            json!({
                selector_name: selector_value,
                "candidates": entries
                    .into_iter()
                    .map(|entry| entry.capability_id.as_str().to_string())
                    .collect::<Vec<_>>(),
            }),
        )),
    }
}

async fn invoke_resolved(
    resolved: ResolvedInvocation,
    args: JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> InvokeCapabilityResponse {
    if let Err(response) = validate_invocation_args(&resolved.capability, &args) {
        return response;
    }

    match &resolved.capability.upstream_binding {
        UpstreamBinding::FileRead(binding) => invoke_file_read(&resolved, binding, &args),
        UpstreamBinding::Rest(binding) => invoke_rest(&resolved, binding, args, runtime).await,
        UpstreamBinding::Graphql(binding) => {
            invoke_graphql(&resolved, binding, args, runtime).await
        }
        UpstreamBinding::McpTool(binding) => invoke_mcp(&resolved, binding, args, runtime).await,
    }
}

fn validate_invocation_args(
    capability: &Capability,
    args: &JsonMap<String, JsonValue>,
) -> Result<(), InvokeCapabilityResponse> {
    let schema = code_mode_tool_input_schema(capability);
    let unresolved_refs = executable_schema_unresolved_refs(&schema);
    if !unresolved_refs.is_empty() {
        return Err(error_response(
            "artifact_mismatch",
            format!(
                "installed capability '{}' has unresolved executable input schema references; re-add the source to regenerate materialized artifacts",
                capability.capability_id
            ),
            json!({
                "capability_id": capability.capability_id.as_str(),
                "unresolved_refs": unresolved_refs,
            }),
        ));
    }
    let compiled = jsonschema::JSONSchema::compile(&schema).map_err(|error| {
        error_response(
            "artifact_mismatch",
            format!(
                "installed capability '{}' has an invalid executable input schema; re-add the source to regenerate materialized artifacts",
                capability.capability_id
            ),
            json!({
                "capability_id": capability.capability_id.as_str(),
                "schema_error": error.to_string(),
            }),
        )
    })?;
    let value = JsonValue::Object(args.clone());
    let errors = match compiled.validate(&value) {
        Ok(()) => return Ok(()),
        Err(errors) => errors.map(|error| error.to_string()).collect::<Vec<_>>(),
    };
    Err(error_response(
        "invalid_args",
        format!(
            "capability args failed executable input schema validation for '{}'",
            capability.capability_id
        ),
        json!({
            "capability_id": capability.capability_id.as_str(),
            "errors": errors,
        }),
    ))
}

async fn invoke_rest(
    resolved: &ResolvedInvocation,
    binding: &RestUpstreamBinding,
    args: JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> InvokeCapabilityResponse {
    let plan = match rest_invocation_plan(resolved, binding, &args, runtime).await {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    invoke_upstream_plan(resolved, plan).await
}

async fn invoke_graphql(
    resolved: &ResolvedInvocation,
    binding: &GraphqlOperationBinding,
    args: JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> InvokeCapabilityResponse {
    let plan = match graphql_invocation_plan(resolved, binding, &args, runtime).await {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    match execute_plan(&plan).await {
        Ok(envelope) => match &envelope {
            UpstreamResponseEnvelope::Graphql(response) if !response.errors.is_empty() => {
                if graphql_singleton_not_found(resolved, binding, response) {
                    return success_response(
                        JsonValue::Null,
                        upstream_success_envelope(resolved, &envelope),
                    );
                }
                graphql_error_response(resolved, response)
            }
            _ => success_response(
                upstream_value(&envelope),
                upstream_success_envelope(resolved, &envelope),
            ),
        },
        Err(error) => upstream_error_response(resolved, error),
    }
}

async fn invoke_mcp(
    resolved: &ResolvedInvocation,
    binding: &McpToolUpstreamBinding,
    args: JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> InvokeCapabilityResponse {
    let plan = match mcp_invocation_plan(resolved, binding, args, runtime).await {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    invoke_upstream_plan(resolved, plan).await
}

async fn invoke_upstream_plan(
    resolved: &ResolvedInvocation,
    plan: UpstreamInvocationPlan,
) -> InvokeCapabilityResponse {
    match execute_plan(&plan).await {
        Ok(envelope) => success_response(
            upstream_value(&envelope),
            upstream_success_envelope(resolved, &envelope),
        ),
        Err(error) => upstream_error_response(resolved, error),
    }
}

async fn rest_invocation_plan(
    resolved: &ResolvedInvocation,
    binding: &RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> Result<UpstreamInvocationPlan, InvokeCapabilityResponse> {
    let spec = load_installed_source_spec(&resolved.source_materialized_dir)?;
    let interface = openapi_interface(&spec, &resolved.capability.interface_id)?;
    let base_url = rest_base_url(&resolved.source_materialized_dir, interface, resolved)?;
    reject_unconsumed_rest_args(binding, args)?;
    let mut url = rest_url(&base_url, binding, args)?;
    let auth_headers = auth_headers(
        resolved,
        runtime,
        &spec.declared_inputs,
        interface.auth.as_ref(),
    )
    .await?;
    let auth_header_names = auth_headers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let mut headers = Vec::new();
    apply_rest_parameters(
        &mut url,
        &mut headers,
        &auth_header_names,
        &binding.parameter_bindings,
        args,
    )?;
    let selected_body = rest_request_body(binding, args)?;
    if let Some(content_type) = selected_body.content_type {
        upsert_header(&mut headers, "Content-Type", content_type);
    }
    for (name, value) in auth_headers {
        upsert_header(&mut headers, &name, value.expose_secret().to_string());
    }
    Ok(UpstreamInvocationPlan::Http(HttpRequestPlan {
        method: binding.method,
        url,
        headers,
        body: selected_body.body,
        timeout: Some(DEFAULT_PROVIDER_INVOCATION_TIMEOUT),
        trace_labels: trace_labels(resolved),
    }))
}

fn rest_base_url(
    materialized_dir: &Path,
    interface: &OpenApiInterface,
    resolved: &ResolvedInvocation,
) -> Result<String, InvokeCapabilityResponse> {
    if let Some(base_url) = &interface.base_url {
        return render_source_input_template(
            base_url,
            "openapi.base_url",
            &resolved.source_variables,
        );
    }

    let document_path = materialized_dir
        .join("interfaces")
        .join(&interface.id)
        .join("source-document.raw");
    let bytes = std::fs::read(&document_path).map_err(|error| {
        error_response(
            "artifact_mismatch",
            "installed OpenAPI source document is missing",
            json!({
                "interface_id": interface.id,
                "source_document_path": document_path.display().to_string(),
                "error": error.to_string(),
            }),
        )
    })?;
    let metadata = openapi_document_metadata(&bytes).map_err(|error| {
        error_response(
            "artifact_mismatch",
            "installed OpenAPI source document is not valid OpenAPI 3.0",
            json!({
                "interface_id": interface.id,
                "source_document_path": document_path.display().to_string(),
                "error": error.to_string(),
            }),
        )
    })?;
    metadata
        .server_url
        .filter(|url| is_allowed_runtime_base_url(url))
        .ok_or_else(|| {
            error_response(
                "unsupported",
                "REST invocation requires SourceSpec openapi.base_url or an HTTPS OpenAPI servers[].url; localhost HTTP is allowed for development",
                json!({
                    "interface_id": interface.id,
                    "source_document_path": document_path.display().to_string(),
                }),
            )
        })
}

fn is_allowed_runtime_base_url(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        },
        _ => false,
    })
}

async fn mcp_invocation_plan(
    resolved: &ResolvedInvocation,
    binding: &McpToolUpstreamBinding,
    args: JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> Result<UpstreamInvocationPlan, InvokeCapabilityResponse> {
    let spec = load_installed_source_spec(&resolved.source_materialized_dir)?;
    let interface = mcp_interface(&spec, &resolved.capability.interface_id)?;
    let server = match &interface.server.transport {
        McpTransportDescriptor::StreamableHttp { url } => {
            let headers = auth_headers(
                resolved,
                runtime,
                &spec.declared_inputs,
                interface.server.auth.as_ref(),
            )
            .await?;
            let endpoint = render_source_input_template(
                url,
                "mcp.server.transport.url",
                &resolved.source_variables,
            )?;
            let endpoint = Url::parse(&endpoint).map_err(|error| {
                error_response(
                    "invalid_request",
                    format!("MCP Streamable HTTP endpoint is not a valid URL: {error}"),
                    json!({ "endpoint": endpoint }),
                )
            })?;
            McpConnectionTarget::StreamableHttp {
                url: endpoint,
                headers,
            }
        }
        McpTransportDescriptor::Stdio { command, args } => McpConnectionTarget::Stdio {
            command: command.clone(),
            args: args.clone(),
            env: mcp_stdio_env(
                resolved,
                runtime,
                &spec.declared_inputs,
                &interface.server.env,
            )
            .await?,
        },
    };
    Ok(UpstreamInvocationPlan::McpToolCall(McpToolCallPlan {
        server,
        tool_name: binding.tool_name.clone(),
        arguments: args,
        timeout: Some(DEFAULT_PROVIDER_INVOCATION_TIMEOUT),
        trace_labels: trace_labels(resolved),
    }))
}

async fn graphql_invocation_plan(
    resolved: &ResolvedInvocation,
    binding: &GraphqlOperationBinding,
    args: &JsonMap<String, JsonValue>,
    runtime: Option<InvocationRuntime<'_>>,
) -> Result<UpstreamInvocationPlan, InvokeCapabilityResponse> {
    let spec = load_installed_source_spec(&resolved.source_materialized_dir)?;
    let interface = graphql_interface(&spec, &resolved.capability.interface_id)?;
    let variables = graphql_variables(binding, args)?;
    let endpoint = render_source_input_template(
        &interface.endpoint,
        "graphql.endpoint",
        &resolved.source_variables,
    )?;
    let endpoint = Url::parse(&endpoint).map_err(|error| {
        error_response(
            "invalid_request",
            format!("GraphQL endpoint is not a valid URL: {error}"),
            json!({ "endpoint": endpoint }),
        )
    })?;
    let headers = auth_headers(
        resolved,
        runtime,
        &spec.declared_inputs,
        interface.auth.as_ref(),
    )
    .await?;
    if binding.graphql_operation_kind == coral_capabilities::GraphqlOperationKind::Subscription {
        return Err(error_response(
            "unsupported",
            "GraphQL subscriptions are not invokable in this runtime",
            json!({ "operation_name": binding.operation_name }),
        ));
    }
    let document = read_graphql_operation_document(
        &resolved.source_materialized_dir,
        &resolved.capability.interface_id,
        binding,
    )?;
    Ok(UpstreamInvocationPlan::Graphql(GraphqlRequestPlan {
        endpoint,
        headers,
        operation_name: binding.operation_name.clone(),
        graphql_operation_kind: binding.graphql_operation_kind,
        document,
        variables,
        timeout: Some(DEFAULT_PROVIDER_INVOCATION_TIMEOUT),
        trace_labels: trace_labels(resolved),
    }))
}

fn graphql_variables(
    binding: &GraphqlOperationBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<JsonMap<String, JsonValue>, InvokeCapabilityResponse> {
    let allowed_args = binding
        .variable_bindings
        .iter()
        .filter_map(|binding| binding.argument_path.first())
        .collect::<std::collections::BTreeSet<_>>();
    if let Some(unexpected) = args.keys().find(|key| !allowed_args.contains(key)) {
        return Err(error_response(
            "invalid_request",
            format!("GraphQL argument '{unexpected}' is not defined for this capability"),
            json!({
                "operation_name": binding.operation_name,
                "argument": unexpected,
            }),
        ));
    }
    let mut variables = JsonMap::new();
    for variable_binding in &binding.variable_bindings {
        let Some(argument_name) = variable_binding.argument_path.first() else {
            continue;
        };
        match args.get(argument_name) {
            Some(value) => {
                variables.insert(variable_binding.variable_name.clone(), value.clone());
            }
            None if variable_binding.required => {
                return Err(error_response(
                    "invalid_request",
                    format!("missing required GraphQL argument '{argument_name}'"),
                    json!({
                        "operation_name": binding.operation_name,
                        "argument": argument_name,
                    }),
                ));
            }
            None => {}
        }
    }
    Ok(variables)
}

fn read_graphql_operation_document(
    source_materialized_dir: &Path,
    interface_id: &str,
    binding: &GraphqlOperationBinding,
) -> Result<String, InvokeCapabilityResponse> {
    let document_path = operation_document_path(source_materialized_dir, interface_id, binding)
        .map_err(|error| {
            error_response(
                "artifact_mismatch",
                "GraphQL generated operation document reference is invalid",
                json!({
                    "operation_name": binding.operation_name,
                    "document_ref": binding.document_ref,
                    "error": error,
                }),
            )
        })?;
    std::fs::read_to_string(&document_path).map_err(|error| {
        error_response(
            "artifact_mismatch",
            "GraphQL generated operation document is missing; re-add the source to regenerate artifacts",
            json!({
                "operation_name": binding.operation_name,
                "document_ref": binding.document_ref,
                "document_path": document_path.display().to_string(),
                "error": error.to_string(),
            }),
        )
    })
}

fn load_installed_source_spec(
    materialized_dir: &Path,
) -> Result<SourceSpec, InvokeCapabilityResponse> {
    let source_dir = materialized_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                "source materialized directory is not under a source directory",
                json!({ "materialized_dir": materialized_dir.display().to_string() }),
            )
        })?;
    let manifest_path = source_dir.join("manifest.yaml");
    let raw = std::fs::read_to_string(&manifest_path).map_err(|error| {
        error_response(
            "artifact_mismatch",
            "installed source manifest is missing",
            json!({ "manifest_path": manifest_path.display().to_string(), "error": error.to_string() }),
        )
    })?;
    parse_source_manifest_yaml(&raw).map_err(|error| {
        error_response(
            "artifact_mismatch",
            "installed source manifest is not a valid SourceSpec",
            json!({ "manifest_path": manifest_path.display().to_string(), "error": error.to_string() }),
        )
    })
}

async fn mcp_stdio_env(
    resolved: &ResolvedInvocation,
    runtime: Option<InvocationRuntime<'_>>,
    inputs: &[ManifestInputSpec],
    env_bindings: &[McpEnvBinding],
) -> Result<Vec<(String, RedactableString)>, InvokeCapabilityResponse> {
    let mut env = minimal_stdio_env();
    if !env_bindings.is_empty() {
        let material =
            source_input_material_for_env(resolved, runtime, inputs, env_bindings).await?;
        for binding in env_bindings {
            env.push((
                binding.name.clone(),
                RedactableString::new(required_source_input(&material, &binding.key, resolved)?),
            ));
        }
    }
    Ok(env)
}

fn minimal_stdio_env() -> Vec<(String, RedactableString)> {
    stdio_path_env()
        .map(|value| {
            vec![(
                "PATH".to_string(),
                RedactableString::new(value.to_string_lossy().to_string()),
            )]
        })
        .unwrap_or_default()
}

fn openapi_interface<'a>(
    spec: &'a SourceSpec,
    interface_id: &str,
) -> Result<&'a OpenApiInterface, InvokeCapabilityResponse> {
    spec.interfaces
        .iter()
        .find_map(|interface| match interface {
            SourceInterface::OpenApi(interface) if interface.id == interface_id => Some(interface),
            _ => None,
        })
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                "REST capability references an OpenAPI interface missing from SourceSpec",
                json!({ "interface_id": interface_id }),
            )
        })
}

fn mcp_interface<'a>(
    spec: &'a SourceSpec,
    interface_id: &str,
) -> Result<&'a McpInterface, InvokeCapabilityResponse> {
    spec.interfaces
        .iter()
        .find_map(|interface| match interface {
            SourceInterface::Mcp(interface) if interface.id == interface_id => Some(interface),
            _ => None,
        })
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                "MCP capability references an interface missing from SourceSpec",
                json!({ "interface_id": interface_id }),
            )
        })
}

fn graphql_interface<'a>(
    spec: &'a SourceSpec,
    interface_id: &str,
) -> Result<&'a GraphqlInterface, InvokeCapabilityResponse> {
    spec.interfaces
        .iter()
        .find_map(|interface| match interface {
            SourceInterface::Graphql(interface) if interface.id == interface_id => Some(interface),
            _ => None,
        })
        .ok_or_else(|| {
            error_response(
                "artifact_mismatch",
                "GraphQL capability references an interface missing from SourceSpec",
                json!({ "interface_id": interface_id }),
            )
        })
}

fn render_source_input_template(
    template: &ParsedTemplate,
    field: &str,
    source_variables: &BTreeMap<String, String>,
) -> Result<String, InvokeCapabilityResponse> {
    let mut rendered = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(literal) => rendered.push_str(literal),
            TemplatePart::Token(token) => {
                if token.namespace() != &TemplateNamespace::Input {
                    return Err(error_response(
                        "invalid_request",
                        format!(
                            "{field} contains unsupported template token '{}'",
                            token.raw()
                        ),
                        json!({ "field": field, "token": token.raw() }),
                    ));
                }
                let value = source_variables.get(token.key()).ok_or_else(|| {
                    error_response(
                        "credential_failure",
                        format!(
                            "{field} requires source variable '{}', but the installed source has no value",
                            token.key()
                        ),
                        json!({ "field": field, "input": token.key() }),
                    )
                })?;
                rendered.push_str(value);
            }
        }
    }
    if !is_allowed_runtime_base_url(&rendered) {
        return Err(error_response(
            "invalid_request",
            format!("{field} rendered to an unsupported provider URL"),
            json!({ "field": field, "url": rendered }),
        ));
    }
    Ok(rendered)
}

fn rest_url(
    base_url: &str,
    binding: &RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<Url, InvokeCapabilityResponse> {
    let mut url = Url::parse(base_url).map_err(|error| {
        error_response(
            "invalid_request",
            format!("REST base_url is not a valid URL: {error}"),
            json!({ "base_url": base_url }),
        )
    })?;
    url.set_query(None);
    url.set_fragment(None);
    let rendered_segments = render_rest_path_segments(binding, args)?;
    {
        let mut path_segments = url.path_segments_mut().map_err(|()| {
            error_response(
                "invalid_request",
                "REST base_url cannot be used as a base URL",
                json!({ "base_url": base_url }),
            )
        })?;
        path_segments.extend(rendered_segments);
    }
    Ok(url)
}

fn render_rest_path_segments(
    binding: &RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<Vec<String>, InvokeCapabilityResponse> {
    binding
        .path_template
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| render_rest_path_segment(segment, binding, args))
        .collect()
}

fn render_rest_path_segment(
    segment: &str,
    binding: &RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<String, InvokeCapabilityResponse> {
    let mut rendered = segment.to_string();
    for parameter in binding
        .parameter_bindings
        .iter()
        .filter(|parameter| parameter.location == RestParameterLocation::Path)
    {
        let placeholder = format!("{{{}}}", parameter.name);
        if rendered.contains(&placeholder) {
            let value = argument_scalar(args, parameter)?;
            rendered = rendered.replace(&placeholder, &value);
        }
    }
    for parameter_name in path_template_parameter_names(&rendered) {
        let parameter = RestParameterBinding {
            name: parameter_name,
            location: RestParameterLocation::Path,
            required: true,
            style: "simple".to_string(),
            explode: false,
            allow_reserved: false,
        };
        let value = argument_scalar(args, &parameter)?;
        rendered = rendered.replace(&format!("{{{}}}", parameter.name), &value);
    }
    if rendered.contains('{') || rendered.contains('}') {
        return Err(error_response(
            "invalid_request",
            format!("REST path template segment '{segment}' contains an invalid placeholder"),
            json!({ "segment": segment }),
        ));
    }
    Ok(rendered)
}

fn path_template_parameter_names(segment: &str) -> Vec<String> {
    let mut names = BTreeSet::new();
    let mut remaining = segment;
    while let Some((_, after_start)) = remaining.split_once('{') {
        let Some((name, after_end)) = after_start.split_once('}') else {
            break;
        };
        let name = name.trim();
        if !name.is_empty() {
            names.insert(name.to_string());
        }
        remaining = after_end;
    }
    names.into_iter().collect()
}

fn reject_unconsumed_rest_args(
    binding: &RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<(), InvokeCapabilityResponse> {
    let top_level = top_level_rest_argument_names(binding);
    let request_body_allowed = !binding.request_bodies.is_empty();
    for (key, value) in args {
        if let Some(location) = rest_location_from_key(key) {
            if let Some(location_args) = value.as_object() {
                reject_unconsumed_rest_location_args(binding, location, location_args)?;
                continue;
            }
            if top_level.contains(key) {
                continue;
            }
            return Err(error_response(
                "invalid_args",
                format!("REST '{key}' arguments must be an object"),
                json!({ "argument": key }),
            ));
        }
        if top_level.contains(key) {
            continue;
        }
        if request_body_allowed && rest_body_argument_key(key) {
            continue;
        }
        if rest_body_argument_key(key) {
            return Err(error_response(
                "invalid_args",
                format!("REST capability does not accept request body argument '{key}'"),
                json!({ "argument": key }),
            ));
        }
        return Err(error_response(
            "invalid_args",
            format!("unexpected REST argument '{key}'"),
            json!({ "argument": key }),
        ));
    }
    Ok(())
}

fn reject_unconsumed_rest_location_args(
    binding: &RestUpstreamBinding,
    location: RestParameterLocation,
    args: &JsonMap<String, JsonValue>,
) -> Result<(), InvokeCapabilityResponse> {
    let allowed = rest_argument_names_for_location(binding, location);
    for key in args.keys() {
        if allowed.contains(key) {
            continue;
        }
        return Err(error_response(
            "invalid_args",
            format!(
                "unexpected REST {} argument '{}'",
                rest_location_key(location),
                key
            ),
            json!({
                "argument": key,
                "location": rest_location_key(location),
            }),
        ));
    }
    Ok(())
}

fn top_level_rest_argument_names(binding: &RestUpstreamBinding) -> BTreeSet<String> {
    let mut names = binding
        .parameter_bindings
        .iter()
        .map(|parameter| parameter.name.clone())
        .collect::<BTreeSet<_>>();
    names.extend(rest_path_template_parameter_names(binding));
    names
}

fn rest_argument_names_for_location(
    binding: &RestUpstreamBinding,
    location: RestParameterLocation,
) -> BTreeSet<String> {
    let mut names = binding
        .parameter_bindings
        .iter()
        .filter(|parameter| parameter.location == location)
        .map(|parameter| parameter.name.clone())
        .collect::<BTreeSet<_>>();
    if location == RestParameterLocation::Path {
        names.extend(rest_path_template_parameter_names(binding));
    }
    names
}

fn rest_path_template_parameter_names(binding: &RestUpstreamBinding) -> BTreeSet<String> {
    binding
        .path_template
        .trim_start_matches('/')
        .split('/')
        .flat_map(path_template_parameter_names)
        .collect()
}

fn rest_location_from_key(key: &str) -> Option<RestParameterLocation> {
    match key {
        "path" => Some(RestParameterLocation::Path),
        "query" => Some(RestParameterLocation::Query),
        "header" => Some(RestParameterLocation::Header),
        "cookie" => Some(RestParameterLocation::Cookie),
        _ => None,
    }
}

fn rest_body_argument_key(key: &str) -> bool {
    matches!(
        key,
        "body" | "json" | "contentType" | "content_type" | "body_media_type" | "media_type"
    )
}

fn apply_rest_parameters(
    url: &mut Url,
    headers: &mut Vec<(String, RedactableString)>,
    auth_header_names: &[String],
    parameters: &[RestParameterBinding],
    args: &JsonMap<String, JsonValue>,
) -> Result<(), InvokeCapabilityResponse> {
    for parameter in parameters {
        match parameter.location {
            RestParameterLocation::Path => {}
            RestParameterLocation::Query => {
                let pairs = optional_query_argument_pairs(args, parameter)?;
                if !pairs.is_empty() {
                    let mut query = url.query_pairs_mut();
                    for (name, value) in pairs {
                        query.append_pair(&name, &value);
                    }
                }
            }
            RestParameterLocation::Header => {
                if let Some(value) = optional_argument_serialized(args, parameter)? {
                    if auth_header_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&parameter.name))
                    {
                        return Err(error_response(
                            "invalid_request",
                            format!(
                                "REST header parameter '{}' conflicts with a source auth header",
                                parameter.name
                            ),
                            json!({ "header": parameter.name }),
                        ));
                    }
                    upsert_header(headers, &parameter.name, value);
                }
            }
            RestParameterLocation::Cookie => {
                if optional_argument_serialized(args, parameter)?.is_none() {
                    continue;
                }
                return Err(error_response(
                    "unsupported",
                    "REST cookie parameters are not implemented in capability invocation yet",
                    json!({ "parameter": parameter.name }),
                ));
            }
        }
    }
    Ok(())
}

fn upsert_header(
    headers: &mut Vec<(String, RedactableString)>,
    name: &str,
    value: impl Into<String>,
) {
    let value = value.into();
    if let Some((_, existing)) = headers
        .iter_mut()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
    {
        *existing = RedactableString::new(value);
        return;
    }
    headers.push((name.to_string(), RedactableString::new(value)));
}

fn optional_argument_serialized(
    args: &JsonMap<String, JsonValue>,
    parameter: &RestParameterBinding,
) -> Result<Option<String>, InvokeCapabilityResponse> {
    let Some(value) = rest_argument_value(args, parameter) else {
        if parameter.required {
            return Err(missing_required_rest_argument(parameter));
        }
        return Ok(None);
    };
    Ok(Some(serialize_rest_parameter_value(value, parameter)?))
}

fn optional_query_argument_pairs(
    args: &JsonMap<String, JsonValue>,
    parameter: &RestParameterBinding,
) -> Result<Vec<(String, String)>, InvokeCapabilityResponse> {
    let Some(value) = rest_argument_value(args, parameter) else {
        if parameter.required {
            return Err(missing_required_rest_argument(parameter));
        }
        return Ok(Vec::new());
    };
    query_argument_pairs(value, parameter)
}

fn missing_required_rest_argument(parameter: &RestParameterBinding) -> InvokeCapabilityResponse {
    error_response(
        "invalid_args",
        format!(
            "missing required REST argument '{}' (or '{}.{}')",
            parameter.name,
            rest_location_key(parameter.location),
            parameter.name
        ),
        json!({
            "argument": parameter.name,
            "location": rest_location_key(parameter.location),
        }),
    )
}

fn query_argument_pairs(
    value: &JsonValue,
    parameter: &RestParameterBinding,
) -> Result<Vec<(String, String)>, InvokeCapabilityResponse> {
    match (parameter.style.as_str(), value) {
        ("deepObject", JsonValue::Object(object)) => object
            .iter()
            .map(|(key, value)| {
                Ok((
                    format!("{}[{key}]", parameter.name),
                    rest_parameter_part(value, parameter)?,
                ))
            })
            .collect(),
        ("form", JsonValue::Array(values)) if parameter.explode => values
            .iter()
            .map(|value| {
                Ok((
                    parameter.name.clone(),
                    rest_parameter_part(value, parameter)?,
                ))
            })
            .collect(),
        ("form", JsonValue::Object(object)) if parameter.explode => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), rest_parameter_part(value, parameter)?)))
            .collect(),
        _ => Ok(vec![(
            parameter.name.clone(),
            serialize_rest_parameter_value(value, parameter)?,
        )]),
    }
}

fn serialize_rest_parameter_value(
    value: &JsonValue,
    parameter: &RestParameterBinding,
) -> Result<String, InvokeCapabilityResponse> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .map(|value| rest_parameter_part(value, parameter))
            .collect::<Result<Vec<_>, _>>()
            .map(|values| values.join(rest_parameter_delimiter(parameter))),
        JsonValue::Object(object) => serialize_rest_object_parameter(object, parameter),
        _ => rest_parameter_part(value, parameter),
    }
}

fn serialize_rest_object_parameter(
    object: &JsonMap<String, JsonValue>,
    parameter: &RestParameterBinding,
) -> Result<String, InvokeCapabilityResponse> {
    let mut parts = Vec::new();
    for (key, value) in object {
        let value = rest_parameter_part(value, parameter)?;
        if parameter.explode {
            parts.push(format!("{key}={value}"));
        } else {
            parts.push(key.clone());
            parts.push(value);
        }
    }
    Ok(parts.join(rest_parameter_delimiter(parameter)))
}

fn rest_parameter_part(
    value: &JsonValue,
    parameter: &RestParameterBinding,
) -> Result<String, InvokeCapabilityResponse> {
    json_scalar_to_string(value).ok_or_else(|| {
        error_response(
            "invalid_args",
            format!(
                "REST argument '{}' (or '{}.{}') contains a value that cannot be serialized as a REST parameter",
                parameter.name,
                rest_location_key(parameter.location),
                parameter.name
            ),
            json!({
                "argument": parameter.name,
                "location": rest_location_key(parameter.location),
            }),
        )
    })
}

fn rest_parameter_delimiter(parameter: &RestParameterBinding) -> &'static str {
    match parameter.style.as_str() {
        "spaceDelimited" => " ",
        "pipeDelimited" => "|",
        _ => ",",
    }
}

fn rest_argument_value<'a>(
    args: &'a JsonMap<String, JsonValue>,
    parameter: &RestParameterBinding,
) -> Option<&'a JsonValue> {
    args.get(rest_location_key(parameter.location))
        .and_then(JsonValue::as_object)
        .and_then(|location_args| location_args.get(&parameter.name))
        .or_else(|| args.get(&parameter.name))
}

const fn rest_location_key(location: RestParameterLocation) -> &'static str {
    match location {
        RestParameterLocation::Path => "path",
        RestParameterLocation::Query => "query",
        RestParameterLocation::Header => "header",
        RestParameterLocation::Cookie => "cookie",
    }
}

fn argument_scalar(
    args: &JsonMap<String, JsonValue>,
    parameter: &RestParameterBinding,
) -> Result<String, InvokeCapabilityResponse> {
    optional_argument_serialized(args, parameter)?.ok_or_else(|| {
        error_response(
            "invalid_args",
            format!("missing required REST argument '{}'", parameter.name),
            json!({ "argument": parameter.name }),
        )
    })
}

struct SelectedRestRequestBody {
    body: Option<UpstreamRequestBody>,
    content_type: Option<String>,
}

fn rest_request_body(
    binding: &RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<SelectedRestRequestBody, InvokeCapabilityResponse> {
    let Some(body_binding) = rest_request_body_binding(binding, args)? else {
        return Ok(SelectedRestRequestBody {
            body: None,
            content_type: None,
        });
    };
    let body = requested_rest_body_value(args)?;
    if !is_json_media_type(&body_binding.media_type) {
        if body.is_none() && !body_binding.required {
            return Ok(SelectedRestRequestBody {
                body: None,
                content_type: None,
            });
        }
        return Err(error_response(
            "unsupported",
            "REST invocation currently supports JSON request bodies only",
            json!({
                "media_type": body_binding.media_type,
                "available_media_types": binding.request_bodies.iter().map(|body| body.media_type.as_str()).collect::<Vec<_>>(),
            }),
        ));
    }
    match body {
        Some(value) => Ok(SelectedRestRequestBody {
            body: Some(UpstreamRequestBody::Json(value.clone())),
            content_type: Some(body_binding.media_type.clone()),
        }),
        None if body_binding.required => Err(error_response(
            "invalid_args",
            "REST request body is required; pass it as args.body",
            json!({ "media_type": body_binding.media_type }),
        )),
        None => Ok(SelectedRestRequestBody {
            body: None,
            content_type: None,
        }),
    }
}

fn requested_rest_body_value(
    args: &JsonMap<String, JsonValue>,
) -> Result<Option<&JsonValue>, InvokeCapabilityResponse> {
    match (args.get("body"), args.get("json")) {
        (Some(_), Some(_)) => Err(error_response(
            "invalid_args",
            "REST request body aliases are mutually exclusive",
            json!({
                "first_argument": "body",
                "duplicate_argument": "json",
            }),
        )),
        (Some(value), None) | (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn rest_request_body_binding<'a>(
    binding: &'a RestUpstreamBinding,
    args: &JsonMap<String, JsonValue>,
) -> Result<Option<&'a RestRequestBody>, InvokeCapabilityResponse> {
    let requested = requested_rest_body_media_type(args)?;
    if let Some(media_type) = requested {
        return binding
            .request_bodies
            .iter()
            .find(|body| media_type.eq_ignore_ascii_case(&body.media_type))
            .map(Some)
            .ok_or_else(|| {
                error_response(
                    "invalid_args",
                    format!("REST request body media type '{media_type}' is not declared"),
                    json!({
                        "media_type": media_type,
                        "available_media_types": binding.request_bodies.iter().map(|body| body.media_type.as_str()).collect::<Vec<_>>(),
                    }),
                )
            });
    }
    Ok(binding
        .request_bodies
        .iter()
        .find(|body| is_json_media_type(&body.media_type))
        .or_else(|| binding.request_bodies.first()))
}

fn requested_rest_body_media_type(
    args: &JsonMap<String, JsonValue>,
) -> Result<Option<&str>, InvokeCapabilityResponse> {
    let mut selected = None;
    for key in [
        "contentType",
        "content_type",
        "body_media_type",
        "media_type",
    ] {
        let Some(value) = args.get(key) else {
            continue;
        };
        let media_type = value.as_str().ok_or_else(|| {
            error_response(
                "invalid_args",
                format!("REST request body media selector '{key}' must be a string"),
                json!({ "argument": key }),
            )
        })?;
        if let Some((selected_key, _)) = selected {
            return Err(error_response(
                "invalid_args",
                "REST request body media selector aliases are mutually exclusive",
                json!({
                    "first_argument": selected_key,
                    "duplicate_argument": key,
                }),
            ));
        }
        selected = Some((key, media_type));
    }
    Ok(selected.map(|(_, media_type)| media_type))
}

fn is_json_media_type(media_type: &str) -> bool {
    let media_type = media_type
        .split(';')
        .next()
        .unwrap_or(media_type)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json" || media_type.ends_with("+json")
}

async fn auth_headers(
    resolved: &ResolvedInvocation,
    runtime: Option<InvocationRuntime<'_>>,
    inputs: &[ManifestInputSpec],
    auth: Option<&AuthDescriptor>,
) -> Result<Vec<(String, RedactableString)>, InvokeCapabilityResponse> {
    match auth {
        None | Some(AuthDescriptor::None) => Ok(Vec::new()),
        Some(AuthDescriptor::BearerInput { key }) => {
            let material = credential_material(resolved, runtime, inputs).await?;
            let value = required_credential(&material, key, resolved)?;
            Ok(vec![(
                "Authorization".to_string(),
                RedactableString::new(format!("Bearer {value}")),
            )])
        }
        Some(AuthDescriptor::HeaderInput { name, key }) => {
            let material = credential_material(resolved, runtime, inputs).await?;
            let value = required_credential(&material, key, resolved)?;
            Ok(vec![(name.clone(), RedactableString::new(value))])
        }
        Some(AuthDescriptor::Headers { headers }) => {
            let material = credential_material(resolved, runtime, inputs).await?;
            headers
                .iter()
                .map(|header| {
                    let value = required_credential(&material, &header.key, resolved)?;
                    Ok((header.name.clone(), RedactableString::new(value)))
                })
                .collect()
        }
    }
}

async fn credential_material(
    resolved: &ResolvedInvocation,
    runtime: Option<InvocationRuntime<'_>>,
    inputs: &[ManifestInputSpec],
) -> Result<BTreeMap<String, String>, InvokeCapabilityResponse> {
    let Some(runtime) = runtime else {
        return Err(error_response(
            "credential_failure",
            "capability invocation requires credential resolution, but no app credential context was provided",
            json!({ "source_id": resolved.entry.source_id.as_str() }),
        ));
    };
    let credential_set_id = CredentialSetId::for_source(&resolved.source_name);
    runtime
        .credentials
        .read_material_for_inputs(
            runtime.workspace_name,
            &credential_set_id,
            resolved.credential_storage,
            inputs,
        )
        .await
        .map_err(|error| {
            error_response(
                "credential_failure",
                format!(
                    "credential material could not be read for source '{}': {error}",
                    resolved.source_name
                ),
                json!({ "source_id": resolved.entry.source_id.as_str() }),
            )
        })
}

async fn source_input_material_for_env(
    resolved: &ResolvedInvocation,
    runtime: Option<InvocationRuntime<'_>>,
    inputs: &[ManifestInputSpec],
    env_bindings: &[McpEnvBinding],
) -> Result<BTreeMap<String, String>, InvokeCapabilityResponse> {
    let mut material = resolved.source_variables.clone();
    if env_bindings.iter().any(|binding| {
        inputs
            .iter()
            .any(|input| input.key == binding.key && input.kind == ManifestInputKind::Secret)
    }) {
        material.extend(credential_material(resolved, runtime, inputs).await?);
    }
    Ok(material)
}

fn required_credential(
    material: &BTreeMap<String, String>,
    key: &str,
    resolved: &ResolvedInvocation,
) -> Result<String, InvokeCapabilityResponse> {
    material
        .get(key)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| {
            error_response(
                "credential_failure",
                format!(
                    "credential input '{key}' is required for source '{}' but no value is stored",
                    resolved.source_name
                ),
                json!({
                    "source_id": resolved.entry.source_id.as_str(),
                    "credential_key": key,
                }),
            )
        })
}

fn required_source_input(
    material: &BTreeMap<String, String>,
    key: &str,
    resolved: &ResolvedInvocation,
) -> Result<String, InvokeCapabilityResponse> {
    material
        .get(key)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| {
            error_response(
                "credential_failure",
                format!(
                    "source input '{key}' is required for source '{}' but no value is stored",
                    resolved.source_name
                ),
                json!({
                    "source_id": resolved.entry.source_id.as_str(),
                    "input_key": key,
                }),
            )
        })
}

fn json_scalar_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn trace_labels(resolved: &ResolvedInvocation) -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([
        (
            "capability_id".to_string(),
            resolved.capability.capability_id.as_str().to_string(),
        ),
        (
            "source_id".to_string(),
            resolved.entry.source_id.as_str().to_string(),
        ),
    ])
}

fn upstream_value(envelope: &UpstreamResponseEnvelope) -> JsonValue {
    match envelope {
        UpstreamResponseEnvelope::Http(response) => response_body_value(&response.body),
        UpstreamResponseEnvelope::Graphql(response) => {
            response.data.clone().unwrap_or(JsonValue::Null)
        }
        UpstreamResponseEnvelope::Mcp(response) => response
            .structured_content
            .clone()
            .unwrap_or_else(|| json!({ "content": response.content })),
    }
}

fn public_upstream_envelope(envelope: &UpstreamResponseEnvelope) -> JsonValue {
    match envelope {
        UpstreamResponseEnvelope::Http(response) => json!({
            "kind": "http",
            "status": response.status,
            "headers": response.headers,
            "media_type": response.media_type,
            "body": response_body_value(&response.body),
            "response_trust": response.response_trust,
        }),
        UpstreamResponseEnvelope::Graphql(response) => json!({
            "kind": "graphql",
            "http_status": response.http_status,
            "data": response.data,
            "errors": response.errors,
            "extensions": response.extensions,
            "partial_data": response.partial_data,
            "response_trust": response.response_trust,
        }),
        UpstreamResponseEnvelope::Mcp(response) => json!({
            "kind": "mcp",
            "structured_content": response.structured_content,
            "content": response.content,
            "is_error": response.is_error,
            "meta": response.meta,
            "response_trust": response.response_trust,
        }),
    }
}

fn upstream_success_envelope(
    resolved: &ResolvedInvocation,
    envelope: &UpstreamResponseEnvelope,
) -> JsonValue {
    json!({
        "kind": upstream_binding_label(&resolved.capability.upstream_binding),
        "capability_id": resolved.capability.capability_id.as_str(),
        "source_id": resolved.entry.source_id.as_str(),
        "provider": public_upstream_envelope(envelope),
    })
}

fn response_body_value(body: &UpstreamResponseBody) -> JsonValue {
    match body {
        UpstreamResponseBody::Json(value) => value.clone(),
        UpstreamResponseBody::Text(value) => JsonValue::String(value.clone()),
        UpstreamResponseBody::Bytes(value) => json!({ "bytes": value }),
        UpstreamResponseBody::Empty => JsonValue::Null,
    }
}

fn graphql_singleton_not_found(
    resolved: &ResolvedInvocation,
    binding: &GraphqlOperationBinding,
    response: &coral_upstream::GraphqlUpstreamResponse,
) -> bool {
    if resolved.capability.shape_hints.result_shape != ResultShapeHint::Singleton {
        return false;
    }
    let Some(data) = response.data.as_ref() else {
        return false;
    };
    let path = graphql_lookup_path(resolved, binding);
    if !matches!(
        json_value_at_path(data, &path),
        None | Some(JsonValue::Null)
    ) {
        return false;
    }
    !response.errors.is_empty()
        && response
            .errors
            .iter()
            .all(|error| graphql_error_is_not_found_for_path(error, &path))
}

fn graphql_lookup_path(
    resolved: &ResolvedInvocation,
    binding: &GraphqlOperationBinding,
) -> Vec<String> {
    resolved
        .capability
        .shape_hints
        .row_path_candidates
        .first()
        .filter(|path| !path.is_empty())
        .cloned()
        .unwrap_or_else(|| binding.response_path.clone())
}

fn json_value_at_path<'a>(value: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path {
        let JsonValue::Object(object) = current else {
            return None;
        };
        current = object.get(segment)?;
    }
    Some(current)
}

fn graphql_error_is_not_found_for_path(error: &JsonValue, path: &[String]) -> bool {
    let message = error
        .get("message")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let code = error
        .pointer("/extensions/code")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let not_found =
        message.contains("not found") || message.contains("could not find") || code == "not_found";
    if !not_found {
        return false;
    }
    let Some(error_path) = error.get("path").and_then(JsonValue::as_array) else {
        return false;
    };
    error_path.len() == path.len()
        && error_path
            .iter()
            .zip(path)
            .all(|(error_segment, path_segment)| {
                error_segment
                    .as_str()
                    .is_some_and(|error_segment| error_segment == path_segment)
            })
}

fn graphql_error_response(
    resolved: &ResolvedInvocation,
    response: &coral_upstream::GraphqlUpstreamResponse,
) -> InvokeCapabilityResponse {
    let errors = bounded_provider_diagnostic_value(JsonValue::Array(response.errors.clone()));
    let partial_data = response
        .partial_data
        .clone()
        .map_or(JsonValue::Null, bounded_provider_diagnostic_value);
    let data = response
        .data
        .clone()
        .map_or(JsonValue::Null, bounded_provider_diagnostic_value);
    error_response(
        "provider_error",
        format!(
            "capability '{}' upstream invocation failed: graphql_error",
            resolved.capability.capability_id
        ),
        json!({
            "provider_error": {
                "kind": "graphql_error",
                "errors": errors,
                "partial_data": partial_data,
                "data": data,
                "http_status": response.http_status,
                "media_type": response.media_type,
            },
            "capability_id": resolved.capability.capability_id.as_str(),
            "source_id": resolved.entry.source_id.as_str(),
        }),
    )
}

fn upstream_error_response(
    resolved: &ResolvedInvocation,
    error: UpstreamError,
) -> InvokeCapabilityResponse {
    let (kind, detail) = match error {
        UpstreamError::Provider { kind, detail } => (kind.to_string(), detail),
        UpstreamError::Unsupported(detail) => ("unsupported".to_string(), detail),
        UpstreamError::InvalidResponse(detail) => ("invalid_response".to_string(), detail),
        UpstreamError::Transport(detail) => ("transport_error".to_string(), detail),
    };
    let detail = upstream_error_detail_value(detail);
    error_response(
        "provider_error",
        format!(
            "capability '{}' upstream invocation failed: {kind}",
            resolved.capability.capability_id
        ),
        json!({
            "provider_error": {
                "kind": kind,
                "detail": detail,
            },
            "capability_id": resolved.capability.capability_id.as_str(),
            "source_id": resolved.entry.source_id.as_str(),
        }),
    )
}

fn upstream_error_detail_value(detail: String) -> JsonValue {
    serde_json::from_str(&detail).unwrap_or(JsonValue::String(detail))
}

fn invoke_file_read(
    resolved: &ResolvedInvocation,
    binding: &FileScanBinding,
    args: &JsonMap<String, JsonValue>,
) -> InvokeCapabilityResponse {
    let limit = match file_read_limit(args) {
        Ok(limit) => limit,
        Err(response) => return response,
    };
    let selected_file_id = match optional_file_id(args) {
        Ok(file_id) => file_id,
        Err(response) => return response,
    };
    for key in args.keys() {
        if key != "limit" && key != "file_id" {
            return error_response(
                "invalid_args",
                format!("unsupported FileRead argument '{key}'"),
                json!({ "supported_args": ["limit", "file_id"] }),
            );
        }
    }

    let files = binding
        .file_refs
        .iter()
        .filter(|file| {
            selected_file_id
                .as_ref()
                .is_none_or(|selected| selected == &file.id)
        })
        .collect::<Vec<_>>();
    if selected_file_id.is_some() && files.is_empty() {
        return error_response(
            "not_found",
            "requested file_id is not part of this capability",
            json!({ "file_id": selected_file_id }),
        );
    }

    let file_count = files.len();
    let mut rows = Vec::new();
    for file in files {
        let remaining = limit.saturating_sub(rows.len());
        if remaining == 0 {
            break;
        }
        let artifact_path =
            match trusted_file_artifact_path(&resolved.source_materialized_dir, file) {
                Ok(path) => path,
                Err(response) => return response,
            };
        let mut file_rows =
            match read_file_artifact_rows(&artifact_path, &binding.format, remaining) {
                Ok(value) => value,
                Err(response) => return response,
            };
        rows.append(&mut file_rows);
    }

    let row_count = rows.len();
    success_response(
        JsonValue::Array(rows),
        json!({
            "kind": "file_read",
            "capability_id": resolved.capability.capability_id.as_str(),
            "source_id": resolved.entry.source_id.as_str(),
            "file_count": file_count,
            "row_count": row_count,
        }),
    )
}

fn file_read_limit(args: &JsonMap<String, JsonValue>) -> Result<usize, InvokeCapabilityResponse> {
    let Some(limit) = args.get("limit") else {
        return Ok(DEFAULT_FILE_READ_LIMIT);
    };
    let Some(limit) = limit.as_u64() else {
        return Err(error_response(
            "invalid_args",
            "FileRead limit must be a positive integer",
            json!({ "arg": "limit" }),
        ));
    };
    if limit == 0 {
        return Err(error_response(
            "invalid_args",
            "FileRead limit must be greater than zero",
            json!({ "arg": "limit" }),
        ));
    }
    let limit = usize::try_from(limit).unwrap_or(MAX_FILE_READ_LIMIT);
    Ok(limit.min(MAX_FILE_READ_LIMIT))
}

fn optional_file_id(
    args: &JsonMap<String, JsonValue>,
) -> Result<Option<String>, InvokeCapabilityResponse> {
    let Some(file_id) = args.get("file_id") else {
        return Ok(None);
    };
    let Some(file_id) = file_id.as_str() else {
        return Err(error_response(
            "invalid_args",
            "FileRead file_id must be a string",
            json!({ "arg": "file_id" }),
        ));
    };
    let file_id = file_id.trim();
    if file_id.is_empty() {
        return Err(error_response(
            "invalid_args",
            "FileRead file_id must not be empty",
            json!({ "arg": "file_id" }),
        ));
    }
    Ok(Some(file_id.to_string()))
}

fn trusted_file_artifact_path(
    source_materialized_dir: &Path,
    file: &FileArtifactRef,
) -> Result<PathBuf, InvokeCapabilityResponse> {
    let relative = Path::new(&file.source_local_path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(error_response(
            "artifact_mismatch",
            "FileRead artifact ref must be source-local",
            json!({ "file_id": file.id }),
        ));
    }
    let path = source_materialized_dir.join(relative);
    let base = source_materialized_dir.canonicalize().map_err(|error| {
        error_response(
            "not_found",
            "installed source materialization is missing",
            json!({ "source_materialization_error": error.to_string() }),
        )
    })?;
    let canonical = path.canonicalize().map_err(|error| {
        error_response(
            "not_found",
            "FileRead artifact is missing",
            json!({ "file_id": file.id, "artifact_error": error.to_string() }),
        )
    })?;
    if !canonical.starts_with(&base) {
        return Err(error_response(
            "artifact_mismatch",
            "FileRead artifact escaped the installed source directory",
            json!({ "file_id": file.id }),
        ));
    }
    Ok(canonical)
}

fn read_file_artifact_rows(
    path: &Path,
    format: &FileFormatDescriptor,
    limit: usize,
) -> Result<Vec<JsonValue>, InvokeCapabilityResponse> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        error_response(
            "not_found",
            "FileRead artifact metadata is unavailable",
            json!({ "artifact_error": error.to_string() }),
        )
    })?;
    if metadata.len() > MAX_FILE_READ_BYTES {
        return Err(error_response(
            "unsupported",
            format!("FileRead artifact exceeds the {MAX_FILE_READ_BYTES} byte invocation limit"),
            json!({ "limit_bytes": MAX_FILE_READ_BYTES, "artifact_bytes": metadata.len() }),
        ));
    }
    let raw = std::fs::read_to_string(path).map_err(|error| {
        error_response(
            "unsupported",
            "FileRead artifact is not UTF-8 text in the invocation path",
            json!({ "artifact_error": error.to_string() }),
        )
    })?;
    match format {
        FileFormatDescriptor::Json => read_json_rows(&raw, limit),
        FileFormatDescriptor::Jsonl => read_jsonl_rows(&raw, limit),
        FileFormatDescriptor::Csv => read_csv_rows(&raw, limit),
        FileFormatDescriptor::Parquet => Err(error_response(
            "unsupported",
            "FileRead Parquet invocation is not implemented yet; use the SQL projection",
            json!({ "format": "parquet" }),
        )),
    }
}

fn read_json_rows(raw: &str, limit: usize) -> Result<Vec<JsonValue>, InvokeCapabilityResponse> {
    let value = serde_json::from_str::<JsonValue>(raw).map_err(|error| {
        error_response(
            "provider_error",
            "FileRead JSON artifact could not be parsed",
            json!({ "provider_error": { "kind": "invalid_response", "detail": error.to_string() } }),
        )
    })?;
    match value {
        JsonValue::Array(values) => Ok(values.into_iter().take(limit).collect()),
        value => Ok(vec![value].into_iter().take(limit).collect()),
    }
}

fn read_jsonl_rows(raw: &str, limit: usize) -> Result<Vec<JsonValue>, InvokeCapabilityResponse> {
    let mut rows = Vec::new();
    for (line_index, line) in raw.lines().enumerate() {
        if rows.len() >= limit {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = serde_json::from_str::<JsonValue>(line).map_err(|error| {
            error_response(
                "provider_error",
                "FileRead JSONL artifact could not be parsed",
                json!({
                    "provider_error": {
                        "kind": "invalid_response",
                        "line": line_index + 1,
                        "detail": error.to_string(),
                    }
                }),
            )
        })?;
        rows.push(value);
    }
    Ok(rows)
}

fn read_csv_rows(raw: &str, limit: usize) -> Result<Vec<JsonValue>, InvokeCapabilityResponse> {
    let mut reader = csv::Reader::from_reader(raw.as_bytes());
    let headers = reader
        .headers()
        .map_err(|error| {
            error_response(
                "provider_error",
                "FileRead CSV artifact header could not be parsed",
                json!({ "provider_error": { "kind": "invalid_response", "detail": error.to_string() } }),
            )
        })?
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for record in reader.records().take(limit) {
        let record = record.map_err(|error| {
            error_response(
                "provider_error",
                "FileRead CSV artifact row could not be parsed",
                json!({ "provider_error": { "kind": "invalid_response", "detail": error.to_string() } }),
            )
        })?;
        let mut row = JsonMap::new();
        for (index, header) in headers.iter().enumerate() {
            let value = record.get(index).unwrap_or_default();
            row.insert(header.clone(), JsonValue::String(value.to_string()));
        }
        rows.push(JsonValue::Object(row));
    }
    Ok(rows)
}

fn trimmed_nonempty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn normalized_binding_path(
    raw_path: &[String],
) -> Result<Option<Vec<String>>, InvokeCapabilityResponse> {
    if raw_path.is_empty() {
        return Ok(None);
    }
    let mut path = Vec::with_capacity(raw_path.len());
    for segment in raw_path {
        let segment = segment.trim();
        if segment.is_empty() {
            return Err(error_response(
                "invalid_request",
                "binding_path must not contain empty segments",
                JsonValue::Null,
            ));
        }
        path.push(segment.to_string());
    }
    Ok(Some(path))
}

fn error_response(
    kind: impl Into<String>,
    message: impl Into<String>,
    details: JsonValue,
) -> InvokeCapabilityResponse {
    InvokeCapabilityResponse {
        ok: false,
        value: None,
        error: Some(InvokeCapabilityError {
            kind: kind.into(),
            message: message.into(),
            details: Some(json_value_to_proto(details)),
        }),
        envelope: None,
    }
}

fn success_response(value: JsonValue, envelope: JsonValue) -> InvokeCapabilityResponse {
    InvokeCapabilityResponse {
        ok: true,
        value: Some(json_value_to_proto(value)),
        error: None,
        envelope: Some(json_value_to_proto(envelope)),
    }
}

fn upstream_binding_label(binding: &UpstreamBinding) -> &'static str {
    match binding {
        UpstreamBinding::Rest(_) => "rest",
        UpstreamBinding::McpTool(_) => "mcp_tool",
        UpstreamBinding::Graphql(_) => "graphql",
        UpstreamBinding::FileRead(_) => "file_read",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::credentials::{
        CredentialManager, CredentialSetId, CredentialStorageKind, CredentialStore,
    };
    use crate::discovery::manager::LoadedSourceRuntime;
    use crate::sources::SourceName;
    use crate::state::AppStateLayout;
    use coral_api::v1::json_value as proto_json_value;
    use coral_capabilities::{
        EffectProfile, FileArtifactRef, FileFormatDescriptor, FileScanBinding,
        GraphqlOperationKind, GraphqlVariableBinding, HttpMethod, InvocationSchema, McpTaskSupport,
        ProviderOrigin, ProviderOriginKind, RestRequestBody, RestResponseVariant, ShapeHints,
        SourceId, StatusRange,
    };
    use coral_exports::{
        BindingBuildContext, ExportRef, SourceKey, TypescriptBinding, WorkspaceExportSource,
        WorkspaceExports,
    };
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn workspace_exports(
        capability: Capability,
        binding_path: Vec<String>,
        source_materialized_dir: PathBuf,
    ) -> LoadedWorkspaceExports {
        let source_id = capability.source_id.clone();
        let source_key = SourceKey("github".to_string());
        let ctx = BindingBuildContext {
            source_id: source_id.clone(),
            display_name: "GitHub".to_string(),
            source_key: source_key.clone(),
        };
        let mut entry = CapabilityExport::from_capability(&capability, &ctx);
        entry.bindings.push(Binding::Typescript(TypescriptBinding {
            ref_: ExportRef::typescript(&binding_path),
            path: binding_path,
            args_type_name: "Args".to_string(),
            result_type_name: "Result".to_string(),
        }));
        LoadedWorkspaceExports {
            exports: WorkspaceExports {
                artifact_schema_version: 1,
                workspace_id: "default".to_string(),
                sources: vec![WorkspaceExportSource {
                    source_id: source_id.clone(),
                    display_name: "GitHub".to_string(),
                    source_key,
                    source_exports_generator_version: "test".to_string(),
                }],
                entries: vec![entry],
                diagnostics: Vec::new(),
            },
            capability_by_id: BTreeMap::from([(capability.capability_id.clone(), capability)]),
            source_materialized_dir_by_id: BTreeMap::from([(
                source_id.clone(),
                source_materialized_dir,
            )]),
            source_runtime_by_id: BTreeMap::from([(
                source_id,
                LoadedSourceRuntime {
                    name: SourceName::parse("github").expect("source name"),
                    credential_storage: CredentialStorageKind::File,
                    variables: BTreeMap::new(),
                },
            )]),
        }
    }

    fn response_value(response: &InvokeCapabilityResponse) -> JsonValue {
        response
            .value
            .clone()
            .map_or(JsonValue::Null, proto_json_value_to_json)
    }

    fn response_envelope(response: &InvokeCapabilityResponse) -> JsonValue {
        response
            .envelope
            .clone()
            .map_or(JsonValue::Null, proto_json_value_to_json)
    }

    fn assert_json_pointer(value: &JsonValue, pointer: &str, expected: &JsonValue) {
        assert_eq!(value.pointer(pointer), Some(expected));
    }

    fn assert_rest_response_headers_are_exposed(envelope: &JsonValue) {
        assert_json_pointer(
            envelope,
            "/provider/headers/link",
            &json!(
                r#"<https://user:pass@api.example.test/api/v1/items?page=2&access_token=link-token&password=secret&code=oauth-code&session=session-id&jwt=jwt-value#access_token=fragment-token>; rel="next""#
            ),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/location",
            &json!("https://api.example.test/api/v1/items?page=2&X-Amz-Signature=signed"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/content-location",
            &json!("https://api.example.test/api/v1/items?page=2&token=content-token"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/authorization",
            &json!("Bearer provider-token"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/proxy-authorization",
            &json!("Bearer proxy-token"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/x-ratelimit-remaining",
            &json!("4999"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/x-ratelimit-token",
            &json!("provider-rate-limit-token"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/set-cookie",
            &json!("provider-session=secret"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/refresh",
            &json!("0; url=https://api.example.test/items?access_token=refresh-token"),
        );
        assert_json_pointer(
            envelope,
            "/provider/headers/x-api-key",
            &json!("provider-api-key"),
        );
    }

    fn proto_json_value_to_json(value: coral_api::v1::JsonValue) -> JsonValue {
        match value.kind {
            Some(proto_json_value::Kind::NullValue(_)) | None => JsonValue::Null,
            Some(proto_json_value::Kind::BoolValue(value)) => JsonValue::Bool(value),
            Some(proto_json_value::Kind::IntegerValue(value)) => json!(value),
            Some(proto_json_value::Kind::UnsignedIntegerValue(value)) => json!(value),
            Some(proto_json_value::Kind::DoubleValue(value)) => json!(value),
            Some(proto_json_value::Kind::StringValue(value)) => JsonValue::String(value),
            Some(proto_json_value::Kind::ArrayValue(array)) => JsonValue::Array(
                array
                    .values
                    .into_iter()
                    .map(proto_json_value_to_json)
                    .collect(),
            ),
            Some(proto_json_value::Kind::ObjectValue(object)) => JsonValue::Object(
                object
                    .fields
                    .into_iter()
                    .map(|(key, value)| (key, proto_json_value_to_json(value)))
                    .collect(),
            ),
        }
    }

    fn file_capability() -> Capability {
        let mut capability = Capability::new(
            SourceId("src_github".to_string()),
            "files",
            "read_files",
            ProviderOrigin {
                kind: ProviderOriginKind::FileRelation,
                snapshot_ref: "interfaces/files/provider-snapshot.yaml".to_string(),
                provider_name: "files".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: vec![FileArtifactRef {
                    id: "file_0".to_string(),
                    source_local_path: "interfaces/files/files/file_0".to_string(),
                    display_name: Some("issues.jsonl".to_string()),
                }],
                format: FileFormatDescriptor::Jsonl,
                schema_ref: None,
            }),
        );
        capability.effect_profile = EffectProfile::read();
        capability
    }

    fn rest_capability() -> Capability {
        let mut capability = Capability::new(
            SourceId("src_github".to_string()),
            "rest",
            "get_item",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: "interfaces/rest/provider-snapshot.yaml#/operations/get_item"
                    .to_string(),
                provider_name: "getItem".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Rest(RestUpstreamBinding {
                operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/get_item"
                    .to_string(),
                method: HttpMethod::Get,
                path_template: "/items/{id}".to_string(),
                parameter_bindings: vec![RestParameterBinding {
                    name: "id".to_string(),
                    location: RestParameterLocation::Path,
                    required: true,
                    style: "simple".to_string(),
                    explode: false,
                    allow_reserved: false,
                }],
                request_bodies: Vec::new(),
                responses: vec![RestResponseVariant {
                    status: StatusRange::Code { code: 200 },
                    media_type: "application/json".to_string(),
                    schema: InvocationSchema::new(json!({"type": "object"})),
                }],
                pagination: None,
            }),
        );
        capability.effect_profile = EffectProfile::read();
        capability
    }

    fn mutation_rest_capability() -> Capability {
        let mut capability = rest_capability();
        capability.effect_profile = EffectProfile::write();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.method = HttpMethod::Post;
        capability
    }

    fn mcp_capability() -> Capability {
        let mut capability = Capability::new(
            SourceId("src_github".to_string()),
            "mcp",
            "search_issues",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/search_issues"
                    .to_string(),
                provider_name: "search_issues".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_github/interface/mcp/server/default".to_string(),
                tool_name: "search_issues".to_string(),
                task_support: McpTaskSupport::Unknown,
            }),
        );
        capability.effect_profile = EffectProfile::read();
        capability
    }

    #[test]
    fn rest_url_preserves_base_path_and_escapes_path_arguments() {
        let capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
            panic!("expected REST binding");
        };
        let url = rest_url(
            "https://api.example.com/api/v1",
            binding,
            &JsonMap::from_iter([("path".to_string(), json!({ "id": "hello world/a/b" }))]),
        )
        .expect("REST URL");

        assert_eq!(
            url.as_str(),
            "https://api.example.com/api/v1/items/hello%20world%2Fa%2Fb"
        );
    }

    #[test]
    fn rest_url_accepts_flat_path_arguments() {
        let capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
            panic!("expected REST binding");
        };
        let url = rest_url(
            "https://api.example.com/api/v1",
            binding,
            &JsonMap::from_iter([("id".to_string(), json!("hello world/a/b"))]),
        )
        .expect("REST URL");

        assert_eq!(
            url.as_str(),
            "https://api.example.com/api/v1/items/hello%20world%2Fa%2Fb"
        );
    }

    #[test]
    fn rest_url_infers_path_arguments_from_template_when_bindings_are_stale() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/repos/{owner}/{repo}/pulls/{pull_number}/reviews".to_string();
        binding.parameter_bindings.clear();

        let url = rest_url(
            "https://api.github.test",
            binding,
            &JsonMap::from_iter([
                ("owner".to_string(), json!("withcoral")),
                ("repo".to_string(), json!("coral")),
                ("pull_number".to_string(), json!(1051)),
            ]),
        )
        .expect("REST URL");

        assert_eq!(
            url.as_str(),
            "https://api.github.test/repos/withcoral/coral/pulls/1051/reviews"
        );
    }

    #[test]
    fn rest_url_rejects_missing_inferred_path_argument_before_upstream_call() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/users/{username}/events".to_string();
        binding.parameter_bindings.clear();

        let error = rest_url("https://api.github.test", binding, &JsonMap::new())
            .expect_err("missing template argument should fail locally");

        assert_eq!(
            error.error.as_ref().map(|error| error.kind.as_str()),
            Some("invalid_args")
        );
        assert!(
            error
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains("username"))
        );
    }

    #[test]
    fn rest_url_location_arguments_take_precedence_over_flat_arguments() {
        let capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
            panic!("expected REST binding");
        };
        let url = rest_url(
            "https://api.example.com/api/v1",
            binding,
            &JsonMap::from_iter([
                ("id".to_string(), json!("flat")),
                ("path".to_string(), json!({ "id": "nested" })),
            ]),
        )
        .expect("REST URL");

        assert_eq!(url.as_str(), "https://api.example.com/api/v1/items/nested");
    }

    #[test]
    fn invocation_validation_accepts_mixed_grouped_and_flat_rest_args() {
        let capability = rest_capability();

        validate_invocation_args(
            &capability,
            &JsonMap::from_iter([
                ("id".to_string(), json!("flat")),
                ("path".to_string(), json!({ "id": "nested" })),
            ]),
        )
        .expect("mixed duplicate grouped and flat args should validate");

        validate_invocation_args(
            &capability,
            &JsonMap::from_iter([
                ("id".to_string(), json!("flat")),
                ("path".to_string(), json!({})),
            ]),
        )
        .expect("flat fallback should satisfy a required grouped arg");
    }

    #[test]
    fn rest_query_parameters_accept_flat_arguments() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/search/issues".to_string();
        binding.parameter_bindings = vec![RestParameterBinding {
            name: "q".to_string(),
            location: RestParameterLocation::Query,
            required: true,
            style: "form".to_string(),
            explode: true,
            allow_reserved: false,
        }];
        let mut url = rest_url(
            "https://api.example.com/api/v1",
            binding,
            &JsonMap::from_iter([("q".to_string(), json!("repo:withcoral/coral is:pr"))]),
        )
        .expect("REST URL");
        let mut headers = Vec::new();
        apply_rest_parameters(
            &mut url,
            &mut headers,
            &[],
            &binding.parameter_bindings,
            &JsonMap::from_iter([("q".to_string(), json!("repo:withcoral/coral is:pr"))]),
        )
        .expect("REST parameters");

        assert_eq!(
            url.as_str(),
            "https://api.example.com/api/v1/search/issues?q=repo%3Awithcoral%2Fcoral+is%3Apr"
        );
    }

    #[test]
    fn rest_argument_validation_accepts_nested_query_objects() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/search/issues".to_string();
        binding.parameter_bindings = vec![RestParameterBinding {
            name: "q".to_string(),
            location: RestParameterLocation::Query,
            required: true,
            style: "form".to_string(),
            explode: true,
            allow_reserved: false,
        }];

        reject_unconsumed_rest_args(
            binding,
            &JsonMap::from_iter([(
                "query".to_string(),
                json!({ "q": "repo:withcoral/coral is:pr" }),
            )]),
        )
        .expect("nested query args should be accepted");
    }

    #[test]
    fn rest_argument_validation_rejects_unknown_top_level_args() {
        let capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
            panic!("expected REST binding");
        };

        let error = reject_unconsumed_rest_args(
            binding,
            &JsonMap::from_iter([
                ("id".to_string(), json!(42)),
                ("unused".to_string(), json!("ignored")),
            ]),
        )
        .expect_err("unknown top-level arg should fail");

        assert_eq!(
            error.error.as_ref().map(|error| error.kind.as_str()),
            Some("invalid_args")
        );
        assert!(
            error
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains("unused"))
        );
    }

    #[test]
    fn rest_argument_validation_rejects_unknown_nested_args() {
        let capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
            panic!("expected REST binding");
        };

        let error = reject_unconsumed_rest_args(
            binding,
            &JsonMap::from_iter([("path".to_string(), json!({ "id": 42, "unused": "ignored" }))]),
        )
        .expect_err("unknown nested arg should fail");

        assert_eq!(
            error.error.as_ref().map(|error| error.kind.as_str()),
            Some("invalid_args")
        );
        assert!(
            error
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains("path argument 'unused'"))
        );
    }

    #[test]
    fn rest_argument_validation_rejects_body_args_without_request_body() {
        let capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
            panic!("expected REST binding");
        };

        let error = reject_unconsumed_rest_args(
            binding,
            &JsonMap::from_iter([("body".to_string(), json!({ "title": "hello" }))]),
        )
        .expect_err("body args without request body should fail");

        assert_eq!(
            error.error.as_ref().map(|error| error.kind.as_str()),
            Some("invalid_args")
        );
        assert!(
            error
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains("does not accept request body"))
        );
    }

    #[test]
    fn rest_query_parameters_serialize_exploded_arrays() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/issues".to_string();
        binding.parameter_bindings = vec![RestParameterBinding {
            name: "labels".to_string(),
            location: RestParameterLocation::Query,
            required: false,
            style: "form".to_string(),
            explode: true,
            allow_reserved: false,
        }];
        let mut url =
            rest_url("https://api.example.com/api/v1", binding, &JsonMap::new()).expect("url");
        apply_rest_parameters(
            &mut url,
            &mut Vec::new(),
            &[],
            &binding.parameter_bindings,
            &JsonMap::from_iter([("labels".to_string(), json!(["bug", "help wanted"]))]),
        )
        .expect("REST parameters");

        assert_eq!(
            url.query_pairs()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<Vec<_>>(),
            vec![
                ("labels".to_string(), "bug".to_string()),
                ("labels".to_string(), "help wanted".to_string()),
            ]
        );
    }

    #[test]
    fn rest_query_parameters_serialize_deep_objects() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/issues".to_string();
        binding.parameter_bindings = vec![RestParameterBinding {
            name: "filter".to_string(),
            location: RestParameterLocation::Query,
            required: false,
            style: "deepObject".to_string(),
            explode: true,
            allow_reserved: false,
        }];
        let mut url =
            rest_url("https://api.example.com/api/v1", binding, &JsonMap::new()).expect("url");
        apply_rest_parameters(
            &mut url,
            &mut Vec::new(),
            &[],
            &binding.parameter_bindings,
            &JsonMap::from_iter([(
                "query".to_string(),
                json!({ "filter": { "state": "open", "author": "jsummerfield" } }),
            )]),
        )
        .expect("REST parameters");

        assert_eq!(
            url.query_pairs()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                ("filter[author]".to_string(), "jsummerfield".to_string()),
                ("filter[state]".to_string(), "open".to_string()),
            ])
        );
    }

    #[test]
    fn rest_header_parameters_reject_auth_header_collisions() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.parameter_bindings = vec![RestParameterBinding {
            name: "X-Api-Token".to_string(),
            location: RestParameterLocation::Header,
            required: false,
            style: "simple".to_string(),
            explode: false,
            allow_reserved: false,
        }];
        let mut url = rest_url(
            "https://api.example.com/api/v1",
            binding,
            &JsonMap::from_iter([("path".to_string(), json!({ "id": 42 }))]),
        )
        .expect("url");
        let error = apply_rest_parameters(
            &mut url,
            &mut Vec::new(),
            &["x-api-token".to_string()],
            &binding.parameter_bindings,
            &JsonMap::from_iter([(
                "header".to_string(),
                json!({ "X-Api-Token": "caller-token" }),
            )]),
        )
        .expect_err("auth header collision should be rejected");

        assert_eq!(
            error.error.as_ref().map(|error| error.kind.as_str()),
            Some("invalid_request")
        );
        assert!(
            error
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains("source auth header"))
        );
    }

    #[test]
    fn rest_cookie_parameters_ignore_absent_optional_values() {
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.parameter_bindings.push(RestParameterBinding {
            name: "session".to_string(),
            location: RestParameterLocation::Cookie,
            required: false,
            style: "form".to_string(),
            explode: true,
            allow_reserved: false,
        });
        let mut url = rest_url(
            "https://api.example.com/api/v1",
            binding,
            &JsonMap::from_iter([("path".to_string(), json!({ "id": 42 }))]),
        )
        .expect("url");
        let mut headers = Vec::new();

        apply_rest_parameters(
            &mut url,
            &mut headers,
            &[],
            &binding.parameter_bindings,
            &JsonMap::from_iter([("path".to_string(), json!({ "id": 42 }))]),
        )
        .expect("absent optional cookie should not block invocation");

        assert!(headers.is_empty());
    }

    #[test]
    fn rest_cookie_parameters_reject_required_or_supplied_values() {
        for (required, args) in [
            (true, JsonMap::new()),
            (
                false,
                JsonMap::from_iter([("cookie".to_string(), json!({ "session": "abc" }))]),
            ),
        ] {
            let parameter = RestParameterBinding {
                name: "session".to_string(),
                location: RestParameterLocation::Cookie,
                required,
                style: "form".to_string(),
                explode: true,
                allow_reserved: false,
            };
            let mut url = Url::parse("https://api.example.com/items").expect("url");
            let error = apply_rest_parameters(&mut url, &mut Vec::new(), &[], &[parameter], &args)
                .expect_err("cookie should fail");

            assert!(
                error.error.as_ref().is_some_and(|error| {
                    matches!(error.kind.as_str(), "invalid_args" | "unsupported")
                }),
                "{error:?}"
            );
        }
    }

    #[test]
    fn rest_request_body_uses_selected_json_media_type() {
        let mut capability = mutation_rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.request_bodies = vec![
            RestRequestBody {
                media_type: "application/xml".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({"type": "string"})),
            },
            RestRequestBody {
                media_type: "application/vnd.github+json".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({"type": "object"})),
            },
        ];

        let selected = rest_request_body(
            binding,
            &JsonMap::from_iter([
                (
                    "contentType".to_string(),
                    json!("application/vnd.github+json"),
                ),
                ("body".to_string(), json!({ "title": "hello" })),
            ]),
        )
        .expect("request body");

        assert_eq!(
            selected.body,
            Some(UpstreamRequestBody::Json(json!({ "title": "hello" })))
        );
        assert_eq!(
            selected.content_type.as_deref(),
            Some("application/vnd.github+json")
        );
    }

    #[test]
    fn rest_request_body_rejects_multiple_body_aliases() {
        let mut capability = mutation_rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.request_bodies = vec![RestRequestBody {
            media_type: "application/json".to_string(),
            required: true,
            schema: InvocationSchema::new(json!({"type": "object"})),
        }];

        let error = rest_request_body(
            binding,
            &JsonMap::from_iter([
                ("body".to_string(), json!({ "title": "old" })),
                ("json".to_string(), json!({ "title": "new" })),
            ]),
        )
        .err()
        .expect("duplicate body aliases should fail");

        assert!(error.error.as_ref().is_some_and(|error| {
            error.kind == "invalid_args"
                && error
                    .message
                    .contains("request body aliases are mutually exclusive")
        }));
    }

    #[test]
    fn invocation_validation_rejects_unresolved_rest_body_refs() {
        let mut capability = mutation_rest_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "properties": {
                "body": { "$ref": "#/components/schemas/Issue" }
            },
            "additionalProperties": false
        }));
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/items".to_string();
        binding.parameter_bindings.clear();
        binding.request_bodies = vec![RestRequestBody {
            media_type: "application/json".to_string(),
            required: true,
            schema: InvocationSchema::new(json!({
                "$ref": "#/components/schemas/Issue"
            })),
        }];

        let error = validate_invocation_args(
            &capability,
            &JsonMap::from_iter([("body".to_string(), json!({ "title": "hello" }))]),
        )
        .expect_err("unresolved request body refs should fail closed");

        assert!(error.error.as_ref().is_some_and(|error| {
            error.kind == "artifact_mismatch"
                && error
                    .message
                    .contains("unresolved executable input schema references")
                && error.message.contains("re-add the source")
        }));
    }

    #[test]
    fn rest_request_body_rejects_multiple_media_type_selectors() {
        let mut capability = mutation_rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.request_bodies = vec![
            RestRequestBody {
                media_type: "application/vnd.github+json".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({"type": "object"})),
            },
            RestRequestBody {
                media_type: "application/json".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({"type": "object"})),
            },
        ];

        let error = rest_request_body(
            binding,
            &JsonMap::from_iter([
                (
                    "contentType".to_string(),
                    json!("application/vnd.github+json"),
                ),
                ("media_type".to_string(), json!("application/json")),
                ("body".to_string(), json!({ "title": "hello" })),
            ]),
        )
        .err()
        .expect("duplicate media selectors should fail");

        assert!(error.error.as_ref().is_some_and(|error| {
            error.kind == "invalid_args"
                && error
                    .message
                    .contains("media selector aliases are mutually exclusive")
        }));
    }

    fn graphql_capability() -> Capability {
        let mut capability = Capability::new(
            SourceId("src_github".to_string()),
            "graph",
            "query_ratelimit",
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref:
                    "interfaces/graph/provider-snapshot.yaml#/root_fields/query_ratelimit"
                        .to_string(),
                provider_name: "rateLimit".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(GraphqlOperationBinding {
                endpoint_ref: "source/src_github/interface/graph/endpoint/default".to_string(),
                operation_name: "QueryRatelimit".to_string(),
                graphql_operation_kind: GraphqlOperationKind::Query,
                document_ref: "source/src_github/interface/graph/generated/query_ratelimit.graphql"
                    .to_string(),
                selection_set: None,
                variable_bindings: Vec::<GraphqlVariableBinding>::new(),
                response_path: vec!["rateLimit".to_string()],
            }),
        );
        capability.effect_profile = EffectProfile::read();
        capability
    }

    fn write_installed_manifest(source_dir: &Path, manifest_yaml: &str) -> PathBuf {
        std::fs::create_dir_all(source_dir.join("materialized/source")).expect("source dirs");
        std::fs::write(source_dir.join("manifest.yaml"), manifest_yaml).expect("manifest");
        source_dir.join("materialized/source")
    }

    fn write_graphql_operation_document(
        materialized_dir: &Path,
        interface_id: &str,
        filename: &str,
        document: &str,
    ) {
        let operations_dir = materialized_dir
            .join("interfaces")
            .join(interface_id)
            .join(crate::graphql_documents::GENERATED_GRAPHQL_OPERATIONS_DIR);
        std::fs::create_dir_all(&operations_dir).expect("graphql operations dir");
        std::fs::write(operations_dir.join(filename), document).expect("graphql operation doc");
    }

    fn graphql_rate_limit_workspace(
        endpoint: &str,
    ) -> (tempfile::TempDir, LoadedWorkspaceExports, String) {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { rateLimit: String }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: graph
    type: graphql
    endpoint: {endpoint}
    schema:
      kind: sdl_file
      file: ./schema.graphql
"
            ),
        );
        let mut capability = graphql_capability();
        capability.shape_hints = ShapeHints::singleton_at_path(vec!["rateLimit".to_string()]);
        write_graphql_operation_document(
            &materialized_dir,
            "graph",
            "query_ratelimit.graphql",
            "query QueryRatelimit { rateLimit }",
        );
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "graph".to_string(),
                "rateLimit".to_string(),
            ],
            materialized_dir,
        );
        (temp, workspace, capability_id)
    }

    fn graphql_singleton_issue_workspace(
        endpoint: &str,
    ) -> (tempfile::TempDir, LoadedWorkspaceExports, String) {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { issue: Issue } type Issue { id: ID identifier: String }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: graph
    type: graphql
    endpoint: {endpoint}
    schema:
      kind: sdl_file
      file: ./schema.graphql
"
            ),
        );
        let mut capability = graphql_capability();
        capability.shape_hints = ShapeHints::singleton_at_path(vec!["issue".to_string()]);
        let UpstreamBinding::Graphql(binding) = &mut capability.upstream_binding else {
            panic!("expected GraphQL binding");
        };
        binding.operation_name = "QueryIssue".to_string();
        binding.document_ref =
            "source/src_github/interface/graph/generated/query_issue.graphql".to_string();
        binding.response_path = vec!["issue".to_string()];
        write_graphql_operation_document(
            &materialized_dir,
            "graph",
            "query_issue.graphql",
            "query QueryIssue { issue { id identifier } }",
        );
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "graph".to_string(),
                "issue".to_string(),
            ],
            materialized_dir,
        );
        (temp, workspace, capability_id)
    }

    #[tokio::test]
    async fn rest_invocation_plan_sets_provider_timeout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
    base_url: https://api.example.com
",
        );
        let capability = rest_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "id": 42 }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let UpstreamBinding::Rest(binding) = resolved.capability.upstream_binding.clone() else {
            panic!("expected REST binding");
        };

        let plan = rest_invocation_plan(
            &resolved,
            &binding,
            &JsonMap::from_iter([("id".to_string(), json!(42))]),
            None,
        )
        .await
        .expect("REST plan");

        let UpstreamInvocationPlan::Http(plan) = plan else {
            panic!("expected HTTP plan");
        };
        assert_eq!(plan.timeout, Some(DEFAULT_PROVIDER_INVOCATION_TIMEOUT));
    }

    #[tokio::test]
    async fn graphql_invocation_plan_sets_provider_timeout() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { rateLimit: Int }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: sdl_file
      file: ./schema.graphql
",
        );
        let capability = graphql_capability();
        write_graphql_operation_document(
            &materialized_dir,
            "graph",
            "query_ratelimit.graphql",
            "query QueryRatelimit { rateLimit }",
        );
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "graph".to_string(),
                "rateLimit".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.rateLimit".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let UpstreamBinding::Graphql(binding) = resolved.capability.upstream_binding.clone() else {
            panic!("expected GraphQL binding");
        };

        let plan = graphql_invocation_plan(&resolved, &binding, &JsonMap::new(), None)
            .await
            .expect("GraphQL plan");

        let UpstreamInvocationPlan::Graphql(plan) = plan else {
            panic!("expected GraphQL plan");
        };
        assert_eq!(plan.timeout, Some(DEFAULT_PROVIDER_INVOCATION_TIMEOUT));
    }

    #[tokio::test]
    async fn graphql_invocation_plan_builds_variables_and_selection_set() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { issues(first: Int): IssueConnection }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: linear_graphql
interfaces:
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: sdl_file
      file: ./schema.graphql
",
        );
        let mut capability = Capability::new(
            SourceId("src_github".to_string()),
            "graph",
            "query_issues",
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref: "interfaces/graph/provider-snapshot.yaml#/root_fields/query_issues"
                    .to_string(),
                provider_name: "issues".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(GraphqlOperationBinding {
                endpoint_ref: "source/src_github/interface/graph/endpoint/default".to_string(),
                operation_name: "QueryIssues".to_string(),
                graphql_operation_kind: GraphqlOperationKind::Query,
                document_ref: "source/src_github/interface/graph/generated/query_issues.graphql"
                    .to_string(),
                selection_set: Some(
                    "nodes { id title } pageInfo { hasNextPage endCursor }".to_string(),
                ),
                variable_bindings: vec![GraphqlVariableBinding {
                    variable_name: "first".to_string(),
                    graphql_type: Some("Int".to_string()),
                    argument_path: vec!["first".to_string()],
                    required: false,
                }],
                response_path: vec!["issues".to_string()],
            }),
        );
        capability.effect_profile = EffectProfile::read();
        write_graphql_operation_document(
            &materialized_dir,
            "graph",
            "query_issues.graphql",
            "query QueryIssues($first: Int) { issues(first: $first) { nodes { id title } pageInfo { hasNextPage endCursor } } }",
        );
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "linear_graphql".to_string(),
                "graph".to_string(),
                "issues".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:linear_graphql.graph.issues".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "first": 2 }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let UpstreamBinding::Graphql(binding) = resolved.capability.upstream_binding.clone() else {
            panic!("expected GraphQL binding");
        };

        let plan = graphql_invocation_plan(
            &resolved,
            &binding,
            &JsonMap::from_iter([("first".to_string(), json!(2))]),
            None,
        )
        .await
        .expect("GraphQL plan");

        let UpstreamInvocationPlan::Graphql(plan) = plan else {
            panic!("expected GraphQL plan");
        };
        assert_eq!(
            plan.document,
            "query QueryIssues($first: Int) { issues(first: $first) { nodes { id title } pageInfo { hasNextPage endCursor } } }"
        );
        assert_eq!(
            plan.variables,
            JsonMap::from_iter([("first".to_string(), json!(2))])
        );
    }

    #[tokio::test]
    async fn graphql_invocation_plan_requires_generated_operation_document() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { rateLimit: Int }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: sdl_file
      file: ./schema.graphql
",
        );
        let capability = graphql_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "graph".to_string(),
                "rateLimit".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.rateLimit".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let UpstreamBinding::Graphql(binding) = resolved.capability.upstream_binding.clone() else {
            panic!("expected GraphQL binding");
        };

        let error = graphql_invocation_plan(&resolved, &binding, &JsonMap::new(), None)
            .await
            .expect_err("missing generated operation document should fail before network");

        assert!(error.error.as_ref().is_some_and(|error| {
            error.kind == "artifact_mismatch"
                && error
                    .message
                    .contains("generated operation document is missing")
        }));
    }

    #[tokio::test]
    async fn mcp_invocation_plan_sets_provider_timeout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.example.com/mcp
",
        );
        let capability = mcp_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "mcp".to_string(),
                "searchIssues".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.mcp.searchIssues".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let UpstreamBinding::McpTool(binding) = resolved.capability.upstream_binding.clone() else {
            panic!("expected MCP binding");
        };

        let plan = mcp_invocation_plan(&resolved, &binding, JsonMap::new(), None)
            .await
            .expect("MCP plan");

        let UpstreamInvocationPlan::McpToolCall(plan) = plan else {
            panic!("expected MCP plan");
        };
        assert_eq!(plan.timeout, Some(DEFAULT_PROVIDER_INVOCATION_TIMEOUT));
    }

    #[tokio::test]
    async fn mcp_stdio_env_uses_installed_source_variables() {
        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
inputs:
  - key: WORKSPACE_ID
    kind: variable
interfaces:
  - id: mcp
    type: mcp
    server:
      env:
        - name: WORKSPACE_ID
          key: WORKSPACE_ID
      transport:
        type: stdio
        command: sh
        args: []
",
        );
        let capability = mcp_capability();
        let capability_id = capability.capability_id.to_string();
        let mut workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "mcp".to_string(),
                "searchIssues".to_string(),
            ],
            materialized_dir,
        );
        workspace
            .source_runtime_by_id
            .get_mut(&SourceId("src_github".to_string()))
            .expect("source runtime")
            .variables
            .insert("WORKSPACE_ID".to_string(), "acme".to_string());
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.mcp.searchIssues".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let UpstreamBinding::McpTool(binding) = resolved.capability.upstream_binding.clone() else {
            panic!("expected MCP binding");
        };

        let plan = mcp_invocation_plan(&resolved, &binding, JsonMap::new(), None)
            .await
            .expect("MCP plan");

        let UpstreamInvocationPlan::McpToolCall(plan) = plan else {
            panic!("expected MCP plan");
        };
        let McpConnectionTarget::Stdio { env, .. } = plan.server else {
            panic!("expected stdio MCP target");
        };
        let (_, value) = env
            .iter()
            .find(|(name, _)| name == "WORKSPACE_ID")
            .expect("WORKSPACE_ID env");
        assert_eq!(value.expose_secret(), "acme");
    }

    #[tokio::test]
    async fn invokes_current_file_read_binding_ref() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_dir = temp.path().join("interfaces/files/files");
        std::fs::create_dir_all(&artifact_dir).expect("artifact dir");
        std::fs::write(
            artifact_dir.join("file_0"),
            r#"{"id":1}
{"id":2}
"#,
        )
        .expect("write artifact");
        let capability = file_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "files".to_string(),
                "readFiles".to_string(),
            ],
            temp.path().to_path_buf(),
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.files.readFiles".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(response.ok);
        assert!(response.error.is_none());
        assert_eq!(response_value(&response), json!([{ "id": 1 }, { "id": 2 }]));
        assert_json_pointer(&response_envelope(&response), "/kind", &json!("file_read"));
    }

    #[tokio::test]
    async fn invokes_csv_file_read_as_header_keyed_rows() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_dir = temp.path().join("interfaces/files/files");
        std::fs::create_dir_all(&artifact_dir).expect("artifact dir");
        std::fs::write(
            artifact_dir.join("file_0"),
            "id,title\n1,\"hello, world\"\n2,plain\n",
        )
        .expect("write artifact");
        let mut capability = file_capability();
        let UpstreamBinding::FileRead(binding) = &mut capability.upstream_binding else {
            panic!("expected file binding");
        };
        binding.format = FileFormatDescriptor::Csv;
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "files".to_string(),
                "readFiles".to_string(),
            ],
            temp.path().to_path_buf(),
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.files.readFiles".to_string(),
            binding_path: Vec::new(),
            args_json: r#"{"limit":1}"#.to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            JsonMap::from_iter([("limit".to_string(), json!(1))]),
            None,
        )
        .await;

        assert!(response.ok);
        assert_eq!(
            response_value(&response),
            json!([{ "id": "1", "title": "hello, world" }])
        );
        assert_json_pointer(&response_envelope(&response), "/row_count", &json!(1));
    }

    #[tokio::test]
    async fn invokes_rest_capability_through_upstream_http() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/items/42"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Authorization", "Bearer provider-token")
                    .insert_header(
                        "Content-Location",
                        "https://api.example.test/api/v1/items?page=2&token=content-token",
                    )
                    .insert_header("Proxy-Authorization", "Bearer proxy-token")
                    .insert_header(
                        "Refresh",
                        "0; url=https://api.example.test/items?access_token=refresh-token",
                    )
                    .insert_header("Set-Cookie", "provider-session=secret")
                    .insert_header(
                        "Link",
                        r#"<https://user:pass@api.example.test/api/v1/items?page=2&access_token=link-token&password=secret&code=oauth-code&session=session-id&jwt=jwt-value#access_token=fragment-token>; rel="next""#,
                    )
                    .insert_header(
                        "Location",
                        "https://api.example.test/api/v1/items?page=2&X-Amz-Signature=signed",
                    )
                    .insert_header("X-Api-Key", "provider-api-key")
                    .insert_header("X-Auth-Token", "provider-auth-token")
                    .insert_header("X-Access-Key", "provider-access-key")
                    .insert_header("X-CSRF-Token", "provider-csrf-token")
                    .insert_header("X-Password", "provider-password")
                    .insert_header("X-RateLimit-Remaining", "4999")
                    .insert_header("X-RateLimit-Token", "provider-rate-limit-token")
                    .insert_header("X-Subject-Token", "provider-subject-token")
                    .set_body_json(json!({
                        "id": 42,
                        "name": "answer"
                    })),
            )
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
    base_url: {}/api/v1
",
                server.uri()
            ),
        );
        let capability = rest_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "path": { "id": 42 } }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            None,
        )
        .await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_json_pointer(&response_value(&response), "/name", &json!("answer"));
        let envelope = response_envelope(&response);
        assert_json_pointer(&envelope, "/kind", &json!("rest"));
        assert_json_pointer(&envelope, "/provider/status", &json!(200));
        assert_rest_response_headers_are_exposed(&envelope);
    }

    #[tokio::test]
    async fn rest_provider_error_preserves_status_and_body_details() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/items/42"))
            .respond_with(
                ResponseTemplate::new(400)
                    .append_header("Content-Type", "application/json")
                    .set_body_json(json!({
                        "errors": ["invalid monitor query"],
                        "status": "error"
                    })),
            )
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
    base_url: {}/api/v1
",
                server.uri()
            ),
        );
        let capability = rest_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "path": { "id": 42 } }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            None,
        )
        .await;

        assert!(!response.ok);
        let error = response.error.expect("error");
        assert_eq!(error.kind, "provider_error");
        let details = error
            .details
            .clone()
            .map(proto_json_value_to_json)
            .expect("error details");
        assert_json_pointer(&details, "/provider_error/detail/http_status", &json!(400));
        assert_json_pointer(
            &details,
            "/provider_error/detail/body/errors/0",
            &json!("invalid monitor query"),
        );
    }

    #[tokio::test]
    async fn invokes_github_pull_reviews_rest_capability_with_flat_arguments() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/withcoral/coral/pulls/1051/reviews"))
            .and(query_param("per_page", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {
                    "id": 1,
                    "state": "APPROVED"
                }
            ])))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
    base_url: {}
",
                server.uri()
            ),
        );
        let mut capability = rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.path_template = "/repos/{owner}/{repo}/pulls/{pull_number}/reviews".to_string();
        binding.parameter_bindings = github_pull_reviews_parameter_bindings();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "pulls".to_string(),
                "listReviews".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.pulls.listReviews".to_string(),
            binding_path: Vec::new(),
            args_json: json!({
                "owner": "withcoral",
                "repo": "coral",
                "pull_number": 1051,
                "per_page": 5
            })
            .to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            None,
        )
        .await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_json_pointer(&response_value(&response), "/0/state", &json!("APPROVED"));
    }

    fn github_pull_reviews_parameter_bindings() -> Vec<RestParameterBinding> {
        vec![
            RestParameterBinding {
                name: "owner".to_string(),
                location: RestParameterLocation::Path,
                required: true,
                style: "simple".to_string(),
                explode: false,
                allow_reserved: false,
            },
            RestParameterBinding {
                name: "repo".to_string(),
                location: RestParameterLocation::Path,
                required: true,
                style: "simple".to_string(),
                explode: false,
                allow_reserved: false,
            },
            RestParameterBinding {
                name: "pull_number".to_string(),
                location: RestParameterLocation::Path,
                required: true,
                style: "simple".to_string(),
                explode: false,
                allow_reserved: false,
            },
            RestParameterBinding {
                name: "per_page".to_string(),
                location: RestParameterLocation::Query,
                required: false,
                style: "form".to_string(),
                explode: true,
                allow_reserved: false,
            },
        ]
    }

    #[tokio::test]
    async fn invokes_rest_capability_using_openapi_server_url_when_base_url_is_omitted() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/items/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": 42,
                "name": "from-server-url"
            })))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
",
        );
        let interface_dir = materialized_dir.join("interfaces/rest");
        std::fs::create_dir_all(&interface_dir).expect("interface dir");
        std::fs::write(
            interface_dir.join("source-document.raw"),
            format!(
                r"
openapi: 3.0.3
servers:
  - url: {}
paths: {{}}
",
                server.uri()
            ),
        )
        .expect("source document");
        let capability = rest_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "path": { "id": 42 } }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            None,
        )
        .await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_json_pointer(
            &response_value(&response),
            "/name",
            &json!("from-server-url"),
        );
    }

    #[tokio::test]
    async fn rejects_remote_http_openapi_server_url_fallback() {
        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
",
        );
        let interface_dir = materialized_dir.join("interfaces/rest");
        std::fs::create_dir_all(&interface_dir).expect("interface dir");
        std::fs::write(
            interface_dir.join("source-document.raw"),
            r"
openapi: 3.0.3
servers:
  - url: http://api.example.com
paths: {}
",
        )
        .expect("source document");
        let capability = rest_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "path": { "id": 42 } }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            None,
        )
        .await;

        assert!(!response.ok);
        let error = response.error.expect("structured error");
        assert_eq!(error.kind, "unsupported");
        assert!(error.message.contains("HTTPS OpenAPI servers[].url"));
    }

    #[tokio::test]
    async fn invokes_rest_capability_with_stored_header_auth() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/items/42"))
            .and(header("X-Api-Token", "stored-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": 42,
                "name": "authorized"
            })))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("config");
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        layout.ensure().expect("layout dirs");
        let credential_manager = CredentialManager::new(CredentialStore::new(layout));
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github").expect("source name");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        credential_manager
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "stored-token".to_string())]),
            )
            .expect("seed credential");

        let materialized_dir = write_installed_manifest(
            &temp.path().join("source"),
            &format!(
                r"
spec_version: 1
kind: source
name: github
inputs:
  - key: API_TOKEN
    kind: secret
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
    base_url: {}
    auth:
      kind: header_input
      name: X-Api-Token
      key: API_TOKEN
",
                server.uri()
            ),
        );
        let capability = rest_capability();
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({ "path": { "id": 42 } }).to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            Some(InvocationRuntime {
                workspace_name: &workspace_name,
                credentials: &credential_manager,
            }),
        )
        .await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_json_pointer(&response_value(&response), "/name", &json!("authorized"));
        assert!(
            !response_envelope(&response)
                .to_string()
                .contains("stored-token")
        );
    }

    #[tokio::test]
    async fn invokes_mutation_by_default() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/items/42"))
            .and(header("Content-Type", "application/vnd.github+json"))
            .and(body_json(json!({ "title": "hello" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": 42,
                "updated": true
            })))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.json
    base_url: {}
",
                server.uri()
            ),
        );
        let mut capability = mutation_rest_capability();
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("expected REST binding");
        };
        binding.request_bodies = vec![RestRequestBody {
            media_type: "application/vnd.github+json".to_string(),
            required: true,
            schema: InvocationSchema::new(json!({"type": "object"})),
        }];
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "rest".to_string(),
                "items".to_string(),
                "getItem".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.rest.items.getItem".to_string(),
            binding_path: Vec::new(),
            args_json: json!({
                "path": { "id": 42 },
                "contentType": "application/vnd.github+json",
                "body": { "title": "hello" }
            })
            .to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(
            resolved,
            serde_json::from_str(&request.args_json).expect("args"),
            None,
        )
        .await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_json_pointer(&response_value(&response), "/updated", &json!(true));
        assert_json_pointer(&response_envelope(&response), "/kind", &json!("rest"));
    }

    #[tokio::test]
    async fn invokes_graphql_capability_through_upstream_http() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryRatelimit { rateLimit }",
                "variables": {},
                "operationName": "QueryRatelimit"
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Set-Cookie", "graphql-session=secret")
                    .set_body_json(json!({
                        "data": {
                            "rateLimit": 42
                        }
                    })),
            )
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { rateLimit: Int }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: graph
    type: graphql
    endpoint: {}
    schema:
      kind: sdl_file
      file: ./schema.graphql
",
                server.uri()
            ),
        );
        let capability = graphql_capability();
        write_graphql_operation_document(
            &materialized_dir,
            "graph",
            "query_ratelimit.graphql",
            "query QueryRatelimit { rateLimit }",
        );
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "graph".to_string(),
                "rateLimit".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.rateLimit".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_eq!(response_value(&response), json!({ "rateLimit": 42 }));
        let envelope = response_envelope(&response);
        assert_json_pointer(&envelope, "/kind", &json!("graphql"));
        assert_json_pointer(&envelope, "/provider/http_status", &json!(200));
        assert!(!envelope.to_string().contains("graphql-session=secret"));
    }

    #[tokio::test]
    async fn graphql_singleton_not_found_returns_null_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryIssue { issue { id identifier } }",
                "variables": {},
                "operationName": "QueryIssue"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "issue": null
                },
                "errors": [{
                    "message": "Could not find issue",
                    "path": ["issue"],
                    "extensions": {
                        "code": "NOT_FOUND"
                    }
                }]
            })))
            .mount(&server)
            .await;

        let (_temp, workspace, capability_id) = graphql_singleton_issue_workspace(&server.uri());
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.issue".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(response.ok, "response error: {:?}", response.error);
        assert_eq!(response_value(&response), JsonValue::Null);
        assert_json_pointer(
            &response_envelope(&response),
            "/provider/errors/0/extensions/code",
            &json!("NOT_FOUND"),
        );
    }

    #[tokio::test]
    async fn graphql_singleton_pathless_not_found_fails_closed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryIssue { issue { id identifier } }",
                "variables": {},
                "operationName": "QueryIssue"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "issue": null
                },
                "errors": [{
                    "message": "Project not found",
                    "extensions": {
                        "code": "NOT_FOUND"
                    }
                }]
            })))
            .mount(&server)
            .await;

        let (_temp, workspace, capability_id) = graphql_singleton_issue_workspace(&server.uri());
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.issue".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(!response.ok, "pathless not-found errors must fail closed");
        let error = response.error.as_ref().expect("error");
        assert_eq!(error.kind, "provider_error");
        let details = error
            .details
            .clone()
            .map_or(JsonValue::Null, proto_json_value_to_json);
        assert_json_pointer(
            &details,
            "/provider_error/errors/0/extensions/code",
            &json!("NOT_FOUND"),
        );
        assert_json_pointer(
            &details,
            "/provider_error/partial_data/issue",
            &JsonValue::Null,
        );
        assert_json_pointer(
            &details,
            "/provider_error/media_type",
            &json!("application/json"),
        );
    }

    #[tokio::test]
    async fn graphql_singleton_mixed_errors_fail_closed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryIssue { issue { id identifier } }",
                "variables": {},
                "operationName": "QueryIssue"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "issue": null
                },
                "errors": [
                    {
                        "message": "Could not find issue",
                        "path": ["issue"],
                        "extensions": {
                            "code": "NOT_FOUND"
                        }
                    },
                    {
                        "message": "Rate limit exceeded",
                        "path": ["viewer"],
                        "extensions": {
                            "code": "RATE_LIMITED"
                        }
                    }
                ]
            })))
            .mount(&server)
            .await;

        let (_temp, workspace, capability_id) = graphql_singleton_issue_workspace(&server.uri());
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.issue".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(!response.ok, "mixed errors must fail closed");
        let error = response.error.as_ref().expect("error");
        assert_eq!(error.kind, "provider_error");
        let details = error
            .details
            .clone()
            .map_or(JsonValue::Null, proto_json_value_to_json);
        assert_json_pointer(
            &details,
            "/provider_error/errors/1/extensions/code",
            &json!("RATE_LIMITED"),
        );
    }

    #[tokio::test]
    async fn graphql_data_with_non_lookup_error_fails_with_details() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryRatelimit { rateLimit }",
                "variables": {},
                "operationName": "QueryRatelimit"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "rateLimit": null
                },
                "errors": [{
                    "message": "Rate limit exceeded",
                    "path": ["rateLimit"],
                    "extensions": {
                        "code": "RATE_LIMITED"
                    }
                }]
            })))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("schema.graphql"),
            "type Query { rateLimit: Int }",
        )
        .expect("schema");
        let materialized_dir = write_installed_manifest(
            temp.path(),
            &format!(
                r"
spec_version: 1
kind: source
name: github
interfaces:
  - id: graph
    type: graphql
    endpoint: {}
    schema:
      kind: sdl_file
      file: ./schema.graphql
",
                server.uri()
            ),
        );
        let mut capability = graphql_capability();
        capability.shape_hints = ShapeHints::singleton_at_path(vec!["rateLimit".to_string()]);
        write_graphql_operation_document(
            &materialized_dir,
            "graph",
            "query_ratelimit.graphql",
            "query QueryRatelimit { rateLimit }",
        );
        let capability_id = capability.capability_id.to_string();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "graph".to_string(),
                "rateLimit".to_string(),
            ],
            materialized_dir,
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.rateLimit".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(!response.ok);
        let error = response.error.expect("structured error");
        assert_eq!(error.kind, "provider_error");
        let details = error
            .details
            .map_or(JsonValue::Null, proto_json_value_to_json);
        assert_json_pointer(
            &details,
            "/provider_error/errors/0/extensions/code",
            &json!("RATE_LIMITED"),
        );
        assert_json_pointer(
            &details,
            "/provider_error/partial_data/rateLimit",
            &JsonValue::Null,
        );
    }

    #[tokio::test]
    async fn graphql_error_details_bound_provider_payloads() {
        let server = MockServer::start().await;
        let oversized = "x".repeat(coral_upstream::MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES + 1024);
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryRatelimit { rateLimit }",
                "variables": {},
                "operationName": "QueryRatelimit"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "rateLimit": {
                        "blob": oversized.clone()
                    }
                },
                "errors": [{
                    "message": "Rate limit exceeded",
                    "path": ["rateLimit"],
                    "extensions": {
                        "code": "RATE_LIMITED",
                        "blob": oversized
                    }
                }]
            })))
            .mount(&server)
            .await;

        let (_temp, workspace, capability_id) = graphql_rate_limit_workspace(&server.uri());
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.rateLimit".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(!response.ok);
        let error = response.error.expect("structured error");
        assert_eq!(error.kind, "provider_error");
        let details = error
            .details
            .map_or(JsonValue::Null, proto_json_value_to_json);
        assert_json_pointer(&details, "/provider_error/http_status", &json!(200));
        assert_json_pointer(&details, "/provider_error/errors/truncated", &json!(true));
        assert_json_pointer(
            &details,
            "/provider_error/partial_data/truncated",
            &json!(true),
        );
        assert_json_pointer(&details, "/provider_error/data/truncated", &json!(true));
        for pointer in [
            "/provider_error/errors/json_preview",
            "/provider_error/partial_data/json_preview",
            "/provider_error/data/json_preview",
        ] {
            let preview = details
                .pointer(pointer)
                .and_then(JsonValue::as_str)
                .expect("preview string");
            assert!(preview.len() <= coral_upstream::MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES);
        }
    }

    #[tokio::test]
    async fn graphql_error_only_upstream_detail_bounds_provider_payloads() {
        let server = MockServer::start().await;
        let oversized = "x".repeat(coral_upstream::MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES + 1024);
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_json(json!({
                "query": "query QueryRatelimit { rateLimit }",
                "variables": {},
                "operationName": "QueryRatelimit"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": [{
                    "message": "Rate limit exceeded",
                    "path": ["rateLimit"],
                    "extensions": {
                        "code": "RATE_LIMITED",
                        "blob": oversized
                    }
                }]
            })))
            .mount(&server)
            .await;

        let (_temp, workspace, capability_id) = graphql_rate_limit_workspace(&server.uri());
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id,
            binding_ref: "typescript:github.graph.rateLimit".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let resolved = resolve_invocation(&workspace, &request).expect("resolve");
        let response = invoke_resolved(resolved, JsonMap::new(), None).await;

        assert!(!response.ok);
        let error = response.error.expect("structured error");
        assert_eq!(error.kind, "provider_error");
        let details = error
            .details
            .map_or(JsonValue::Null, proto_json_value_to_json);
        assert_json_pointer(
            &details,
            "/provider_error/detail/errors/truncated",
            &json!(true),
        );
        assert_json_pointer(
            &details,
            "/provider_error/detail/partial_data",
            &JsonValue::Null,
        );
        let preview = details
            .pointer("/provider_error/detail/errors/json_preview")
            .and_then(JsonValue::as_str)
            .expect("preview string");
        assert!(preview.len() <= coral_upstream::MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES);
    }

    #[test]
    fn returns_stale_binding_when_ref_points_at_different_capability() {
        let capability = file_capability();
        let workspace = workspace_exports(
            capability,
            vec![
                "github".to_string(),
                "files".to_string(),
                "readFiles".to_string(),
            ],
            tempfile::tempdir().expect("tempdir").path().to_path_buf(),
        );
        let request = InvokeCapabilityRequest {
            workspace: None,
            capability_id: "source/src_other/interface/files/operation/read_files".to_string(),
            binding_ref: "typescript:github.files.readFiles".to_string(),
            binding_path: Vec::new(),
            args_json: "{}".to_string(),
        };
        let response = resolve_invocation(&workspace, &request).expect_err("stale binding");

        assert_eq!(
            response.error.expect("structured error").kind,
            "stale_binding"
        );
    }

    #[test]
    fn rejects_non_object_args_json() {
        let response = parse_args_json("[]").expect_err("invalid args");

        assert_eq!(
            response.error.expect("structured error").kind,
            "invalid_args"
        );
    }
}
